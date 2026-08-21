//! The feature that consumes the other two, and never names either of them.
//!
//! `orders-bundle` depends on `ledger-contracts` and `audit-contracts`. It does
//! not depend on `ledger-bundle` or `audit-bundle`, and it could not: a
//! `*-bundle` crate is never in another `*-bundle` crate's dependency list, and
//! `ci/check-bundle-graph.sh` fails the build if one appears. Everything this
//! feature needs from the other two arrives as [`Ledger`] and [`Sink`] —
//! traits — resolved from the container after phase three has proved somebody
//! provides them.
//!
//! Read that as a constraint on *this* file: there is no concrete type from
//! either other feature anywhere below. Not a struct, not an enum, not a
//! constructor. If one were wanted, the rule would be working.
//!
//! # What it carries
//!
//! * `Book` — an implementation of [`OrderBook`], bound as a contract so that
//!   anything holding `Arc<dyn OrderBook>` can place an order without knowing
//!   this crate exists.
//! * `Desk` — a runnable that works in batches and returns when the shutdown
//!   token fires, which is the one test every runnable owes.
//! * `Slip` — a [`Lifetime::Scoped`] binding. Each batch opens a [`Scope`],
//!   and everything inside that batch reaches the same slip by resolving it,
//!   with nothing threaded through the calls. That is what a scope is *for*.
//! * Three listeners on [`OrderPlaced`] at three priorities, dispatched
//!   sequentially, where the highest can veto.
//! * One event of this feature's own, `BatchClosed`, emitted rather than
//!   dispatched — and the reason that is the right choice for it.
//! * `DeskProbe`, a health probe contributed to the point the kernel
//!   declares.
//! * A real reason to stop the process, asked for through [`KernelHandle`].
//!
//! [`KernelHandle`]: kernel::KernelHandle

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::sync::{Arc, Mutex, PoisonError};

use audit_contracts::{ARCHIVE, Record, Sink};
use kernel::core::telemetry::{Level, Record as Diagnostic};
use kernel::core::{
    BuildError, ConfigError, Extension, Health, HealthProbe, ListenerError, RegisterError, RunError,
};
use kernel::{
    BoxFuture, Bundle, BundleManifest, ContractRef, Criticality, Event, Flow, Lifetime, Listener,
    ListenerContext, Priority, Probe, Provider, Registry, RunContext, Runnable, RunnableDescriptor,
    Scope, Stage,
};
use ledger_contracts::{Entry, Ledger};
use orders_contracts::{Order, OrderBook, OrderError, OrderPlaced};

/// The name this bundle publishes, and the source every record it writes is
/// attributed to.
const NAME: &str = "orders";

/// What this bundle needs someone else to provide.
///
/// Two entries, not three. The third — the *unnamed* [`Sink`] the audit trail
/// writes to — cannot be claimed here: the only place this feature resolves it
/// is a listener, a listener declares no requirements, and a manifest entry
/// that none of the bundle's own providers declares is a `ManifestMismatch` in
/// phase three. So the honest manifest is the short one, and the trail's
/// dependency is checked only when it runs.
static REQUIRES: [ContractRef; 2] = [
    ContractRef::of::<dyn Ledger>(),
    ContractRef::named::<dyn Sink>(ARCHIVE),
];

/// What one order is worth, multiplied by its position in the batch.
///
/// Fixed rather than configurable: it exists so that some orders in every
/// batch clear the screening cap and some do not.
const AMOUNT: i64 = 100;

/// How many refusals in a row mean the book cannot do its job at all.
const GIVE_UP: u64 = 3;

/// How long between two batches when nothing says otherwise.
const EVERY: Duration = Duration::from_millis(500);

/// How many orders one batch places when nothing says otherwise.
const BATCH: u32 = 3;

/// The largest order the screening listener lets through when nothing says
/// otherwise.
const CAP: i64 = 250;

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// What this feature reads out of the configuration tree.
///
/// Every field has a default, so the bundle registers in an application that
/// configured nothing. A source listed by the application overrides them leaf
/// by leaf, and the defaults are written once, above.
#[derive(Clone, Copy, Debug)]
struct Settings {
    /// How long between two batches.
    every: Duration,
    /// How many orders one batch places.
    batch: u32,
    /// The largest order the screen lets through.
    cap: i64,
}

impl Settings {
    /// Reads the three keys, each optional.
    ///
    /// `Option<T>` is what makes a key optional: an absent path reads as
    /// `None` instead of refusing the registration, and a *present* path that
    /// holds the wrong kind of value still fails, with the path in the message.
    fn read(registry: &Registry) -> Result<Self, ConfigError> {
        Ok(Self {
            every: registry
                .config::<Option<Duration>>("orders.every")?
                .unwrap_or(EVERY),
            batch: registry
                .config::<Option<u32>>("orders.batch")?
                .unwrap_or(BATCH),
            cap: registry.config::<Option<i64>>("orders.cap")?.unwrap_or(CAP),
        })
    }
}

// ---------------------------------------------------------------------------
// The contract this feature provides
// ---------------------------------------------------------------------------

/// An order book backed by a ledger.
///
/// It holds `Arc<dyn Ledger>` and nothing else about the ledger feature. Which
/// ledger it got, what that ledger writes to, and which bundle registered it
/// are all unknown here and stay unknown.
struct Book {
    /// Where a placed order is written down.
    ledger: Arc<dyn Ledger>,
    /// How many orders this book has placed.
    count: AtomicU64,
}

impl OrderBook for Book {
    fn place(&self, order: Order) -> BoxFuture<'_, Result<u64, OrderError>> {
        Box::pin(async move {
            if order.amount <= 0 {
                return Err(OrderError::Rejected(format!(
                    "{} is worth nothing",
                    order.reference
                )));
            }

            // `LedgerError` is another feature's error type. It is boxed into
            // `OrderError::Downstream` rather than named in this crate's own
            // error, which is why `orders-contracts` never depends on
            // `ledger-contracts`.
            self.ledger
                .append(Entry::new(order.reference, order.amount))
                .await
                .map_err(OrderError::downstream)?;

            Ok(self.count.fetch_add(1, Ordering::Relaxed) + 1)
        })
    }

    fn placed(&self) -> BoxFuture<'_, Result<u64, OrderError>> {
        Box::pin(async move { Ok(self.count.load(Ordering::Relaxed)) })
    }
}

// ---------------------------------------------------------------------------
// The unit of work
// ---------------------------------------------------------------------------

/// The working note of one batch.
///
/// Bound [`Lifetime::Scoped`], so it is built once per [`Scope`] and shared by
/// everything that resolves it inside that scope. Resolved outside a scope it
/// fails: there is no unit of work to attach it to.
struct Slip {
    /// Which unit of work this is, counting from one.
    id: u64,
    /// What happened in it, in order.
    lines: Mutex<Vec<String>>,
}

impl Slip {
    /// Writes one line.
    fn note(&self, line: String) {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }

    /// Everything written so far.
    fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The slip of the unit of work `scope` stands for.
///
/// It *resolves* the slip rather than receiving it, and that is the point: two
/// callers in one batch reach the same object with nothing passed between them.
/// The scope is the thread the slip travels on.
///
/// Phase three validated the binding, so a kernel that started cannot fail
/// here; the failure is recorded rather than panicked on because a working note
/// is not worth taking a process down for.
async fn slip_of(scope: &Scope, cx: &RunContext) -> Option<Arc<Slip>> {
    match scope.get::<Slip>().await {
        Ok(slip) => Some(slip),
        Err(error) => {
            cx.telemetry().record(
                Diagnostic::new(Level::Error, "orders.slip_unreachable")
                    .with("error", error.to_string()),
            );
            None
        }
    }
}

// ---------------------------------------------------------------------------
// The feature's own event
// ---------------------------------------------------------------------------

/// One batch finished.
///
/// Declared here, in the bundle, and not in `orders-contracts` — which is
/// correct for exactly as long as it stays true that no other feature listens
/// for it. A contracts crate holds what appears in two features' code; this
/// appears in one. The day another feature wants it, the type moves and the
/// emitter does not change.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BatchClosed {
    /// Which batch, counting from one.
    number: u64,
    /// What the batch's slip ended up holding.
    notes: Vec<String>,
}

impl Event for BatchClosed {
    /// Diagnostics only: dispatch routes on the type.
    const NAME: &'static str = "orders.batch_closed";
}

// ---------------------------------------------------------------------------
// Listeners
// ---------------------------------------------------------------------------

/// Refuses to let an order over the cap be announced.
///
/// Registered at [`Priority::HIGH`], so it runs first and its veto is what the
/// two listeners below never see. Returning [`Flow::Stop`] is not an error: the
/// emitter gets a successful [`Dispatched`](kernel::Dispatched) whose `stopped`
/// flag says a decision was taken.
///
/// The note it leaves on the event travels back to the emitter, because
/// sequential dispatch hands the listener `&mut` and the emitter still owns the
/// event afterwards. That is the half of `dispatch` an `emit` cannot do.
struct Screen {
    /// The largest order that may be announced.
    cap: i64,
}

impl Listener<OrderPlaced> for Screen {
    fn on_event<'a>(
        &'a self,
        event: &'a mut OrderPlaced,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            if event.order.amount > self.cap {
                event.notes.push(format!(
                    "held: {} over cap {}",
                    event.order.amount, self.cap
                ));
                return Ok(Flow::Stop);
            }
            Ok(Flow::Continue)
        })
    }
}

/// Adds the sequence number to the event, for whoever reads it next.
///
/// [`Priority::NORMAL`]: after the screen, before the trail. It exists to show
/// that a veto stops *everything* below it, not merely the last listener.
struct Stamp;

impl Listener<OrderPlaced> for Stamp {
    fn on_event<'a>(
        &'a self,
        event: &'a mut OrderPlaced,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            event.notes.push(format!("stamped #{}", event.sequence));
            Ok(Flow::Continue)
        })
    }
}

/// Writes every announced order to the audit trail.
///
/// [`Priority::LOW`], so it sees the notes both listeners above it left. It
/// resolves `Arc<dyn Sink>` from the container it is handed rather than holding
/// one, because a listener is registered in phase two, when nothing is built
/// yet.
///
/// It asks for the *default* binding: this caller wants a sink and does not
/// care which one the audit feature made default.
struct Trail;

impl Listener<OrderPlaced> for Trail {
    fn on_event<'a>(
        &'a self,
        event: &'a mut OrderPlaced,
        cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            let sink = cx
                .container()
                .get::<dyn Sink>()
                .await
                .map_err(|error| ListenerError::new(OrderPlaced::NAME, Box::new(error)))?;

            sink.write(Record::new(
                NAME,
                format!(
                    "placed {} for {} [{}]",
                    event.order.reference,
                    event.order.amount,
                    event.notes.join(", ")
                ),
            ))
            .await
            .map_err(|error| ListenerError::new(OrderPlaced::NAME, Box::new(error)))?;

            Ok(Flow::Continue)
        })
    }
}

/// Writes a line per closed batch.
///
/// Its event arrives through [`EventDispatcher::emit`](kernel::EventDispatcher::emit),
/// so a failure here reaches telemetry and nobody else. That is the trade the
/// emitter made deliberately; see [`Desk::batch`].
struct Summary;

impl Listener<BatchClosed> for Summary {
    fn on_event<'a>(
        &'a self,
        event: &'a mut BatchClosed,
        cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            let sink = cx
                .container()
                .get::<dyn Sink>()
                .await
                .map_err(|error| ListenerError::new(BatchClosed::NAME, Box::new(error)))?;

            sink.write(Record::new(
                NAME,
                format!("batch {} closed: {}", event.number, event.notes.join(", ")),
            ))
            .await
            .map_err(|error| ListenerError::new(BatchClosed::NAME, Box::new(error)))?;

            Ok(Flow::Continue)
        })
    }
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

/// What the desk has done, shared with the probe that reports on it.
///
/// The probe is contributed in phase two, before anything is built, and a
/// [`HealthProbe`] is handed no container — so a probe over a runnable's state
/// has to share an object that already exists at registration time. This is it.
#[derive(Debug, Default)]
struct Tally {
    /// Batches started.
    batches: AtomicU64,
    /// Orders announced with nobody objecting.
    placed: AtomicU64,
    /// Orders the screen vetoed.
    held: AtomicU64,
    /// Placements the book refused.
    refused: AtomicU64,
}

/// Reports on the desk without being the desk.
struct DeskProbe {
    /// The counters the desk writes.
    tally: Arc<Tally>,
    /// How many refusals mean the book is unusable.
    give_up: u64,
}

impl Extension for DeskProbe {}

impl HealthProbe for DeskProbe {
    fn name(&self) -> &'static str {
        "orders.desk"
    }

    fn check<'a>(&'a self) -> BoxFuture<'a, Health> {
        Box::pin(async move {
            let refused = self.tally.refused.load(Ordering::Relaxed);
            if refused >= self.give_up {
                return Health::down(format!("the book refused {refused} placements"));
            }

            let held = self.tally.held.load(Ordering::Relaxed);
            if held > 0 {
                return Health::degraded(format!("{held} order(s) held by screening"));
            }

            Health::Up
        })
    }
}

// ---------------------------------------------------------------------------
// The runnable
// ---------------------------------------------------------------------------

/// Places a batch of orders every `every`, until it is asked to stop or until
/// the book turns out to be unusable.
struct Desk {
    /// Where orders go. The contract, resolved — not [`Book`], which this
    /// runnable has no reason to name even though the same crate defines it.
    book: Arc<dyn OrderBook>,
    /// Where the closing record goes: the archive binding, by name, because
    /// this caller means the archive and not whichever sink is default.
    archive: Arc<dyn Sink>,
    /// The counters the health probe reads.
    tally: Arc<Tally>,
    /// How long between two batches.
    every: Duration,
    /// How many orders one batch places.
    batch: u32,
}

impl Runnable for Desk {
    fn name() -> &'static str {
        "desk"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        // Ancillary on purpose. An `Essential` runnable stops the process by
        // returning, which would make the shutdown request below decorative —
        // and that request is the thing worth showing.
        RunnableDescriptor::new().criticality(Criticality::Ancillary)
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            loop {
                // The token wins the race. A loop that only slept would be
                // abandoned at its deadline instead of returning, and the run
                // would be counted a dirty stop.
                //
                // `draining` is the stage this runnable cares about: it means
                // stop taking new work, and a batch is new work. What it holds
                // when the stage moves is at most one batch, and `batch`
                // abandons that one on `stopping`.
                tokio::select! {
                    () = cx.shutdown().draining() => break,
                    () = tokio::time::sleep(self.every) => {}
                }

                if !self.batch(&cx).await {
                    // A real condition, not a timer: the book cannot place
                    // anything at all, so this process would only be pretending
                    // to work. Asking through the handle is how any unit
                    // requests a stop — it does not drive one.
                    cx.handle().shutdown();
                    break;
                }
            }

            self.close(&cx).await;
            Ok(())
        })
    }
}

impl Desk {
    /// One unit of work: one scope, one slip, `batch` orders.
    ///
    /// Answers `false` when the book has refused so often that there is nothing
    /// left to stay up for.
    async fn batch(&self, cx: &RunContext) -> bool {
        // The unit of work. Everything `Scoped` resolved inside it is built
        // once and shared until this function returns; the next batch opens a
        // scope of its own and gets its own.
        let scope = cx.container().scope();
        self.tally.batches.fetch_add(1, Ordering::Relaxed);

        let Some(slip) = slip_of(&scope, cx).await else {
            return true;
        };
        let number = slip.id;

        for index in 0..self.batch {
            // `stopping` means the clock is running: whatever is left of this
            // batch is dropped rather than finished.
            if cx.shutdown().stage() >= Stage::Stopping {
                break;
            }

            let order = Order::new(
                format!("order-{number}-{index}"),
                AMOUNT * i64::from(index + 1),
            );

            match self.book.place(order.clone()).await {
                Ok(sequence) => self.announce(&scope, cx, order, sequence).await,
                Err(error) => {
                    let refused = self.tally.refused.fetch_add(1, Ordering::Relaxed) + 1;
                    slip.note(format!("refused: {error}"));
                    if refused >= GIVE_UP {
                        return false;
                    }
                    // One refusal is not a verdict. Give the batch up, keep the
                    // desk.
                    break;
                }
            }
        }

        // `announce` was handed the scope and never the slip, yet its lines
        // are on this one. Same unit of work, same object — that is the whole
        // of what a scoped lifetime buys.
        let notes = slip.lines();

        // `emit`, not `dispatch`, and the difference is the whole reason there
        // are two methods. Nothing here depends on what a listener decides: the
        // batch is over, no listener can change that, and a slow subscriber
        // must not delay the next batch. What is given up in exchange is
        // ordering and the failure — a listener that fails is recorded to
        // telemetry and this caller never hears of it.
        cx.dispatcher().emit(BatchClosed { number, notes });

        true
    }

    /// Announces one placed order, sequentially, and acts on the answer.
    ///
    /// This is `dispatch` because the emitter's control flow depends on the
    /// walk: a veto by the screen is what makes the order *held* rather than
    /// placed, and the notes the listeners leave come back on the event.
    async fn announce(&self, scope: &Scope, cx: &RunContext, order: Order, sequence: u64) {
        let mut event = OrderPlaced {
            order,
            sequence,
            notes: Vec::new(),
        };

        let line = match cx.dispatcher().dispatch(&mut event).await {
            Ok(walk) if walk.stopped => {
                self.tally.held.fetch_add(1, Ordering::Relaxed);
                format!("held {}: {}", event.order.reference, event.notes.join(", "))
            }
            Ok(_) => {
                self.tally.placed.fetch_add(1, Ordering::Relaxed);
                format!(
                    "placed {}: {}",
                    event.order.reference,
                    event.notes.join(", ")
                )
            }
            // A listener failed. The order is already in the book and a broken
            // subscriber does not un-place it, so this is written down and the
            // batch continues.
            Err(error) => format!("announcement failed for {}: {error}", event.order.reference),
        };

        // The slip is reached through the scope, not through an argument. That
        // is what a scope is for.
        if let Some(slip) = slip_of(scope, cx).await {
            slip.note(line);
        }
    }

    /// Writes one closing record to the archive.
    ///
    /// Named binding, resolved once when the desk was built: this record is
    /// meant for the archive specifically, so asking for the default sink would
    /// be asking for the wrong thing.
    async fn close(&self, cx: &RunContext) {
        let record = Record::new(
            NAME,
            format!(
                "desk closing after {} batch(es): {} placed, {} held, {} refused",
                self.tally.batches.load(Ordering::Relaxed),
                self.tally.placed.load(Ordering::Relaxed),
                self.tally.held.load(Ordering::Relaxed),
                self.tally.refused.load(Ordering::Relaxed),
            ),
        );

        if let Err(error) = self.archive.write(record).await {
            cx.telemetry().record(
                Diagnostic::new(Level::Error, "orders.archive_refused")
                    .with("error", error.to_string()),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The bundle
// ---------------------------------------------------------------------------

/// Registers the book, the desk, the listeners and the probe.
///
/// Registration is deaf: it gets no container, sees no other bundle, and builds
/// nothing. Everything below is a declaration that phase three checks and phase
/// four acts on.
#[derive(Debug, Default)]
pub struct Bundled;

impl Bundle for Bundled {
    fn manifest(&self) -> BundleManifest {
        // No `after`. Nothing here depends on the order the application listed
        // its bundles in — the contracts are what order the boot, and saying
        // otherwise would be a claim this feature cannot back up.
        BundleManifest::new(NAME, "0.1.0").requires(&REQUIRES)
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        let settings =
            Settings::read(registry).map_err(|error| RegisterError::new(NAME, Box::new(error)))?;

        // The contract this feature publishes. Anything may resolve it; nothing
        // may name `Book`.
        registry.provide(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    let ledger = container
                        .get::<dyn Ledger>()
                        .await
                        .map_err(|error| BuildError::new("Book", Box::new(error)))?;
                    Ok(Arc::new(Book {
                        ledger,
                        count: AtomicU64::new(0),
                    }) as Arc<dyn OrderBook>)
                })
            })
            // Declared, and checked: a debug build panics if this provider
            // resolves anything else.
            .requires([ContractRef::of::<dyn Ledger>()]),
        );

        // One slip per unit of work. The counter lives in the closure, so it
        // survives every build and numbers them in order.
        let slips = Arc::new(AtomicU64::new(0));
        registry.provide(
            Provider::from_fn(move |_container| {
                let slips = Arc::clone(&slips);
                Box::pin(async move {
                    Ok(Arc::new(Slip {
                        id: slips.fetch_add(1, Ordering::Relaxed) + 1,
                        lines: Mutex::new(Vec::new()),
                    }))
                })
            })
            .lifetime(Lifetime::Scoped),
        );

        // Three listeners on one event, at three priorities. The table is built
        // once in phase three and never changes; the order below is the order
        // they will run in, on every run of this program.
        registry.listen(Screen { cap: settings.cap }, Priority::HIGH);
        registry.listen(Stamp, Priority::NORMAL);
        registry.listen(Trail, Priority::LOW);

        // The feature's own event, and the only listener for it.
        registry.listen(Summary, Priority::NORMAL);

        let tally = Arc::new(Tally::default());

        // A point the kernel declared. This bundle names no probe type but its
        // own, and no aggregator.
        registry.contribute(Probe::new(DeskProbe {
            tally: Arc::clone(&tally),
            give_up: GIVE_UP,
        }));

        registry.runnable(
            Provider::from_fn(move |container| {
                let tally = Arc::clone(&tally);
                Box::pin(async move {
                    let book = container
                        .get::<dyn OrderBook>()
                        .await
                        .map_err(|error| BuildError::new("Desk", Box::new(error)))?;
                    let archive = container
                        .get_named::<dyn Sink>(ARCHIVE)
                        .await
                        .map_err(|error| BuildError::new("Desk", Box::new(error)))?;
                    Ok(Arc::new(Desk {
                        book,
                        archive,
                        tally,
                        every: settings.every,
                        batch: settings.batch,
                    }))
                })
            })
            .requires([
                ContractRef::of::<dyn OrderBook>(),
                ContractRef::named::<dyn Sink>(ARCHIVE),
            ]),
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use kernel::core::{ConfigNode, ConfigTree, Outcome, ShutdownPolicy};
    use kernel::{Kernel, KernelHandle, MemorySource, ShutdownController};
    use ledger_contracts::LedgerError;

    use super::*;

    /// A ledger that accepts `open_for` entries and refuses everything after.
    ///
    /// It stands in for the whole ledger feature. Writing it is what the
    /// missing-contract list from phase three asks for, and it names no type of
    /// `ledger-bundle` — there is none to name.
    struct Notebook {
        /// What has been appended.
        entries: Mutex<Vec<Entry>>,
        /// How many entries this ledger accepts before closing.
        open_for: u64,
    }

    impl Notebook {
        /// A ledger that closes after `open_for` entries.
        fn new(open_for: u64) -> Arc<Self> {
            Arc::new(Self {
                entries: Mutex::new(Vec::new()),
                open_for,
            })
        }

        /// How many entries it holds.
        fn len(&self) -> u64 {
            let held = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
            u64::try_from(held.len()).unwrap_or(u64::MAX)
        }
    }

    impl Ledger for Notebook {
        fn append(&self, entry: Entry) -> BoxFuture<'_, Result<u64, LedgerError>> {
            Box::pin(async move {
                if self.len() >= self.open_for {
                    return Err(LedgerError::Closed);
                }
                self.entries
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(entry);
                Ok(self.len())
            })
        }

        fn count(&self) -> BoxFuture<'_, Result<u64, LedgerError>> {
            Box::pin(async move { Ok(self.len()) })
        }
    }

    /// A sink that keeps what it is given.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<Record>>);

    impl Recorder {
        /// Every message written so far, in order.
        fn messages(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .map(|record| record.message.clone())
                .collect()
        }

        /// The messages the audit trail wrote, which are the only ones this
        /// feature emits in a defined order.
        fn placements(&self) -> Vec<String> {
            self.messages()
                .into_iter()
                .filter(|message| message.starts_with("placed "))
                .collect()
        }
    }

    impl Sink for Recorder {
        fn write(&self, record: Record) -> BoxFuture<'_, Result<(), audit_contracts::SinkError>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(record);
                Ok(())
            })
        }
    }

    /// Stands in for the two features this one consumes.
    ///
    /// One bundle providing three bindings, none of which this crate could
    /// otherwise reach. A bundle can be booted alone, and the contracts phase
    /// three reports missing are exactly the doubles to write.
    struct Doubles {
        /// The ledger the book writes through.
        ledger: Arc<dyn Ledger>,
        /// The default sink, where the trail writes.
        sink: Arc<dyn Sink>,
        /// The named sink, where the desk writes its closing record.
        archive: Arc<dyn Sink>,
    }

    impl Bundle for Doubles {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("doubles", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            registry.provide(Provider::from_value(Arc::clone(&self.ledger)));
            registry.provide(Provider::from_value(Arc::clone(&self.sink)));
            registry.provide_named(ARCHIVE, Provider::from_value(Arc::clone(&self.archive)));
            Ok(())
        }
    }

    /// What one run of the feature left behind.
    struct Run {
        /// How the kernel ended.
        outcome: Outcome,
        /// The handle, to see whether the desk asked for the stop.
        handle: KernelHandle,
        /// The default sink.
        sink: Arc<Recorder>,
        /// The archive.
        archive: Arc<Recorder>,
    }

    /// Three keys, so the configuration path is exercised rather than assumed.
    fn settings() -> MemorySource {
        let mut tree = ConfigTree::empty();
        for (path, node) in [
            ("orders.every", ConfigNode::from("10ms")),
            ("orders.batch", ConfigNode::from(3_i64)),
            ("orders.cap", ConfigNode::from(250_i64)),
        ] {
            tree.insert(path, node)
                .expect("literal paths cannot collide");
        }
        MemorySource::named("test", tree)
    }

    /// Builds the kernel this feature plus its doubles make up.
    async fn build(open_for: u64) -> (Kernel, Arc<Recorder>, Arc<Recorder>) {
        let sink = Arc::new(Recorder::default());
        let archive = Arc::new(Recorder::default());

        let kernel = Kernel::builder()
            .capture_signals(false)
            .shutdown_policy(ShutdownPolicy::new(
                Duration::from_millis(50),
                Duration::from_millis(50),
            ))
            .config_source(settings())
            .bundle(Bundled)
            .bundle(Doubles {
                ledger: Notebook::new(open_for),
                sink: Arc::clone(&sink) as Arc<dyn Sink>,
                archive: Arc::clone(&archive) as Arc<dyn Sink>,
            })
            .build()
            .await
            .expect("the graph closes");

        (kernel, sink, archive)
    }

    /// Runs the whole feature until the desk decides to stop.
    async fn run(open_for: u64) -> Run {
        let (kernel, sink, archive) = build(open_for).await;
        let handle = kernel.handle();
        let outcome = kernel.run().await;

        Run {
            outcome,
            handle,
            sink,
            archive,
        }
    }

    /// The test section 18 makes mandatory for every runnable: the token has
    /// fired, so `run` returns instead of waiting for its timer.
    ///
    /// Nothing waits here — the ladder moves before the runnable is entered —
    /// so paused time would change nothing.
    #[tokio::test]
    async fn yields_on_shutdown() {
        let (cx, controller): (RunContext, ShutdownController) = RunContext::detached();
        controller.begin_draining();

        let archive = Arc::new(Recorder::default());
        let desk = Arc::new(Desk {
            book: Arc::new(Book {
                ledger: Notebook::new(u64::MAX),
                count: AtomicU64::new(0),
            }),
            archive: Arc::clone(&archive) as Arc<dyn Sink>,
            tally: Arc::new(Tally::default()),
            // Long enough that a runnable which slept first would hang the test
            // rather than pass it slowly.
            every: Duration::from_secs(3600),
            batch: 3,
        });

        assert!(desk.run(cx).await.is_ok());
        assert_eq!(archive.messages().len(), 1);
    }

    /// A scope is one unit of work: the same slip throughout, a different one
    /// next time, and none at all outside.
    #[tokio::test]
    async fn scope_holds_one_slip() {
        let (kernel, _sink, _archive) = build(u64::MAX).await;

        let batch = kernel.container().scope();
        let first = batch.get::<Slip>().await.expect("a slip");
        let again = batch.get::<Slip>().await.expect("the same slip");
        assert!(Arc::ptr_eq(&first, &again));

        let next = kernel.container().scope();
        let other = next.get::<Slip>().await.expect("another slip");
        assert!(!Arc::ptr_eq(&first, &other));
        assert_ne!(first.id, other.id);

        // No unit of work, nothing to attach the value to.
        assert!(kernel.container().get::<Slip>().await.is_err());
    }

    /// The screen runs first and stops the two listeners below it, so an order
    /// over the cap never reaches the audit trail.
    #[tokio::test(start_paused = true)]
    async fn veto_stops_the_trail() {
        // Six entries: two full batches of three, then the ledger closes and
        // the desk gives up.
        let run = run(6).await;

        // Three orders per batch at 100, 200 and 300; the third clears the cap
        // of 250 and is held.
        let placements = run.sink.placements();
        assert_eq!(placements.len(), 4);
        assert!(placements.iter().all(|line| line.contains("stamped")));
        assert!(!placements.iter().any(|line| line.contains("order-1-2")));
        assert!(placements.iter().any(|line| line.contains("order-1-0")));

        // The batch summaries reach the same sink through `emit`, which is
        // detached: whether they arrived before the run ended is not something
        // this feature promises, so nothing here asserts on them.
    }

    /// A book that cannot place anything asks the kernel to stop.
    #[tokio::test(start_paused = true)]
    async fn stops_when_the_book_fails() {
        let run = run(0).await;

        assert!(run.handle.is_shutting_down());
        assert!(run.outcome.is_success());
        assert!(run.sink.placements().is_empty());

        let closing = run.archive.messages();
        assert_eq!(closing.len(), 1);
        assert!(closing[0].contains("3 refused"));
        assert!(closing[0].contains("0 placed"));
    }

    /// The probe reports on what the desk did, and nothing else.
    #[tokio::test]
    async fn probe_grades_the_desk() {
        let tally = Arc::new(Tally::default());
        let probe = DeskProbe {
            tally: Arc::clone(&tally),
            give_up: GIVE_UP,
        };

        assert_eq!(probe.name(), "orders.desk");
        assert!(probe.check().await.is_up());

        tally.held.fetch_add(1, Ordering::Relaxed);
        let degraded = probe.check().await;
        assert!(!degraded.is_up());
        assert!(degraded.detail().unwrap_or_default().contains("held"));

        tally.refused.store(GIVE_UP, Ordering::Relaxed);
        assert_eq!(
            probe.check().await,
            Health::down("the book refused 3 placements")
        );
    }

    /// The book turns a ledger failure into a downstream order failure without
    /// naming the ledger's error in its own type.
    #[tokio::test]
    async fn book_boxes_the_cause() {
        let book = Book {
            ledger: Notebook::new(0),
            count: AtomicU64::new(0),
        };

        let refused = book
            .place(Order::new("order-1", 250))
            .await
            .expect_err("the ledger is closed");
        assert!(std::error::Error::source(&refused).is_some());

        let empty = book
            .place(Order::new("order-2", 0))
            .await
            .expect_err("worth nothing");
        assert!(matches!(empty, OrderError::Rejected(_)));
    }
}
