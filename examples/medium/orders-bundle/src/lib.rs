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
//!   token fires, which is the one test every runnable owes. Its periodic wait
//!   is [`Shutdown::sleep_until_draining`](kernel::Shutdown::sleep_until_draining),
//!   which is why this crate names no runtime and no timer of its own.
//! * `Slip` — a [`Lifetime::Scoped`] binding. Each batch opens a [`Scope`],
//!   and everything inside that batch reaches the same slip by resolving it,
//!   with nothing threaded through the calls. That is what a scope is *for*.
//! * Three listeners on [`OrderProposed`] at three priorities, dispatched
//!   sequentially, where the highest can veto — and the veto runs *before* the
//!   order reaches the book, which is the only order in which a veto is one.
//! * One event of this feature's own, `BatchClosed`, emitted rather than
//!   dispatched — and the reason that is the right choice for it.
//! * `DeskProbe`, a health probe contributed to the point the kernel
//!   declares.
//! * A real reason to stop the process, asked for through [`KernelHandle`].
//!
//! [`KernelHandle`]: kernel::KernelHandle

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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
use orders_contracts::{Order, OrderBook, OrderError, OrderProposed};

/// The name this bundle publishes, and the source every record it writes is
/// attributed to.
const NAME: &str = "orders";

/// What this bundle needs someone else to provide.
///
/// Three entries, and the third is the one worth explaining. The *unnamed*
/// [`Sink`] the audit trail writes to is resolved by a listener rather than by
/// a provider, and it is declared a second time where it is resolved: on the
/// handle [`Registry::listen`] hands back. Phase three checks that declaration
/// exactly as it checks a provider's, and it reads both when it checks this
/// manifest — so a contract only a listener consumes belongs here like any
/// other.
///
/// The two places state two facts about the same graph: this one is what the
/// bundle needs, the declaration on the listener is where the resolution
/// happens. Both are checked before anything boots.
static REQUIRES: [ContractRef; 3] = [
    ContractRef::of::<dyn Ledger>(),
    ContractRef::named::<dyn Sink>(ARCHIVE),
    ContractRef::of::<dyn Sink>(),
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
        self.held().push(line);
    }

    /// Everything written so far.
    fn lines(&self) -> Vec<String> {
        self.held().clone()
    }

    /// The lines, whatever a previous panic did to the lock.
    ///
    /// The recovery is written once, here, because the argument for it is one
    /// argument. One field sits under this lock and every mutation of it is a
    /// single `push`, so a caller that unwound while holding the guard left a
    /// complete list of complete lines: there is no half-written value for the
    /// next caller to read. A working note is also not worth refusing every
    /// later batch over.
    ///
    /// A lock whose state *can* be half written earns no such permission, and
    /// the honest call there is `.expect()` — a stopped process beats a caller
    /// acting on half a value.
    fn held(&self) -> MutexGuard<'_, Vec<String>> {
        self.lines.lock().unwrap_or_else(PoisonError::into_inner)
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
///
/// `None` is a *finding*, not a shrug. Every caller acts on it: a batch that
/// cannot reach its slip produces nothing and writes nothing down, so it ends
/// the run rather than answering that it went fine. Recording and then
/// reporting success would leave a desk counting empty batches and a health
/// probe grading the silence [`Health::Up`].
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

/// Refuses to let an order over the cap be placed.
///
/// Registered at [`Priority::HIGH`], so it runs first and its veto is what the
/// two listeners below never see. Returning [`Flow::Stop`] is not an error: the
/// emitter gets a successful [`Dispatched`](kernel::Dispatched) whose `stopped`
/// flag says a decision was taken.
///
/// The veto works because [`Desk::offer`] dispatches *before* it calls the
/// book. Were the order committed first, this listener would still return
/// `Flow::Stop` and the emitter would still read `stopped` — and the order
/// would be in the ledger regardless, described as held. The order of the two
/// calls is the whole of what makes this a veto.
///
/// The note it leaves on the event travels back to the emitter, because
/// sequential dispatch hands the listener `&mut` and the emitter still owns the
/// event afterwards. That is the half of `dispatch` an `emit` cannot do.
struct Screen {
    /// The largest order that may be placed.
    cap: i64,
}

impl Listener<OrderProposed> for Screen {
    fn on_event<'a>(
        &'a self,
        event: &'a mut OrderProposed,
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

/// Numbers the proposals this feature has screened, and says so on the event.
///
/// [`Priority::NORMAL`]: after the screen, before the trail. It exists to show
/// that a veto stops *everything* below it, not merely the last listener — an
/// order the screen held carries no stamp, which is checked.
///
/// The number is the listener's own count of what it has seen, not the book's
/// sequence number: nothing has been committed yet at this point in the walk,
/// so there is no sequence number to read.
#[derive(Debug, Default)]
struct Stamp {
    /// How many proposals have reached this listener.
    seen: AtomicU64,
}

impl Listener<OrderProposed> for Stamp {
    fn on_event<'a>(
        &'a self,
        event: &'a mut OrderProposed,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            let seen = self.seen.fetch_add(1, Ordering::Relaxed) + 1;
            event.notes.push(format!("stamped #{seen}"));
            Ok(Flow::Continue)
        })
    }
}

/// Writes every order that cleared the screen to the audit trail.
///
/// [`Priority::LOW`], so it sees the notes both listeners above it left. It
/// resolves `Arc<dyn Sink>` from the container it is handed rather than holding
/// one, because a listener is registered in phase two, when nothing is built
/// yet — and that resolution is declared on the handle `Registry::listen`
/// returns, which is what puts it in front of phase three.
///
/// It asks for the *default* binding: this caller wants a sink and does not
/// care which one the audit feature made default.
///
/// What it records is a decision, not a placement: the walk it is part of runs
/// before the book is called, so the line says the order was *proposed* and
/// nobody objected. Whether the commit that follows succeeds is written on the
/// batch's slip, by the caller that made it.
struct Trail;

impl Listener<OrderProposed> for Trail {
    fn on_event<'a>(
        &'a self,
        event: &'a mut OrderProposed,
        cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            let sink = cx
                .container()
                .get::<dyn Sink>()
                .await
                .map_err(|error| ListenerError::new(OrderProposed::NAME, Box::new(error)))?;

            sink.write(Record::new(
                NAME,
                format!(
                    "proposed {} for {} [{}]",
                    event.order.reference,
                    event.order.amount,
                    event.notes.join(", ")
                ),
            ))
            .await
            .map_err(|error| ListenerError::new(OrderProposed::NAME, Box::new(error)))?;

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

/// Offers a batch of orders every `every`, until it is asked to stop or until
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
                // The wait the kernel publishes, not a race between a timer and
                // the token written out here. A loop that only slept would be
                // abandoned at its deadline instead of returning, and the run
                // would be counted a dirty stop.
                //
                // `draining` is the stage this runnable cares about: it means
                // stop taking new work, and a batch is new work. What it holds
                // when the stage moves is at most one batch, and `batch`
                // abandons that one on `stopping`. A tick that did not elapse
                // is the ladder moving, which is the only other way this wait
                // can end.
                if !cx
                    .shutdown()
                    .sleep_until_draining(self.every)
                    .await
                    .is_elapsed()
                {
                    break;
                }

                if !self.batch(&cx).await {
                    // A real condition, not a timer: the desk cannot do its
                    // work at all, so this process would only be pretending to
                    // work. Asking through the handle is how any unit requests
                    // a stop — it does not drive one.
                    cx.handle().shutdown();
                    break;
                }
            }

            self.close(&cx).await;
            Ok(())
        })
    }
}

/// What one offered order does to the run it is part of.
///
/// Three answers because there are three, and a `bool` would have to pick two
/// of them. Every caller of [`Desk::offer`] has to decide what to do next, and
/// this is the decision rather than a status the caller re-derives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Screened, and committed unless a listener held it. The batch goes on.
    Continue,
    /// This batch ends and the desk stays: the book refused once, or the walk
    /// did not finish, and neither is a verdict on the process.
    EndBatch,
    /// Nothing left to stay up for: the book refuses everything, or the scoped
    /// binding each batch is written on cannot be resolved.
    EndRun,
}

impl Desk {
    /// One unit of work: one scope, one slip, `batch` orders.
    ///
    /// Answers `false` when there is nothing left to stay up for — see
    /// [`Step::EndRun`]. It never answers `true` for a batch that did nothing:
    /// a batch whose slip cannot be resolved writes no line and produces no
    /// order, and reporting that as a batch that went fine is how a broken
    /// graph reaches an operator as a healthy one.
    async fn batch(&self, cx: &RunContext) -> bool {
        // The unit of work. Everything `Scoped` resolved inside it is built
        // once and shared until this function returns; the next batch opens a
        // scope of its own and gets its own.
        let scope = cx.container().scope();

        // Resolved before the batch is counted. Counting first would put a
        // batch that never happened in the tally the health probe reads.
        let Some(slip) = slip_of(&scope, cx).await else {
            return false;
        };
        self.tally.batches.fetch_add(1, Ordering::Relaxed);
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

            match self.offer(&scope, cx, order).await {
                Step::Continue => {}
                Step::EndBatch => break,
                Step::EndRun => return false,
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

    /// Offers one order for screening, and commits it if nobody objected.
    ///
    /// # The order of these two calls is the whole method
    ///
    /// The dispatch happens **first**, and the book is called **after** it, and
    /// only when the walk finished without a veto. Reverse them and every line
    /// below still runs, every counter still moves and the emitter still reads
    /// `walk.stopped` — but the entry is in the ledger before the listener that
    /// was supposed to forbid it ever ran, and what the code calls "held" is an
    /// order that was placed and then described. A veto only vetoes while the
    /// thing it forbids has not happened yet.
    ///
    /// That is not a detail of this example. The same shape is a payment taken
    /// before the fraud check answers, a quota spent before the limit is read,
    /// a message sent before the approval. If the effect is irreversible — and
    /// a committed ledger entry is — then dispatching afterwards buys nothing
    /// except a record of an objection nobody could act on.
    ///
    /// This is `dispatch` and not `emit` for the same reason: `emit` returns
    /// before any listener has run, so the commit would race the veto rather
    /// than wait for it.
    async fn offer(&self, scope: &Scope, cx: &RunContext, order: Order) -> Step {
        let mut event = OrderProposed {
            order,
            notes: Vec::new(),
        };

        let (line, step) = match cx.dispatcher().dispatch(&mut event).await {
            // Vetoed. The book is not called, so nothing is committed and
            // `held` means what it says.
            Ok(walk) if walk.stopped => {
                self.tally.held.fetch_add(1, Ordering::Relaxed);
                (
                    format!("held {}: {}", event.order.reference, event.notes.join(", ")),
                    Step::Continue,
                )
            }
            // The walk finished and nobody objected. Now, and only now.
            Ok(_) => match self.book.place(event.order.clone()).await {
                Ok(sequence) => {
                    self.tally.placed.fetch_add(1, Ordering::Relaxed);
                    (
                        format!(
                            "placed {} as #{sequence}: {}",
                            event.order.reference,
                            event.notes.join(", ")
                        ),
                        Step::Continue,
                    )
                }
                Err(error) => {
                    let refused = self.tally.refused.fetch_add(1, Ordering::Relaxed) + 1;
                    let step = if refused >= GIVE_UP {
                        Step::EndRun
                    } else {
                        // One refusal is not a verdict. Give the batch up,
                        // keep the desk.
                        Step::EndBatch
                    };
                    (format!("refused {}: {error}", event.order.reference), step)
                }
            },
            // A listener failed, so the screening did not finish. Nothing is
            // committed: this caller does not know whether the order was
            // allowed, and committing on "we never found out" is the same
            // defect the ordering above exists to avoid.
            Err(error) => (
                format!("screening failed for {}: {error}", event.order.reference),
                Step::EndBatch,
            ),
        };

        // The slip is reached through the scope, not through an argument. That
        // is what a scope is for — and a scope that stops answering mid-batch
        // is a broken graph, not a line to skip.
        let Some(slip) = slip_of(scope, cx).await else {
            return Step::EndRun;
        };
        slip.note(line);
        step
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
        registry.listen(Stamp::default(), Priority::NORMAL);

        // The two listeners that resolve something say so. A listener is the
        // one registered thing whose needs nothing can observe — it resolves
        // during dispatch, long after phase three could have looked — so this
        // declaration is the only way the graph is checked for them. Without
        // it the resolution fails on every event and reaches telemetry alone.
        registry
            .listen(Trail, Priority::LOW)
            .requires([ContractRef::of::<dyn Sink>()]);

        // The feature's own event, and the only listener for it.
        registry
            .listen(Summary, Priority::NORMAL)
            .requires([ContractRef::of::<dyn Sink>()]);

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
            ])
            // What one batch resolves, not what the desk is built from: the
            // slip lives in the scope `Desk::batch` opens, so phase three
            // checks it is provided and `Scoped`, and the debug guard checks
            // the batch resolves nothing else.
            .requires_scoped([ContractRef::of::<Slip>()]),
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use kernel::core::telemetry::{RecordingTelemetry, Telemetry};
    use kernel::core::{ConfigNode, ConfigTree, Outcome, ShutdownPolicy};
    use kernel::{Kernel, KernelHandle, MemorySource, ShutdownController};
    use kernel_testkit::{FnBundle, Recorder};
    use ledger_contracts::LedgerError;

    use super::*;

    /// A ledger that accepts `open_for` entries and refuses everything after.
    ///
    /// It stands in for the whole ledger feature. Writing it is what the
    /// missing-contract list from phase three asks for, and it names no type of
    /// `ledger-bundle` — there is none to name.
    ///
    /// What it keeps, it keeps in a [`Recorder`]: the shared, poison-tolerant
    /// log every recording double used to write out by hand. The double is
    /// still written here, because only a crate that can name [`Ledger`] can
    /// implement it — what the testkit ships is the part underneath.
    struct Notebook {
        /// What has been appended.
        entries: Recorder<Entry>,
        /// How many entries this ledger accepts before closing.
        open_for: u64,
    }

    impl Notebook {
        /// A ledger that closes after `open_for` entries.
        fn new(open_for: u64) -> Arc<Self> {
            Arc::new(Self {
                entries: Recorder::new(),
                open_for,
            })
        }

        /// How many entries it holds.
        fn len(&self) -> u64 {
            u64::try_from(self.entries.len()).unwrap_or(u64::MAX)
        }

        /// What every entry is worth, in arrival order.
        fn amounts(&self) -> Vec<i64> {
            self.entries
                .items()
                .iter()
                .map(|entry| entry.amount)
                .collect()
        }
    }

    impl Ledger for Notebook {
        fn append(&self, entry: Entry) -> BoxFuture<'_, Result<u64, LedgerError>> {
            Box::pin(async move {
                if self.len() >= self.open_for {
                    return Err(LedgerError::Closed);
                }
                self.entries.record(entry);
                Ok(self.len())
            })
        }

        fn count(&self) -> BoxFuture<'_, Result<u64, LedgerError>> {
            Box::pin(async move { Ok(self.len()) })
        }
    }

    /// A sink that keeps what it is given.
    #[derive(Default)]
    struct Paper(Recorder<Record>);

    impl Paper {
        /// Every message written so far, in order.
        fn messages(&self) -> Vec<String> {
            self.0
                .items()
                .iter()
                .map(|record| record.message.clone())
                .collect()
        }

        /// The lines the audit trail wrote, which are the only ones this
        /// feature emits in a defined order.
        fn proposals(&self) -> Vec<String> {
            self.messages()
                .into_iter()
                .filter(|message| message.starts_with("proposed "))
                .collect()
        }
    }

    impl Sink for Paper {
        fn write(&self, record: Record) -> BoxFuture<'_, Result<(), audit_contracts::SinkError>> {
            Box::pin(async move {
                self.0.record(record);
                Ok(())
            })
        }
    }

    /// Stands in for the two features this one consumes.
    ///
    /// One bundle providing three bindings, none of which this crate could
    /// otherwise reach — and the three are exactly what phase three reports
    /// missing when `orders` is resolved alone.
    ///
    /// [`FnBundle`] rather than a `struct` and an `impl Bundle`: a bundle that
    /// exists to register three values in a test has nothing to say in a
    /// manifest and nothing to hold between calls.
    fn doubles(ledger: Arc<dyn Ledger>, sink: Arc<dyn Sink>, archive: Arc<dyn Sink>) -> FnBundle {
        FnBundle::new("doubles", move |registry| {
            registry.provide(Provider::from_value(Arc::clone(&ledger)));
            registry.provide(Provider::from_value(Arc::clone(&sink)));
            registry.provide_named(ARCHIVE, Provider::from_value(Arc::clone(&archive)));
            Ok(())
        })
    }

    /// What one run of the feature left behind.
    struct Run {
        /// How the kernel ended.
        outcome: Outcome,
        /// The handle, to see whether the desk asked for the stop.
        handle: KernelHandle,
        /// The ledger the book wrote through, to see what was committed.
        ledger: Arc<Notebook>,
        /// The default sink.
        sink: Arc<Paper>,
        /// The archive.
        archive: Arc<Paper>,
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
    async fn build(open_for: u64) -> (Kernel, Arc<Notebook>, Arc<Paper>, Arc<Paper>) {
        let ledger = Notebook::new(open_for);
        let sink = Arc::new(Paper::default());
        let archive = Arc::new(Paper::default());

        let kernel = Kernel::builder()
            .capture_signals(false)
            .shutdown_policy(ShutdownPolicy::new(
                Duration::from_millis(50),
                Duration::from_millis(50),
            ))
            .config_source(settings())
            .bundle(Bundled)
            .bundle(doubles(
                Arc::clone(&ledger) as Arc<dyn Ledger>,
                Arc::clone(&sink) as Arc<dyn Sink>,
                Arc::clone(&archive) as Arc<dyn Sink>,
            ))
            .build()
            .await
            .expect("the graph closes");

        (kernel, ledger, sink, archive)
    }

    /// Runs the whole feature until the desk decides to stop.
    async fn run(open_for: u64) -> Run {
        let (kernel, ledger, sink, archive) = build(open_for).await;
        let handle = kernel.handle();
        let outcome = kernel.run().await;

        Run {
            outcome,
            handle,
            ledger,
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

        let archive = Arc::new(Paper::default());
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
        let (kernel, _ledger, _sink, _archive) = build(u64::MAX).await;

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
        // Four entries: two batches commit two orders each, then the ledger
        // closes and the desk gives up.
        let run = run(4).await;

        // Three orders per batch at 100, 200 and 300; the third clears the cap
        // of 250, is held by the screen, and the trail below the screen never
        // sees it.
        let proposals = run.sink.proposals();
        assert!(proposals.iter().all(|line| line.contains("stamped")));
        assert!(!proposals.iter().any(|line| line.contains("order-1-2")));
        assert!(proposals.iter().any(|line| line.contains("order-1-0")));

        // The batch summaries reach the same sink through `emit`, which is
        // detached: whether they arrived before the run ended is not something
        // this feature promises, so nothing here asserts on them.
    }

    /// The veto is a veto: what the screen held is in no ledger.
    ///
    /// This is the assertion the ordering inside [`Desk::offer`] exists for,
    /// and the one a dispatch-after-commit passes only by accident. The cap is
    /// 250 and every third order is worth 300, so a desk that placed first and
    /// screened afterwards would leave 300s in the ledger and still report them
    /// held.
    #[tokio::test(start_paused = true)]
    async fn veto_precedes_the_commit() {
        let run = run(4).await;

        let committed = run.ledger.amounts();
        assert_eq!(committed, [100, 200, 100, 200]);
        assert!(
            committed.iter().all(|amount| *amount <= 250),
            "an order over the cap reached the ledger: {committed:?}"
        );
    }

    /// A book that cannot place anything asks the kernel to stop.
    #[tokio::test(start_paused = true)]
    async fn stops_when_the_book_fails() {
        let run = run(0).await;

        assert!(run.handle.is_shutting_down());
        assert!(run.outcome.is_success());
        // Nothing was committed. The trail still holds three lines: screening
        // happens before the commit, so an order that was proposed, allowed and
        // then refused by the book is recorded as proposed — and the refusal is
        // on the batch's own slip.
        assert_eq!(run.ledger.len(), 0);
        assert_eq!(run.sink.proposals().len(), 3);

        let closing = run.archive.messages();
        assert_eq!(closing.len(), 1);
        assert!(closing[0].contains("3 refused"));
        assert!(closing[0].contains("0 placed"));
    }

    /// A batch that cannot reach its slip reports a failure, not a success.
    ///
    /// A detached context carries an empty container, so the scoped binding
    /// this feature registers is not there to resolve — the shape a broken
    /// graph has. Answering `true` there — "the batch went fine" — is what
    /// would let a desk count empty batches while the probe grades the silence
    /// `Up`, and it is what this asserts against.
    #[tokio::test]
    async fn empty_batch_is_not_success() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let (cx, _controller) = RunContext::builder()
            .with_telemetry(Arc::clone(&telemetry) as Arc<dyn Telemetry>)
            .build();

        let archive = Arc::new(Paper::default());
        let desk = Desk {
            book: Arc::new(Book {
                ledger: Notebook::new(u64::MAX),
                count: AtomicU64::new(0),
            }),
            archive: Arc::clone(&archive) as Arc<dyn Sink>,
            tally: Arc::new(Tally::default()),
            every: Duration::from_secs(3600),
            batch: 3,
        };

        assert!(!desk.batch(&cx).await);
        assert!(telemetry.contains("orders.slip_unreachable"));
        // Not counted either: a batch that never happened is not in the tally
        // the health probe reads.
        assert_eq!(desk.tally.batches.load(Ordering::Relaxed), 0);
        assert!(archive.messages().is_empty());
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
