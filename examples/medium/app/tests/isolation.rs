//! One bundle, booted with nothing but doubles.
//!
//! This is the round trip design section 18 promises, run against a real
//! bundle instead of a fixture:
//!
//! 1. ask [`missing_contracts`] what `orders` needs and nobody here provides;
//! 2. write one double per line of that answer;
//! 3. boot `orders` alone on those doubles and drive it from a test.
//!
//! Step 1 is what makes step 2 finite. Nothing in this file reads
//! `orders-bundle`'s source to find out what to stub, and nothing in it names
//! `ledger-bundle` or `audit-bundle` — the doubles are written against
//! `ledger-contracts` and `audit-contracts`, which is the same door the real
//! implementations come through.
//!
//! The last test does not confirm the promise, it bounds it: a contract a
//! *listener* resolves is invisible to phase three, so it is absent from the
//! list and the bundle boots without it anyway. See `listener_needs_go_unlisted`.

use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::sync::{Arc, Mutex, PoisonError};

use audit_contracts::{ARCHIVE, Record, Sink, SinkError};
use kernel::core::{ComponentDescriptor, ComponentError, ConfigNode, ConfigTree, ContractRef};
use kernel::{BootContext, BoxFuture, Component, MemorySource, ShutdownContext};
use kernel_testkit::{TestBuilder, missing_contracts};
use ledger_contracts::{Entry, Ledger, LedgerError};
use orders_contracts::{Order, OrderBook};

// ---------------------------------------------------------------------------
// The doubles — one per line of the list, and no more
// ---------------------------------------------------------------------------

/// Stands in for `dyn Ledger`: keeps every entry, refuses nothing.
///
/// It is also a [`Component`], because the real ledger is one. A double keeps
/// the nature of what it replaces, so this one is handed to
/// `substitute_component` as well and the kernel boots and stops it like any
/// other — `double_is_booted_too` is where that is checked.
#[derive(Debug, Default)]
struct Notebook {
    entries: Mutex<Vec<Entry>>,
    booted: AtomicBool,
    stopped: AtomicBool,
}

impl Notebook {
    fn len(&self) -> u64 {
        let held = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        u64::try_from(held.len()).unwrap_or(u64::MAX)
    }
}

impl Ledger for Notebook {
    fn append(&self, entry: Entry) -> BoxFuture<'_, Result<u64, LedgerError>> {
        Box::pin(async move {
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

impl Component for Notebook {
    fn name() -> &'static str {
        "notebook"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.booted.store(true, Ordering::Relaxed);
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        _cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.stopped.store(true, Ordering::Relaxed);
            Ok(())
        })
    }
}

/// Stands in for `dyn Sink`: keeps what was written, in order.
#[derive(Debug, Default)]
struct Paper(Mutex<Vec<Record>>);

impl Paper {
    fn messages(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|record| record.message.clone())
            .collect()
    }
}

impl Sink for Paper {
    fn write(&self, record: Record) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(record);
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// The graph under test
// ---------------------------------------------------------------------------

/// Configuration for the bundle under test.
///
/// `every` is a parameter because the two questions asked here need opposite
/// clocks: a test about shutdown wants a desk that never reaches its first
/// batch, and a test about what a batch does wants one that reaches it at once.
fn settings(every: &'static str) -> MemorySource {
    let mut tree = ConfigTree::empty();
    for (path, node) in [
        ("orders.every", ConfigNode::from(every)),
        ("orders.batch", ConfigNode::from(2_i64)),
        ("orders.cap", ConfigNode::from(250_i64)),
    ] {
        tree.insert(path, node)
            .expect("literal paths cannot collide");
    }
    MemorySource::named("isolation", tree)
}

/// The doubles the list asked for, held so a test can read them afterwards.
struct Stand {
    ledger: Arc<Notebook>,
    archive: Arc<Paper>,
}

impl Stand {
    fn new() -> Self {
        Self {
            ledger: Arc::new(Notebook::default()),
            archive: Arc::new(Paper::default()),
        }
    }

    /// `orders` alone, with exactly one substitution per missing contract.
    fn builder(&self, every: &'static str) -> TestBuilder {
        TestBuilder::new()
            .config_source(settings(every))
            .bundle(orders_bundle::Bundled)
            .substitute::<dyn Ledger>(Arc::clone(&self.ledger) as Arc<dyn Ledger>)
            .substitute_named::<dyn Sink>(ARCHIVE, Arc::clone(&self.archive) as Arc<dyn Sink>)
    }
}

// ---------------------------------------------------------------------------
// The round trip
// ---------------------------------------------------------------------------

/// Phase three names the two contracts nobody in this graph provides.
///
/// `Ok` with a list is the answer being asked for; `Err` would mean the
/// assembly never reached phase three, which is a different fact and would say
/// nothing about what to stub.
#[test]
fn lists_what_orders_needs() {
    let missing = missing_contracts(orders_bundle::Bundled).expect("orders reaches phase three");

    assert_eq!(
        missing,
        [
            ContractRef::of::<dyn Ledger>(),
            ContractRef::named::<dyn Sink>(ARCHIVE),
        ]
    );
    // The named binding is not the default one, and the list says which is
    // wanted: a double provided under no name would leave this unsatisfied.
    assert_eq!(missing[1].name(), Some(ARCHIVE));
}

/// The bundle boots alone on one double per listed contract.
///
/// No timer fires here: `every` is an hour, so the desk is parked on its
/// shutdown token for the whole test and everything asserted below is the work
/// of the contract, not of the runnable.
#[tokio::test]
async fn boots_on_those_doubles() {
    let stand = Stand::new();
    let harness = stand
        .builder("1h")
        .start()
        .await
        .expect("orders boots with no other bundle present");

    // What the bundle publishes is resolvable, and it reaches the double
    // behind it: `orders` never named a ledger implementation, and this test
    // never named `orders`' own `Book`.
    let book = harness
        .container()
        .get::<dyn OrderBook>()
        .await
        .expect("the order book is bound");
    assert_eq!(book.place(Order::new("order-1", 120)).await.unwrap(), 1);
    assert_eq!(stand.ledger.count().await.unwrap(), 1);

    assert!(harness.stop().await.is_success());
}

/// A double of a component is booted and stopped like the real one.
///
/// The same `Arc` is substituted twice: once as the contract `orders` resolves,
/// once as the component the kernel owns. That is the shape the real feature
/// has, and substitution does not flatten it.
#[tokio::test]
async fn double_is_booted_too() {
    let stand = Stand::new();
    let harness = stand
        .builder("1h")
        .substitute_component(Arc::clone(&stand.ledger))
        .start()
        .await
        .expect("orders boots alongside the component double");

    assert!(stand.ledger.booted.load(Ordering::Relaxed));
    assert!(!stand.ledger.stopped.load(Ordering::Relaxed));

    assert!(harness.stop().await.is_success());
    assert!(stand.ledger.stopped.load(Ordering::Relaxed));
}

/// The desk returns on the shutdown token rather than being abandoned.
///
/// The closing line is written after the run loop breaks, so the archive
/// holding it is the proof the runnable finished on its own. A runnable that
/// only slept would be dropped at its deadline and the archive would be empty.
#[tokio::test]
async fn desk_returns_on_shutdown() {
    let stand = Stand::new();
    let harness = stand.builder("1h").start().await.expect("the desk runs");
    let telemetry = harness.telemetry();

    assert!(harness.stop().await.is_success());

    let closing = stand.archive.messages();
    assert_eq!(closing.len(), 1);
    assert!(closing[0].starts_with("desk closing after 0 batch(es)"));
    assert!(telemetry.contains("runnable.finished"));
}

/// What a listener resolves is not on the list, and phase three never sees it.
///
/// `orders` has two listeners that resolve the *default* `dyn Sink` from the
/// container while an event is being dispatched. Nothing declares that:
/// `Registry::listen` takes no requirements, and the bundle manifest names only
/// the archive. So phase three has nothing to check, the list above is short by
/// one, and a graph built from that list boots clean and then fails a listener
/// on every batch.
///
/// This test pins the current behaviour so that closing the hole breaks it
/// loudly. Until then, the promise "the list IS the doubles to write" holds for
/// providers, runnables and manifests — not for listeners.
#[tokio::test(start_paused = true)]
async fn listener_needs_go_unlisted() {
    let stand = Stand::new();
    let harness = stand.builder("10ms").start().await.expect("the desk runs");
    let telemetry = harness.telemetry();

    // Long enough for two batches on a clock that advances itself.
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(harness.stop().await.is_success());

    // Orders did reach the ledger: the graph is not broken, only blind.
    assert!(stand.ledger.count().await.unwrap() > 0);

    let unresolved: Vec<String> = telemetry
        .records()
        .iter()
        .filter(|record| record.event == "dispatcher.listener_failed")
        .filter_map(|record| record.field("error").map(ToString::to_string))
        .collect();
    assert!(!unresolved.is_empty(), "a listener resolved nothing at all");
    assert!(
        unresolved
            .iter()
            .all(|error| error.contains("no provider for dyn audit_contracts::Sink")),
        "{unresolved:?}"
    );
}
