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
//! The list holds for listeners too, and that is the last test here: a contract
//! a listener resolves is declared where it is resolved, so phase three checks
//! it with everything else and it is on the list like everything else.

use core::time::Duration;
use std::sync::Arc;

use audit_contracts::{ARCHIVE, Record, Sink, SinkError};
use kernel::core::{
    ComponentDescriptor, ComponentError, ConfigNode, ConfigTree, ContractRef, KernelError,
    ResolveError,
};
use kernel::{BootContext, BoxFuture, Component, MemorySource, ShutdownContext};
use kernel_testkit::{LifecycleLog, Recorder, TestBuilder, missing_contracts};
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
///
/// Neither half is written out by hand. What it keeps goes in a [`Recorder`],
/// and the lifecycle calls the kernel makes on it are counted by a
/// [`LifecycleLog`] it delegates both hooks to: the testkit ships the parts
/// that are not about `Ledger`, and only the part that names the contract is
/// written here — because only a crate that can name the contract can
/// implement it.
#[derive(Debug, Default)]
struct Notebook {
    entries: Recorder<Entry>,
    lifecycle: LifecycleLog,
}

impl Notebook {
    fn len(&self) -> u64 {
        u64::try_from(self.entries.len()).unwrap_or(u64::MAX)
    }
}

impl Ledger for Notebook {
    fn append(&self, entry: Entry) -> BoxFuture<'_, Result<u64, LedgerError>> {
        Box::pin(async move {
            self.entries.record(entry);
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

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        self.lifecycle.boot(cx)
    }

    fn shutdown<'a>(
        &'a self,
        cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        self.lifecycle.shutdown(cx)
    }
}

/// Stands in for `dyn Sink`: keeps what was written, in order.
#[derive(Debug, Default)]
struct Paper(Recorder<Record>);

impl Paper {
    fn messages(&self) -> Vec<String> {
        self.0
            .items()
            .iter()
            .map(|record| record.message.clone())
            .collect()
    }
}

impl Sink for Paper {
    fn write(&self, record: Record) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            self.0.record(record);
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
    journal: Arc<Paper>,
    archive: Arc<Paper>,
}

impl Stand {
    fn new() -> Self {
        Self {
            ledger: Arc::new(Notebook::default()),
            journal: Arc::new(Paper::default()),
            archive: Arc::new(Paper::default()),
        }
    }

    /// `orders` alone, with exactly one substitution per missing contract.
    fn builder(&self, every: &'static str) -> TestBuilder {
        TestBuilder::new()
            .config_source(settings(every))
            .bundle(orders_bundle::Bundled)
            .substitute::<dyn Ledger>(Arc::clone(&self.ledger) as Arc<dyn Ledger>)
            .substitute::<dyn Sink>(Arc::clone(&self.journal) as Arc<dyn Sink>)
            .substitute_named::<dyn Sink>(ARCHIVE, Arc::clone(&self.archive) as Arc<dyn Sink>)
    }
}

// ---------------------------------------------------------------------------
// The round trip
// ---------------------------------------------------------------------------

/// Phase three names the three contracts nobody in this graph provides.
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
            ContractRef::of::<dyn Sink>(),
        ]
    );
    // The named binding is not the default one, and the list says which is
    // wanted: a double provided under no name would leave this unsatisfied,
    // and the default one is a separate line rather than the same line twice.
    assert_eq!(missing[1].name(), Some(ARCHIVE));
    assert_eq!(missing[2].name(), None);
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

    assert_eq!(stand.ledger.lifecycle.boots(), 1);
    assert_eq!(stand.ledger.lifecycle.stops(), 0);

    assert!(harness.stop().await.is_success());
    assert_eq!(stand.ledger.lifecycle.stops(), 1);
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

/// What a listener resolves is on the list, and phase three refuses the graph
/// without it.
///
/// `orders` has two listeners that resolve the *default* `dyn Sink` from the
/// container while an event is being dispatched. Nothing can observe that from
/// outside the listener, so the bundle declares it on the handle
/// `Registry::listen` hands back — and from there it is an ordinary
/// requirement: reported before anything boots, attributed to the event whose
/// handling needs it, and counted in the list above.
///
/// The graph built here is the one a reader would have assembled from a list
/// short by one. It does not boot and then fail a listener on every batch; it
/// does not boot at all.
#[tokio::test]
async fn listener_needs_are_listed() {
    let stand = Stand::new();

    let refused = TestBuilder::new()
        .config_source(settings("1h"))
        .bundle(orders_bundle::Bundled)
        .substitute::<dyn Ledger>(Arc::clone(&stand.ledger) as Arc<dyn Ledger>)
        .substitute_named::<dyn Sink>(ARCHIVE, Arc::clone(&stand.archive) as Arc<dyn Sink>)
        .start()
        .await
        .expect_err("phase three refuses a graph whose listener needs are unmet");

    let KernelError::Resolve(errors) = refused else {
        panic!("expected a resolution failure, got {refused:?}");
    };
    let blamed: Vec<&'static str> = errors
        .iter()
        .filter_map(|error| match error {
            ResolveError::MissingContract {
                required_by,
                contract,
            } if *contract == ContractRef::of::<dyn Sink>() => Some(*required_by),
            _ => None,
        })
        .collect();
    // Two listeners, and the manifest that declares what they resolve: three
    // things to change, so three lines rather than one.
    assert_eq!(blamed, ["orders.proposed", "orders.batch_closed", "orders"]);

    // Nothing was booted, so nothing was stopped either.
    assert_eq!(stand.ledger.lifecycle.boots(), 0);
}

/// The listeners that were checked do their work, on the sink they asked for.
///
/// The complement of the test above: with every listed contract provided, the
/// two listeners resolve what they declared and the audit trail fills.
#[tokio::test(start_paused = true)]
async fn declared_listeners_write() {
    let stand = Stand::new();
    let harness = stand.builder("10ms").start().await.expect("the desk runs");
    let telemetry = harness.telemetry();

    // Long enough for two batches on a clock that advances itself.
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(harness.stop().await.is_success());

    assert!(stand.ledger.count().await.unwrap() > 0);
    assert!(!telemetry.contains("dispatcher.listener_failed"));

    let trail = stand.journal.messages();
    assert!(
        trail.iter().any(|line| line.starts_with("proposed ")),
        "{trail:?}"
    );
}

/// A batch never leaves the ledger holding what the screen held.
///
/// The cap is 250 and the second order of every batch is worth 200, so nothing
/// here is vetoed — what this pins is the other direction: every entry in the
/// ledger is one the walk allowed, counted by the desk as placed.
#[tokio::test(start_paused = true)]
async fn ledger_holds_only_allowed() {
    let stand = Stand::new();
    let harness = stand.builder("10ms").start().await.expect("the desk runs");

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(harness.stop().await.is_success());

    let placed = stand
        .journal
        .messages()
        .iter()
        .filter(|line| line.starts_with("proposed "))
        .count();
    assert_eq!(
        u64::try_from(placed).unwrap_or(u64::MAX),
        stand.ledger.count().await.unwrap()
    );
}
