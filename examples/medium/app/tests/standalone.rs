//! The other half of the round trip: the two bundles `isolation.rs` never asks
//! about.
//!
//! `isolation.rs` runs the trip for `orders` — a bundle with a non-empty list.
//! The list has two other shapes, and both are claims `kernel-testkit` makes in
//! its own documentation:
//!
//! * `Ok(vec![])` — "this bundle stands alone", which is only worth anything if
//!   the bundle then *does* boot alone. `audit` is the one here that needs no
//!   configuration to say so, and booting it is also the only place the
//!   ancillary restart is asserted against a real kernel rather than against a
//!   hand-driven `run`.
//! * `Err` — "it never reached phase three". [`missing_contracts`] builds with
//!   no configuration source at all, so a bundle that reads its configuration in
//!   `register` lands here, and a reader who took `Err` for "nothing missing"
//!   would stub nothing and boot nothing. `ledger` is that bundle — and it is
//!   also why the question has a second form,
//!   [`missing_contracts_with`], which is asked the same
//!   thing with the sources the bundle needs to get as far as being asked.

use kernel::core::KernelError;
use kernel_testkit::{TestBuilder, missing_contracts, missing_contracts_with};

/// `audit` asks nothing of anybody, and the empty list is checked by booting it.
///
/// The list is the question; the boot is the answer being worth asking. A
/// bundle can report an empty list and still fail to stand up — a provider that
/// resolves something nobody declared would.
#[tokio::test]
async fn audit_stands_alone() {
    let missing = missing_contracts(audit_bundle::Bundled).expect("audit reaches phase three");
    assert_eq!(missing, []);

    let harness = TestBuilder::new()
        .bundle(audit_bundle::Bundled)
        .start()
        .await
        .expect("audit boots with nothing else present");

    // The ancillary demonstration, asserted against the supervisor rather than
    // against a hand-driven `run`: the bookend fails twice on purpose, the
    // kernel restarts it twice, and the process is still up to be stopped.
    assert_eq!(harness.wait_for_record("runnable.restarted", 2).await, 2);
    assert!(harness.is_running(), "{harness:?}");

    let outcome = harness.stop().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

/// `ledger` never reaches phase three when nothing configures it, so it has no
/// list to give.
///
/// [`missing_contracts`] builds with no configuration source, and `ledger` reads
/// three keys in `register` with no fallback. The refusal is a
/// [`KernelError::Register`] naming the bundle — not an empty list, which is
/// the answer that would have been a lie.
#[test]
fn ledger_never_reaches_the_list() {
    let error = missing_contracts(ledger_bundle::LedgerBundle::new())
        .expect_err("a bundle that reads configuration in `register` has no list");

    assert!(matches!(error, KernelError::Register(_)), "{error:?}");
    assert!(error.to_string().contains("ledger"), "{error}");
}

/// Given the configuration it reads, `ledger` asks nothing of anybody.
///
/// The same question as above with the sources the bundle needs to answer it:
/// `register` succeeds, phase three walks the graph, and the list is empty.
/// That is the trip [`missing_contracts`] cannot run for a bundle that reads a
/// key, and it is not a workaround for the refusal above — the two answers are
/// about two different assemblies, and both are facts worth having.
#[test]
fn configured_ledger_has_no_list() {
    let missing = missing_contracts_with(ledger_bundle::LedgerBundle::new(), [ledger_settings()])
        .expect("a configured ledger reaches phase three");

    assert_eq!(missing, []);
}

/// The empty list is worth what booting on it proves.
///
/// `ledger` registers a component and a contract, and no runnable — the exact
/// shape design section 17 gives its `storage` illustration. A kernel with zero
/// runnables has nothing to wait for: phase five publishes `Running` and
/// requests the stop in the same breath, so by the time `start` handed a
/// harness back the book would already be shut and `append` would answer
/// `Closed`.
///
/// That is correct for a program and useless for a test, which is what
/// [`TestBuilder::keep_running`] is for: it registers a runnable that returns
/// on the shutdown token and does nothing else, so the graph stays open until
/// this test asks for the stop. It is asked for explicitly, because a harness
/// that decided on its own when a kernel stops would make the test agree with a
/// kernel nobody runs.
#[tokio::test]
async fn ledger_stands_alone_once_configured() {
    let harness = TestBuilder::new()
        .config_source(ledger_settings())
        .bundle(ledger_bundle::LedgerBundle::new())
        .keep_running()
        .start()
        .await
        .expect("ledger boots with nothing else present");

    let ledger = harness
        .container()
        .get::<dyn ledger_contracts::Ledger>()
        .await
        .expect("the ledger is bound");
    assert_eq!(
        ledger
            .append(ledger_contracts::Entry::new("order-1", 250))
            .await
            .expect("the journal is open"),
        1
    );

    let outcome = harness.stop().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

/// Without the keep-alive, the same graph is already stopped.
///
/// The complement of the test above, and the reason `keep_running` is a call
/// rather than a default: a runnable-less kernel exiting the moment it has
/// started is the behaviour of the program, and the harness does not hide it.
#[tokio::test]
async fn runnable_less_kernel_ends() {
    let harness = TestBuilder::new()
        .config_source(ledger_settings())
        .bundle(ledger_bundle::LedgerBundle::new())
        .start()
        .await
        .expect("ledger boots with nothing else present");

    let ledger = harness
        .container()
        .get::<dyn ledger_contracts::Ledger>()
        .await
        .expect("the ledger is bound");

    let outcome = harness.wait().await;
    assert!(outcome.is_success(), "{outcome:?}");
    assert!(
        ledger
            .append(ledger_contracts::Entry::new("order-1", 250))
            .await
            .is_err(),
        "a kernel that has stopped left its component shut"
    );
}

/// The three keys `ledger` reads, with no fallback of its own.
fn ledger_settings() -> kernel::MemorySource {
    use kernel::core::{ConfigNode, ConfigTree};

    let mut tree = ConfigTree::empty();
    for (path, node) in [
        ("ledger.batch", ConfigNode::from(4_i64)),
        ("ledger.signing_key", ConfigNode::from("key")),
        ("ledger.flush_timeout", ConfigNode::from("2s")),
    ] {
        tree.insert(path, node)
            .expect("literal paths cannot collide");
    }
    kernel::MemorySource::named("standalone", tree)
}
