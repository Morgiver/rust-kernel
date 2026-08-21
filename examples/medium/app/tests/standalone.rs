//! The other half of the round trip: the two bundles `isolation.rs` never asks
//! about.
//!
//! `isolation.rs` runs the trip for `orders` — a bundle with a non-empty list.
//! The list has two other shapes, and both are claims `kernel-testkit` makes in
//! its own documentation:
//!
//! * `Ok(vec![])` — "this bundle stands alone", which is only worth anything if
//!   the bundle then *does* boot alone. `audit` is the one here that should,
//!   and booting it is also the only place the ancillary restart is asserted
//!   against a real kernel rather than against a hand-driven `run`.
//! * `Err` — "it never reached phase three". `missing_contracts` builds with no
//!   configuration source at all, so a bundle that reads its configuration in
//!   `register` lands here. `ledger` is that bundle, and a reader who took
//!   `Err` for "nothing missing" would stub nothing and boot nothing.

use core::time::Duration;

use kernel::core::KernelError;
use kernel_testkit::{TestBuilder, missing_contracts};

/// How long to let the supervisor work through the deliberate failures.
///
/// Two failures at a fixed 50 ms backoff. The wait polls rather than sleeps the
/// whole budget, so a working kernel finishes in about a tenth of this.
const PATIENCE: Duration = Duration::from_secs(5);

/// Waits until `event` has been recorded `count` times, or gives up.
async fn until(telemetry: &kernel::core::RecordingTelemetry, event: &str, count: usize) -> usize {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let seen = telemetry
            .records()
            .iter()
            .filter(|record| record.event == event)
            .count();
        if seen >= count || tokio::time::Instant::now() >= deadline {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

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
    let telemetry = harness.telemetry();
    assert_eq!(until(&telemetry, "runnable.restarted", 2).await, 2);

    let outcome = harness.stop().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

/// `ledger` never reaches phase three, so it has no list to give.
///
/// `missing_contracts` builds with no configuration source, and `ledger` reads
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

/// Given the configuration it reads, `ledger` asks nothing of anybody either —
/// and boots alone.
///
/// This is the trip `missing_contracts` cannot run, done by hand: the list it
/// would have returned is empty, and the proof is a kernel that starts with
/// this bundle and no other.
///
/// # This test fails, and the failure is the finding
///
/// `ledger` registers a component and a contract, and no runnable — the exact
/// shape design section 17 gives its `storage` illustration. A kernel with zero
/// runnables publishes `Running` and immediately requests its own stop with
/// reason `completed`, so by the time `start` has handed a harness back, the
/// book has been shut down and `append` answers `Closed`.
///
/// Nothing here is wrong about `ledger`. What is missing is a way to hold a
/// runnable-less graph open: `TestBuilder` offers no keep-alive and
/// `TestHarness` offers no "still running" state, so the promise "a bundle can
/// be booted alone" is only reachable for bundles that happen to own a
/// runnable. The workaround — `substitute_runnable` with a parking double the
/// testkit does not ship — is the shape of the fix.
#[ignore = "gap 4: a runnable-less kernel stops itself before a test can use it"]
#[tokio::test]
async fn ledger_stands_alone_once_configured() {
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
