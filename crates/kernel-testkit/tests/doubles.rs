//! The doubles the testkit ships, driven the way a test drives them.

use core::time::Duration;
use std::sync::Arc;

use kernel::RunContext;
use kernel::core::Outcome;
use kernel::runnable::Runnable;
use kernel_testkit::{Call, LifecycleLog, Parking, Recorder, TestBuilder};

/// How far the paused clock is moved to prove that nothing ended on its own.
const A_WHILE: Duration = Duration::from_secs(60);

// One recording, however many handles: the copy a double holds and the copy
// the test reads are the same log.
#[test]
fn recorder_shares_recording() {
    let recorder: Recorder<u32> = Recorder::new();
    let held = recorder.clone();

    assert!(recorder.is_empty());
    held.record(1);
    held.record(2);

    assert_eq!(recorder.items(), [1, 2]);
    assert_eq!(recorder.len(), 2);
    assert!(!recorder.is_empty());
    assert!(format!("{recorder:?}").contains('2'), "{recorder:?}");

    recorder.clear();
    assert!(held.is_empty());
}

// `take` hands back what was recorded and empties the log, which is what a
// double recording something unclonable has instead of `items`.
#[test]
fn recorder_take_empties() {
    let recorder: Recorder<String> = Recorder::default();
    recorder.record("only".to_owned());

    assert_eq!(recorder.take(), ["only"]);
    assert!(recorder.is_empty());
}

// The parking runnable returns when the stop is asked for, and not before —
// which is the whole of what holding a graph open needs.
#[tokio::test(start_paused = true)]
async fn parking_waits_for_stop() {
    let (cx, controller) = RunContext::detached();
    let task = tokio::spawn(Arc::new(Parking).run(cx));

    tokio::time::sleep(A_WHILE).await;
    assert!(!task.is_finished(), "parking ended without being asked to");

    controller.begin_stopping();

    task.await.expect("no panic").expect("a clean end");
}

// A component double the kernel drove: booted on the way up, stopped on the
// way down, in that order and once each.
#[tokio::test(start_paused = true)]
async fn lifecycle_log_records_calls() {
    let log = Arc::new(LifecycleLog::new());

    let harness = TestBuilder::new()
        .substitute_component(Arc::clone(&log))
        .keep_running()
        .start()
        .await
        .expect("start");

    assert_eq!(log.calls(), [Call::Boot]);
    assert_eq!(log.boots(), 1);

    let outcome = harness.stop().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert_eq!(log.calls(), [Call::Boot, Call::Shutdown]);
    assert_eq!(log.stops(), 1);
}

// A component nobody drove recorded nothing, so a log that reports a boot is
// reporting the kernel rather than its own construction.
#[test]
fn undriven_log_is_empty() {
    let log = LifecycleLog::default();

    assert!(log.calls().is_empty());
    assert_eq!(log.boots(), 0);
    assert_eq!(log.stops(), 0);
}
