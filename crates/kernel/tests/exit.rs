//! Whether a process that ran a kernel is free to end.
//!
//! Every rung of section 13's ladder is bounded, and the kernel keeps that
//! promise: a runnable that overruns its budget is aborted, recorded, and never
//! waited for. The promise ends where [`Kernel::run`] returns — and one wait
//! comes after it that no rung covers. A task is only cancelled at an await
//! point, so a runnable that never reaches one goes on occupying a worker
//! thread, and a multi-threaded `Runtime` joins every worker thread in its own
//! `Drop`, with no bound at all. That is the shape of the failure this file
//! guards: the ladder finishes, reports a successful outcome, and the process
//! never exits.
//!
//! The bound is [`tokio::runtime::Runtime::shutdown_timeout`], which is what
//! `examples/minimal` and `examples/medium/app` end their `main` with, instead
//! of the plain drop `#[tokio::main]` performs. This asserts the bound holds —
//! against a runnable built to defeat everything except it.
//!
//! The run happens on a thread of its own so that the assertion can be made
//! from outside it: a test that tore the runtime down on its own thread would
//! hang with it, and wedge the suite instead of failing.

use core::time::Duration;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use kernel::core::{
    BoxFuture, Criticality, Outcome, RecordingTelemetry, RunError, RunnableDescriptor,
    ShutdownPolicy, Telemetry,
};
use kernel::{FnBundle, Kernel, Provider, RunContext, Runnable};

/// Budgets short enough that the stop is over in the time a test takes.
const POLICY: ShutdownPolicy =
    ShutdownPolicy::new(Duration::from_millis(50), Duration::from_millis(50));

/// How long the runtime is given to let go of its worker threads.
const TEARDOWN: Duration = Duration::from_millis(500);

/// How long this test waits for the thread that owns the runtime.
///
/// Far above [`TEARDOWN`], and bounded on purpose: an unbounded teardown must
/// fail this test rather than hang the suite that runs it.
const PATIENCE: Duration = Duration::from_secs(10);

/// Enough workers that the graph still runs with one of them lost.
///
/// [`Deaf`] takes a worker and never gives it back, so a single-worker runtime
/// would starve the kernel itself rather than exercise the teardown.
const WORKERS: usize = 4;

/// A runnable that never reaches an await point.
///
/// It therefore never observes its own abort: the supervisor abandons it at the
/// stop deadline, records `runnable.abandoned`, and the task goes on running.
/// This is the misbehaviour the kernel documents rather than prevents — and the
/// reason the wait after `run` has to be bounded too.
struct Deaf;

impl Runnable for Deaf {
    fn name() -> &'static str {
        "deaf"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new().criticality(Criticality::Ancillary)
    }

    fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            loop {
                thread::sleep(Duration::from_millis(20));
            }
        })
    }
}

/// What the thread that owns the runtime reports back.
struct Ended {
    /// What the run returned.
    outcome: Outcome,
    /// How long the teardown took.
    teardown: Duration,
    /// Whether the supervisor recorded an abandonment.
    abandoned: bool,
}

/// The kernel abandons what will not stop; the process must still end.
#[test]
fn teardown_is_bounded() {
    let (report, ended) = mpsc::channel();

    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKERS)
            .enable_all()
            .build()
            .expect("a runtime");
        let telemetry = Arc::new(RecordingTelemetry::new());
        let sink = Arc::clone(&telemetry);

        let outcome = runtime.block_on(async move {
            let kernel = Kernel::builder()
                .telemetry(sink as Arc<dyn Telemetry>)
                .capture_signals(false)
                .shutdown_policy(POLICY)
                .bundle(FnBundle::new("deaf", |registry| {
                    registry.runnable(Provider::from_value(Arc::new(Deaf)));
                    Ok(())
                }))
                .build()
                .await
                .expect("one runnable and nothing else");

            let handle = kernel.handle();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                handle.shutdown();
            });

            kernel.run().await
        });

        // The bound. Dropping the runtime instead would join the worker thread
        // `Deaf` is sitting on, and this thread would never report anything.
        let started = Instant::now();
        runtime.shutdown_timeout(TEARDOWN);
        let _ = report.send(Ended {
            outcome,
            teardown: started.elapsed(),
            abandoned: telemetry.contains("runnable.abandoned"),
        });
    });

    let ended = ended
        .recv_timeout(PATIENCE)
        .expect("a run that ended must leave the process free to end");

    // Without this the test could pass on a kernel that stopped `Deaf` cleanly,
    // which is the one thing `Deaf` is built never to do.
    assert!(ended.abandoned, "the deaf runnable was not abandoned");
    // The sting: the kernel reports a run that ended well, and the wait after it
    // is what decides whether the process ever exits.
    assert!(ended.outcome.is_success(), "{:?}", ended.outcome);
    assert!(
        ended.teardown < PATIENCE,
        "teardown took {:?}",
        ended.teardown
    );
}
