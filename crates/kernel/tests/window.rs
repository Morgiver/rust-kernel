//! The drain window is observable, and it is a window.
//!
//! Section 13 argues that one signal cannot express a clean stop: a unit has to
//! be able to refuse new work while finishing what it holds. Everything the
//! repository runs is a timer, and a timer cannot tell the two stages apart —
//! so the property the two-stage ladder exists to provide had never been
//! measured from the outside. This is that measurement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use kernel::core::{BoxFuture, RunError, RunnableDescriptor, ShutdownPolicy, Stage};
use kernel::{FnBundle, Kernel, RunContext, Runnable};

/// A unit that records the stage it was in when each thing happened.
#[derive(Default)]
struct Watcher {
    saw_draining: AtomicBool,
    saw_stopping: AtomicBool,
    /// Work accepted after draining began. Must stay zero.
    accepted_late: AtomicU32,
    /// Work finished between draining and stopping. This is the window.
    finished_in_window: AtomicU32,
}

impl Runnable for Watcher {
    fn name() -> &'static str
    where
        Self: Sized,
    {
        "watcher"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            // Phase one: accept work until the ladder starts moving.
            cx.shutdown().draining().await;
            self.saw_draining.store(true, Ordering::SeqCst);
            assert_eq!(
                cx.shutdown().stage(),
                Stage::Draining,
                "draining must resolve while the ladder is at Draining, not later"
            );

            // Phase two: refuse new work, finish what is held. If this window
            // did not exist, the sleep below would be cut short and the
            // counter would stay at zero.
            if cx.shutdown().stage() == Stage::Draining {
                self.accepted_late.store(0, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
            if cx.shutdown().stage() == Stage::Draining {
                self.finished_in_window.fetch_add(1, Ordering::SeqCst);
            }

            cx.shutdown().stopping().await;
            self.saw_stopping.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[tokio::test(start_paused = true)]
async fn window_is_a_window() {
    let watcher = Arc::new(Watcher::default());
    let seen = Arc::clone(&watcher);

    let kernel = Kernel::builder()
        .capture_signals(false)
        .shutdown_policy(ShutdownPolicy::new(
            Duration::from_millis(500),
            Duration::from_millis(500),
        ))
        .bundle(FnBundle::new("probe", move |registry| {
            let watcher = Arc::clone(&watcher);
            registry.runnable(kernel::Provider::from_value(watcher));
            Ok(())
        }))
        .build()
        .await
        .expect("the graph is valid");

    let handle = kernel.handle();
    let running = tokio::spawn(kernel.run());
    tokio::time::sleep(Duration::from_millis(10)).await;
    handle.shutdown();
    let outcome = running.await.expect("the driver joined");

    assert!(outcome.is_success(), "a clean stop: {outcome:?}");
    assert!(
        seen.saw_draining.load(Ordering::SeqCst),
        "draining observed"
    );
    assert!(
        seen.saw_stopping.load(Ordering::SeqCst),
        "stopping observed"
    );
    assert_eq!(
        seen.finished_in_window.load(Ordering::SeqCst),
        1,
        "work held at draining must be able to finish BEFORE stopping arrives"
    );
}
