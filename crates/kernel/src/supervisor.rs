//! Phase five: start the runnables and watch them.
//!
//! # What the supervisor owes the rest of the kernel
//!
//! Three promises, and each one is a test in this module rather than a
//! sentence:
//!
//! 1. **An essential runnable returning ends the run.** Whatever its result —
//!    a clean `Ok(())`, a failure, or a panic — watching resolves. An
//!    ancillary one never resolves it: it is recorded, restarted per its
//!    [`RestartPolicy`], and once its attempts are spent the kernel keeps
//!    running without it.
//! 2. **A panic is caught at the join.** It arrives as
//!    [`RunErrorKind::Panicked`](kernel_core::RunErrorKind::Panicked) and never
//!    reaches the caller as an unwind. A supervisor that dies with its runnable
//!    is worse than no supervisor.
//! 3. **The stop never blocks indefinitely.** A runnable that ignores its
//!    shutdown token is abandoned when its budget runs out, recorded as
//!    [`RunErrorKind::DeadlineExceeded`](kernel_core::RunErrorKind::DeadlineExceeded),
//!    and `stop` returns anyway.
//!
//! # The two-stage ladder, and what a descriptor may do to it
//!
//! Stopping drives `Draining` then `Stopping`, each with the budget
//! the [`ShutdownPolicy`](kernel_core::ShutdownPolicy) gave it. A stage ends as
//! soon as every runnable still alive has spent its own budget for that stage,
//! and never later than the global deadline: a descriptor's `drain_timeout` and
//! `stop_timeout` can therefore only shorten a stage, never extend it. Whatever
//! is still alive when the second stage ends is aborted, not awaited.

use core::fmt;
use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::task::Poll;
use core::time::Duration;
use std::sync::Arc;

use kernel_core::{
    Criticality, Level, Record, RestartPolicy, RunError, RunnableDescriptor, RunnableId, Stage,
    Telemetry,
};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;

use crate::container::Container;
use crate::dispatcher::EventDispatcher;
use crate::events::ShutdownReason;
use crate::runnable::{RunContext, Runnable};
use crate::shutdown::{KernelHandle, Shutdown, ShutdownController};

/// How a supervised task came back: its own result, or the join failing.
type Ended = Result<Result<(), RunError>, JoinError>;

/// One supervised runnable and everything the supervisor remembers about it.
struct Slot {
    /// Identity every record and every error is attributed to.
    id: RunnableId,
    /// The runnable itself, kept so that a restart can start it again.
    runnable: Arc<dyn Runnable>,
    /// Read once, at start: the contract says it must not change between calls,
    /// and reading it once is what makes that observable rather than assumed.
    descriptor: RunnableDescriptor,
    /// Restarts already spent, not counting the first run.
    restarts: u32,
    /// The live task, or `None` once it has ended for good.
    task: Option<JoinHandle<Result<(), RunError>>>,
}

/// Owns the running tasks and decides when the kernel must stop.
pub(crate) struct Supervisor {
    /// One slot per runnable, in registration order — the order they started
    /// in, since no runnable depends on another.
    slots: Vec<Slot>,
    /// Container handed to every [`RunContext`] this supervisor builds.
    container: Container,
    /// Dispatcher handed to every [`RunContext`] this supervisor builds.
    dispatcher: Arc<EventDispatcher>,
    /// The shutdown token every runnable watches.
    shutdown: Shutdown,
    /// The handle a runnable uses to ask for a stop, and that `watch` observes.
    handle: KernelHandle,
    /// Where restarts, abandonments and failures are recorded.
    telemetry: Arc<dyn Telemetry>,
    /// Every abnormal ending seen since the first start, in the order seen.
    errors: Vec<RunError>,
}

impl Supervisor {
    /// Starts every runnable. They all start after every component has booted,
    /// and no runnable ever depends on another.
    ///
    /// The four trailing arguments are exactly what [`RunContext::new`] takes:
    /// the supervisor builds one context per start, so a restarted runnable
    /// gets a fresh context watching the same token.
    pub(crate) fn start(
        runnables: Vec<(RunnableId, Arc<dyn Runnable>)>,
        container: Container,
        dispatcher: Arc<EventDispatcher>,
        shutdown: Shutdown,
        handle: KernelHandle,
    ) -> Self {
        let telemetry = Arc::clone(container.telemetry());
        let mut supervisor = Self {
            slots: Vec::with_capacity(runnables.len()),
            container,
            dispatcher,
            shutdown,
            handle,
            telemetry,
            errors: Vec::new(),
        };

        for (id, runnable) in runnables {
            let descriptor = runnable.descriptor();
            let task = supervisor.spawn(id, &runnable, None);
            supervisor.record(Level::Info, "runnable.started", id, |record| {
                record.with("attempt", 0u32)
            });
            supervisor.slots.push(Slot {
                id,
                runnable,
                descriptor,
                restarts: 0,
                task: Some(task),
            });
        }

        supervisor
    }

    /// Resolves on the first of: an essential runnable returning, every
    /// runnable having returned, or a stop being requested.
    ///
    /// Ancillary failures are restarted here according to their policy and
    /// never resolve this future.
    ///
    /// A kernel with no runnable at all resolves immediately with
    /// [`ShutdownReason::Completed`]: an object graph with nothing running in
    /// it is a program that has already finished.
    pub(crate) async fn watch(&mut self, shutdown: &Shutdown) -> ShutdownReason {
        loop {
            if self.live() == 0 {
                return ShutdownReason::Completed;
            }

            // Cloned out of `self` so the select's other branch can borrow the
            // slots mutably.
            let handle = self.handle.clone();
            let joined = tokio::select! {
                () = handle.requested() => return ShutdownReason::Programmatic,
                // Nothing but an outside force moves the ladder while `watch`
                // is running: the supervisor itself only touches it in `stop`.
                () = shutdown.draining() => return ShutdownReason::Signal,
                joined = next_ended(&mut self.slots) => joined,
            };

            if let Some(reason) = self.settle(joined.0, joined.1, true) {
                return reason;
            }
        }
    }

    /// Drives the two-stage stop and joins every task.
    ///
    /// A task that outlives its deadline is abandoned, recorded, and does not
    /// hold up the rest: the kernel never blocks indefinitely.
    ///
    /// The returned errors are every abnormal ending of the whole run, not only
    /// of the stop: an ancillary failure that `watch` restarted is in here too.
    /// The abandoned count [`Stopped`](crate::events::Stopped) reports is the
    /// number of them whose kind is
    /// [`DeadlineExceeded`](kernel_core::RunErrorKind::DeadlineExceeded).
    pub(crate) async fn stop(mut self, controller: &ShutdownController) -> Vec<RunError> {
        let watcher = controller.watcher();

        controller.begin_draining();
        self.wind_down(&watcher, Stage::Draining).await;

        controller.begin_stopping();
        self.wind_down(&watcher, Stage::Stopping).await;

        self.abandon();
        self.errors
    }

    /// Spawns one run of a runnable, optionally after a restart delay.
    ///
    /// The delay is served inside the task rather than by the caller, so a
    /// runnable waiting to be restarted never holds up the supervisor — and it
    /// gives up on the restart entirely once stopping has begun.
    fn spawn(
        &self,
        id: RunnableId,
        runnable: &Arc<dyn Runnable>,
        delay: Option<Duration>,
    ) -> JoinHandle<Result<(), RunError>> {
        let context = RunContext::new(
            id,
            self.container.clone(),
            Arc::clone(&self.dispatcher),
            self.shutdown.clone(),
            self.handle.clone(),
        );
        let runnable = Arc::clone(runnable);
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            if let Some(delay) = delay {
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = shutdown.stopping() => return Ok(()),
                }
            }
            runnable.run(context).await
        })
    }

    /// Files one ending: records it, keeps its error, and decides what happens
    /// next.
    ///
    /// Returns a reason only when the run must end, which is only ever an
    /// essential runnable coming back. `restartable` is false during the stop,
    /// where a failing ancillary runnable is recorded but never started again.
    fn settle(&mut self, index: usize, ended: Ended, restartable: bool) -> Option<ShutdownReason> {
        let id = self.slots[index].id;
        let criticality = self.slots[index].descriptor.criticality;
        let failure = failure_of(id, ended);

        match &failure {
            Some(error) => self.record(Level::Error, "runnable.failed", id, |record| {
                record
                    .with("criticality", criticality.as_str())
                    .with("cause", error.kind().to_string())
            }),
            None => self.record(Level::Info, "runnable.finished", id, |record| {
                record.with("criticality", criticality.as_str())
            }),
        }

        let failed = failure.is_some();
        if let Some(error) = failure {
            self.errors.push(error);
        }

        if criticality == Criticality::Essential {
            return Some(ShutdownReason::EssentialFinished(id));
        }

        // A clean return is an end, not a failure, so it never restarts.
        if !failed || !restartable {
            return None;
        }

        let slot = &self.slots[index];
        let policy = slot.descriptor.restart;
        if !policy.allows(slot.restarts) {
            self.record(Level::Warn, "runnable.restarts_exhausted", id, |record| {
                record.with("restarts", slot.restarts)
            });
            return None;
        }

        let attempt = self.slots[index].restarts;
        let delay = match policy {
            RestartPolicy::Never => Duration::ZERO,
            RestartPolicy::OnFailure { backoff, .. } => backoff.delay(attempt),
        };
        let runnable = Arc::clone(&self.slots[index].runnable);
        let task = self.spawn(id, &runnable, Some(delay));

        self.slots[index].restarts = attempt + 1;
        self.slots[index].task = Some(task);
        self.record(Level::Warn, "runnable.restarted", id, |record| {
            record.with("attempt", attempt + 1).with("delay", delay)
        });

        None
    }

    /// Waits out one stage of the ladder, filing whatever comes back.
    ///
    /// The stage ends when every live runnable has returned, when the last of
    /// them has spent its own budget for this stage, or at the global deadline
    /// — whichever comes first. A per-runnable timeout can only bring that
    /// moment forward.
    async fn wind_down(&mut self, watcher: &Shutdown, stage: Stage) {
        let entered = Instant::now();
        let global = watcher.deadline().map(Instant::from_std);

        loop {
            let Some(end) = self.stage_end(stage, entered, global) else {
                return;
            };
            if end <= Instant::now() {
                return;
            }

            let joined = tokio::select! {
                joined = next_ended(&mut self.slots) => Some(joined),
                () = tokio::time::sleep_until(end) => None,
            };

            match joined {
                Some((index, ended)) => {
                    self.settle(index, ended, false);
                }
                None => return,
            }
        }
    }

    /// The instant this stage may run until: the latest budget any live
    /// runnable still holds, or `None` when none is live.
    fn stage_end(
        &self,
        stage: Stage,
        entered: Instant,
        global: Option<Instant>,
    ) -> Option<Instant> {
        self.slots
            .iter()
            .filter(|slot| slot.task.is_some())
            .map(|slot| {
                let own = match stage {
                    Stage::Draining => slot.descriptor.drain_timeout,
                    Stage::Stopping => slot.descriptor.stop_timeout,
                    Stage::Running | Stage::Stopped => None,
                }
                .map(|budget| entered + budget);

                match (own, global) {
                    (Some(own), Some(global)) => own.min(global),
                    (Some(only), None) | (None, Some(only)) => only,
                    // An untimed stage grants no budget at all rather than an
                    // unbounded one: the kernel never blocks indefinitely.
                    (None, None) => entered,
                }
            })
            .max()
    }

    /// Aborts whatever outlived the ladder, and records one deadline error per
    /// task abandoned.
    ///
    /// The aborted tasks are deliberately not awaited. A task that never yields
    /// would never observe its abort, and waiting for it is the one thing this
    /// kernel promises never to do.
    fn abandon(&mut self) {
        for index in 0..self.slots.len() {
            let Some(task) = self.slots[index].task.take() else {
                continue;
            };
            task.abort();

            let id = self.slots[index].id;
            self.errors.push(RunError::deadline_exceeded(id));
            self.record(Level::Error, "runnable.abandoned", id, |record| {
                record.with("restarts", self.slots[index].restarts)
            });
        }
    }

    /// How many runnables are still live: neither ended for good nor abandoned.
    pub(crate) fn live(&self) -> usize {
        self.slots.iter().filter(|slot| slot.task.is_some()).count()
    }

    /// Emits one record naming the runnable it is about.
    fn record(
        &self,
        level: Level,
        event: &'static str,
        id: RunnableId,
        fields: impl FnOnce(Record) -> Record,
    ) {
        let record = Record::new(level, event)
            .with("runnable", id.name())
            .with("index", i64::from(id.index()));
        self.telemetry.record(fields(record));
    }
}

impl fmt::Debug for Supervisor {
    /// Names the runnables and counts what happened to them: `Runnable` carries
    /// no `Debug` supertrait, and requiring one would tax every implementor for
    /// the sake of a diagnostic.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Supervisor")
            .field(
                "runnables",
                &self.slots.iter().map(|slot| slot.id).collect::<Vec<_>>(),
            )
            .field("live", &self.live())
            .field("errors", &self.errors.len())
            .finish_non_exhaustive()
    }
}

/// Turns one ending into an error, or into `None` for a clean return.
///
/// This is where a panic stops being an unwind: the join reports it, and it
/// becomes a [`RunError`] like any other.
fn failure_of(id: RunnableId, ended: Ended) -> Option<RunError> {
    match ended {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(join) if join.is_panic() => Some(RunError::panicked(id, panic_message(join))),
        Err(_) => Some(RunError::cancelled(id)),
    }
}

/// Recovers the message a panic carried, for the two payload shapes `panic!`
/// produces; anything else is reported without inventing a message.
fn panic_message(join: JoinError) -> String {
    let payload = join.into_panic();
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panicked with a payload of unknown type".to_owned()
    }
}

/// Resolves with the first runnable to come back, and empties its slot.
///
/// Polling the join handles from one future is what keeps the index: a task
/// cannot report its own identity when it panics, and the position in this
/// vector is the identity that survives an unwind.
///
/// Pends forever when no slot is live, so every caller checks that first.
async fn next_ended(slots: &mut [Slot]) -> (usize, Ended) {
    poll_fn(|cx| {
        for (index, slot) in slots.iter_mut().enumerate() {
            let Some(task) = slot.task.as_mut() else {
                continue;
            };
            if let Poll::Ready(ended) = Pin::new(task).poll(cx) {
                slot.task = None;
                return Poll::Ready((index, ended));
            }
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use kernel_core::{Backoff, BoxFuture, ConfigTree, RunErrorKind, ShutdownPolicy};
    use tokio::time::{Duration, timeout};

    use super::*;

    /// Fails on its first `failures` runs, then waits for the token.
    struct Flaky {
        descriptor: RunnableDescriptor,
        failures: usize,
        starts: AtomicUsize,
    }

    impl Flaky {
        fn new(descriptor: RunnableDescriptor, failures: usize) -> Arc<Self> {
            Arc::new(Self {
                descriptor,
                failures,
                starts: AtomicUsize::new(0),
            })
        }
    }

    impl Runnable for Flaky {
        fn name() -> &'static str {
            "flaky"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.descriptor
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                let start = self.starts.fetch_add(1, Ordering::Relaxed);
                if start < self.failures {
                    return Err(RunError::failed(cx.id(), "run refused".into()));
                }
                cx.shutdown().stopping().await;
                Ok(())
            })
        }
    }

    /// Returns as soon as it is started, with the result it was built with.
    struct Prompt(RunnableDescriptor, bool);

    impl Runnable for Prompt {
        fn name() -> &'static str {
            "prompt"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            let ok = self.1;
            Box::pin(async move {
                if ok {
                    Ok(())
                } else {
                    Err(RunError::failed(cx.id(), "gave up".into()))
                }
            })
        }
    }

    /// Unwinds instead of returning.
    struct Unwinding(RunnableDescriptor);

    impl Runnable for Unwinding {
        fn name() -> &'static str {
            "unwinding"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move { panic!("came apart") })
        }
    }

    /// Ignores the shutdown token entirely — what the contract forbids, and
    /// what the stop has to survive anyway.
    struct Deaf(RunnableDescriptor);

    impl Runnable for Deaf {
        fn name() -> &'static str {
            "deaf"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        }
    }

    /// Returns at the first rung instead of waiting for the second.
    struct Brisk(RunnableDescriptor);

    impl Runnable for Brisk {
        fn name() -> &'static str {
            "brisk"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                cx.shutdown().draining().await;
                Ok(())
            })
        }
    }

    /// A supervisor over `units`, plus the controller that drives its ladder
    /// and the handle that asks it to stop.
    fn started(
        units: Vec<Arc<dyn Runnable>>,
        policy: ShutdownPolicy,
    ) -> (Supervisor, ShutdownController, KernelHandle) {
        let telemetry: Arc<dyn Telemetry> = Arc::new(kernel_core::NoopTelemetry);
        let handle = KernelHandle::detached();
        let container = Container::new(
            Vec::new(),
            Arc::new(ConfigTree::empty()),
            Arc::clone(&telemetry),
            handle.clone(),
        );
        let dispatcher = Arc::new(EventDispatcher::new(Vec::new(), telemetry));
        let (controller, shutdown) = ShutdownController::new(policy);

        // The kernel names a unit from its own declaration; here the test
        // stands in for the registry and names them by position.
        let runnables = units
            .into_iter()
            .enumerate()
            .map(|(index, unit)| (RunnableId::new(NAMES[index], index as u32), unit))
            .collect();

        let supervisor =
            Supervisor::start(runnables, container, dispatcher, shutdown, handle.clone());

        (supervisor, controller, handle)
    }

    /// The names `started` hands out, by position.
    const NAMES: [&str; 3] = ["alpha", "beta", "gamma"];

    fn essential() -> RunnableDescriptor {
        RunnableDescriptor::new().criticality(Criticality::Essential)
    }

    fn ancillary() -> RunnableDescriptor {
        RunnableDescriptor::new().criticality(Criticality::Ancillary)
    }

    fn brief() -> ShutdownPolicy {
        ShutdownPolicy::new(Duration::from_secs(1), Duration::from_secs(2))
    }

    #[tokio::test(start_paused = true)]
    async fn essential_end_resolves_watch() {
        let unit: Arc<dyn Runnable> = Arc::new(Prompt(essential(), true));
        let (mut supervisor, controller, _handle) = started(vec![unit], brief());
        let watcher = controller.watcher();

        let reason = timeout(Duration::from_secs(1), supervisor.watch(&watcher))
            .await
            .expect("watch resolved");

        match reason {
            ShutdownReason::EssentialFinished(id) => assert_eq!(id.name(), "alpha"),
            other => panic!("unexpected reason: {other:?}"),
        }
    }

    /// Whatever its result: a clean return and a failure both end the run.
    #[tokio::test(start_paused = true)]
    async fn essential_failure_resolves_watch() {
        let unit: Arc<dyn Runnable> = Arc::new(Prompt(essential(), false));
        let (mut supervisor, controller, _handle) = started(vec![unit], brief());
        let watcher = controller.watcher();

        let reason = timeout(Duration::from_secs(1), supervisor.watch(&watcher))
            .await
            .expect("watch resolved");

        assert!(matches!(reason, ShutdownReason::EssentialFinished(_)));

        let errors = supervisor.stop(&controller).await;
        assert!(matches!(errors[0].kind(), RunErrorKind::Failed(_)));
    }

    /// The whole point of `Ancillary`: it fails, it is restarted, and the
    /// kernel is none the wiser.
    #[tokio::test(start_paused = true)]
    async fn ancillary_failure_restarts() {
        let flaky = Flaky::new(
            ancillary().restart(RestartPolicy::on_failure(
                3,
                Backoff::Fixed(Duration::from_millis(10)),
            )),
            2,
        );
        let anchor: Arc<dyn Runnable> = Flaky::new(essential(), 0);
        let units: Vec<Arc<dyn Runnable>> = vec![Arc::clone(&flaky) as Arc<dyn Runnable>, anchor];
        let (mut supervisor, controller, handle) = started(units, brief());
        let watcher = controller.watcher();

        // Two failures, two restarts, and `watch` must sit through all of it.
        let idle = timeout(Duration::from_millis(500), supervisor.watch(&watcher)).await;
        assert!(idle.is_err(), "an ancillary failure must not end the run");
        assert_eq!(flaky.starts.load(Ordering::Relaxed), 3);

        handle.shutdown();
        let reason = timeout(Duration::from_secs(1), supervisor.watch(&watcher))
            .await
            .expect("watch resolved");
        assert!(matches!(reason, ShutdownReason::Programmatic));
    }

    /// Exhausting the attempts is recorded and the kernel keeps running.
    #[tokio::test(start_paused = true)]
    async fn exhausted_restarts_keep_running() {
        let flaky = Flaky::new(
            ancillary().restart(RestartPolicy::on_failure(
                1,
                Backoff::Fixed(Duration::from_millis(10)),
            )),
            usize::MAX,
        );
        let anchor: Arc<dyn Runnable> = Flaky::new(essential(), 0);
        let units: Vec<Arc<dyn Runnable>> = vec![Arc::clone(&flaky) as Arc<dyn Runnable>, anchor];
        let (mut supervisor, controller, handle) = started(units, brief());
        let watcher = controller.watcher();

        let idle = timeout(Duration::from_millis(500), supervisor.watch(&watcher)).await;
        assert!(idle.is_err(), "a spent ancillary must not end the run");
        // One run plus one allowed restart, and no more.
        assert_eq!(flaky.starts.load(Ordering::Relaxed), 2);

        handle.shutdown();
        supervisor.watch(&watcher).await;

        let errors = supervisor.stop(&controller).await;
        let refused = errors
            .iter()
            .filter(|error| matches!(error.kind(), RunErrorKind::Failed(_)))
            .count();
        assert_eq!(refused, 2);
    }

    /// A supervisor that dies with its runnable is worse than no supervisor.
    #[tokio::test(start_paused = true)]
    async fn panic_becomes_run_error() {
        let unit: Arc<dyn Runnable> = Arc::new(Unwinding(essential()));
        let (mut supervisor, controller, _handle) = started(vec![unit], brief());
        let watcher = controller.watcher();

        let reason = timeout(Duration::from_secs(1), supervisor.watch(&watcher))
            .await
            .expect("the supervisor outlived the panic");
        assert!(matches!(reason, ShutdownReason::EssentialFinished(_)));

        let errors = supervisor.stop(&controller).await;
        match errors[0].kind() {
            RunErrorKind::Panicked(message) => assert!(message.contains("came apart")),
            other => panic!("unexpected kind: {other}"),
        }
    }

    /// An ancillary panic is caught the same way, and is restartable like any
    /// other failure.
    #[tokio::test(start_paused = true)]
    async fn ancillary_panic_restarts() {
        let unit: Arc<dyn Runnable> = Arc::new(Unwinding(ancillary().restart(
            RestartPolicy::on_failure(1, Backoff::Fixed(Duration::from_millis(10))),
        )));
        let anchor: Arc<dyn Runnable> = Flaky::new(essential(), 0);
        let (mut supervisor, controller, handle) = started(vec![unit, anchor], brief());
        let watcher = controller.watcher();

        let idle = timeout(Duration::from_millis(500), supervisor.watch(&watcher)).await;
        assert!(idle.is_err());

        handle.shutdown();
        supervisor.watch(&watcher).await;

        let errors = supervisor.stop(&controller).await;
        let panics = errors
            .iter()
            .filter(|error| matches!(error.kind(), RunErrorKind::Panicked(_)))
            .count();
        assert_eq!(panics, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn every_runnable_returning_completes() {
        let one: Arc<dyn Runnable> = Arc::new(Prompt(ancillary(), true));
        let two: Arc<dyn Runnable> = Arc::new(Prompt(ancillary(), true));
        let (mut supervisor, controller, _handle) = started(vec![one, two], brief());
        let watcher = controller.watcher();

        let reason = timeout(Duration::from_secs(1), supervisor.watch(&watcher))
            .await
            .expect("watch resolved");

        assert!(matches!(reason, ShutdownReason::Completed));
    }

    #[tokio::test(start_paused = true)]
    async fn no_runnable_completes() {
        let (mut supervisor, controller, _handle) = started(Vec::new(), brief());
        let watcher = controller.watcher();

        assert!(matches!(
            supervisor.watch(&watcher).await,
            ShutdownReason::Completed
        ));
    }

    /// The ladder moving under a running supervisor can only come from outside
    /// it, since `stop` is the only place the supervisor touches it.
    #[tokio::test(start_paused = true)]
    async fn outside_move_is_signal() {
        let unit: Arc<dyn Runnable> = Flaky::new(essential(), 0);
        let (mut supervisor, controller, _handle) = started(vec![unit], brief());
        let watcher = controller.watcher();

        controller.begin_draining();

        assert!(matches!(
            timeout(Duration::from_secs(1), supervisor.watch(&watcher))
                .await
                .expect("watch resolved"),
            ShutdownReason::Signal
        ));
    }

    /// The claim the whole design rests on, proved rather than restated: a
    /// runnable that never yields is abandoned and the stop still returns.
    #[tokio::test(start_paused = true)]
    async fn deaf_runnable_is_abandoned() {
        let unit: Arc<dyn Runnable> = Arc::new(Deaf(essential()));
        let (supervisor, controller, handle) = started(vec![unit], brief());

        handle.shutdown();
        let started_at = Instant::now();
        let errors = timeout(Duration::from_secs(10), supervisor.stop(&controller))
            .await
            .expect("the kernel stopped anyway");
        let elapsed = Instant::now() - started_at;

        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].kind(), RunErrorKind::DeadlineExceeded));
        // The two budgets, and not a moment more.
        assert!(elapsed >= Duration::from_secs(3), "stopped too early");
        assert!(elapsed < Duration::from_secs(4), "waited past the budget");
    }

    /// A descriptor shortens the global policy; it never extends it.
    #[tokio::test(start_paused = true)]
    async fn descriptor_shortens_the_budget() {
        let unit: Arc<dyn Runnable> = Arc::new(Deaf(
            essential()
                .drain_timeout(Duration::from_millis(100))
                .stop_timeout(Duration::from_millis(200)),
        ));
        // A global policy two orders of magnitude longer than the descriptor's.
        let (supervisor, controller, handle) = started(vec![unit], ShutdownPolicy::default());

        handle.shutdown();
        let started_at = Instant::now();
        let errors = supervisor.stop(&controller).await;
        let elapsed = Instant::now() - started_at;

        assert!(matches!(errors[0].kind(), RunErrorKind::DeadlineExceeded));
        assert!(elapsed >= Duration::from_millis(300));
        assert!(
            elapsed < Duration::from_millis(400),
            "took the global budget"
        );
    }

    /// And it never extends it: a descriptor asking for more than the policy
    /// allows is still abandoned at the global deadline.
    #[tokio::test(start_paused = true)]
    async fn descriptor_never_extends_budget() {
        let unit: Arc<dyn Runnable> = Arc::new(Deaf(
            essential()
                .drain_timeout(Duration::from_secs(600))
                .stop_timeout(Duration::from_secs(600)),
        ));
        let (supervisor, controller, handle) = started(vec![unit], brief());

        handle.shutdown();
        let started_at = Instant::now();
        let errors = supervisor.stop(&controller).await;
        let elapsed = Instant::now() - started_at;

        assert!(matches!(errors[0].kind(), RunErrorKind::DeadlineExceeded));
        assert!(elapsed < Duration::from_secs(4));
    }

    /// A runnable that yields at the first rung does not make the kernel sit
    /// out the second one.
    #[tokio::test(start_paused = true)]
    async fn clean_stop_returns_early() {
        let unit: Arc<dyn Runnable> = Arc::new(Brisk(essential()));
        let (supervisor, controller, handle) = started(vec![unit], brief());

        handle.shutdown();
        let started_at = Instant::now();
        let errors = supervisor.stop(&controller).await;
        let elapsed = Instant::now() - started_at;

        assert!(errors.is_empty());
        assert!(elapsed < Duration::from_millis(100));
        assert_eq!(controller.stage(), Stage::Stopping);
    }

    /// Both rungs are climbed, in order, and each one is offered before the
    /// next: a runnable that only ever watches `stopping` still gets there.
    #[tokio::test(start_paused = true)]
    async fn stop_climbs_both_rungs() {
        let unit: Arc<dyn Runnable> = Flaky::new(essential(), 0);
        let (supervisor, controller, handle) = started(vec![unit], brief());

        handle.shutdown();
        let started_at = Instant::now();
        let errors = supervisor.stop(&controller).await;
        let elapsed = Instant::now() - started_at;

        assert!(errors.is_empty());
        // It waited out the drain budget, then returned as soon as stopping
        // was announced rather than sitting out the stop budget too.
        assert!(elapsed >= Duration::from_secs(1));
        assert!(elapsed < Duration::from_secs(2));
    }

    /// A restart still pending when the stop begins is dropped rather than
    /// started, and does not delay the ladder.
    #[tokio::test(start_paused = true)]
    async fn pending_restart_is_dropped() {
        let flaky = Flaky::new(
            ancillary().restart(RestartPolicy::on_failure(
                5,
                Backoff::Fixed(Duration::from_secs(30)),
            )),
            usize::MAX,
        );
        let (supervisor, controller, handle) =
            started(vec![Arc::clone(&flaky) as Arc<dyn Runnable>], brief());

        // Let the first run fail so a restart is sleeping out its backoff.
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.shutdown();

        let started_at = Instant::now();
        let errors = supervisor.stop(&controller).await;
        let elapsed = Instant::now() - started_at;

        assert!(elapsed < Duration::from_secs(2), "waited on the backoff");
        assert!(
            !errors
                .iter()
                .any(|error| matches!(error.kind(), RunErrorKind::DeadlineExceeded))
        );
        assert_eq!(flaky.starts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn debug_names_the_runnables() {
        let unit: Arc<dyn Runnable> = Flaky::new(essential(), 0);
        let (supervisor, _controller, _handle) = started(vec![unit], brief());

        let rendered = format!("{supervisor:?}");

        assert!(rendered.contains("alpha"));
        assert!(rendered.contains("live"));
    }
}
