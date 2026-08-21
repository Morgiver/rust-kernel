//! The two-stage shutdown ladder and the handle that asks for it.
//!
//! ```text
//! Running -> Draining -> Stopping -> Stopped
//! ```
//!
//! * `Draining` — new work is refused, work in flight continues.
//! * `Stopping` — work in flight must end, and the clock is running.
//!
//! Two stages and not one, because without the separation a clean stop is
//! impossible: a runnable cannot both refuse new work and finish old work if it
//! only ever gets a single signal. One signal forces the implementor to pick
//! which of the two meanings it carries, and whichever it picks the other one
//! is lost.
//!
//! [`ShutdownController`] is the writing end, held by whoever drives the
//! lifecycle; [`Shutdown`] is the reading end, cloned into every unit. The
//! ladder only ever moves forward — [`begin_draining`](ShutdownController::begin_draining)
//! after [`begin_stopping`](ShutdownController::begin_stopping) is a no-op, never
//! a regression — and a waiter that arrives after its stage has passed resolves
//! immediately rather than waiting for a transition that will never come again.
//!
//! [`KernelHandle`] is the other direction: it does not observe the ladder, it
//! *asks* for it. It is clonable and resolvable from the container, so any unit
//! can request a stop without holding the lifecycle driver.

use core::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use kernel_core::{ShutdownPolicy, Stage};
use tokio::sync::watch;

/// Reads a mutex that only ever guards a `Copy` value.
///
/// A panic while the guard is held cannot leave the value half-written, so the
/// poison flag carries no information here and is deliberately discarded.
fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The shared state behind one controller and every watcher cloned from it.
struct Ladder {
    /// Current stage. The watch channel is what makes a late waiter cheap: the
    /// value is readable without waiting for the next transition.
    stage: watch::Sender<Stage>,
    /// Budgets the stages are given.
    policy: ShutdownPolicy,
    /// Instant the current stage must end by; `None` when the stage is untimed.
    deadline: Mutex<Option<Instant>>,
}

impl Ladder {
    fn new(policy: ShutdownPolicy) -> Self {
        Self {
            stage: watch::Sender::new(Stage::Running),
            policy,
            deadline: Mutex::new(None),
        }
    }

    fn stage(&self) -> Stage {
        *self.stage.borrow()
    }

    fn deadline(&self) -> Option<Instant> {
        *guard(&self.deadline)
    }

    /// Move to `target` unless the ladder is already there or beyond.
    ///
    /// The comparison and the write happen under the channel's own lock, so two
    /// concurrent callers can never leave the stage and its deadline
    /// disagreeing about where the ladder is.
    fn advance(&self, target: Stage) {
        let policy = self.policy;
        self.stage.send_if_modified(|current| {
            if *current >= target {
                return false;
            }
            *current = target;
            *guard(&self.deadline) = policy
                .budget(target)
                .map(|budget| (tokio::time::Instant::now() + budget).into_std());
            true
        });
    }

    /// Resolve once the ladder has reached `target` — or passed it.
    async fn reached(&self, target: Stage) {
        let mut receiver = self.stage.subscribe();
        loop {
            if *receiver.borrow_and_update() >= target {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

/// The reading end of the shutdown ladder, cloned into every unit.
///
/// Cheap to clone: every clone reads the same ladder.
///
/// # Examples
///
/// ```
/// use kernel::ShutdownController;
/// use kernel::core::{ShutdownPolicy, Stage};
///
/// let (controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());
///
/// assert_eq!(shutdown.stage(), Stage::Running);
/// assert!(!shutdown.is_shutting_down());
/// assert_eq!(shutdown.deadline(), None);
///
/// controller.begin_draining();
/// assert_eq!(shutdown.stage(), Stage::Draining);
/// assert!(shutdown.is_shutting_down());
/// ```
///
/// Awaiting a stage:
///
/// ```
/// # async fn work(shutdown: kernel::Shutdown) {
/// shutdown.draining().await;
/// // stop taking new work here, then finish what is in flight
/// shutdown.stopping().await;
/// # }
/// ```
#[derive(Clone)]
pub struct Shutdown {
    ladder: Arc<Ladder>,
}

impl Shutdown {
    /// The stage the ladder is on right now.
    #[must_use]
    pub fn stage(&self) -> Stage {
        self.ladder.stage()
    }

    /// Whether the ladder has been entered at all, i.e. anything but
    /// [`Stage::Running`].
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.stage().is_shutting_down()
    }

    /// The budgets this ladder was built with.
    #[must_use]
    pub fn policy(&self) -> ShutdownPolicy {
        self.ladder.policy
    }

    /// The instant the current stage must end by.
    ///
    /// `None` while [`Stage::Running`], and `None` again once
    /// [`Stage::Stopped`]: neither stage is timed. Between the two it is the
    /// start of the stage plus that stage's budget.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.ladder.deadline()
    }

    /// Resolves when draining starts, or immediately if it already has.
    ///
    /// "Already has" includes stages past draining: a ladder that jumped
    /// straight to [`Stage::Stopping`] has passed draining, so a waiter that
    /// arrives late is released rather than parked forever.
    pub async fn draining(&self) {
        self.ladder.reached(Stage::Draining).await;
    }

    /// Resolves when stopping starts, or immediately if it already has.
    pub async fn stopping(&self) {
        self.ladder.reached(Stage::Stopping).await;
    }
}

impl fmt::Debug for Shutdown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shutdown")
            .field("stage", &self.stage())
            .field("deadline", &self.deadline().is_some())
            .finish_non_exhaustive()
    }
}

/// The writing end of the shutdown ladder.
///
/// Held by whoever drives the lifecycle. Every transition is monotonic: a
/// transition to a stage that has already been reached or passed does nothing
/// at all, so an out-of-order caller cannot pull a unit back into a stage it has
/// already left.
///
/// # Examples
///
/// ```
/// use kernel::ShutdownController;
/// use kernel::core::{ShutdownPolicy, Stage};
///
/// let (controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());
///
/// controller.begin_stopping();
/// controller.begin_draining(); // no-op: the ladder never climbs back down
///
/// assert_eq!(shutdown.stage(), Stage::Stopping);
/// ```
pub struct ShutdownController {
    ladder: Arc<Ladder>,
}

impl ShutdownController {
    /// A fresh ladder on [`Stage::Running`], with its first watcher.
    ///
    /// Further watchers come from [`watcher`](Self::watcher) or from cloning
    /// the one returned here.
    #[must_use]
    pub fn new(policy: ShutdownPolicy) -> (Self, Shutdown) {
        let ladder = Arc::new(Ladder::new(policy));
        let watcher = Shutdown {
            ladder: Arc::clone(&ladder),
        };
        (Self { ladder }, watcher)
    }

    /// Enter [`Stage::Draining`]: refuse new work, let work in flight continue.
    ///
    /// A no-op once the ladder is at draining or beyond.
    pub fn begin_draining(&self) {
        self.ladder.advance(Stage::Draining);
    }

    /// Enter [`Stage::Stopping`]: work in flight must end, deadline running.
    ///
    /// A no-op once the ladder is at stopping or beyond. Called from
    /// [`Stage::Running`] it skips draining entirely, which still releases
    /// every waiter on [`Shutdown::draining`] — the stage has been passed.
    pub fn begin_stopping(&self) {
        self.ladder.advance(Stage::Stopping);
    }

    /// Enter [`Stage::Stopped`]: nothing is left to run, no stage is timed.
    pub fn finish(&self) {
        self.ladder.advance(Stage::Stopped);
    }

    /// The stage the ladder is on right now.
    #[must_use]
    pub fn stage(&self) -> Stage {
        self.ladder.stage()
    }

    /// Another reader of the same ladder.
    #[must_use]
    pub fn watcher(&self) -> Shutdown {
        Shutdown {
            ladder: Arc::clone(&self.ladder),
        }
    }
}

impl fmt::Debug for ShutdownController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownController")
            .field("stage", &self.stage())
            .finish_non_exhaustive()
    }
}

/// A clonable request to stop, resolvable from the container.
///
/// This is the trigger, not the ladder: `shutdown` records that someone asked,
/// and the lifecycle driver is what turns that request into the transitions of
/// a [`ShutdownController`]. Keeping the two apart is what lets any unit ask for
/// a stop without being handed the authority to drive one.
///
/// # Examples
///
/// ```
/// use kernel::KernelHandle;
///
/// let handle = KernelHandle::detached();
/// assert!(!handle.is_shutting_down());
///
/// handle.clone().shutdown();
/// handle.shutdown(); // idempotent
/// assert!(handle.is_shutting_down());
/// ```
#[derive(Clone)]
pub struct KernelHandle {
    requested: Arc<watch::Sender<bool>>,
}

impl KernelHandle {
    /// A handle nobody has asked anything of yet.
    pub(crate) fn new() -> Self {
        Self {
            requested: Arc::new(watch::Sender::new(false)),
        }
    }

    /// Ask the kernel to stop.
    ///
    /// Idempotent: the second and every later call observe a request that has
    /// already been made and change nothing.
    pub fn shutdown(&self) {
        self.requested.send_if_modified(|asked| {
            if *asked {
                return false;
            }
            *asked = true;
            true
        });
    }

    /// Whether someone has already asked the kernel to stop.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        *self.requested.borrow()
    }

    /// Resolves when someone has asked the kernel to stop.
    ///
    /// Resolves immediately if the request was made before this call, so a
    /// waiter that arrives late is never parked forever.
    pub async fn requested(&self) {
        let mut receiver = self.requested.subscribe();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }

    /// A handle with no kernel behind it, for testing a unit in isolation.
    ///
    /// It records requests exactly like a real one; what it lacks is a
    /// lifecycle driver watching [`requested`](Self::requested), so nothing
    /// happens when it fires.
    #[must_use]
    pub fn detached() -> Self {
        Self::new()
    }
}

impl fmt::Debug for KernelHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelHandle")
            .field("requested", &self.is_shutting_down())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;
    use tokio::time::{Instant as TokioInstant, sleep, timeout};

    fn policy() -> ShutdownPolicy {
        ShutdownPolicy::new(Duration::from_secs(3), Duration::from_secs(7))
    }

    #[test]
    fn starts_running() {
        let (controller, shutdown) = ShutdownController::new(policy());

        assert_eq!(controller.stage(), Stage::Running);
        assert_eq!(shutdown.stage(), Stage::Running);
        assert!(!shutdown.is_shutting_down());
        assert_eq!(shutdown.deadline(), None);
    }

    #[test]
    fn climbs_in_order() {
        let (controller, shutdown) = ShutdownController::new(policy());

        controller.begin_draining();
        assert_eq!(shutdown.stage(), Stage::Draining);
        controller.begin_stopping();
        assert_eq!(shutdown.stage(), Stage::Stopping);
        controller.finish();
        assert_eq!(shutdown.stage(), Stage::Stopped);
    }

    #[test]
    fn never_moves_backward() {
        let (controller, shutdown) = ShutdownController::new(policy());

        controller.begin_stopping();
        controller.begin_draining();
        assert_eq!(shutdown.stage(), Stage::Stopping);

        controller.finish();
        controller.begin_draining();
        controller.begin_stopping();
        assert_eq!(shutdown.stage(), Stage::Stopped);
    }

    #[test]
    fn watchers_share_ladder() {
        let (controller, first) = ShutdownController::new(policy());
        let second = controller.watcher();
        let third = first.clone();

        controller.begin_draining();

        assert_eq!(first.stage(), Stage::Draining);
        assert_eq!(second.stage(), Stage::Draining);
        assert_eq!(third.stage(), Stage::Draining);
        assert_eq!(second.policy(), policy());
    }

    #[tokio::test]
    async fn wakes_on_transition() {
        let (controller, shutdown) = ShutdownController::new(policy());
        let waiter = shutdown.clone();
        let task = tokio::spawn(async move {
            waiter.draining().await;
            waiter.stage()
        });

        // Let the waiter park before anything moves.
        tokio::task::yield_now().await;
        controller.begin_draining();

        assert_eq!(task.await.expect("waiter panicked"), Stage::Draining);
    }

    #[tokio::test]
    async fn resolves_when_passed() {
        let (controller, shutdown) = ShutdownController::new(policy());
        controller.begin_stopping();

        // Both stages are behind the ladder; neither waiter may park.
        shutdown.draining().await;
        shutdown.stopping().await;
    }

    #[tokio::test]
    async fn resolves_after_finish() {
        let (controller, shutdown) = ShutdownController::new(policy());
        controller.finish();

        shutdown.draining().await;
        shutdown.stopping().await;
    }

    #[tokio::test(start_paused = true)]
    async fn pends_while_running() {
        let (_controller, shutdown) = ShutdownController::new(policy());

        let waited = timeout(Duration::from_secs(600), shutdown.draining()).await;
        assert!(waited.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_follows_stage() {
        let (controller, shutdown) = ShutdownController::new(policy());
        let start = TokioInstant::now();

        controller.begin_draining();
        assert_eq!(
            shutdown.deadline(),
            Some((start + policy().drain).into_std())
        );

        sleep(Duration::from_secs(1)).await;
        let middle = TokioInstant::now();
        controller.begin_stopping();
        assert_eq!(
            shutdown.deadline(),
            Some((middle + policy().stop).into_std())
        );

        controller.finish();
        assert_eq!(shutdown.deadline(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_ignores_noop() {
        let (controller, shutdown) = ShutdownController::new(policy());

        controller.begin_stopping();
        let pinned = shutdown.deadline();

        sleep(Duration::from_secs(1)).await;
        controller.begin_draining();

        assert_eq!(shutdown.deadline(), pinned);
    }

    #[test]
    fn handle_is_idempotent() {
        let handle = KernelHandle::detached();

        assert!(!handle.is_shutting_down());
        handle.shutdown();
        handle.shutdown();
        assert!(handle.is_shutting_down());
    }

    #[test]
    fn handle_clones_share() {
        let handle = KernelHandle::detached();
        let other = handle.clone();

        other.shutdown();

        assert!(handle.is_shutting_down());
    }

    #[test]
    fn handles_are_independent() {
        let handle = KernelHandle::detached();
        KernelHandle::detached().shutdown();

        assert!(!handle.is_shutting_down());
    }

    #[tokio::test]
    async fn handle_wakes_waiter() {
        let handle = KernelHandle::detached();
        let waiter = handle.clone();
        let task = tokio::spawn(async move { waiter.requested().await });

        tokio::task::yield_now().await;
        handle.shutdown();

        task.await.expect("waiter panicked");
    }

    #[tokio::test]
    async fn handle_resolves_late() {
        let handle = KernelHandle::detached();
        handle.shutdown();

        handle.requested().await;
    }

    #[tokio::test(start_paused = true)]
    async fn handle_pends_unasked() {
        let handle = KernelHandle::detached();

        let waited = timeout(Duration::from_secs(600), handle.requested()).await;
        assert!(waited.is_err());
    }

    #[test]
    fn types_are_shareable() {
        fn assert_shared<T: Send + Sync + 'static>() {}

        assert_shared::<Shutdown>();
        assert_shared::<ShutdownController>();
        assert_shared::<KernelHandle>();
    }
}
