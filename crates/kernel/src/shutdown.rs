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
//! [`KernelHandle`] is the other direction: it does not drive the ladder, it
//! *asks* for it. It is clonable and resolvable from the container, so any unit
//! can request a stop without holding the lifecycle driver. It reads the ladder
//! too, once a driver has [`attach`](ShutdownController::attach)ed one: a unit
//! that reaches no further than the handle still tells `Draining` from
//! `Stopping`, which one boolean cannot say.

use core::fmt;
use core::time::Duration;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
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

/// When the current stage began and when it must end.
///
/// One cell rather than two, so that no reader can ever pair the start of one
/// stage with the deadline of the next: a unit that owns a budget of its own
/// counts it from the start, and a start half a stage out of date is a budget
/// out of date.
#[derive(Clone, Copy)]
struct Bounds {
    /// Instant the current stage was entered; `None` while nothing has moved.
    entered: Option<Instant>,
    /// Instant the current stage must end by; `None` when the stage is untimed.
    deadline: Option<Instant>,
}

/// The shared state behind one controller and every watcher cloned from it.
struct Ladder {
    /// Current stage. The watch channel is what makes a late waiter cheap: the
    /// value is readable without waiting for the next transition.
    stage: watch::Sender<Stage>,
    /// Budgets the stages are given.
    policy: ShutdownPolicy,
    /// When the current stage began, and when it must end.
    bounds: Mutex<Bounds>,
}

impl Ladder {
    fn new(policy: ShutdownPolicy) -> Self {
        Self {
            stage: watch::Sender::new(Stage::Running),
            policy,
            bounds: Mutex::new(Bounds {
                entered: None,
                deadline: None,
            }),
        }
    }

    fn stage(&self) -> Stage {
        *self.stage.borrow()
    }

    fn deadline(&self) -> Option<Instant> {
        guard(&self.bounds).deadline
    }

    fn entered(&self) -> Option<Instant> {
        guard(&self.bounds).entered
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
            let now = tokio::time::Instant::now();
            *guard(&self.bounds) = Bounds {
                entered: Some(now.into_std()),
                deadline: policy
                    .budget(target)
                    .map(|budget| (now + budget).into_std()),
            };
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

    /// The instant the current stage must end by.
    ///
    /// `None` while [`Stage::Running`], and `None` again once
    /// [`Stage::Stopped`]: neither stage is timed. Between the two it is the
    /// start of the stage plus that stage's budget.
    ///
    /// # `None` is not "unbounded"
    ///
    /// An untimed stage grants no grace, it does not grant endless grace —
    /// the same rule the supervisor applies when it ends a stage nobody
    /// budgeted. A reader that writes `if let Some(deadline) = ..` and leaves
    /// the `None` arm empty has written a cut that silently never happens,
    /// which is the one shape this accessor invites and cannot prevent.
    /// [`sleep_until_deadline`](Self::sleep_until_deadline) is this value with
    /// the `None` arm already decided, and with the conversion to the
    /// runtime's own clock already done.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.ladder.deadline()
    }

    /// The instant the current stage began.
    ///
    /// `None` until something moves. What a unit holding a budget of its own
    /// counts from; kept crate-private because a budget counted from here by
    /// hand is the arithmetic
    /// [`RunContext::deadline`](crate::runnable::RunContext::deadline) exists
    /// to do once.
    pub(crate) fn entered(&self) -> Option<Instant> {
        self.ladder.entered()
    }

    /// Makes `handle` report this ladder's stage, unless a ladder is already
    /// bound to it.
    ///
    /// Crate-private, and deliberately not on the reading end's public
    /// surface: the public verb is [`ShutdownController::attach`], held by
    /// whoever drives the lifecycle. A watcher can bind a handle it was handed
    /// beside — which is what a run context does — but nothing outside the
    /// kernel gets to decide which ladder a handle speaks for.
    pub(crate) fn attach(&self, handle: &KernelHandle) {
        handle.attach(&self.ladder);
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

    /// Waits `period`, or returns early once draining starts.
    ///
    /// The wait every periodic unit needs: it does its work, waits for the
    /// next turn, and stops waiting the moment the ladder moves. Without it
    /// each such unit writes the same race between a timer and the token by
    /// hand, and takes a dependency on a runtime's timer to do it.
    ///
    /// Draining means *stop taking new work*, and a new turn of a loop is new
    /// work, which is why this is the wait a poller wants. A unit that must
    /// keep working through the draining stage waits with
    /// [`sleep_until_stopping`](Self::sleep_until_stopping) instead — calling
    /// this one again after it reported draining returns immediately every
    /// time, because the stage does not un-happen.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::time::Duration;
    /// # use kernel::Shutdown;
    /// # async fn poll(shutdown: Shutdown, every: Duration) {
    /// while shutdown.sleep_until_draining(every).await.is_elapsed() {
    ///     // one turn of the loop
    /// }
    /// # }
    /// ```
    pub async fn sleep_until_draining(&self, period: Duration) -> Tick {
        self.bounded(Stage::Draining, period).await
    }

    /// Waits `period`, or returns early once stopping starts.
    ///
    /// The same wait as [`sleep_until_draining`](Self::sleep_until_draining),
    /// one rung higher: draining leaves it waiting, so a unit that must keep
    /// working while the process refuses new work still gets its turns.
    pub async fn sleep_until_stopping(&self, period: Duration) -> Tick {
        self.bounded(Stage::Stopping, period).await
    }

    /// Waits until the current stage's deadline, and says whether there was
    /// one.
    ///
    /// The wait a unit that must cut something off needs, written once. It
    /// differs from `sleep_until(shutdown.deadline()?)` in three ways, and
    /// every one of them is a defect somebody hand-rolled:
    ///
    /// 1. **An untimed stage answers immediately.** A stage with no deadline
    ///    grants no grace rather than endless grace, so the wait returns
    ///    [`Budget::Untimed`] at once and the caller cuts. The hand-rolled
    ///    form skips the wait *and* skips the cut, and the work it was meant
    ///    to bound runs on unbounded.
    /// 2. **It follows the ladder.** A deadline read once is the deadline of
    ///    the stage that was current when it was read; when the ladder climbs
    ///    a rung mid-wait, this wait picks the new stage's deadline up rather
    ///    than firing at the old one.
    /// 3. **It converts.** [`deadline`](Self::deadline) is a
    ///    [`std::time::Instant`], and sleeping to it needs the runtime's own
    ///    clock — a conversion that is invisible under a paused clock until a
    ///    test hangs.
    ///
    /// # Examples
    ///
    /// ```
    /// # use kernel::{Budget, ShutdownController};
    /// # use kernel::core::ShutdownPolicy;
    /// # async fn probe() {
    /// let (controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());
    /// controller.begin_stopping();
    ///
    /// if shutdown.sleep_until_deadline().await == Budget::Spent {
    ///     // whatever is still in flight is over budget: cut it
    /// }
    /// # }
    /// ```
    pub async fn sleep_until_deadline(&self) -> Budget {
        let mut receiver = self.ladder.stage.subscribe();
        loop {
            let Some(deadline) = self.deadline() else {
                return Budget::Untimed;
            };
            let until = tokio::time::Instant::from_std(deadline);

            // Biased so that a deadline already past wins over a transition
            // that is also ready: the answer must not depend on which of two
            // ready branches the runtime happens to poll first.
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(until) => return Budget::Spent,
                changed = receiver.changed() => {
                    if changed.is_err() {
                        // The ladder can no longer move, so the deadline in
                        // hand is final. Waiting it out beats looping on a
                        // channel that will report the same failure forever.
                        tokio::time::sleep_until(until).await;
                        return Budget::Spent;
                    }
                }
            }
        }
    }

    /// The wait both public forms are made of.
    ///
    /// Biased so that a ladder already at or past `target` wins over a period
    /// that is also ready: the answer must not depend on which of two ready
    /// branches the runtime happens to poll first.
    async fn bounded(&self, target: Stage, period: Duration) -> Tick {
        tokio::select! {
            biased;
            () = self.ladder.reached(target) => Tick::Interrupted(self.stage()),
            () = tokio::time::sleep(period) => Tick::Elapsed,
        }
    }
}

/// How a bounded wait ended.
///
/// Returned by [`Shutdown::sleep_until_draining`] and
/// [`Shutdown::sleep_until_stopping`], which is what makes the wait usable as
/// a loop condition: the caller reads the reason it woke up rather than
/// re-reading the stage and guessing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tick {
    /// The whole period passed with the awaited stage still ahead.
    Elapsed,
    /// The wait was cut short: the ladder had reached the awaited stage, or
    /// passed it. The stage carried is the one the ladder is on, which is not
    /// always the one that was awaited — a ladder that jumped straight to
    /// [`Stage::Stopping`] releases a wait on draining.
    Interrupted(Stage),
}

impl Tick {
    /// Whether the period passed rather than being cut short.
    #[must_use]
    pub fn is_elapsed(self) -> bool {
        matches!(self, Self::Elapsed)
    }

    /// The stage that cut the wait short, or `None` if the period passed.
    #[must_use]
    pub fn stage(self) -> Option<Stage> {
        match self {
            Self::Elapsed => None,
            Self::Interrupted(stage) => Some(stage),
        }
    }
}

/// Whether a stage had a budget, and whether it is gone.
///
/// Returned by [`Shutdown::sleep_until_deadline`]. Two variants and not a
/// `bool`, because the two answers ask for the same thing for opposite
/// reasons: `Spent` says the grace this stage granted has run out, `Untimed`
/// says it granted none — and a caller that cannot tell them apart cannot
/// report which of the two cut its work off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Budget {
    /// The stage's deadline has passed. Whatever is still in flight is over
    /// budget.
    Spent,
    /// The stage carries no deadline: [`Stage::Running`] because no stop has
    /// begun, [`Stage::Stopped`] because everything is already over. Nothing
    /// was waited for.
    Untimed,
}

impl Budget {
    /// Whether a budget existed and has run out.
    #[must_use]
    pub fn is_spent(self) -> bool {
        matches!(self, Self::Spent)
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

    /// Makes `handle` report this ladder's stage.
    ///
    /// The handle is the only end of the lifecycle an arbitrary unit can
    /// resolve, and on its own it carries one boolean. Attaching is what lets
    /// that unit — or a test, or an application holding nothing else — tell
    /// [`Stage::Draining`] from [`Stage::Stopping`] instead of guessing which
    /// of the two a `true` meant.
    ///
    /// The first ladder attached is the one the handle reports, and a later
    /// call changes nothing: a process runs one lifecycle, and the second
    /// ladder a shutdown walk opens for its own units must not redefine what
    /// the whole process reports.
    ///
    /// # Examples
    ///
    /// ```
    /// use kernel::{KernelHandle, ShutdownController};
    /// use kernel::core::{ShutdownPolicy, Stage};
    ///
    /// let (controller, _shutdown) = ShutdownController::new(ShutdownPolicy::default());
    /// let handle = KernelHandle::detached();
    /// controller.attach(&handle);
    ///
    /// controller.begin_draining();
    /// assert_eq!(handle.stage(), Stage::Draining);
    /// ```
    pub fn attach(&self, handle: &KernelHandle) {
        handle.attach(&self.ladder);
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
    /// The ladder this handle reports, once a driver has attached one. Empty
    /// until then, and never replaced afterwards.
    ladder: Arc<OnceLock<Arc<Ladder>>>,
}

impl KernelHandle {
    /// A handle nobody has asked anything of yet.
    pub(crate) fn new() -> Self {
        Self {
            requested: Arc::new(watch::Sender::new(false)),
            ladder: Arc::new(OnceLock::new()),
        }
    }

    /// Binds this handle to `ladder`, unless one is already bound.
    ///
    /// Private to this module, because `Ladder` is: the verbs that reach it
    /// from outside are [`ShutdownController::attach`], held by whoever drives
    /// the lifecycle, and `Shutdown::attach` beside it.
    fn attach(&self, ladder: &Arc<Ladder>) {
        let _ = self.ladder.set(Arc::clone(ladder));
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
    ///
    /// This is the REQUEST, not the ladder: it is `true` because
    /// [`shutdown`](Self::shutdown) was called, and it stays `false` for a
    /// stop that began some other way — a signal, an essential runnable
    /// returning — however far the ladder has since climbed. What a unit
    /// deciding whether to keep taking work should read is
    /// [`stage`](Self::stage).
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        *self.requested.borrow()
    }

    /// The stage the kernel's ladder is on.
    ///
    /// The whole ladder, not the boolean beside it: `Draining` means new work
    /// is refused while work in flight continues, and `Stopping` means the
    /// clock is running — two instructions a unit cannot act on differently if
    /// all it can read is that a stop happened.
    ///
    /// [`Stage::Running`] until a lifecycle driver has
    /// [`attach`](ShutdownController::attach)ed its ladder, which is also the
    /// honest answer for a handle with no kernel behind it: nothing has moved
    /// because there is nothing to move. Asking for a stop does not move it
    /// either — [`shutdown`](Self::shutdown) records a request, and the driver
    /// is what turns that request into a transition.
    ///
    /// # Examples
    ///
    /// ```
    /// use kernel::KernelHandle;
    /// use kernel::core::Stage;
    ///
    /// let handle = KernelHandle::detached();
    /// handle.shutdown();
    ///
    /// // Asked for, not yet begun: the request is recorded, the ladder is not
    /// // this handle's to move.
    /// assert!(handle.is_shutting_down());
    /// assert_eq!(handle.stage(), Stage::Running);
    /// ```
    #[must_use]
    pub fn stage(&self) -> Stage {
        self.ladder
            .get()
            .map_or(Stage::Running, |ladder| ladder.stage())
    }

    /// The instant the current stage must end by, once a ladder is attached.
    ///
    /// `None` when no ladder is attached, and `None` on an untimed stage —
    /// which does not mean unbounded; see [`Shutdown::deadline`].
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.ladder.get().and_then(|ladder| ladder.deadline())
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
            .field("stage", &self.stage())
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

    #[tokio::test(start_paused = true)]
    async fn sleep_elapses_quietly() {
        let (_controller, shutdown) = ShutdownController::new(policy());
        let started = TokioInstant::now();

        let tick = shutdown.sleep_until_draining(Duration::from_secs(60)).await;

        assert_eq!(tick, Tick::Elapsed);
        assert!(tick.is_elapsed());
        assert_eq!(tick.stage(), None);
        assert!(started.elapsed() >= Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn draining_cuts_sleep() {
        let (controller, shutdown) = ShutdownController::new(policy());
        let waiter = shutdown.clone();
        let task = tokio::spawn(async move { waiter.sleep_until_draining(Duration::MAX).await });

        tokio::task::yield_now().await;
        controller.begin_draining();

        let tick = timeout(Duration::from_secs(1), task)
            .await
            .expect("the wait must end with the stage")
            .expect("waiter panicked");

        assert_eq!(tick, Tick::Interrupted(Stage::Draining));
        assert!(!tick.is_elapsed());
        assert_eq!(tick.stage(), Some(Stage::Draining));
    }

    // The rung matters: a unit that must finish what it holds keeps its turns
    // through draining and loses them only at stopping.
    #[tokio::test(start_paused = true)]
    async fn draining_keeps_stopping_sleep() {
        let (controller, shutdown) = ShutdownController::new(policy());
        controller.begin_draining();

        let elapsed = shutdown.sleep_until_stopping(Duration::from_secs(1)).await;
        controller.begin_stopping();
        let cut = shutdown
            .sleep_until_stopping(Duration::from_secs(600))
            .await;

        assert_eq!(elapsed, Tick::Elapsed);
        assert_eq!(cut, Tick::Interrupted(Stage::Stopping));
    }

    // A stage already passed wins over a period that is also ready, so the
    // answer does not depend on which branch the runtime polls first.
    #[tokio::test(start_paused = true)]
    async fn passed_stage_wins() {
        let (controller, shutdown) = ShutdownController::new(policy());
        controller.begin_stopping();

        let tick = shutdown.sleep_until_draining(Duration::ZERO).await;

        assert_eq!(tick, Tick::Interrupted(Stage::Stopping));
    }

    // The pattern section 14 asks every periodic unit to write, written once:
    // the loop ends because the ladder moved, not because a timer fired.
    #[tokio::test(start_paused = true)]
    async fn loop_ends_on_stage() {
        let (controller, shutdown) = ShutdownController::new(policy());
        let waiter = shutdown.clone();
        let task = tokio::spawn(async move {
            let mut turns = 0_u32;
            while waiter
                .sleep_until_draining(Duration::from_secs(1))
                .await
                .is_elapsed()
            {
                turns += 1;
            }
            turns
        });

        sleep(Duration::from_millis(3_500)).await;
        controller.begin_draining();

        let turns = timeout(Duration::from_secs(1), task)
            .await
            .expect("the loop must end")
            .expect("waiter panicked");

        assert!(turns >= 3, "{turns}");
    }

    // ----------------------------------------------------------------------
    // The stage, read from the handle
    // ----------------------------------------------------------------------

    #[test]
    fn handle_reads_stage() {
        let (controller, _shutdown) = ShutdownController::new(policy());
        let handle = KernelHandle::detached();
        controller.attach(&handle);

        assert_eq!(handle.stage(), Stage::Running);
        controller.begin_draining();
        assert_eq!(handle.stage(), Stage::Draining);
        // The rung a boolean cannot tell from the one above it.
        controller.begin_stopping();
        assert_eq!(handle.stage(), Stage::Stopping);
        controller.finish();
        assert_eq!(handle.stage(), Stage::Stopped);
    }

    #[test]
    fn attached_handle_clones() {
        let (controller, _shutdown) = ShutdownController::new(policy());
        let handle = KernelHandle::detached();
        let clone = handle.clone();
        controller.attach(&handle);
        controller.begin_stopping();

        // Attaching one clone attaches every clone: they are one handle.
        assert_eq!(clone.stage(), Stage::Stopping);
        assert!(format!("{handle:?}").contains("Stopping"));
    }

    #[test]
    fn handle_keeps_first_ladder() {
        let (first, _one) = ShutdownController::new(policy());
        let (second, _two) = ShutdownController::new(policy());
        let handle = KernelHandle::detached();

        first.attach(&handle);
        second.attach(&handle);
        second.begin_stopping();

        // The second ladder a shutdown walk opens does not get to redefine
        // what the whole process reports.
        assert_eq!(handle.stage(), Stage::Running);
        first.begin_draining();
        assert_eq!(handle.stage(), Stage::Draining);
    }

    #[test]
    fn request_is_not_a_stage() {
        let handle = KernelHandle::detached();
        handle.shutdown();

        assert!(handle.is_shutting_down());
        assert_eq!(handle.stage(), Stage::Running);
        assert_eq!(handle.deadline(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn handle_reads_deadline() {
        let (controller, shutdown) = ShutdownController::new(policy());
        let handle = KernelHandle::detached();
        controller.attach(&handle);

        controller.begin_stopping();

        assert_eq!(handle.deadline(), shutdown.deadline());
        assert!(handle.deadline().is_some());
    }

    // ----------------------------------------------------------------------
    // Waiting out the stage's own deadline
    // ----------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn deadline_wait_spends() {
        let (controller, shutdown) = ShutdownController::new(policy());
        controller.begin_stopping();
        let started = TokioInstant::now();

        let budget = shutdown.sleep_until_deadline().await;

        assert_eq!(budget, Budget::Spent);
        assert!(budget.is_spent());
        assert!(started.elapsed() >= policy().stop);
    }

    // An untimed stage grants no grace rather than endless grace: the caller
    // is told at once, so the cut it was going to make still happens.
    #[tokio::test(start_paused = true)]
    async fn untimed_answers_now() {
        let (_controller, shutdown) = ShutdownController::new(policy());
        let started = TokioInstant::now();

        let budget = timeout(Duration::from_secs(600), shutdown.sleep_until_deadline())
            .await
            .expect("an untimed stage must not park the caller");

        assert_eq!(budget, Budget::Untimed);
        assert!(!budget.is_spent());
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    // A deadline read once belongs to the stage that was current when it was
    // read. This wait belongs to the stage that is current when it fires.
    #[tokio::test(start_paused = true)]
    async fn wait_follows_ladder() {
        let (controller, shutdown) = ShutdownController::new(policy());
        controller.begin_draining();
        let started = TokioInstant::now();
        let waiter = shutdown.clone();
        let task = tokio::spawn(async move { waiter.sleep_until_deadline().await });

        tokio::task::yield_now().await;
        sleep(Duration::from_secs(1)).await;
        controller.begin_stopping();

        let budget = task.await.expect("waiter panicked");

        assert_eq!(budget, Budget::Spent);
        // Not the drain deadline three seconds in: the stopping budget,
        // counted from the rung the ladder actually climbed.
        let landed = started.elapsed();
        assert!(landed >= Duration::from_secs(8), "{landed:?}");
        assert!(landed < Duration::from_secs(9), "{landed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn finish_ends_the_wait() {
        let (controller, shutdown) = ShutdownController::new(policy());
        controller.begin_stopping();
        let waiter = shutdown.clone();
        let task = tokio::spawn(async move { waiter.sleep_until_deadline().await });

        tokio::task::yield_now().await;
        sleep(Duration::from_secs(1)).await;
        controller.finish();

        let budget = timeout(Duration::from_secs(1), task)
            .await
            .expect("a ladder past its last rung must not hold the wait")
            .expect("waiter panicked");

        assert_eq!(budget, Budget::Untimed);
    }

    #[tokio::test(start_paused = true)]
    async fn entry_follows_stage() {
        let (controller, shutdown) = ShutdownController::new(policy());
        assert_eq!(shutdown.entered(), None);

        controller.begin_draining();
        let drained = shutdown.entered().expect("the stage was entered");
        assert_eq!(drained, TokioInstant::now().into_std());

        sleep(Duration::from_secs(2)).await;
        controller.begin_stopping();

        // The start moves with the rung: a budget counted from a stale start
        // is a budget already spent.
        assert_eq!(shutdown.entered(), Some(drained + Duration::from_secs(2)));
    }

    #[test]
    fn types_are_shareable() {
        fn assert_shared<T: Send + Sync + 'static>() {}

        assert_shared::<Shutdown>();
        assert_shared::<ShutdownController>();
        assert_shared::<KernelHandle>();
    }
}
