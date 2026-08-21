//! Runnables: the units that actually run, and the context they run with.
//!
//! # A component has a lifecycle; a runnable is what runs
//!
//! [`crate::component`] and this module exist to keep those two apart:
//!
//! * A [`Component`](crate::component::Component) is booted and stopped by the
//!   kernel. It typically owns a resource and the kernel guarantees when that
//!   resource is opened and closed. Both of its lifecycle methods must
//!   *return*.
//! * A [`Runnable`] is a task. It is started once every component has booted,
//!   it is supervised for as long as it lives, and it keeps running until the
//!   shutdown token fires.
//!
//! **`boot` prepares, `run` runs.** A component that must keep something going
//! continuously does not do it in `boot` — `boot` is bounded by a timeout and
//! everything after it waits on the component returning. It registers a
//! [`Runnable`] and lets `boot` do only what makes that runnable startable. An
//! object graph with no runnable in it is a program that exits immediately;
//! a `boot` that never returns is a program that never starts.
//!
//! Runnables are never ordered against each other. They all start after the
//! last component has booted, and one runnable may not depend on another.
//!
//! A runnable that fans work out onto tasks of its own owes that work the same
//! two-stage ending it owes the kernel, and [`Children`] is that ending
//! written once: it refuses new children while draining, lets the ones in
//! flight finish, and cuts whatever outlives the stopping budget.

use core::fmt;
use core::future::Future;
use std::sync::Arc;
use std::time::Instant;

use kernel_core::{
    BoxFuture, ConfigTree, NoopTelemetry, RunError, RunnableDescriptor, RunnableId, ShutdownPolicy,
    Stage, Telemetry,
};
use tokio::task::JoinSet;

use crate::container::Container;
use crate::dispatcher::EventDispatcher;
use crate::extension::ExtensionPoints;
use crate::shutdown::{KernelHandle, Shutdown, ShutdownController};

/// A supervised, long-running task.
///
/// # The contract imposed on implementors
///
/// 1. **`run` must return when the shutdown token fires.** A runnable that
///    ignores it is killed at the end of the grace period and counted as a
///    dirty stop. Two moments are offered, and they mean different things:
///    [`Shutdown::draining`] says *stop accepting new work, finish what you
///    hold*, and [`Shutdown::stopping`] says *finish now, the clock is
///    running*. A runnable that only ever watches one of them cannot both
///    refuse new work and drain old work.
/// 2. A panic inside `run` is caught at the join and reported as a
///    [`RunError`]; it never reaches the kernel as a panic.
/// 3. Returning `Ok(())` on its own is a clean end, not a failure, so it never
///    triggers a restart. What that end *means* for the rest of the process is
///    decided by [`criticality`](field@RunnableDescriptor::criticality), not by the return value.
///
/// # Why the receiver is `Arc<Self>`
///
/// `run` takes `self: Arc<Self>` rather than `&self`. That keeps the trait
/// dyn-compatible while handing the runnable the shared ownership it needs: the
/// returned future is `'static`, so it can be detached onto a task, and the
/// runnable stays alive for exactly as long as that task does.
///
/// # The test every runnable owes
///
/// A runnable is testable without a kernel: [`RunContext::detached`] hands back
/// a context and the controller that drives it. Proving that `run` returns when
/// the token fires is required of every runnable — see the example below, which
/// is that test.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use kernel::{BoxFuture, RunContext, Runnable, RunnableDescriptor};
/// use kernel::core::RunError;
///
/// struct Beacon;
///
/// impl Runnable for Beacon {
///     fn name() -> &'static str {
///         "beacon"
///     }
///
///     fn descriptor(&self) -> RunnableDescriptor {
///         RunnableDescriptor::new()
///     }
///
///     fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
///         Box::pin(async move {
///             cx.shutdown().stopping().await;
///             Ok(())
///         })
///     }
/// }
///
/// let runtime = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
///
/// runtime.block_on(async {
///     let (cx, controller) = RunContext::detached();
///     let task = tokio::spawn(Arc::new(Beacon).run(cx));
///
///     controller.begin_stopping();
///
///     task.await.unwrap().unwrap();
/// });
/// ```
pub trait Runnable: Send + Sync + 'static {
    /// The one declared name of this runnable.
    ///
    /// Read once, at registration, where the concrete type is still known: it
    /// becomes the [`RunnableId`] the plan indexes and every record and every
    /// [`RunError`] blames. There is no second place to declare it.
    ///
    /// It is `where Self: Sized` — never dispatched through `dyn Runnable` —
    /// which is what keeps the trait dyn-compatible.
    fn name() -> &'static str
    where
        Self: Sized;

    /// Criticality and time bounds of this runnable.
    ///
    /// Read by the supervisor before every start and after every end, so it
    /// must be cheap and must not change between calls.
    fn descriptor(&self) -> RunnableDescriptor;

    /// Run until the shutdown token fires.
    ///
    /// The future is `'static`: the supervisor spawns it and holds the join
    /// handle. Everything it needs comes from `cx` or from `self`.
    ///
    /// Returning early is allowed and is not an error, but for an
    /// [`Essential`](kernel_core::Criticality::Essential) runnable it is what
    /// stops the process — which is the point of that criticality, not a
    /// surprise.
    ///
    /// It also decides the exit code. An essential runnable that returns while
    /// other runnables are still running takes the process down before their
    /// work was done, and the kernel exits non-zero even though this call
    /// returned `Ok`. Only a run in which *every* runnable returned on its own
    /// is a completion.
    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>>;
}

/// What a runnable is handed when it starts.
///
/// Owned and cheap to clone, unlike the boot and shutdown contexts: the future
/// returned by [`Runnable::run`] is `'static`, so it can borrow nothing from
/// the caller. Every clone watches the same shutdown token and resolves against
/// the same container.
#[derive(Clone)]
pub struct RunContext {
    id: RunnableId,
    container: Container,
    dispatcher: Arc<EventDispatcher>,
    shutdown: Shutdown,
    handle: KernelHandle,
    /// The bounds this run was started under. Read by
    /// [`deadline`](RunContext::deadline), which is the only thing that needs
    /// them: they are the runnable's own declaration, not the ladder's.
    descriptor: RunnableDescriptor,
}

impl RunContext {
    /// Assembles a run context.
    ///
    /// The supervisor calls this once per start, so a restarted runnable gets a
    /// fresh context watching the same token.
    ///
    /// It also binds `handle` to the ladder `shutdown` reads, so that a unit
    /// holding nothing but the handle — the only end of the lifecycle an
    /// arbitrary unit can resolve — reads the STAGE rather than a boolean. The
    /// binding happens once: the first ladder wins and a later one changes
    /// nothing, so the second ladder a shutdown walk opens for its own units
    /// cannot redefine what the process reports.
    ///
    /// The context carries no bounds of its own until
    /// [`with_descriptor`](Self::with_descriptor) names them, and
    /// [`deadline`](Self::deadline) then reports the ladder's.
    pub fn new(
        id: RunnableId,
        container: Container,
        dispatcher: Arc<EventDispatcher>,
        shutdown: Shutdown,
        handle: KernelHandle,
    ) -> Self {
        shutdown.attach(&handle);
        Self {
            id,
            container,
            dispatcher,
            shutdown,
            handle,
            descriptor: RunnableDescriptor::new(),
        }
    }

    /// The same context, bounded the way this runnable's descriptor asks.
    ///
    /// What the supervisor names before it starts a runnable: the budgets a
    /// [`RunnableDescriptor`] declares are the runnable's own, and
    /// [`deadline`](Self::deadline) cannot report them unless the context is
    /// told what they are.
    #[must_use]
    pub fn with_descriptor(mut self, descriptor: RunnableDescriptor) -> Self {
        self.descriptor = descriptor;
        self
    }

    /// The container.
    ///
    /// Resolving here is permitted — the graph is validated — but a first
    /// instantiation of a shared binding is not: by the time runnables start,
    /// the container is sealed. A runnable resolves what it needs once, at the
    /// top of `run`, or holds it as a field.
    #[must_use]
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// The dispatcher, for emitting and for awaited dispatch.
    #[must_use]
    pub fn dispatcher(&self) -> &EventDispatcher {
        &self.dispatcher
    }

    /// The telemetry sink every unit reports through.
    #[must_use]
    pub fn telemetry(&self) -> &Arc<dyn Telemetry> {
        self.container.telemetry()
    }

    /// The shutdown token this runnable must yield to.
    ///
    /// Select over [`Shutdown::draining`] and [`Shutdown::stopping`] alongside
    /// the runnable's own work. Both resolve immediately if their stage has
    /// already been entered, so a runnable started late is not left waiting for
    /// a transition that already happened.
    #[must_use]
    pub fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }

    /// A handle on the kernel, for a runnable that needs to ask for a stop.
    #[must_use]
    pub fn handle(&self) -> &KernelHandle {
        &self.handle
    }

    /// The instant this runnable's own budget for the current stage lands on.
    ///
    /// # The ladder says which stage, this says how long
    ///
    /// [`shutdown`](Self::shutdown) reports WHICH STAGE the kernel is in, and
    /// [`Shutdown::deadline`] is when that stage ends for everybody.
    /// This one is a property of the UNIT: the start of the current stage plus
    /// the budget this runnable's own
    /// [`RunnableDescriptor`] declared for it, never later than the ladder's —
    /// a descriptor may shorten a stage, never extend it. A runnable that
    /// declared no budget for the stage in hand is bounded by the ladder alone,
    /// and reads the same instant either way.
    ///
    /// This is the arithmetic it exists to stop a runnable doing: recomputing
    /// its own bound from the start of the stage is exactly the calculation a
    /// component was found getting wrong, and reading the ladder's where its
    /// own was meant makes it hurry for a deadline nobody is enforcing on it.
    ///
    /// `None` while [`Stage::Running`] and once [`Stage::Stopped`]: neither
    /// stage is timed and neither bounds this runnable. It is not a promise of
    /// endless time — see [`Shutdown::deadline`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::time::Duration;
    /// # use kernel::RunContext;
    /// # use kernel::core::RunnableDescriptor;
    /// # async fn probe() {
    /// let (cx, controller) = RunContext::builder()
    ///     .with_descriptor(RunnableDescriptor::new().stop_timeout(Duration::from_secs(1)))
    ///     .build();
    ///
    /// assert_eq!(cx.deadline(), None);
    /// controller.begin_stopping();
    ///
    /// // One second, not the twenty the ladder grants the process.
    /// let own = cx.deadline().expect("the stage is timed");
    /// assert!(own < cx.shutdown().deadline().expect("so is the ladder"));
    /// # }
    /// ```
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        let own = match self.shutdown.stage() {
            Stage::Draining => self.descriptor.drain_timeout,
            Stage::Stopping => self.descriptor.stop_timeout,
            Stage::Running | Stage::Stopped => None,
        };
        let own = own.and_then(|budget| {
            self.shutdown
                .entered()
                .and_then(|entered| entered.checked_add(budget))
        });

        match (own, self.shutdown.deadline()) {
            (Some(own), Some(ladder)) => Some(own.min(ladder)),
            (Some(only), None) | (None, Some(only)) => Some(only),
            (None, None) => None,
        }
    }

    /// The identity this run is recorded under.
    #[must_use]
    pub fn id(&self) -> RunnableId {
        self.id
    }

    /// A context with no kernel behind it, for testing a runnable in isolation.
    ///
    /// The container is empty, the dispatcher has no listener, the
    /// configuration tree is empty, telemetry is discarded, and the returned
    /// [`ShutdownController`] is the only thing that can move the stage — which
    /// makes the mandatory test writable: start the runnable, drive the
    /// controller, assert that `run` returned.
    ///
    /// This is not a convenience. "The runnable yields when the shutdown token
    /// fires" is required of every runnable ever written, and without a context
    /// that can be built without a kernel that test could only be written by
    /// assembling one.
    ///
    /// Every one of those parts is empty and none of them has to be:
    /// [`builder`](Self::builder) takes a container, a dispatcher, a
    /// configuration tree, a telemetry sink and an identity, so a runnable
    /// that resolves, dispatches, reads config or reports inside `run` is
    /// testable as it is written rather than after being restructured to suit
    /// the context it is tested with.
    #[must_use]
    pub fn detached() -> (Self, ShutdownController) {
        RunBuilder::new().build()
    }

    /// The same context, with the parts a test needs to choose.
    ///
    /// # Examples
    ///
    /// ```
    /// # use kernel::{RunContext, ShutdownController};
    /// # use kernel::core::{ConfigTree, RunnableId};
    /// let (cx, controller): (RunContext, ShutdownController) = RunContext::builder()
    ///     .with_id(RunnableId::new("probe", 3))
    ///     .with_config(ConfigTree::empty())
    ///     .build();
    ///
    /// assert_eq!(cx.id().name(), "probe");
    /// # let _ = controller;
    /// ```
    #[must_use]
    pub fn builder() -> RunBuilder {
        RunBuilder::new()
    }
}

/// Chooses what a detached [`RunContext`] is made of.
///
/// [`RunContext::detached`] is every part left empty. This is the same context
/// with the parts a test names: a runnable that resolves a dependency,
/// dispatches an event, reads its configuration or reports a record inside
/// `run` needs those parts to exist for the mandatory shutdown test to be
/// writable at all, and shaping production code around an empty context is the
/// test dictating the design.
pub struct RunBuilder {
    id: RunnableId,
    container: Option<Container>,
    dispatcher: Option<Arc<EventDispatcher>>,
    config: Arc<ConfigTree>,
    telemetry: Arc<dyn Telemetry>,
    handle: Option<KernelHandle>,
    extensions: Option<Arc<ExtensionPoints>>,
    policy: ShutdownPolicy,
    descriptor: RunnableDescriptor,
}

impl RunBuilder {
    /// A builder with every default in place: the parts
    /// [`RunContext::detached`] hands out.
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: RunnableId::new("detached", 0),
            container: None,
            dispatcher: None,
            config: Arc::new(ConfigTree::empty()),
            telemetry: Arc::new(NoopTelemetry),
            handle: None,
            extensions: None,
            policy: ShutdownPolicy::default(),
            descriptor: RunnableDescriptor::new(),
        }
    }

    /// Record this run under `id` instead of `detached#0`.
    #[must_use]
    pub fn with_id(mut self, id: RunnableId) -> Self {
        self.id = id;
        self
    }

    /// Resolve through this container instead of an empty one.
    ///
    /// Its own configuration and telemetry are what the context reports, so
    /// [`with_config`](Self::with_config) and
    /// [`with_telemetry`](Self::with_telemetry) are ignored once a container
    /// is given: a container carries both, and two answers to one question
    /// would be worse than none.
    #[must_use]
    pub fn with_container(mut self, container: Container) -> Self {
        self.container = Some(container);
        self
    }

    /// Dispatch through this dispatcher instead of one with no listener.
    #[must_use]
    pub fn with_dispatcher(mut self, dispatcher: Arc<EventDispatcher>) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }

    /// Read this configuration tree, when no container is given.
    #[must_use]
    pub fn with_config(mut self, config: ConfigTree) -> Self {
        self.config = Arc::new(config);
        self
    }

    /// Report through this telemetry sink, when no container is given.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: Arc<dyn Telemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Ask for a stop through this handle.
    ///
    /// What a test asserting that a runnable requested a stop holds the other
    /// end of. Left unset, the context carries a handle of its own, readable
    /// through [`RunContext::handle`].
    #[must_use]
    pub fn with_handle(mut self, handle: KernelHandle) -> Self {
        self.handle = Some(handle);
        self
    }

    /// Collect from this contribution table.
    ///
    /// Attached to the container, so a runnable that serves health reads it
    /// through [`Container::extensions`](crate::container::Container::extensions)
    /// exactly as it does under a kernel. The table itself comes from a
    /// booted component's
    /// [`BootContext::extensions`](crate::component::BootContext::extensions)
    /// or from another container.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Arc<ExtensionPoints>) -> Self {
        self.extensions = Some(extensions);
        self
    }

    /// Budget the ladder's stages the way this policy says.
    #[must_use]
    pub fn with_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Bound the run the way this descriptor asks.
    ///
    /// What [`RunContext::deadline`] reports the runnable's own bound from. A
    /// test of a runnable that hurries for its own budget has to be able to
    /// give the context the budget the supervisor would have given it.
    #[must_use]
    pub fn with_descriptor(mut self, descriptor: RunnableDescriptor) -> Self {
        self.descriptor = descriptor;
        self
    }

    /// Assembles the context, and opens the ladder that drives it.
    #[must_use]
    pub fn build(self) -> (RunContext, ShutdownController) {
        let Self {
            id,
            container,
            dispatcher,
            config,
            telemetry,
            handle,
            extensions,
            policy,
            descriptor,
        } = self;

        let (controller, shutdown) = ShutdownController::new(policy);

        // A container carries a handle of its own, and the context reports the
        // same one unless the caller named another.
        let (container, handle) = match (container, handle) {
            (Some(container), Some(handle)) => (container, handle),
            (Some(container), None) => {
                let handle = container.handle();
                (container, handle)
            }
            (None, handle) => {
                let handle = handle.unwrap_or_else(KernelHandle::detached);
                let container = Container::new(Vec::new(), config, telemetry, handle.clone());
                (container, handle)
            }
        };
        let container = match extensions {
            Some(extensions) => container.with_extensions(extensions),
            None => container,
        };

        let dispatcher = match dispatcher {
            Some(dispatcher) => dispatcher,
            None => {
                // Attached only when it was built here: a dispatcher handed in
                // already belongs to a container of its own.
                let built = Arc::new(EventDispatcher::new(
                    Vec::new(),
                    Arc::clone(container.telemetry()),
                ));
                built.attach(container.clone());
                built
            }
        };

        let context = RunContext {
            id,
            container,
            dispatcher,
            shutdown,
            handle,
            descriptor,
        };
        // Same binding the supervisor's context makes: a test reading the
        // stage off the handle reads what it would read under a kernel.
        controller.attach(&context.handle);

        (context, controller)
    }
}

impl Default for RunBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for RunBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunBuilder")
            .field("id", &self.id)
            .field("container", &self.container.is_some())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for RunContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunContext")
            .field("id", &self.id)
            .field("stage", &self.shutdown.stage())
            .finish_non_exhaustive()
    }
}

/// Child work run under the shutdown ladder, and bounded by it.
///
/// A runnable that fans work out onto tasks of its own owes that work the same
/// two-stage ending it owes itself, and the shape of that ending is always the
/// same: refuse new children once the ladder drains, let the ones in flight
/// finish, and cut whatever outlives the stopping budget. Written by hand it is
/// sixty lines with four ways to get it wrong — a select that is not biased, a
/// reap loop that ends one child early, a deadline read from the wrong clock,
/// a cut that never happens because the stage was untimed.
///
/// It knows nothing about what the children do. It spawns futures, counts
/// them, and ends them; whatever a child is holding is the child's business.
///
/// # Fan-out is bounded here or nowhere
///
/// Nothing else bounds it: the kernel supervises runnables, not the tasks a
/// runnable spawns. [`with_limit`](Self::with_limit) is where a caller says how
/// many children may be in flight at once, and [`spawn`](Self::spawn) refuses
/// past it rather than queueing — a refusal the caller can answer, where an
/// unbounded set of tasks is a process that dies with no answer at all.
///
/// # Examples
///
/// The shape a unit that takes work in and hands it to a task per item needs:
///
/// ```
/// use core::time::Duration;
/// use std::sync::Arc;
///
/// use kernel::{BoxFuture, Children, RunContext, Runnable, RunnableDescriptor};
/// use kernel::core::RunError;
///
/// /// Stands in for whatever this runnable takes its work from.
/// async fn next_item() -> u32 {
///     tokio::time::sleep(Duration::from_millis(1)).await;
///     7
/// }
///
/// struct Accepting;
///
/// impl Runnable for Accepting {
///     fn name() -> &'static str {
///         "accepting"
///     }
///
///     fn descriptor(&self) -> RunnableDescriptor {
///         RunnableDescriptor::new()
///     }
///
///     fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
///         Box::pin(async move {
///             let mut children = Children::new(cx.shutdown().clone()).with_limit(512);
///
///             loop {
///                 tokio::select! {
///                     biased;
///                     // Draining: refuse new work, keep what is in flight.
///                     () = cx.shutdown().draining() => break,
///                     item = next_item() => {
///                         let spawned = children.spawn(async move {
///                             let _ = item;
///                         });
///                         if !spawned.is_started() {
///                             // Over the limit, or the ladder moved under us:
///                             // this is where the work is turned away.
///                             break;
///                         }
///                     }
///                 }
///             }
///
///             // What is in flight finishes; what outlives the stopping
///             // budget is cut.
///             let cut = children.finish().await;
///             assert_eq!(cut, 0);
///             Ok(())
///         })
///     }
/// }
///
/// let runtime = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
///
/// runtime.block_on(async {
///     let (cx, controller) = RunContext::detached();
///     let task = tokio::spawn(Arc::new(Accepting).run(cx));
///
///     controller.begin_stopping();
///
///     task.await.unwrap().unwrap();
/// });
/// ```
pub struct Children {
    shutdown: Shutdown,
    tasks: JoinSet<()>,
    limit: Option<usize>,
}

impl Children {
    /// A set with no children and no bound on how many there may be.
    ///
    /// The ladder is the one the parent watches — under a kernel,
    /// `cx.shutdown().clone()`.
    #[must_use]
    pub fn new(shutdown: Shutdown) -> Self {
        Self {
            shutdown,
            tasks: JoinSet::new(),
            limit: None,
        }
    }

    /// Refuse a child once `limit` of them are already in flight.
    ///
    /// A limit of zero refuses every child, which is a way of saying the fan-out
    /// is closed rather than an error.
    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Starts one child, unless the ladder or the limit refuses it.
    ///
    /// Refused while the ladder is anywhere but [`Stage::Running`]: draining
    /// means *no new work*, and a child started after that is work the stop was
    /// meant to have refused. Refused again once the limit is reached. Either
    /// way nothing is spawned and the future is dropped, so a caller that has
    /// something to answer — a reply, a rejection, a counter — reads which of
    /// the two it was.
    ///
    /// Must be called from inside a runtime: the child is spawned onto the
    /// current one.
    pub fn spawn<F>(&mut self, work: F) -> Spawned
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Freeing what has already finished is what makes the limit a limit on
        // work in flight rather than on work ever done.
        self.reap();

        if self.shutdown.stage().is_shutting_down() {
            return Spawned::Draining;
        }
        if self.limit.is_some_and(|limit| self.tasks.len() >= limit) {
            return Spawned::AtLimit;
        }

        self.tasks.spawn(work);
        Spawned::Started
    }

    /// How many children are in flight.
    ///
    /// Takes `&mut self` because counting means reaping first: a count that
    /// includes children that have already finished is not a count of work in
    /// flight, and that is the only count worth reporting.
    pub fn len(&mut self) -> usize {
        self.reap();
        self.tasks.len()
    }

    /// Whether any child is still in flight.
    pub fn is_empty(&mut self) -> bool {
        self.len() == 0
    }

    /// Waits the children out under the ladder, and cuts what outlives it.
    ///
    /// Three moves, in this order:
    ///
    /// 1. **Reap until empty.** While the ladder has not reached
    ///    [`Stage::Stopping`], children are simply awaited: draining says
    ///    finish what you hold.
    /// 2. **Reap against the clock.** Once stopping is reached — or if it
    ///    already was — the same wait runs bounded by
    ///    [`Shutdown::sleep_until_deadline`], which is what makes an untimed
    ///    stage cut immediately instead of silently not cutting at all.
    /// 3. **Cut.** Whatever is still in flight is aborted, and the count of it
    ///    is returned. The aborted children are deliberately not awaited: a
    ///    child that never yields would never observe its abort, and waiting
    ///    for it is the one thing this kernel promises never to do.
    ///
    /// Returns how many children were cut — `0` when every one of them ended
    /// on its own, which is the number a clean stop reports.
    ///
    /// Called while the ladder is still [`Stage::Running`] it waits for every
    /// child with no bound at all, which is the right answer for a parent that
    /// is ending on its own rather than being stopped.
    pub async fn finish(mut self) -> usize {
        // Draining: what is in flight finishes on its own.
        loop {
            if self.tasks.is_empty() {
                return 0;
            }
            // Biased: a ladder already at stopping must not lose a coin toss
            // against a child that happens to be ready at the same instant.
            tokio::select! {
                biased;
                () = self.shutdown.stopping() => break,
                _ = self.tasks.join_next() => {}
            }
        }

        // Stopping: the same wait, now against the clock.
        loop {
            if self.tasks.is_empty() {
                return 0;
            }
            tokio::select! {
                biased;
                _ = self.shutdown.sleep_until_deadline() => break,
                _ = self.tasks.join_next() => {}
            }
        }

        self.reap();
        let cut = self.tasks.len();
        self.tasks.abort_all();
        cut
    }

    /// Frees every child that has already finished.
    fn reap(&mut self) {
        while self.tasks.try_join_next().is_some() {}
    }
}

impl fmt::Debug for Children {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Children")
            .field("stage", &self.shutdown.stage())
            .field("children", &self.tasks.len())
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// What became of a call to [`Children::spawn`].
///
/// Three answers and not a `bool`, because the two refusals ask the caller for
/// different things: `Draining` is the process ending and the work should be
/// turned away for good, `AtLimit` is this parent being full right now and the
/// same work may be offered again later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use = "a refused child never ran, and the work it was given is gone"]
pub enum Spawned {
    /// The child is running.
    Started,
    /// The ladder has left [`Stage::Running`]: new work is refused.
    Draining,
    /// The limit on children in flight is reached.
    AtLimit,
}

impl Spawned {
    /// Whether the child is running.
    #[must_use]
    pub fn is_started(self) -> bool {
        matches!(self, Self::Started)
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::time::Duration;

    use kernel_core::{Criticality, Stage};

    use super::*;

    /// The reference runnable: it does its own work until the token fires, and
    /// distinguishes the two stages the way the contract asks.
    struct Ticker {
        ticks: AtomicUsize,
        drained: AtomicUsize,
    }

    impl Ticker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                ticks: AtomicUsize::new(0),
                drained: AtomicUsize::new(0),
            })
        }
    }

    impl Runnable for Ticker {
        fn name() -> &'static str {
            "ticker"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            RunnableDescriptor::new().criticality(Criticality::Ancillary)
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                loop {
                    tokio::select! {
                        () = cx.shutdown().stopping() => return Ok(()),
                        () = cx.shutdown().draining(), if self.drained.load(Ordering::Relaxed) == 0 => {
                            self.drained.fetch_add(1, Ordering::Relaxed);
                        }
                        () = tokio::time::sleep(Duration::from_millis(10)) => {
                            self.ticks.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        }
    }

    /// A runnable that ignores the token entirely — what the contract forbids.
    struct Deaf;

    impl Runnable for Deaf {
        fn name() -> &'static str {
            "deaf"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            RunnableDescriptor::new()
        }

        fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        }
    }

    /// The test section 18 makes mandatory for every runnable: it keeps running
    /// while the stage is `Running`, and it returns once the token fires.
    #[tokio::test(start_paused = true)]
    async fn yields_on_shutdown() {
        let unit = Ticker::new();
        let (cx, controller) = RunContext::detached();
        let mut task = tokio::spawn(Arc::clone(&unit).run(cx));

        // Still running: the token has not fired.
        let early = tokio::time::timeout(Duration::from_millis(50), &mut task).await;
        assert!(early.is_err());
        assert!(unit.ticks.load(Ordering::Relaxed) > 0);

        // Draining first, and observed on its own: a runnable that watches
        // only one of the two stages cannot both refuse new work and finish
        // what it holds.
        controller.begin_draining();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(unit.drained.load(Ordering::Relaxed), 1);

        controller.begin_stopping();

        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("runnable did not yield to the shutdown token")
            .expect("join");

        assert!(result.is_ok());
    }

    /// The counter-example the same test catches.
    #[tokio::test(start_paused = true)]
    async fn deaf_runnable_hangs() {
        let (cx, controller) = RunContext::detached();
        let task = tokio::spawn(Arc::new(Deaf).run(cx));

        controller.begin_stopping();

        let outcome = tokio::time::timeout(Duration::from_secs(1), task).await;

        assert!(outcome.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_alone_is_enough() {
        let unit = Ticker::new();
        let (cx, controller) = RunContext::detached();
        let task = tokio::spawn(unit.run(cx));

        // No draining stage at all: a runnable must not require the ladder to
        // be climbed rung by rung.
        controller.begin_stopping();

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("yielded")
            .expect("join")
            .expect("clean end");
    }

    #[tokio::test]
    async fn detached_starts_running() {
        let (cx, controller) = RunContext::detached();

        assert_eq!(cx.shutdown().stage(), Stage::Running);
        assert!(!cx.shutdown().is_shutting_down());
        assert!(!cx.handle().is_shutting_down());
        assert_eq!(cx.id().name(), "detached");
        assert_eq!(controller.stage(), Stage::Running);
    }

    #[tokio::test]
    async fn clone_shares_the_token() {
        let (cx, controller) = RunContext::detached();
        let twin = cx.clone();

        controller.begin_draining();

        assert_eq!(twin.shutdown().stage(), Stage::Draining);
        assert_eq!(twin.id(), cx.id());
        assert_eq!(
            twin.container().handle().is_shutting_down(),
            cx.handle().is_shutting_down()
        );
    }

    #[tokio::test]
    async fn detached_container_is_empty() {
        trait Surface: Send + Sync + 'static {}

        let (cx, _controller) = RunContext::detached();

        assert!(cx.container().get::<dyn Surface>().await.is_err());
        let _ = cx.telemetry();
        let _ = cx.dispatcher();
    }

    #[tokio::test(start_paused = true)]
    async fn runs_behind_dyn() {
        let unit: Arc<dyn Runnable> = Ticker::new();
        let (cx, controller) = RunContext::detached();

        assert_eq!(Ticker::name(), "ticker");
        assert_eq!(unit.descriptor().criticality, Criticality::Ancillary);

        let task = tokio::spawn(unit.run(cx));
        controller.begin_stopping();

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("yielded")
            .expect("join")
            .expect("clean end");
    }

    // ----------------------------------------------------------------------
    // What the empty context cannot do
    // ----------------------------------------------------------------------

    trait Surface: Send + Sync + 'static {
        fn mark(&self) -> u8;
    }

    struct Plain;

    impl Surface for Plain {
        fn mark(&self) -> u8 {
            7
        }
    }

    /// Counts the records a runnable reports while it runs.
    #[derive(Default)]
    struct Counting(AtomicUsize);

    impl Telemetry for Counting {
        fn record(&self, _record: kernel_core::Record) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The runnable an empty context cannot test: it resolves, reads its
    /// configuration and reports, all inside `run`.
    struct Wired {
        mark: AtomicUsize,
        batch: AtomicUsize,
    }

    impl Wired {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                mark: AtomicUsize::new(0),
                batch: AtomicUsize::new(0),
            })
        }
    }

    impl Runnable for Wired {
        fn name() -> &'static str {
            "wired"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            RunnableDescriptor::new()
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                let batch = cx
                    .container()
                    .config()
                    .get("limits.batch")
                    .and_then(|node| match node {
                        kernel_core::ConfigNode::Scalar(kernel_core::Scalar::Int(value)) => {
                            usize::try_from(*value).ok()
                        }
                        _ => None,
                    })
                    .unwrap_or_default();
                self.batch.store(batch, Ordering::Relaxed);

                cx.telemetry().record(kernel_core::Record::new(
                    kernel_core::Level::Info,
                    "wired.started",
                ));

                let surface = cx
                    .container()
                    .get::<dyn Surface>()
                    .await
                    .map_err(|error| RunError::failed(cx.id(), Box::new(error)))?;
                self.mark.store(surface.mark() as usize, Ordering::Relaxed);

                cx.shutdown().stopping().await;
                Ok(())
            })
        }
    }

    fn bound() -> Container {
        let build: crate::provider::BuildFn<dyn Surface> =
            Box::new(|_container| Box::pin(async { Ok(Arc::new(Plain) as Arc<dyn Surface>) }));
        let contract = kernel_core::ContractRef::of::<dyn Surface>();

        Container::new(
            vec![crate::container::BindingEntry {
                id: contract.id(),
                contract,
                bundle: "probe",
                lifetime: kernel_core::Lifetime::Shared,
                requires: Vec::new(),
                requires_scoped: Vec::new(),
                build: crate::container::erased::erase_build(build),
                is_default: false,
                order: 0,
            }],
            Arc::new(kernel_core::ConfigTree::empty()),
            Arc::new(kernel_core::NoopTelemetry),
            KernelHandle::detached(),
        )
    }

    /// The mandatory test, for a runnable that does its work through the
    /// context: the parts it needs exist, so the runnable is tested as it is
    /// written rather than rewritten to suit the test.
    #[tokio::test(start_paused = true)]
    async fn builder_wires_the_parts() {
        let mut config = kernel_core::ConfigTree::empty();
        config
            .insert("limits.batch", kernel_core::ConfigNode::from(9_i64))
            .expect("insert");
        let telemetry = Arc::new(Counting::default());
        let handle = KernelHandle::detached();

        let (cx, controller) = RunContext::builder()
            .with_id(RunnableId::new("wired", 2))
            .with_container(bound())
            .with_config(config)
            .with_telemetry(Arc::clone(&telemetry) as Arc<dyn Telemetry>)
            .with_handle(handle.clone())
            .build();

        assert_eq!(cx.id(), RunnableId::new("wired", 2));

        let unit = Wired::new();
        let task = tokio::spawn(Arc::clone(&unit).run(cx));
        controller.begin_stopping();

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("yielded")
            .expect("join")
            .expect("clean end");

        assert_eq!(unit.mark.load(Ordering::Relaxed), 7);
        assert!(!handle.is_shutting_down());
        // The container was given, so its own configuration and telemetry are
        // what the context reports — the two setters above are ignored.
        assert_eq!(unit.batch.load(Ordering::Relaxed), 0);
        assert_eq!(telemetry.0.load(Ordering::Relaxed), 0);
    }

    /// The same setters, with no container to override them.
    #[tokio::test(start_paused = true)]
    async fn builder_carries_config() {
        let mut config = kernel_core::ConfigTree::empty();
        config
            .insert("limits.batch", kernel_core::ConfigNode::from(9_i64))
            .expect("insert");
        let telemetry = Arc::new(Counting::default());

        let (cx, controller) = RunContext::builder()
            .with_config(config)
            .with_telemetry(Arc::clone(&telemetry) as Arc<dyn Telemetry>)
            .with_policy(ShutdownPolicy::new(
                Duration::from_secs(1),
                Duration::from_secs(2),
            ))
            .build();

        let unit = Wired::new();
        let task = tokio::spawn(Arc::clone(&unit).run(cx));
        controller.begin_stopping();

        // No binding: the resolution fails, which is the empty container's
        // answer and the reason the setter above exists.
        let outcome = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("yielded")
            .expect("join");

        assert!(outcome.is_err());
        // Both setters were read: the config tree given here is the one the
        // runnable read, and the sink given here is the one it reported to.
        assert_eq!(unit.batch.load(Ordering::Relaxed), 9);
        assert_eq!(telemetry.0.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn builder_reports_the_handle() {
        let handle = KernelHandle::detached();
        let (cx, controller) = RunContext::builder()
            .with_handle(handle.clone())
            .with_policy(ShutdownPolicy::new(
                Duration::from_secs(3),
                Duration::from_secs(5),
            ))
            .build();

        cx.handle().shutdown();
        controller.begin_draining();

        assert!(handle.is_shutting_down());
        // The policy given is the one the ladder budgets its stages with.
        assert!(cx.shutdown().deadline().is_some());
        assert!(format!("{:?}", RunContext::builder()).contains("RunBuilder"));
        assert!(format!("{:?}", RunBuilder::default()).contains("detached"));
    }

    // The dispatcher a test hands over is publicly reachable: a detached boot
    // owns one, so a component and a runnable can be tested against the same
    // one without a kernel to produce it.
    #[tokio::test]
    async fn builder_shares_dispatcher() {
        let detached = crate::component::BootContext::detached();
        let shared = Arc::clone(detached.dispatcher());

        let (cx, _controller) = RunContext::builder()
            .with_dispatcher(Arc::clone(&shared))
            .build();

        assert!(core::ptr::eq(cx.dispatcher(), Arc::as_ptr(&shared)));
    }

    #[tokio::test]
    async fn builder_carries_points() {
        use kernel_core::{Extension, ExtensionId};

        struct Marker;

        impl Extension for Marker {}

        let table = Arc::new(crate::extension::ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Marker>()],
            vec![crate::registry::ContributionEntry {
                extension: ExtensionId::of::<Marker>(),
                bundle: "probe",
                order: 0,
                item: Box::new(Marker),
            }],
        ));

        let (cx, _controller) = RunContext::builder()
            .with_extensions(Arc::clone(&table))
            .build();

        // What a runnable that serves health reads on every request.
        assert_eq!(cx.container().extensions().count::<Marker>(), 1);
    }

    // ----------------------------------------------------------------------
    // The stage, and the budget that is this runnable's own
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn context_binds_handle() {
        let handle = KernelHandle::detached();
        let (cx, controller) = RunContext::builder().with_handle(handle.clone()).build();

        controller.begin_draining();

        // The one end of the lifecycle an arbitrary unit can resolve now tells
        // the two rungs apart, which a boolean cannot.
        assert_eq!(handle.stage(), Stage::Draining);
        assert_eq!(cx.handle().stage(), Stage::Draining);
        // And it is still not a request: nobody asked, the kernel is stopping.
        assert!(!handle.is_shutting_down());
    }

    #[tokio::test]
    async fn new_binds_handle() {
        let (controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());
        let handle = KernelHandle::detached();
        let container = bound();
        let dispatcher = Arc::new(EventDispatcher::new(
            Vec::new(),
            Arc::clone(container.telemetry()),
        ));

        let cx = RunContext::new(
            RunnableId::new("probe", 0),
            container,
            dispatcher,
            shutdown,
            handle.clone(),
        );
        controller.begin_stopping();

        assert_eq!(handle.stage(), Stage::Stopping);
        assert_eq!(cx.handle().stage(), Stage::Stopping);
    }

    #[tokio::test(start_paused = true)]
    async fn own_budget_wins() {
        let (cx, controller) = RunContext::builder()
            .with_policy(ShutdownPolicy::new(
                Duration::from_secs(10),
                Duration::from_secs(20),
            ))
            .with_descriptor(
                RunnableDescriptor::new()
                    .drain_timeout(Duration::from_secs(1))
                    .stop_timeout(Duration::from_secs(2)),
            )
            .build();

        assert_eq!(cx.deadline(), None);

        controller.begin_draining();
        let drained = tokio::time::Instant::now().into_std();
        assert_eq!(cx.deadline(), Some(drained + Duration::from_secs(1)));
        assert_eq!(
            cx.shutdown().deadline(),
            Some(drained + Duration::from_secs(10))
        );

        tokio::time::sleep(Duration::from_secs(3)).await;
        controller.begin_stopping();
        let climbed = tokio::time::Instant::now().into_std();

        // Counted from the rung the ladder climbed, which is the arithmetic
        // this accessor exists to stop a runnable doing by hand.
        assert_eq!(cx.deadline(), Some(climbed + Duration::from_secs(2)));
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_defers_to_ladder() {
        let (cx, controller) = RunContext::detached();

        controller.begin_stopping();

        // No budget of its own: the ladder's is the one being enforced.
        assert!(cx.deadline().is_some());
        assert_eq!(cx.deadline(), cx.shutdown().deadline());
    }

    #[tokio::test(start_paused = true)]
    async fn ladder_caps_own_budget() {
        let (cx, controller) = RunContext::builder()
            .with_policy(ShutdownPolicy::new(
                Duration::from_secs(1),
                Duration::from_secs(2),
            ))
            .with_descriptor(RunnableDescriptor::new().stop_timeout(Duration::from_secs(600)))
            .build();

        controller.begin_stopping();

        // A descriptor may shorten a stage, never extend it.
        assert_eq!(cx.deadline(), cx.shutdown().deadline());
    }

    #[tokio::test(start_paused = true)]
    async fn untimed_stages_bound_nothing() {
        let (cx, controller) = RunContext::builder()
            .with_descriptor(RunnableDescriptor::new().stop_timeout(Duration::from_secs(1)))
            .build();

        assert_eq!(cx.deadline(), None);
        controller.finish();
        assert_eq!(cx.deadline(), None);
    }

    // ----------------------------------------------------------------------
    // Child work under the ladder
    // ----------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn children_finish_on_their_own() {
        let (cx, controller) = RunContext::builder()
            .with_policy(ShutdownPolicy::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
            ))
            .build();
        let done = Arc::new(AtomicUsize::new(0));
        let mut children = Children::new(cx.shutdown().clone());

        for _ in 0..3 {
            let flag = Arc::clone(&done);
            assert_eq!(
                children.spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    flag.fetch_add(1, Ordering::Relaxed);
                }),
                Spawned::Started
            );
        }
        assert_eq!(children.len(), 3);
        assert!(format!("{children:?}").contains("Children"));

        controller.begin_draining();
        let cut = children.finish().await;

        // Draining lets what is in flight finish: nothing was cut.
        assert_eq!(cut, 0);
        assert_eq!(done.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_cuts_children() {
        let (cx, controller) = RunContext::builder()
            .with_policy(ShutdownPolicy::new(
                Duration::from_secs(1),
                Duration::from_secs(2),
            ))
            .build();
        let done = Arc::new(AtomicUsize::new(0));
        let mut children = Children::new(cx.shutdown().clone());
        let flag = Arc::clone(&done);
        assert_eq!(
            children.spawn(async move {
                tokio::time::sleep(Duration::from_secs(600)).await;
                flag.fetch_add(1, Ordering::Relaxed);
            }),
            Spawned::Started
        );

        controller.begin_stopping();
        let started = tokio::time::Instant::now();
        let cut = children.finish().await;

        assert_eq!(cut, 1);
        assert_eq!(done.load(Ordering::Relaxed), 0);
        // It waited out the stopping budget before it cut, and no longer.
        assert!(started.elapsed() >= Duration::from_secs(2));
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    // An untimed stage grants no grace rather than endless grace: the cut
    // still happens, which is what the hand-rolled `if let Some(deadline)`
    // silently skips.
    #[tokio::test(start_paused = true)]
    async fn untimed_stop_cuts_now() {
        let (cx, controller) = RunContext::detached();
        let mut children = Children::new(cx.shutdown().clone());
        assert_eq!(children.spawn(core::future::pending()), Spawned::Started);

        // Straight past both rungs: `Stopped` is timed by nothing.
        controller.finish();
        let started = tokio::time::Instant::now();
        let cut = children.finish().await;

        assert_eq!(cut, 1);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_refuses_child() {
        let (cx, _controller) = RunContext::detached();
        let mut children = Children::new(cx.shutdown().clone()).with_limit(1);

        assert_eq!(children.spawn(core::future::pending()), Spawned::Started);
        assert_eq!(children.spawn(core::future::pending()), Spawned::AtLimit);
        assert!(!Spawned::AtLimit.is_started());
        assert!(Spawned::Started.is_started());
        assert_eq!(children.len(), 1);
    }

    #[tokio::test]
    async fn draining_refuses_child() {
        let (cx, controller) = RunContext::detached();
        let mut children = Children::new(cx.shutdown().clone());

        controller.begin_draining();

        // New work is refused, and the caller is told which refusal it was.
        assert_eq!(children.spawn(async {}), Spawned::Draining);
        assert!(children.is_empty());
        assert_eq!(children.finish().await, 0);
    }

    // The limit bounds work in flight, not work ever done: a finished child
    // frees its slot.
    #[tokio::test]
    async fn finished_child_frees_slot() {
        let (cx, _controller) = RunContext::detached();
        let mut children = Children::new(cx.shutdown().clone()).with_limit(1);

        assert_eq!(children.spawn(async {}), Spawned::Started);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        assert_eq!(children.spawn(async {}), Spawned::Started);
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_cuts_a_late_child() {
        let (cx, controller) = RunContext::builder()
            .with_policy(ShutdownPolicy::new(
                Duration::from_secs(1),
                Duration::from_secs(2),
            ))
            .build();
        let done = Arc::new(AtomicUsize::new(0));
        let mut children = Children::new(cx.shutdown().clone());

        // One child ends inside the budget, one outlives it.
        let flag = Arc::clone(&done);
        let _ = children.spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            flag.fetch_add(1, Ordering::Relaxed);
        });
        let _ = children.spawn(core::future::pending());

        controller.begin_stopping();
        let cut = children.finish().await;

        // The one that fitted inside the budget ran to its end; the one that
        // did not was cut, and only it.
        assert_eq!(cut, 1);
        assert_eq!(done.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn debug_names_the_run() {
        let (cx, controller) = RunContext::detached();
        controller.begin_draining();

        let rendered = format!("{cx:?}");

        assert!(rendered.contains("detached"));
        assert!(rendered.contains("Draining"));
    }
}
