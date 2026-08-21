//! Components: the units whose lifecycle the kernel owns, and the two contexts
//! they are handed.
//!
//! # A component has a lifecycle; a runnable is what runs
//!
//! This module and [`crate::runnable`] exist to keep those two apart, and the
//! separation is operational rather than philosophical:
//!
//! * A **component** is instantiated in a known order, told to
//!   [`boot`](Component::boot), told to [`drain`](Component::drain) when the
//!   ladder reaches [`Stage::Draining`](kernel_core::Stage::Draining), and
//!   later told to [`shutdown`](Component::shutdown). It typically owns a
//!   resource — a pool, a connection, a cache — and the kernel guarantees when
//!   that resource is opened, when it stops accepting new work, and when it is
//!   closed. Everything the kernel knows about a component is its identity and
//!   those three moments.
//! * A **runnable** is a task. It is started once every component has booted
//!   and it keeps running until the shutdown token fires.
//!
//! A value resolved from the container that has neither is neither: it is built
//! on first resolution and the kernel never speaks to it again.
//!
//! **`boot` prepares, `run` runs.** A component that must keep something going
//! continuously does *not* do it in `boot`: `boot` is bounded by
//! [`boot_timeout`](field@ComponentDescriptor::boot_timeout) and everything after it waits on the
//! component returning. Such a component registers a [`Runnable`] and lets
//! `boot` do only what is needed to make that runnable startable.
//!
//! [`Runnable`]: crate::runnable::Runnable

use core::fmt;
use std::sync::Arc;
use std::time::Instant;

use kernel_core::{
    BoxFuture, ComponentDescriptor, ComponentError, ConfigTree, Extension, ExtensionId,
    NoopTelemetry, ShutdownPolicy, Telemetry,
};

use crate::container::Container;
use crate::dispatcher::EventDispatcher;
use crate::extension::ExtensionPoints;
use crate::registry::ContributionEntry;
use crate::shutdown::{KernelHandle, Shutdown, ShutdownController};

/// A unit whose lifecycle the kernel manages.
///
/// The kernel instantiates a component in dependency order, calls
/// [`boot`](Self::boot) on it, calls [`drain`](Self::drain) on it when the
/// shutdown ladder reaches its first rung, and — in reverse order, once every
/// runnable has wound down — calls [`shutdown`](Self::shutdown). Nothing else
/// about the type is the kernel's business.
///
/// # `boot` prepares, `run` runs
///
/// A component may **not** block indefinitely in `boot`. The kernel applies
/// [`boot_timeout`](field@ComponentDescriptor::boot_timeout), and exceeding it is a boot failure,
/// not a slow start. A component that needs to keep something going
/// continuously — a loop, a listener, a poller — registers a
/// [`Runnable`](crate::runnable::Runnable) instead and returns from `boot` as
/// soon as that runnable can be started. `boot` opens the resource; `run` uses
/// it.
///
/// Booting is where a component may resolve freely: the graph has already been
/// validated, so a resolution that would fail has already been reported.
///
/// # Bounds
///
/// A component is stored for the life of the process and touched from any
/// task, which is what `Send + Sync + 'static` says. Interior mutability is the
/// component's own business — the kernel only ever holds `&self`.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
///
/// use kernel::{BootContext, BoxFuture, Component, ComponentDescriptor};
/// use kernel::core::ComponentError;
///
/// struct Holder {
///     ready: AtomicBool,
/// }
///
/// impl Component for Holder {
///     fn name() -> &'static str {
///         "holder"
///     }
///
///     fn descriptor(&self) -> ComponentDescriptor {
///         ComponentDescriptor::new()
///     }
///
///     fn boot<'a>(&'a self, _cx: &'a BootContext<'a>)
///         -> BoxFuture<'a, Result<(), ComponentError>>
///     {
///         Box::pin(async move {
///             self.ready.store(true, Ordering::Release);
///             Ok(())
///         })
///     }
/// }
///
/// let held: Arc<dyn Component> = Arc::new(Holder { ready: AtomicBool::new(false) });
/// assert_eq!(Holder::name(), "holder");
/// assert_eq!(held.descriptor().boot_timeout, None);
/// ```
pub trait Component: Send + Sync + 'static {
    /// The one declared name of this component.
    ///
    /// Read once, at registration, where the concrete type is still known: it
    /// becomes the [`ComponentId`](kernel_core::ComponentId) the plan indexes
    /// and every diagnostic blames. There is no second place to declare it.
    ///
    /// It is `where Self: Sized` — never dispatched through
    /// `dyn Component` — which is what keeps the trait dyn-compatible.
    fn name() -> &'static str
    where
        Self: Sized;

    /// Time bounds of this component.
    ///
    /// Read by the kernel on every lifecycle transition, so it must be cheap
    /// and must not change between calls.
    fn descriptor(&self) -> ComponentDescriptor;

    /// Prepare this component for use.
    ///
    /// Called once, in dependency order, after every component this one
    /// requires has booted. Resolution, configuration reads and extension point
    /// collection all happen here.
    ///
    /// Must return. See the trait documentation: anything that runs
    /// continuously belongs in a [`Runnable`](crate::runnable::Runnable).
    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>>;

    /// Refuse new work; what is already in flight keeps going.
    ///
    /// Called once, as the shutdown ladder reaches
    /// [`Stage::Draining`](kernel_core::Stage::Draining) and **before any
    /// runnable is asked to wind down**. A component that owns the resource
    /// new work arrives through closes that door here and returns; the
    /// runnables that were reading from it are still running, and what they
    /// already took is still theirs to finish.
    ///
    /// The default does nothing and succeeds, so a component that accepts no
    /// work from outside costs nothing.
    ///
    /// # Why it is not the runnable's job
    ///
    /// Refusing new work is a property of the RESOURCE, not of the loop that
    /// reads it, and the resource belongs to the component. Without this hook a
    /// component that owns one has to be split in two — a component holding it
    /// and a runnable closing it — for no reason other than that the runnable
    /// was the only unit that could see the drain.
    ///
    /// # Bounds
    ///
    /// Bounded like every other lifecycle call, on the
    /// [`ShutdownPolicy`]'s drain budget: every component drains at once, each
    /// with the whole budget, so no component's overrun shortens another's.
    /// A component still running when that budget elapses is dropped, its
    /// overrun is recorded, and the ladder goes on — the rest of the shutdown
    /// is not held up by it.
    ///
    /// # What the context reports
    ///
    /// [`ShutdownContext::shutdown`] is on
    /// [`Stage::Draining`](kernel_core::Stage::Draining), and
    /// [`ShutdownContext::deadline`] is the instant this component's own drain
    /// is cut at.
    fn drain<'a>(
        &'a self,
        cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        let _ = cx;
        Box::pin(async { Ok(()) })
    }

    /// Release what [`boot`](Self::boot) acquired.
    ///
    /// Called in reverse boot order, after every runnable has wound down, so a
    /// component may still use everything it depended on. The default does
    /// nothing and succeeds, because a component that owns no resource has
    /// nothing to release.
    ///
    /// Failing here does not stop the shutdown walk: the error is recorded and
    /// the remaining components are still stopped.
    fn shutdown<'a>(
        &'a self,
        cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        let _ = cx;
        Box::pin(async { Ok(()) })
    }
}

/// What a component is handed while it boots.
///
/// Borrowed, not owned: the context lives exactly as long as the call, which is
/// what lets [`collect`](Self::collect) hand back references into the
/// contribution table rather than clones of it.
pub struct BootContext<'a> {
    container: &'a Container,
    dispatcher: &'a EventDispatcher,
    extensions: &'a Arc<ExtensionPoints>,
}

impl<'a> BootContext<'a> {
    /// Assembles a boot context over the three tables a booting component may
    /// read.
    ///
    /// This is what the kernel's boot phase calls, and the three tables it
    /// takes are the ones phase three produced. A caller outside the kernel
    /// cannot build them — [`Container`] and [`EventDispatcher`] are assembled
    /// by resolution, not by hand — so booting a component outside a kernel
    /// goes through [`detached`](Self::detached), which builds them, or
    /// through [`builder`](Self::builder) when the test needs a container, a
    /// dispatcher or contributions of its own.
    pub fn new(
        container: &'a Container,
        dispatcher: &'a EventDispatcher,
        extensions: &'a Arc<ExtensionPoints>,
    ) -> Self {
        Self {
            container,
            dispatcher,
            extensions,
        }
    }

    /// Tables a component can boot against with no kernel behind it.
    ///
    /// The container is empty, the dispatcher has no listener and no point has
    /// been contributed to. What it makes possible is calling
    /// [`Component::boot`] at all from outside this crate: the context borrows
    /// three tables, so the caller must own them, and this is what owns them.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use kernel::{BootContext, BoxFuture, Component, ComponentDescriptor};
    /// # use kernel::core::ComponentError;
    /// # struct Holder;
    /// # impl Component for Holder {
    /// #     fn name() -> &'static str { "holder" }
    /// #     fn descriptor(&self) -> ComponentDescriptor { ComponentDescriptor::new() }
    /// #     fn boot<'a>(&'a self, _cx: &'a BootContext<'a>)
    /// #         -> BoxFuture<'a, Result<(), ComponentError>> { Box::pin(async { Ok(()) }) }
    /// # }
    /// # async fn probe() {
    /// let detached = BootContext::detached();
    ///
    /// Holder.boot(&detached.context()).await.expect("boot");
    /// # }
    /// ```
    #[must_use]
    pub fn detached() -> DetachedBoot {
        BootBuilder::new().build()
    }

    /// The same tables, with the parts a test needs to choose.
    #[must_use]
    pub fn builder() -> BootBuilder {
        BootBuilder::new()
    }

    /// The container. Resolution is permitted here: the graph is validated.
    #[must_use]
    pub fn container(&self) -> &Container {
        self.container
    }

    /// The configuration tree, frozen before anything booted.
    #[must_use]
    pub fn config(&self) -> &ConfigTree {
        self.container.config()
    }

    /// The dispatcher, for a component that emits during boot or that needs to
    /// keep a handle on it.
    #[must_use]
    pub fn dispatcher(&self) -> &EventDispatcher {
        self.dispatcher
    }

    /// The telemetry sink every unit reports through.
    #[must_use]
    pub fn telemetry(&self) -> &Arc<dyn Telemetry> {
        self.container.telemetry()
    }

    /// A handle on the kernel that is booting this component.
    #[must_use]
    pub fn handle(&self) -> KernelHandle {
        self.container.handle()
    }

    /// Every contribution to this extension point, in bundle registration
    /// order.
    ///
    /// A point that was declared and never contributed to yields an empty
    /// vector; that is a valid outcome, not a missing one.
    ///
    /// # Why the items are borrowed
    ///
    /// The returned references point into the contribution table, which outlives
    /// the whole boot phase. Handing back owned values instead would cost two
    /// things worth more than the convenience: every contributed type would have
    /// to implement `Clone` merely to be collectable, and a point could be
    /// collected only once, because the second collector would find the table
    /// emptied. Borrowing keeps the table intact, keeps the bound at
    /// [`Extension`] alone, and lets two components read the same point.
    ///
    /// A component that needs items past the end of `boot` clones what it keeps
    /// — an explicit cost, paid by the one component that needs it.
    #[must_use]
    pub fn collect<X: Extension>(&self) -> Vec<&'a X> {
        self.extensions.collect::<X>()
    }

    /// The whole contribution table, in a form that outlives this call.
    ///
    /// [`collect`](Self::collect) is the right read for a component that reads
    /// a point once while it boots. It is the wrong one for a component that
    /// must read it again later: a unit that answers a health request runs the
    /// probes on every request, long after this call returned, and borrowed
    /// items cannot leave the boot. Cloning the `Arc` returned here is how a
    /// component keeps the table, and [`aggregate`](crate::health::aggregate)
    /// is what it can then call with it — which is what makes serving health a
    /// bundle's job rather than the kernel's.
    ///
    /// The same table is reachable from
    /// [`Container::extensions`](crate::container::Container::extensions), for
    /// a unit that holds a container and not a boot context.
    #[must_use]
    pub fn extensions(&self) -> &'a Arc<ExtensionPoints> {
        self.extensions
    }
}

impl fmt::Debug for BootContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootContext").finish_non_exhaustive()
    }
}

/// The tables a detached [`BootContext`] borrows, owned by the caller.
///
/// A boot context lends three tables and owns none, which is what lets
/// [`collect`](BootContext::collect) hand back borrowed items. Outside a
/// kernel nothing else owns them, so this does: hold it for as long as the
/// component under test is booting, and take a context from it with
/// [`context`](Self::context).
pub struct DetachedBoot {
    container: Container,
    dispatcher: Arc<EventDispatcher>,
    extensions: Arc<ExtensionPoints>,
}

impl DetachedBoot {
    /// A boot context over these tables.
    ///
    /// Callable as often as needed: every context reads the same tables, so
    /// two components booted from one detached set see one another's
    /// contributions exactly as they would inside a kernel.
    #[must_use]
    pub fn context(&self) -> BootContext<'_> {
        BootContext::new(&self.container, &self.dispatcher, &self.extensions)
    }

    /// The container the booting component resolves through.
    #[must_use]
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// The dispatcher the booting component emits through.
    #[must_use]
    pub fn dispatcher(&self) -> &Arc<EventDispatcher> {
        &self.dispatcher
    }

    /// The contribution table the booting component collects from.
    #[must_use]
    pub fn extensions(&self) -> &Arc<ExtensionPoints> {
        &self.extensions
    }
}

impl fmt::Debug for DetachedBoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DetachedBoot")
            .field("extensions", &self.extensions)
            .finish_non_exhaustive()
    }
}

/// Chooses what a [`DetachedBoot`] is made of.
///
/// Every part has a default that needs no kernel — an empty container, a
/// dispatcher with no listener, an empty configuration tree, a telemetry sink
/// that discards — so a test names only the parts it actually reads.
///
/// # Examples
///
/// ```
/// use kernel::core::Extension;
/// use kernel::BootContext;
///
/// /// The kind of value a bundle contributes.
/// struct Marker(&'static str);
///
/// impl Extension for Marker {}
///
/// let detached = BootContext::builder()
///     .with_contribution(Marker("one"))
///     .build();
/// let cx = detached.context();
///
/// assert_eq!(cx.collect::<Marker>().len(), 1);
/// ```
pub struct BootBuilder {
    container: Option<Container>,
    dispatcher: Option<Arc<EventDispatcher>>,
    config: Arc<ConfigTree>,
    telemetry: Arc<dyn Telemetry>,
    declared: Vec<ExtensionId>,
    contributions: Vec<ContributionEntry>,
}

impl BootBuilder {
    /// A builder with every default in place.
    #[must_use]
    pub fn new() -> Self {
        Self {
            container: None,
            dispatcher: None,
            config: Arc::new(ConfigTree::empty()),
            telemetry: Arc::new(NoopTelemetry),
            declared: Vec::new(),
            contributions: Vec::new(),
        }
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

    /// Emit through this dispatcher instead of one with no listener.
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

    /// Contribute one item to the point `X`, as a bundle would have.
    ///
    /// Declares the point too: a detached boot has no resolution behind it to
    /// catch a contribution to an undeclared point, so contributing is the
    /// declaration. Items come back out of
    /// [`collect`](BootContext::collect) in the order they were added here.
    #[must_use]
    pub fn with_contribution<X: Extension>(mut self, item: X) -> Self {
        let order = u32::try_from(self.contributions.len()).unwrap_or(u32::MAX);
        self.declared.push(ExtensionId::of::<X>());
        self.contributions.push(ContributionEntry {
            extension: ExtensionId::of::<X>(),
            bundle: "detached",
            order,
            item: Box::new(item),
        });
        self
    }

    /// Freezes the tables.
    ///
    /// The contribution table is frozen here and attached to the container, so
    /// a component that keeps [`Container::extensions`] reads the same list a
    /// component that called [`collect`](BootContext::collect) read.
    #[must_use]
    pub fn build(self) -> DetachedBoot {
        let Self {
            container,
            dispatcher,
            config,
            telemetry,
            declared,
            contributions,
        } = self;

        let contributed = !contributions.is_empty();
        let container = container.unwrap_or_else(|| {
            Container::new(Vec::new(), config, telemetry, KernelHandle::detached())
        });

        let (container, extensions) = if contributed {
            let table = Arc::new(ExtensionPoints::from_parts(declared, contributions));
            (container.with_extensions(Arc::clone(&table)), table)
        } else {
            let table = Arc::clone(container.extensions());
            (container, table)
        };

        // Attached only when it was built here: a dispatcher handed in is
        // already attached to whatever container it belongs to, and attaching
        // it twice is what the dispatcher reports as a defect.
        let dispatcher = match dispatcher {
            Some(dispatcher) => dispatcher,
            None => {
                let built = Arc::new(EventDispatcher::new(
                    Vec::new(),
                    Arc::clone(container.telemetry()),
                ));
                built.attach(container.clone());
                built
            }
        };

        DetachedBoot {
            container,
            dispatcher,
            extensions,
        }
    }
}

impl Default for BootBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for BootBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootBuilder")
            .field("container", &self.container.is_some())
            .field("contributions", &self.contributions.len())
            .finish_non_exhaustive()
    }
}

/// What a component is handed while it stops — at
/// [`drain`](Component::drain) and again at [`shutdown`](Component::shutdown).
///
/// One shape for both calls, because both read the same two facts and neither
/// reads anything else; [`shutdown`](Self::shutdown) is what tells them apart,
/// reporting `Draining` for the first and `Stopping` for the second.
///
/// It carries no extension points: collecting during shutdown would mean a
/// component is still assembling itself while the process is being torn down.
/// It carries two time facts instead, and they are not the same fact.
///
/// # The ladder says which stage, the deadline says how long
///
/// [`shutdown`](Self::shutdown) is the ladder shared by every unit: it reports
/// WHICH STAGE the kernel is in, and [`Shutdown::deadline`] is when that stage
/// ends. [`deadline`](Self::deadline) is this component's own: it reports HOW
/// LONG THIS COMPONENT HAS, counted from the moment its `shutdown` was called
/// and bounded by the budget actually enforced on it. The shutdown walk gives
/// each component a budget of its own, so from the second component onwards the
/// two values differ — reading the ladder's where the component's is meant is
/// what makes a component hurry for a deadline that is not being enforced on
/// it.
pub struct ShutdownContext<'a> {
    container: &'a Container,
    dispatcher: &'a EventDispatcher,
    shutdown: &'a Shutdown,
    deadline: Option<Instant>,
}

impl<'a> ShutdownContext<'a> {
    /// Assembles a shutdown context.
    ///
    /// What the kernel's shutdown walk calls when it bounds nothing. The
    /// container and the dispatcher it takes are built by resolution, so a
    /// caller outside the kernel reaches this through
    /// [`detached`](Self::detached), which owns them, or through
    /// [`builder`](Self::builder) when the test chooses them.
    pub fn new(
        container: &'a Container,
        dispatcher: &'a EventDispatcher,
        shutdown: &'a Shutdown,
    ) -> Self {
        Self {
            container,
            dispatcher,
            shutdown,
            deadline: None,
        }
    }

    /// Assembles a shutdown context carrying the budget enforced on this one
    /// component.
    ///
    /// What the kernel's shutdown walk uses: `deadline` is the instant the
    /// caller will cut this component off at, so
    /// [`deadline`](Self::deadline) reports the bound that is actually
    /// enforced rather than the ladder's.
    pub fn with_deadline(
        container: &'a Container,
        dispatcher: &'a EventDispatcher,
        shutdown: &'a Shutdown,
        deadline: Instant,
    ) -> Self {
        Self {
            container,
            dispatcher,
            shutdown,
            deadline: Some(deadline),
        }
    }

    /// The container.
    ///
    /// Still usable: components are stopped in reverse boot order, so anything
    /// this one depends on is still alive. Resolving something for the *first*
    /// time here will fail if the container has been sealed, which is the
    /// intent — shutdown is not the moment to discover a new dependency.
    #[must_use]
    pub fn container(&self) -> &Container {
        self.container
    }

    /// The configuration tree.
    #[must_use]
    pub fn config(&self) -> &ConfigTree {
        self.container.config()
    }

    /// The dispatcher.
    #[must_use]
    pub fn dispatcher(&self) -> &EventDispatcher {
        self.dispatcher
    }

    /// The telemetry sink every unit reports through.
    #[must_use]
    pub fn telemetry(&self) -> &Arc<dyn Telemetry> {
        self.container.telemetry()
    }

    /// The shutdown watcher: which STAGE is being run, and by when that stage
    /// must end.
    ///
    /// A property of the ladder, shared by every unit. For this component's own
    /// bound, read [`deadline`](Self::deadline).
    #[must_use]
    pub fn shutdown(&self) -> &Shutdown {
        self.shutdown
    }

    /// The instant this component's own shutdown must end by.
    ///
    /// A property of the UNIT: the budget the caller is enforcing on this one
    /// component, counted from the moment its `shutdown` was called. `None`
    /// when the caller bounds nothing, which is what a context built with
    /// [`new`](Self::new) reports. It is not [`Shutdown::deadline`] and will
    /// differ from it whenever another unit was stopped first.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Tables a component can be stopped against with no kernel behind it.
    ///
    /// Returns them with the [`ShutdownController`] that moves their ladder,
    /// exactly as [`RunContext::detached`](crate::runnable::RunContext::detached)
    /// does: a component whose `shutdown` reads
    /// [`shutdown`](Self::shutdown) is testable only if the test can move the
    /// stage.
    ///
    /// # Examples
    ///
    /// ```
    /// # use kernel::{BoxFuture, Component, ComponentDescriptor, ShutdownContext};
    /// # use kernel::core::{ComponentError, Stage};
    /// # use kernel::BootContext;
    /// # struct Holder;
    /// # impl Component for Holder {
    /// #     fn name() -> &'static str { "holder" }
    /// #     fn descriptor(&self) -> ComponentDescriptor { ComponentDescriptor::new() }
    /// #     fn boot<'a>(&'a self, _cx: &'a BootContext<'a>)
    /// #         -> BoxFuture<'a, Result<(), ComponentError>> { Box::pin(async { Ok(()) }) }
    /// # }
    /// # async fn probe() {
    /// let (detached, controller) = ShutdownContext::detached();
    /// controller.begin_stopping();
    ///
    /// let cx = detached.context();
    /// assert_eq!(cx.shutdown().stage(), Stage::Stopping);
    /// Holder.shutdown(&cx).await.expect("shutdown");
    /// # }
    /// ```
    #[must_use]
    pub fn detached() -> (DetachedShutdown, ShutdownController) {
        ShutdownBuilder::new().build()
    }

    /// The same tables, with the parts a test needs to choose.
    #[must_use]
    pub fn builder() -> ShutdownBuilder {
        ShutdownBuilder::new()
    }
}

impl fmt::Debug for ShutdownContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownContext")
            .field("stage", &self.shutdown.stage())
            .field("deadline", &self.deadline.is_some())
            .finish_non_exhaustive()
    }
}

/// The tables a detached [`ShutdownContext`] borrows, owned by the caller.
///
/// Holds the ladder's reading end too, so the context it hands out reports the
/// stage the [`ShutdownController`] returned beside it has reached.
pub struct DetachedShutdown {
    container: Container,
    dispatcher: Arc<EventDispatcher>,
    shutdown: Shutdown,
    deadline: Option<Instant>,
}

impl DetachedShutdown {
    /// A shutdown context over these tables.
    ///
    /// Carries the budget given to
    /// [`ShutdownBuilder::with_deadline`], and no budget at all when none was.
    #[must_use]
    pub fn context(&self) -> ShutdownContext<'_> {
        match self.deadline {
            Some(deadline) => ShutdownContext::with_deadline(
                &self.container,
                &self.dispatcher,
                &self.shutdown,
                deadline,
            ),
            None => ShutdownContext::new(&self.container, &self.dispatcher, &self.shutdown),
        }
    }

    /// The container the stopping component resolves through.
    #[must_use]
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// The dispatcher the stopping component emits through.
    #[must_use]
    pub fn dispatcher(&self) -> &Arc<EventDispatcher> {
        &self.dispatcher
    }

    /// The reading end of the ladder the context reports.
    #[must_use]
    pub fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }
}

impl fmt::Debug for DetachedShutdown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DetachedShutdown")
            .field("stage", &self.shutdown.stage())
            .field("deadline", &self.deadline.is_some())
            .finish_non_exhaustive()
    }
}

/// Chooses what a [`DetachedShutdown`] is made of.
///
/// Same defaults as [`BootBuilder`], plus the two facts a stopping component
/// reads: the policy the ladder's stages are budgeted with, and the budget
/// enforced on this one component.
pub struct ShutdownBuilder {
    container: Option<Container>,
    dispatcher: Option<Arc<EventDispatcher>>,
    config: Arc<ConfigTree>,
    telemetry: Arc<dyn Telemetry>,
    policy: ShutdownPolicy,
    deadline: Option<Instant>,
}

impl ShutdownBuilder {
    /// A builder with every default in place.
    #[must_use]
    pub fn new() -> Self {
        Self {
            container: None,
            dispatcher: None,
            config: Arc::new(ConfigTree::empty()),
            telemetry: Arc::new(NoopTelemetry),
            policy: ShutdownPolicy::default(),
            deadline: None,
        }
    }

    /// Resolve through this container instead of an empty one.
    ///
    /// As in [`BootBuilder::with_container`], the container's own
    /// configuration and telemetry win over the ones set here.
    #[must_use]
    pub fn with_container(mut self, container: Container) -> Self {
        self.container = Some(container);
        self
    }

    /// Emit through this dispatcher instead of one with no listener.
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

    /// Budget the ladder's stages the way this policy says.
    #[must_use]
    pub fn with_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Bound this one component at `deadline`.
    ///
    /// What [`ShutdownContext::deadline`] then reports — the budget enforced
    /// on this component, which is not the ladder's own.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Freezes the tables, and opens the ladder that drives them.
    #[must_use]
    pub fn build(self) -> (DetachedShutdown, ShutdownController) {
        let Self {
            container,
            dispatcher,
            config,
            telemetry,
            policy,
            deadline,
        } = self;

        let (controller, shutdown) = ShutdownController::new(policy);
        let container = container.unwrap_or_else(|| {
            Container::new(Vec::new(), config, telemetry, KernelHandle::detached())
        });
        let dispatcher = match dispatcher {
            Some(dispatcher) => dispatcher,
            None => {
                let built = Arc::new(EventDispatcher::new(
                    Vec::new(),
                    Arc::clone(container.telemetry()),
                ));
                built.attach(container.clone());
                built
            }
        };

        (
            DetachedShutdown {
                container,
                dispatcher,
                shutdown,
                deadline,
            },
            controller,
        )
    }
}

impl Default for ShutdownBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ShutdownBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownBuilder")
            .field("container", &self.container.is_some())
            .field("deadline", &self.deadline.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use std::sync::{Mutex, OnceLock};

    use kernel_core::{Health, HealthProbe, Stage};
    use tokio::time::Instant as TokioInstant;

    use super::*;
    use crate::health::{Probe, aggregate};

    struct Alpha(&'static str);

    impl Extension for Alpha {}

    struct Beta;

    impl Extension for Beta {}

    struct Holder {
        booted: AtomicUsize,
        drained: AtomicUsize,
        stopped: AtomicUsize,
        seen: AtomicUsize,
        /// The stage the drain call observed, `None` until it runs.
        at: Mutex<Option<Stage>>,
    }

    impl Holder {
        fn new() -> Self {
            Self {
                booted: AtomicUsize::new(0),
                drained: AtomicUsize::new(0),
                stopped: AtomicUsize::new(0),
                seen: AtomicUsize::new(0),
                at: Mutex::new(None),
            }
        }
    }

    impl Component for Holder {
        fn name() -> &'static str {
            "holder"
        }

        fn descriptor(&self) -> ComponentDescriptor {
            ComponentDescriptor::new()
        }

        fn boot<'a>(
            &'a self,
            cx: &'a BootContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async move {
                self.booted.fetch_add(1, Ordering::Relaxed);
                self.seen
                    .store(cx.collect::<Alpha>().len(), Ordering::Relaxed);
                Ok(())
            })
        }

        fn drain<'a>(
            &'a self,
            cx: &'a ShutdownContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async move {
                self.drained.fetch_add(1, Ordering::Relaxed);
                *self.at.lock().expect("stage") = Some(cx.shutdown().stage());
                Ok(())
            })
        }

        fn shutdown<'a>(
            &'a self,
            _cx: &'a ShutdownContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async move {
                self.stopped.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    struct Bare;

    impl Component for Bare {
        fn name() -> &'static str {
            "bare"
        }

        fn descriptor(&self) -> ComponentDescriptor {
            ComponentDescriptor::new()
        }

        fn boot<'a>(
            &'a self,
            _cx: &'a BootContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn container() -> Container {
        Container::new(
            Vec::new(),
            Arc::new(ConfigTree::empty()),
            Arc::new(NoopTelemetry),
            KernelHandle::detached(),
        )
    }

    fn dispatcher() -> EventDispatcher {
        EventDispatcher::new(Vec::new(), Arc::new(NoopTelemetry))
    }

    fn contributed(items: &[&'static str]) -> Arc<ExtensionPoints> {
        let contributions = items
            .iter()
            .enumerate()
            .map(|(order, label)| ContributionEntry {
                extension: ExtensionId::of::<Alpha>(),
                bundle: "one",
                order: u32::try_from(order).expect("small"),
                item: Box::new(Alpha(label)),
            })
            .collect();

        Arc::new(ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>(), ExtensionId::of::<Beta>()],
            contributions,
        ))
    }

    #[tokio::test]
    async fn boots_then_stops() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&[]);
        let (_controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());
        let holder = Holder::new();

        let boot = BootContext::new(&container, &dispatcher, &extensions);
        holder.boot(&boot).await.expect("boot");

        let stop = ShutdownContext::new(&container, &dispatcher, &shutdown);
        holder.shutdown(&stop).await.expect("shutdown");

        assert_eq!(holder.booted.load(Ordering::Relaxed), 1);
        assert_eq!(holder.stopped.load(Ordering::Relaxed), 1);
    }

    /// The hook the acceptor probe had no way to reach: a component sees the
    /// rung, not only the end.
    #[tokio::test]
    async fn drain_reads_the_rung() {
        let (detached, controller) = ShutdownContext::detached();
        let holder = Holder::new();

        controller.begin_draining();
        let cx = detached.context();
        holder.drain(&cx).await.expect("drain");

        assert_eq!(holder.drained.load(Ordering::Relaxed), 1);
        assert_eq!(holder.stopped.load(Ordering::Relaxed), 0);
        assert_eq!(*holder.at.lock().expect("stage"), Some(Stage::Draining));
    }

    #[tokio::test]
    async fn default_drain_succeeds() {
        let container = container();
        let dispatcher = dispatcher();
        let (_controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());
        let stop = ShutdownContext::new(&container, &dispatcher, &shutdown);

        // A component that accepts no work from outside implements nothing and
        // costs nothing.
        assert!(Bare.drain(&stop).await.is_ok());
    }

    #[tokio::test]
    async fn default_shutdown_succeeds() {
        let container = container();
        let dispatcher = dispatcher();
        let (_controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());
        let stop = ShutdownContext::new(&container, &dispatcher, &shutdown);

        assert!(Bare.shutdown(&stop).await.is_ok());
    }

    #[tokio::test]
    async fn collect_keeps_order() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&["one", "two", "three"]);
        let cx = BootContext::new(&container, &dispatcher, &extensions);

        let labels: Vec<&str> = cx.collect::<Alpha>().into_iter().map(|a| a.0).collect();

        assert_eq!(labels, ["one", "two", "three"]);
    }

    #[tokio::test]
    async fn collect_is_repeatable() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&["one", "two"]);
        let cx = BootContext::new(&container, &dispatcher, &extensions);

        // Borrowed items leave the table intact, so a second consumer of the
        // same point sees exactly what the first one saw.
        assert_eq!(cx.collect::<Alpha>().len(), 2);
        assert_eq!(cx.collect::<Alpha>().len(), 2);
    }

    #[tokio::test]
    async fn collect_outlives_the_call() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&["one"]);

        let kept: Vec<&Alpha> = {
            let cx = BootContext::new(&container, &dispatcher, &extensions);
            cx.collect::<Alpha>()
        };

        // The context is gone; the references are not, because they borrow the
        // table and not the context.
        assert_eq!(kept[0].0, "one");
    }

    #[tokio::test]
    async fn empty_point_collects_empty() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&["one"]);
        let cx = BootContext::new(&container, &dispatcher, &extensions);

        assert!(cx.collect::<Beta>().is_empty());
    }

    #[tokio::test]
    async fn boot_reads_the_component() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&["one", "two"]);
        let holder = Holder::new();

        let cx = BootContext::new(&container, &dispatcher, &extensions);
        holder.boot(&cx).await.expect("boot");

        assert_eq!(holder.seen.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn boots_behind_dyn() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&[]);
        let unit: Arc<dyn Component> = Arc::new(Holder::new());

        let cx = BootContext::new(&container, &dispatcher, &extensions);

        assert_eq!(Holder::name(), "holder");
        assert_eq!(unit.descriptor(), ComponentDescriptor::new());
        assert!(unit.boot(&cx).await.is_ok());
    }

    #[test]
    fn contexts_expose_the_container() {
        let container = container();
        let dispatcher = dispatcher();
        let extensions = contributed(&[]);
        let (controller, shutdown) = ShutdownController::new(ShutdownPolicy::default());

        let boot = BootContext::new(&container, &dispatcher, &extensions);
        assert!(!boot.container().is_sealed());
        assert!(!boot.handle().is_shutting_down());
        assert!(boot.config().get("nothing").is_none());
        let _ = boot.telemetry();
        let _ = boot.dispatcher();

        controller.begin_draining();
        let stop = ShutdownContext::new(&container, &dispatcher, &shutdown);
        assert_eq!(stop.shutdown().stage(), Stage::Draining);
        assert!(stop.config().get("nothing").is_none());
        let _ = stop.container();
        let _ = stop.telemetry();
        let _ = stop.dispatcher();

        assert!(format!("{boot:?}").contains("BootContext"));
        assert!(format!("{stop:?}").contains("Draining"));
    }

    // ----------------------------------------------------------------------
    // Booting and stopping a component with no kernel behind it
    // ----------------------------------------------------------------------

    /// Answers at once, so a report built from it is a report and not a wait.
    struct Ready;

    impl Extension for Ready {}

    impl HealthProbe for Ready {
        fn name(&self) -> &'static str {
            "ready"
        }

        fn check(&self) -> BoxFuture<'_, Health> {
            Box::pin(async { Health::Up })
        }
    }

    /// Keeps the contribution table past its own boot, which is what a unit
    /// that answers a health request has to do.
    struct Vitals {
        points: OnceLock<Arc<ExtensionPoints>>,
    }

    impl Vitals {
        fn new() -> Self {
            Self {
                points: OnceLock::new(),
            }
        }

        fn kept(&self) -> Arc<ExtensionPoints> {
            Arc::clone(self.points.get().expect("boot ran"))
        }
    }

    impl Component for Vitals {
        fn name() -> &'static str {
            "vitals"
        }

        fn descriptor(&self) -> ComponentDescriptor {
            ComponentDescriptor::new()
        }

        fn boot<'a>(
            &'a self,
            cx: &'a BootContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async move {
                let _ = self.points.set(Arc::clone(cx.container().extensions()));
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn detached_boots_component() {
        let detached = BootContext::detached();
        let holder = Holder::new();

        holder.boot(&detached.context()).await.expect("boot");

        assert_eq!(holder.booted.load(Ordering::Relaxed), 1);
        assert_eq!(holder.seen.load(Ordering::Relaxed), 0);
        assert!(detached.extensions().collect::<Alpha>().is_empty());
        assert!(format!("{detached:?}").contains("DetachedBoot"));
    }

    #[tokio::test]
    async fn builder_feeds_contributions() {
        let detached = BootContext::builder()
            .with_contribution(Alpha("one"))
            .with_contribution(Alpha("two"))
            .with_contribution(Beta)
            .build();
        let holder = Holder::new();

        holder.boot(&detached.context()).await.expect("boot");

        assert_eq!(holder.seen.load(Ordering::Relaxed), 2);
        assert_eq!(detached.context().collect::<Beta>().len(), 1);
        assert!(format!("{:?}", BootContext::builder()).contains("BootBuilder"));
    }

    // The gap this closes: the table has to outlive the boot call, because a
    // health request is answered long after boot returned.
    #[tokio::test]
    async fn component_keeps_points() {
        let detached = BootContext::builder()
            .with_contribution(Probe::new(Ready))
            .build();
        let vitals = Vitals::new();

        vitals.boot(&detached.context()).await.expect("boot");
        let kept = vitals.kept();
        drop(detached);

        // Everything the boot call lent is gone; the table is not.
        let report = aggregate(&kept).await;

        assert_eq!(report.overall, Health::Up);
        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].0, "ready");
    }

    #[tokio::test]
    async fn context_lends_the_table() {
        let detached = BootContext::builder()
            .with_contribution(Probe::new(Ready))
            .build();

        let kept: Arc<ExtensionPoints> = {
            let cx = detached.context();
            Arc::clone(cx.extensions())
        };

        assert_eq!(aggregate(&kept).await.probes.len(), 1);
    }

    #[tokio::test]
    async fn builder_takes_container() {
        let detached = BootContext::builder()
            .with_container(container())
            .with_dispatcher(Arc::new(dispatcher()))
            .build();
        let cx = detached.context();

        assert!(!cx.container().is_sealed());
        assert!(cx.config().get("nothing").is_none());
        assert!(cx.collect::<Alpha>().is_empty());
        let _ = detached.dispatcher();
    }

    /// Counts the records a unit reports.
    #[derive(Default)]
    struct Counting(AtomicUsize);

    impl Telemetry for Counting {
        fn record(&self, _record: kernel_core::Record) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn tree() -> ConfigTree {
        let mut config = ConfigTree::empty();
        config
            .insert("limits.batch", kernel_core::ConfigNode::from(9_i64))
            .expect("insert");
        config
    }

    #[tokio::test]
    async fn boot_builder_carries_parts() {
        let telemetry = Arc::new(Counting::default());
        let detached = BootBuilder::default()
            .with_config(tree())
            .with_telemetry(Arc::clone(&telemetry) as Arc<dyn Telemetry>)
            .build();
        let cx = detached.context();

        cx.telemetry().record(kernel_core::Record::new(
            kernel_core::Level::Info,
            "probe.booted",
        ));

        assert!(cx.config().get("limits.batch").is_some());
        assert_eq!(telemetry.0.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn stop_builder_carries_parts() {
        let telemetry = Arc::new(Counting::default());
        let dispatcher = Arc::new(dispatcher());
        let (detached, _controller) = ShutdownBuilder::default()
            .with_config(tree())
            .with_telemetry(Arc::clone(&telemetry) as Arc<dyn Telemetry>)
            .with_dispatcher(Arc::clone(&dispatcher))
            .build();
        let cx = detached.context();

        cx.telemetry().record(kernel_core::Record::new(
            kernel_core::Level::Info,
            "probe.stopped",
        ));

        assert!(cx.config().get("limits.batch").is_some());
        assert_eq!(telemetry.0.load(Ordering::Relaxed), 1);
        assert!(core::ptr::eq(cx.dispatcher(), Arc::as_ptr(&dispatcher)));
        assert!(Arc::ptr_eq(detached.dispatcher(), &dispatcher));
    }

    #[tokio::test]
    async fn detached_stops_component() {
        let (detached, controller) = ShutdownContext::detached();
        let holder = Holder::new();

        controller.begin_draining();
        let cx = detached.context();

        assert_eq!(cx.shutdown().stage(), Stage::Draining);
        assert_eq!(cx.deadline(), None);
        holder.shutdown(&cx).await.expect("shutdown");
        assert_eq!(holder.stopped.load(Ordering::Relaxed), 1);
        assert_eq!(detached.shutdown().stage(), Stage::Draining);
        assert!(format!("{detached:?}").contains("Draining"));
    }

    #[tokio::test(start_paused = true)]
    async fn builder_bounds_the_unit() {
        let deadline = TokioInstant::now().into_std() + core::time::Duration::from_secs(2);
        let (detached, controller) = ShutdownContext::builder()
            .with_container(container())
            .with_policy(ShutdownPolicy::new(
                core::time::Duration::from_secs(1),
                core::time::Duration::from_secs(4),
            ))
            .with_deadline(deadline)
            .build();

        controller.begin_stopping();
        let cx = detached.context();

        // The unit's own budget, and the ladder's: two facts, not one.
        assert_eq!(cx.deadline(), Some(deadline));
        assert_ne!(cx.shutdown().deadline(), cx.deadline());
        assert!(!cx.container().handle().is_shutting_down());
        let _ = detached.container();
        assert!(format!("{:?}", ShutdownContext::builder()).contains("ShutdownBuilder"));
    }
}
