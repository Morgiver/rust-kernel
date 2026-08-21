//! Components: the units whose lifecycle the kernel owns, and the two contexts
//! they are handed.
//!
//! # A component has a lifecycle; a runnable is what runs
//!
//! This module and [`crate::runnable`] exist to keep those two apart, and the
//! separation is operational rather than philosophical:
//!
//! * A **component** is instantiated in a known order, told to
//!   [`boot`](Component::boot), and later told to
//!   [`shutdown`](Component::shutdown). It typically owns a resource — a pool,
//!   a connection, a cache — and the kernel guarantees when that resource is
//!   opened and when it is closed. Everything the kernel knows about a
//!   component is its identity and those two moments.
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
    BoxFuture, ComponentDescriptor, ComponentError, ConfigTree, Extension, Telemetry,
};

use crate::container::Container;
use crate::dispatcher::EventDispatcher;
use crate::extension::ExtensionPoints;
use crate::shutdown::{KernelHandle, Shutdown};

/// A unit whose lifecycle the kernel manages.
///
/// The kernel instantiates a component in dependency order, calls
/// [`boot`](Self::boot) on it, and — in reverse order, when the process stops —
/// calls [`shutdown`](Self::shutdown). Nothing else about the type is the
/// kernel's business.
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

    /// Release what [`boot`](Self::boot) acquired.
    ///
    /// Called in reverse boot order, so a component may still use everything it
    /// depended on. The default does nothing and succeeds, because a component
    /// that owns no resource has nothing to release.
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
    extensions: &'a ExtensionPoints,
}

impl<'a> BootContext<'a> {
    /// Assembles a boot context over the three tables a booting component may
    /// read.
    ///
    /// Public so that a component can be booted outside a kernel — with a
    /// container, a dispatcher and an extension table assembled by the caller.
    pub fn new(
        container: &'a Container,
        dispatcher: &'a EventDispatcher,
        extensions: &'a ExtensionPoints,
    ) -> Self {
        Self {
            container,
            dispatcher,
            extensions,
        }
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
}

impl fmt::Debug for BootContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootContext").finish_non_exhaustive()
    }
}

/// What a component is handed while it stops.
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
    /// Public for the same reason [`BootContext::new`] is: a component can be
    /// stopped outside a kernel.
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
}

impl fmt::Debug for ShutdownContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownContext")
            .field("stage", &self.shutdown.stage())
            .field("deadline", &self.deadline.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use kernel_core::{ExtensionId, NoopTelemetry, ShutdownPolicy, Stage};

    use super::*;
    use crate::registry::ContributionEntry;
    use crate::shutdown::ShutdownController;

    struct Alpha(&'static str);

    impl Extension for Alpha {}

    struct Beta;

    impl Extension for Beta {}

    struct Holder {
        booted: AtomicUsize,
        stopped: AtomicUsize,
        seen: AtomicUsize,
    }

    impl Holder {
        fn new() -> Self {
            Self {
                booted: AtomicUsize::new(0),
                stopped: AtomicUsize::new(0),
                seen: AtomicUsize::new(0),
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

    fn contributed(items: &[&'static str]) -> ExtensionPoints {
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

        ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>(), ExtensionId::of::<Beta>()],
            contributions,
        )
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
}
