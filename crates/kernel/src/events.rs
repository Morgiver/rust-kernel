//! The kernel's own lifecycle events.
//!
//! Every phase transition is published here. This is the main decoupling lever
//! of the design and it is first class: a bundle observes the kernel by
//! listening, never by being called back through a bespoke hook.
//!
//! All of them are notifications — [`crate::EventDispatcher::emit`] — except
//! [`ShutdownRequested`], which is dispatched so a listener can add context
//! before the ladder starts moving.

use core::time::Duration;

use kernel_core::{ComponentId, Event, RunnableId};

/// A bundle has filled the registry.
#[derive(Debug, Clone)]
pub struct BundleRegistered {
    /// The bundle that just registered.
    pub bundle: &'static str,
}

/// The graph closed: every contract is satisfied and the order is known.
#[derive(Debug, Clone)]
pub struct GraphResolved {
    /// How many bindings the container holds.
    pub bindings: usize,
    /// How many components the plan will boot.
    pub components: usize,
    /// How many runnables the supervisor will start.
    pub runnables: usize,
}

/// Phase four is starting.
#[derive(Debug, Clone)]
pub struct BootStarted {
    /// How many components are about to boot.
    pub components: usize,
}

/// One component finished booting.
#[derive(Debug, Clone)]
pub struct ComponentBooted {
    /// Which component.
    pub component: ComponentId,
    /// How long its `boot` took.
    pub elapsed: Duration,
}

/// Every component has booted.
#[derive(Debug, Clone)]
pub struct BootCompleted {
    /// How long the whole phase took.
    pub elapsed: Duration,
}

/// The supervisor has started every runnable.
#[derive(Debug, Clone)]
pub struct Running {
    /// How many runnables were started.
    pub runnables: usize,
}

/// Why the kernel is stopping.
#[derive(Debug, Clone)]
pub enum ShutdownReason {
    /// The process received a stop signal.
    Signal,
    /// Someone called [`crate::KernelHandle::shutdown`].
    Programmatic,
    /// An essential runnable returned, whatever its result.
    EssentialFinished(RunnableId),
    /// Every runnable returned on its own.
    Completed,
}

/// Someone asked the kernel to stop.
///
/// Dispatched rather than emitted: a listener may enrich it before the shutdown
/// ladder moves.
#[derive(Debug, Clone)]
pub struct ShutdownRequested {
    /// What triggered the request.
    pub reason: ShutdownReason,
    /// Free-form context added by listeners, in priority order.
    pub notes: Vec<String>,
}

/// The kernel stopped accepting new work.
#[derive(Debug, Clone)]
pub struct Draining;

/// Work in flight must now end.
#[derive(Debug, Clone)]
pub struct Stopping;

/// The kernel is down.
#[derive(Debug, Clone)]
pub struct Stopped {
    /// How many runnables failed to return before their deadline.
    pub abandoned: usize,
}

macro_rules! event {
    ($ty:ty, $name:literal) => {
        impl Event for $ty {
            const NAME: &'static str = $name;
        }
    };
}

event!(BundleRegistered, "kernel.bundle_registered");
event!(GraphResolved, "kernel.graph_resolved");
event!(BootStarted, "kernel.boot_started");
event!(ComponentBooted, "kernel.component_booted");
event!(BootCompleted, "kernel.boot_completed");
event!(Running, "kernel.running");
event!(ShutdownRequested, "kernel.shutdown_requested");
event!(Draining, "kernel.draining");
event!(Stopping, "kernel.stopping");
event!(Stopped, "kernel.stopped");
