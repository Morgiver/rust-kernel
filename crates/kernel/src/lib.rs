//! The kernel: a state machine that owns an object graph and a set of
//! long-running tasks, and guarantees the order in which they are born, run
//! and die.
//!
//! This crate holds the registration surface, the container, the graph
//! validation and the lifecycle drivers. It depends on an async runtime for
//! spawning, timers and signals — but it never creates one: the application
//! owns its `main` and its runtime, and awaits the kernel as a future.
//!
//! The runtime-free surfaces a `*-contracts` crate needs live in
//! [`kernel_core`], re-exported here as [`core`].

mod boot;
pub mod bundle;
pub mod component;
pub mod config;
pub mod container;
pub mod dispatcher;
pub mod events;
pub mod extension;
pub mod health;
pub mod kernel;
pub mod provider;
pub mod registry;
mod resolve;
pub mod runnable;
pub mod shutdown;
mod supervisor;

pub use kernel_core as core;
pub use kernel_core::future::{YieldNow, yield_now};

pub use bundle::{Bundle, FnBundle};
pub use component::{
    BootBuilder, BootContext, Component, DetachedBoot, DetachedShutdown, ShutdownBuilder,
    ShutdownContext,
};
pub use config::{ConfigChain, EnvSource, MemorySource};
pub use container::{Container, Scope};
pub use dispatcher::{
    DetachedListen, Dispatched, EventDispatcher, ListenBuilder, Listener, ListenerContext,
};
pub use events::{
    BootCompleted, BootStarted, BundleRegistered, ComponentBooted, Draining, GraphResolved,
    Running, ShutdownReason, ShutdownRequested, Stopped, Stopping,
};
pub use extension::ExtensionPoints;
pub use health::{HealthReport, PROBE_TIMEOUT, Probe, aggregate};
pub use kernel::{Ending, Kernel, KernelBuilder};
pub use provider::{Binding, BuildFn, Provider};
pub use registry::{Listening, Registry};
pub use runnable::{Children, RunBuilder, RunContext, Runnable, Spawned};
pub use shutdown::{Budget, KernelHandle, Shutdown, ShutdownController, Tick};

pub use kernel_core::{
    AbortReason, Aborted, BoxFuture, BundleManifest, ComponentDescriptor, ContractRef, Criticality,
    Event, Extension, Flow, Lifetime, Outcome, Priority, RestartPolicy, RunnableDescriptor,
    ShutdownPolicy, Stage, Telemetry,
};
