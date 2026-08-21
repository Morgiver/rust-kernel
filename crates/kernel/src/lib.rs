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

pub use bundle::Bundle;
pub use component::{BootContext, Component, ShutdownContext};
pub use config::{ConfigChain, EnvSource, MemorySource};
pub use container::{Container, Scope};
pub use dispatcher::{Dispatched, EventDispatcher, Listener, ListenerContext};
pub use events::{
    BootCompleted, BootStarted, BundleRegistered, ComponentBooted, Draining, GraphResolved,
    Running, ShutdownReason, ShutdownRequested, Stopped, Stopping,
};
pub use extension::ExtensionPoints;
pub use health::{HealthReport, PROBE_TIMEOUT, Probe, aggregate};
pub use kernel::{Kernel, KernelBuilder};
pub use provider::{Binding, BuildFn, Provider};
pub use registry::Registry;
pub use runnable::{RunContext, Runnable};
pub use shutdown::{KernelHandle, Shutdown, ShutdownController};

pub use kernel_core::{
    BoxFuture, BundleManifest, ComponentDescriptor, ContractRef, Criticality, Event, Extension,
    Flow, Lifetime, Outcome, Priority, RestartPolicy, RunnableDescriptor, ShutdownPolicy, Stage,
    Telemetry,
};
