//! Runtime-free surfaces of the kernel.
//!
//! This crate holds everything a `*-contracts` crate may legitimately need:
//! identities, the error model, the configuration tree, telemetry, health and
//! the descriptors that units attach to themselves. It depends on no async
//! runtime and on no serialization library, which is what keeps contract
//! crates light and stable.
//!
//! Nothing here names a domain entity, a transport or a technology.

pub mod config;
pub mod descriptor;
pub mod error;
pub mod event;
pub mod extension;
pub mod future;
pub mod health;
pub mod id;
pub mod telemetry;

pub use config::{ConfigNode, ConfigSource, ConfigTree, FromConfig, Scalar, Secret};
pub use descriptor::{
    Backoff, BundleManifest, ComponentDescriptor, Criticality, Lifetime, Priority, RestartPolicy,
    RunnableDescriptor, ShutdownPolicy, Stage,
};
pub use error::{
    BuildError, ComponentError, ConfigError, DispatchError, KernelError, ListenerError, Outcome,
    RegisterError, ResolveError, RunError, ShutdownError,
};
pub use event::{Event, Flow};
pub use extension::Extension;
pub use future::BoxFuture;
pub use health::{Health, HealthProbe};
pub use id::{BundleId, ComponentId, ContractId, ContractRef, ExtensionId, RunnableId};
pub use telemetry::{
    Field, FieldValue, Level, NoopTelemetry, Record, RecordingTelemetry, StderrTelemetry, Telemetry,
};
