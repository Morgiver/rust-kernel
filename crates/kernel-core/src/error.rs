//! The error model: one error type per lifecycle phase, aggregation over
//! first-failure, and no foreign error type ever named here.
//!
//! Everything that comes from outside the kernel arrives as a [`BoxSource`] and
//! is wrapped with the identity of the failing unit and the phase it failed in.
//! The kernel therefore never has to name an error type it does not own.
//!
//! The aggregate type is [`KernelError`]. Its [`Display`](core::fmt::Display)
//! prints *every* collected error, one per line. A start-up that reveals one
//! error at a time is the defect this model exists to prevent.

use core::fmt;
use std::process::ExitCode;

use crate::id::{ComponentId, ContractRef, ExtensionId, RunnableId};

/// A foreign error, type-erased.
///
/// This is the only shape in which an error produced outside the kernel enters
/// it. Nothing else about a foreign error is ever assumed.
pub type BoxSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Renders a contract reference for human-readable messages.
fn write_contract(f: &mut fmt::Formatter<'_>, contract: &ContractRef) -> fmt::Result {
    match contract.name() {
        Some(name) => write!(f, "{}#{}", contract.type_name(), name),
        None => f.write_str(contract.type_name()),
    }
}

/// Writes an aggregate: a header line, then one line per collected error.
///
/// An empty aggregate has no error to count, so it renders without one.
fn write_aggregate<E: fmt::Display>(
    f: &mut fmt::Formatter<'_>,
    phase: &str,
    errors: &[E],
) -> fmt::Result {
    if errors.is_empty() {
        return write!(f, "{phase} failed, with no error reported");
    }
    write!(f, "{phase} failed with {} error(s):", errors.len())?;
    for error in errors {
        write!(f, "\n  - {error}")?;
    }
    Ok(())
}

// --------------------------------------------------------------------------
// Configuration
// --------------------------------------------------------------------------

/// What went wrong at one configuration path.
#[derive(Debug)]
pub enum ConfigErrorKind {
    /// The path carries no value and no default is available.
    Missing,
    /// A value is present but has the wrong shape.
    TypeMismatch {
        /// The shape the reader required.
        expected: &'static str,
        /// The shape actually found.
        found: &'static str,
    },
    /// A value has the right shape but an unacceptable content.
    Invalid(String),
    /// A source refused to produce or parse the value.
    Source(BoxSource),
}

impl fmt::Display for ConfigErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("missing required value"),
            Self::TypeMismatch { expected, found } => {
                write!(f, "expected {expected}, found {found}")
            }
            Self::Invalid(detail) => write!(f, "invalid value: {detail}"),
            Self::Source(source) => write!(f, "{source}"),
        }
    }
}

/// A configuration failure, located at a dotted path.
#[derive(Debug)]
pub struct ConfigError {
    path: String,
    kind: ConfigErrorKind,
}

impl ConfigError {
    /// No value at `path`.
    pub fn missing(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            kind: ConfigErrorKind::Missing,
        }
    }

    /// A value of the wrong shape at `path`.
    pub fn type_mismatch(
        path: impl Into<String>,
        expected: &'static str,
        found: &'static str,
    ) -> Self {
        Self {
            path: path.into(),
            kind: ConfigErrorKind::TypeMismatch { expected, found },
        }
    }

    /// A value whose content is unacceptable at `path`.
    pub fn invalid(path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            kind: ConfigErrorKind::Invalid(detail.into()),
        }
    }

    /// A foreign failure while reading `path`.
    pub fn source_error(path: impl Into<String>, source: BoxSource) -> Self {
        Self {
            path: path.into(),
            kind: ConfigErrorKind::Source(source),
        }
    }

    /// The dotted path the failure is attached to.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// What went wrong at that path.
    pub fn kind(&self) -> &ConfigErrorKind {
        &self.kind
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "config `{}`: {}", self.path, self.kind)
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ConfigErrorKind::Source(source) => Some(&**source),
            _ => None,
        }
    }
}

// --------------------------------------------------------------------------
// Registration and construction
// --------------------------------------------------------------------------

/// A bundle failed while registering what it offers.
#[derive(Debug)]
pub struct RegisterError {
    bundle: &'static str,
    source: BoxSource,
}

impl RegisterError {
    /// Wraps a foreign failure with the identity of the registering bundle.
    pub fn new(bundle: &'static str, source: BoxSource) -> Self {
        Self { bundle, source }
    }

    /// The bundle that failed to register.
    pub fn bundle(&self) -> &'static str {
        self.bundle
    }

    /// The wrapped foreign failure.
    ///
    /// Named `cause` so that it does not shadow
    /// [`std::error::Error::source`], whose return type differs.
    pub fn cause(&self) -> &BoxSource {
        &self.source
    }
}

impl fmt::Display for RegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bundle `{}` failed to register: {}",
            self.bundle, self.source
        )
    }
}

impl std::error::Error for RegisterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

/// A provider failed while building the value behind a contract.
#[derive(Debug)]
pub struct BuildError {
    contract: &'static str,
    source: BoxSource,
}

impl BuildError {
    /// Wraps a foreign failure with the identity of the contract being built.
    pub fn new(contract: &'static str, source: BoxSource) -> Self {
        Self { contract, source }
    }

    /// The contract whose provider failed.
    pub fn contract(&self) -> &'static str {
        self.contract
    }

    /// The wrapped foreign failure.
    ///
    /// Named `cause` so that it does not shadow
    /// [`std::error::Error::source`], whose return type differs.
    pub fn cause(&self) -> &BoxSource {
        &self.source
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to build `{}`: {}", self.contract, self.source)
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

// --------------------------------------------------------------------------
// Boot and shutdown of components
// --------------------------------------------------------------------------

/// A component failed to boot or to stop.
#[derive(Debug)]
pub struct ComponentError {
    component: ComponentId,
    source: BoxSource,
}

impl ComponentError {
    /// Wraps a foreign failure with the identity of the component.
    pub fn new(component: ComponentId, source: BoxSource) -> Self {
        Self { component, source }
    }

    /// The component that failed.
    pub fn component(&self) -> ComponentId {
        self.component
    }

    /// The wrapped foreign failure.
    ///
    /// Named `cause` so that it does not shadow
    /// [`std::error::Error::source`], whose return type differs.
    pub fn cause(&self) -> &BoxSource {
        &self.source
    }
}

impl fmt::Display for ComponentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "component {} failed: {}", self.component, self.source)
    }
}

impl std::error::Error for ComponentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

/// How a supervised runnable ended when it was not supposed to.
#[derive(Debug)]
pub enum RunErrorKind {
    /// The runnable returned a failure of its own.
    Failed(BoxSource),
    /// The runnable unwound; the payload is the recovered message.
    Panicked(String),
    /// The runnable was cancelled before it could finish.
    Cancelled,
    /// The runnable outlived the time allowed to it.
    DeadlineExceeded,
}

impl fmt::Display for RunErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(source) => write!(f, "{source}"),
            Self::Panicked(message) => write!(f, "panicked: {message}"),
            Self::Cancelled => f.write_str("cancelled"),
            Self::DeadlineExceeded => f.write_str("deadline exceeded"),
        }
    }
}

/// A supervised runnable ended abnormally.
#[derive(Debug)]
pub struct RunError {
    runnable: RunnableId,
    kind: RunErrorKind,
}

impl RunError {
    /// Wraps an abnormal ending with the identity of the runnable.
    pub fn new(runnable: RunnableId, kind: RunErrorKind) -> Self {
        Self { runnable, kind }
    }

    /// The runnable returned a failure of its own.
    pub fn failed(runnable: RunnableId, source: BoxSource) -> Self {
        Self::new(runnable, RunErrorKind::Failed(source))
    }

    /// The runnable unwound.
    pub fn panicked(runnable: RunnableId, message: impl Into<String>) -> Self {
        Self::new(runnable, RunErrorKind::Panicked(message.into()))
    }

    /// The runnable was cancelled.
    pub fn cancelled(runnable: RunnableId) -> Self {
        Self::new(runnable, RunErrorKind::Cancelled)
    }

    /// The runnable outlived the time allowed to it.
    pub fn deadline_exceeded(runnable: RunnableId) -> Self {
        Self::new(runnable, RunErrorKind::DeadlineExceeded)
    }

    /// The runnable that ended abnormally.
    pub fn runnable(&self) -> RunnableId {
        self.runnable
    }

    /// How it ended.
    pub fn kind(&self) -> &RunErrorKind {
        &self.kind
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "runnable {} failed: {}", self.runnable, self.kind)
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            RunErrorKind::Failed(source) => Some(&**source),
            _ => None,
        }
    }
}

/// How a unit failed while stopping.
#[derive(Debug)]
pub enum ShutdownErrorKind {
    /// The unit returned a failure of its own.
    Failed(BoxSource),
    /// The unit did not stop within the time allowed to it.
    DeadlineExceeded,
}

impl fmt::Display for ShutdownErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(source) => write!(f, "{source}"),
            Self::DeadlineExceeded => f.write_str("deadline exceeded"),
        }
    }
}

/// A unit failed while stopping.
///
/// A shutdown failure never prevents the other units from stopping; it only
/// influences the exit code.
#[derive(Debug)]
pub struct ShutdownError {
    unit: &'static str,
    kind: ShutdownErrorKind,
}

impl ShutdownError {
    /// Wraps a stopping failure with the identity of the unit.
    pub fn new(unit: &'static str, kind: ShutdownErrorKind) -> Self {
        Self { unit, kind }
    }

    /// The unit returned a failure of its own.
    pub fn failed(unit: &'static str, source: BoxSource) -> Self {
        Self::new(unit, ShutdownErrorKind::Failed(source))
    }

    /// The unit did not stop in time.
    pub fn deadline_exceeded(unit: &'static str) -> Self {
        Self::new(unit, ShutdownErrorKind::DeadlineExceeded)
    }

    /// The unit that failed to stop.
    pub fn unit(&self) -> &'static str {
        self.unit
    }

    /// How it failed.
    pub fn kind(&self) -> &ShutdownErrorKind {
        &self.kind
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}` failed to stop: {}", self.unit, self.kind)
    }
}

impl std::error::Error for ShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ShutdownErrorKind::Failed(source) => Some(&**source),
            ShutdownErrorKind::DeadlineExceeded => None,
        }
    }
}

// --------------------------------------------------------------------------
// Events
// --------------------------------------------------------------------------

/// A listener failed while handling an event.
#[derive(Debug)]
pub struct ListenerError {
    event: &'static str,
    source: BoxSource,
}

impl ListenerError {
    /// Wraps a foreign failure with the name of the event being handled.
    pub fn new(event: &'static str, source: BoxSource) -> Self {
        Self { event, source }
    }

    /// The event that was being handled.
    pub fn event(&self) -> &'static str {
        self.event
    }

    /// The wrapped foreign failure.
    ///
    /// Named `cause` so that it does not shadow
    /// [`std::error::Error::source`], whose return type differs.
    pub fn cause(&self) -> &BoxSource {
        &self.source
    }
}

impl fmt::Display for ListenerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "listener for `{}` failed: {}", self.event, self.source)
    }
}

impl std::error::Error for ListenerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

/// A dispatch was interrupted because one of its listeners failed.
#[derive(Debug)]
pub struct DispatchError {
    event: &'static str,
    source: ListenerError,
}

impl DispatchError {
    /// Wraps the listener failure that interrupted the dispatch.
    pub fn new(event: &'static str, source: ListenerError) -> Self {
        Self { event, source }
    }

    /// The event whose dispatch was interrupted.
    pub fn event(&self) -> &'static str {
        self.event
    }

    /// The listener failure that interrupted it.
    ///
    /// Named `cause` so that it does not shadow
    /// [`std::error::Error::source`], whose return type differs.
    pub fn cause(&self) -> &ListenerError {
        &self.source
    }
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dispatch of `{}` failed: {}", self.event, self.source)
    }
}

impl std::error::Error for DispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

// --------------------------------------------------------------------------
// Resolution
// --------------------------------------------------------------------------

/// The wiring graph could not be closed.
///
/// Every variant is a statement about the graph alone: no value has been built
/// when one of these is produced.
#[derive(Debug)]
pub enum ResolveError {
    /// Something requires a contract nothing provides.
    MissingContract {
        /// The unit that stated the requirement.
        required_by: &'static str,
        /// The contract nothing provides.
        contract: ContractRef,
    },
    /// Two units claim the default implementation of one contract.
    DuplicateDefault {
        /// The contested contract.
        contract: ContractRef,
        /// The first claimant, in registration order.
        first: &'static str,
        /// The second claimant.
        second: &'static str,
    },
    /// Two units claim the same name for one contract.
    DuplicateNamed {
        /// The contested contract.
        contract: ContractRef,
        /// The contested name.
        name: &'static str,
        /// The first claimant, in registration order.
        first: &'static str,
        /// The second claimant.
        second: &'static str,
    },
    /// The dependency graph contains a cycle.
    Cycle {
        /// The full cycle, with the first node repeated at the end, so the
        /// rendering reads as a closed walk.
        path: Vec<ContractRef>,
    },
    /// A contribution was offered to an extension point nobody declared.
    UndeclaredExtensionPoint {
        /// The extension point that was targeted.
        extension: ExtensionId,
        /// The unit that offered the contribution.
        contributed_by: &'static str,
    },
    /// What a bundle registered does not match what its manifest declares.
    ManifestMismatch {
        /// The bundle whose manifest is wrong.
        bundle: &'static str,
        /// The contract the manifest declares.
        declared: ContractRef,
    },
    /// The bundle ordering constraints contain a cycle.
    ///
    /// Distinct from [`ResolveError::Cycle`], which walks contracts: a bundle
    /// is named by a string, not by a type, so it cannot be a [`ContractRef`].
    BundleCycle {
        /// The full cycle, with the first bundle repeated at the end, so the
        /// rendering reads as a closed walk.
        path: Vec<&'static str>,
    },
    /// A bundle asks to be ordered after a bundle that is not registered.
    UnknownBundleOrder {
        /// The bundle stating the constraint.
        bundle: &'static str,
        /// The unknown bundle it names.
        after: &'static str,
    },
    /// Two bundles registered under one name.
    ///
    /// The name is what an ordering constraint targets and what every other
    /// variant here blames, so sharing one makes the ordering ambiguous and the
    /// attribution wrong.
    DuplicateBundle {
        /// The contested name.
        name: &'static str,
    },
    /// Something that is not scoped requires something that is.
    ///
    /// A value that is not built per scope is built where there is no scope, so
    /// a scoped requirement it states can never be satisfied: the resolution
    /// that would satisfy it has no scope to be built in. That holds for a
    /// `Shared` binding, for a `Factory` one, and for a listener, which binds
    /// nothing at all and resolves during dispatch — outside every scope by
    /// construction.
    ///
    /// Every fact the check needs — the lifetime of each binding and the
    /// requirement between them — is declared, so this is a statement about the
    /// graph and not about a build that failed.
    LifetimeConflict {
        /// What states the requirement: a binding's contract type, or the event
        /// a listener handles.
        ///
        /// A string rather than a [`ContractRef`], for the same reason
        /// [`MissingContract`](Self::MissingContract) uses one: a listener
        /// binds no contract, and the event it handles is the only identity it
        /// declares.
        required_by: &'static str,
        /// The scoped contract it requires.
        requires: ContractRef,
        /// The bundle that registered the requirer.
        bundle: &'static str,
    },
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingContract {
                required_by,
                contract,
            } => {
                write!(f, "no provider for ")?;
                write_contract(f, contract)?;
                write!(f, ", required by `{required_by}`")
            }
            Self::DuplicateDefault {
                contract,
                first,
                second,
            } => {
                write!(f, "duplicate default provider for ")?;
                write_contract(f, contract)?;
                write!(f, ": `{first}` and `{second}`")
            }
            Self::DuplicateNamed {
                contract,
                name,
                first,
                second,
            } => {
                write!(f, "duplicate provider named `{name}` for ")?;
                write_contract(f, contract)?;
                write!(f, ": `{first}` and `{second}`")
            }
            Self::Cycle { path } => {
                f.write_str("dependency cycle: ")?;
                for (position, contract) in path.iter().enumerate() {
                    if position > 0 {
                        f.write_str(" -> ")?;
                    }
                    write_contract(f, contract)?;
                }
                Ok(())
            }
            Self::BundleCycle { path } => {
                f.write_str("bundle ordering cycle: ")?;
                for (position, bundle) in path.iter().enumerate() {
                    if position > 0 {
                        f.write_str(" -> ")?;
                    }
                    f.write_str(bundle)?;
                }
                Ok(())
            }
            Self::UndeclaredExtensionPoint {
                extension,
                contributed_by,
            } => write!(
                f,
                "`{}` contributes to undeclared extension point {}",
                contributed_by, extension.type_name
            ),
            Self::ManifestMismatch { bundle, declared } => {
                write!(f, "bundle `{bundle}` does not honour its manifest for ")?;
                write_contract(f, declared)
            }
            Self::UnknownBundleOrder { bundle, after } => write!(
                f,
                "bundle `{bundle}` must come after unknown bundle `{after}`"
            ),
            Self::DuplicateBundle { name } => {
                write!(f, "duplicate bundle `{name}`: registered more than once")
            }
            Self::LifetimeConflict {
                required_by,
                requires,
                bundle,
            } => {
                write!(f, "`{required_by}` is not scoped and requires scoped ")?;
                write_contract(f, requires)?;
                write!(f, ", declared by `{bundle}`")
            }
        }
    }
}

impl std::error::Error for ResolveError {}

// --------------------------------------------------------------------------
// Container access
// --------------------------------------------------------------------------

/// A resolution asked of the container at run time failed.
///
/// Distinct from [`ResolveError`] on purpose: that one describes the graph and
/// can only be produced before anything is built, this one describes a single
/// access and can only be produced once the graph is closed. Keeping them apart
/// stops a phase-three aggregate from carrying variants phase three cannot
/// reach.
///
/// # Why this names a Rust type where a lifecycle diagnostic names a unit
///
/// Every variant here renders its contract through
/// [`ContractRef::type_name`], so a diagnostic about a component prints a Rust
/// type path — while the same component is blamed by a declared name in
/// [`ComponentError`], in [`RunError`] and in every lifecycle record. That is
/// deliberate, not an oversight: a unit has two identities and they answer two
/// questions.
///
/// * A **declared name** — `Component::name` / `Runnable::name` — is what the
///   kernel's lifecycle blames. It is chosen by the author, stable across
///   refactors, and is the only identity a lifecycle diagnostic uses.
/// * A **type identity** is what the container resolves by. A contract *is* a
///   type: [`ContractRef`] is generic machinery that knows nothing of unit
///   traits and so cannot reach a declared name, and printing anything else
///   would name something other than the key the lookup actually failed on.
///
/// So a reader who sees a type path here is reading a container diagnostic,
/// and one who sees a declared name is reading a lifecycle diagnostic. Neither
/// is meant to be rewritten into the other.
#[derive(Debug)]
pub enum ContainerError {
    /// Nothing provides the requested contract.
    ///
    /// Phase three makes this unreachable for anything the graph declared, so
    /// in practice it means the caller asked for a contract it never declared
    /// in its `requires`.
    NotProvided {
        /// The contract that was asked for.
        contract: ContractRef,
    },
    /// The stored value is not of the requested type.
    ///
    /// Reaching this is a defect in the erasure layer, not a user error.
    TypeMismatch {
        /// The contract that was asked for.
        contract: ContractRef,
    },
    /// A shared value was asked to be built for the first time after boot.
    ///
    /// Lazy resolution is forbidden once the kernel is running: an error that
    /// would surface on the first unit of work is a design defect, not an
    /// operational one.
    Sealed {
        /// The contract whose first instantiation was refused.
        contract: ContractRef,
    },
    /// A scoped binding was resolved from a container that owns no unit of
    /// work.
    ///
    /// A scoped value has nowhere to live outside a scope. Building one per
    /// call instead would answer a request the caller never made, so the
    /// resolution is refused.
    NoScope {
        /// The contract that was asked for.
        contract: ContractRef,
    },
    /// The provider ran and failed.
    Build(BuildError),
}

impl fmt::Display for ContainerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotProvided { contract } => {
                write!(f, "no provider for ")?;
                write_contract(f, contract)
            }
            Self::TypeMismatch { contract } => {
                write!(f, "stored value has the wrong type for ")?;
                write_contract(f, contract)
            }
            Self::Sealed { contract } => {
                write!(f, "refusing to build ")?;
                write_contract(f, contract)?;
                f.write_str(" after boot: lazy resolution is forbidden")
            }
            Self::NoScope { contract } => {
                write!(f, "refusing to build ")?;
                write_contract(f, contract)?;
                f.write_str(" outside a scope: it is bound per unit of work")
            }
            Self::Build(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ContainerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Build(error) => Some(error),
            _ => None,
        }
    }
}

impl From<BuildError> for ContainerError {
    fn from(error: BuildError) -> Self {
        Self::Build(error)
    }
}

// --------------------------------------------------------------------------
// Aggregate
// --------------------------------------------------------------------------

/// Everything that can stop the kernel, grouped by the phase it happened in.
///
/// The three early phases collect *all* their failures before reporting, and
/// the [`Display`](core::fmt::Display) prints every one of them on its own
/// line.
///
/// # Examples
///
/// ```
/// use kernel_core::error::{ConfigError, KernelError};
///
/// let error = KernelError::Config(vec![
///     ConfigError::missing("alpha.beta"),
///     ConfigError::missing("gamma"),
/// ]);
/// let rendered = error.to_string();
/// assert_eq!(rendered.lines().count(), 3);
/// assert!(rendered.contains("alpha.beta"));
/// assert!(rendered.contains("gamma"));
/// ```
#[derive(Debug)]
pub enum KernelError {
    /// Configuration could not be read or typed. Nothing was instantiated.
    Config(Vec<ConfigError>),
    /// One or more bundles failed to register. Nothing was instantiated.
    Register(Vec<RegisterError>),
    /// The wiring graph could not be closed. Nothing was instantiated.
    Resolve(Vec<ResolveError>),
    /// A component failed to boot; the ones already booted were rolled back.
    Boot {
        /// The component that failed to boot.
        component: ComponentId,
        /// Why it failed.
        source: ComponentError,
        /// The components stopped again, in the order they were stopped.
        rolled_back: Vec<ComponentId>,
    },
    /// One or more runnables ended abnormally.
    Run(Vec<RunError>),
    /// One or more units failed to stop.
    Shutdown(Vec<ShutdownError>),
}

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(errors) => write_aggregate(f, "configuration", errors),
            Self::Register(errors) => write_aggregate(f, "registration", errors),
            Self::Resolve(errors) => write_aggregate(f, "resolution", errors),
            Self::Run(errors) => write_aggregate(f, "run", errors),
            Self::Shutdown(errors) => write_aggregate(f, "shutdown", errors),
            Self::Boot {
                source,
                rolled_back,
                ..
            } => {
                write!(f, "boot failed with 1 error:\n  - {source}")?;
                if !rolled_back.is_empty() {
                    write!(f, "\n  - rolled back {} component(s):", rolled_back.len())?;
                    for component in rolled_back {
                        write!(f, "\n      {component}")?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for KernelError {
    /// Only the single-cause variant exposes a source. An aggregate has no one
    /// cause, and picking the first would hide the others.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Boot { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// How a kernel run ended.
#[derive(Debug)]
pub enum Outcome {
    /// Every runnable finished on its own.
    Completed,
    /// A shutdown was asked for, by signal or programmatically, and honoured.
    ShutdownRequested,
    /// The run ended on a failure.
    Failed(KernelError),
}

impl Outcome {
    /// The failure, if the run ended on one.
    pub fn error(&self) -> Option<&KernelError> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }

    /// Whether the run ended without failure.
    ///
    /// A requested shutdown is a success: it is what was asked for.
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed(_))
    }

    /// Turns the outcome into a process exit code.
    ///
    /// [`Outcome::Failed`] maps to [`ExitCode::FAILURE`], the other two to
    /// [`ExitCode::SUCCESS`].
    pub fn into_exit_code(self) -> ExitCode {
        if self.is_success() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    struct A;
    struct B;
    struct C;

    fn boxed(message: &str) -> BoxSource {
        Box::<dyn std::error::Error + Send + Sync>::from(message.to_owned())
    }

    #[test]
    fn config_error_display() {
        let error = ConfigError::type_mismatch("alpha.beta", "integer", "string");
        assert_eq!(
            error.to_string(),
            "config `alpha.beta`: expected integer, found string"
        );
        assert_eq!(error.path(), "alpha.beta");
        assert!(matches!(error.kind(), ConfigErrorKind::TypeMismatch { .. }));
    }

    #[test]
    fn wraps_foreign_source() {
        let error = RegisterError::new("alpha", boxed("disk on fire"));
        assert_eq!(error.bundle(), "alpha");
        assert_eq!(error.cause().to_string(), "disk on fire");
        assert!(error.source().is_some());
        assert!(error.to_string().contains("disk on fire"));
    }

    #[test]
    fn aggregates_every_error() {
        let error = KernelError::Config(vec![
            ConfigError::missing("one"),
            ConfigError::invalid("two", "out of range"),
            ConfigError::source_error("three", boxed("unreadable")),
        ]);
        let rendered = error.to_string();
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains("3 error(s)"));
        assert!(lines[1].contains("one"));
        assert!(lines[2].contains("out of range"));
        assert!(lines[3].contains("unreadable"));
    }

    #[test]
    fn empty_aggregate_reads() {
        let rendered = KernelError::Run(vec![]).to_string();
        assert_eq!(rendered, "run failed, with no error reported");
        assert!(!rendered.contains("0 error"));
    }

    #[test]
    fn aggregates_run_errors() {
        let error = KernelError::Run(vec![
            RunError::panicked(RunnableId::new("alpha", 0), "boom"),
            RunError::deadline_exceeded(RunnableId::new("beta", 1)),
        ]);
        let rendered = error.to_string();
        assert_eq!(rendered.lines().count(), 3);
        assert!(rendered.contains("alpha#0"));
        assert!(rendered.contains("beta#1"));
        assert!(rendered.contains("boom"));
        assert!(rendered.contains("deadline exceeded"));
    }

    #[test]
    fn boot_lists_rollback() {
        let failing = ComponentId::new("gamma", 2);
        let error = KernelError::Boot {
            component: failing,
            source: ComponentError::new(failing, boxed("no socket")),
            rolled_back: vec![ComponentId::new("beta", 1), ComponentId::new("alpha", 0)],
        };
        let rendered = error.to_string();
        assert_eq!(rendered.lines().count(), 5);
        assert!(rendered.contains("gamma#2"));
        assert!(rendered.contains("no socket"));
        assert!(rendered.contains("beta#1"));
        assert!(rendered.contains("alpha#0"));
        assert!(error.source().is_some());
    }

    #[test]
    fn cycle_path_display() {
        let error = ResolveError::Cycle {
            path: vec![
                ContractRef::of::<A>(),
                ContractRef::of::<B>(),
                ContractRef::of::<C>(),
                ContractRef::of::<A>(),
            ],
        };
        let rendered = error.to_string();
        let path: Vec<&str> = rendered
            .strip_prefix("dependency cycle: ")
            .expect("cycle prefix")
            .split(" -> ")
            .collect();
        assert_eq!(path.len(), 4);
        assert!(path[0].ends_with("::A"));
        assert!(path[1].ends_with("::B"));
        assert!(path[2].ends_with("::C"));
        assert_eq!(path[0], path[3]);
    }

    #[test]
    fn bundle_cycle_display() {
        let error = ResolveError::BundleCycle {
            path: vec!["alpha", "beta", "alpha"],
        };
        assert_eq!(
            error.to_string(),
            "bundle ordering cycle: alpha -> beta -> alpha"
        );
    }

    #[test]
    fn duplicate_bundle_display() {
        let error = ResolveError::DuplicateBundle { name: "alpha" };
        assert_eq!(
            error.to_string(),
            "duplicate bundle `alpha`: registered more than once"
        );
    }

    #[test]
    fn lifetime_conflict_display() {
        let error = ResolveError::LifetimeConflict {
            required_by: "alpha.started",
            requires: ContractRef::named::<B>("primary"),
            bundle: "alpha",
        };
        let rendered = error.to_string();
        assert!(
            rendered.starts_with("`alpha.started` is not scoped"),
            "{rendered}"
        );
        assert!(rendered.contains(" requires scoped "), "{rendered}");
        assert!(rendered.contains("#primary"), "{rendered}");
        assert!(rendered.ends_with("declared by `alpha`"), "{rendered}");
    }

    #[test]
    fn resolve_names_contract() {
        let error = ResolveError::MissingContract {
            required_by: "alpha",
            contract: ContractRef::named::<A>("primary"),
        };
        let rendered = error.to_string();
        assert!(rendered.contains("#primary"));
        assert!(rendered.contains("alpha"));
    }

    #[test]
    fn dispatch_wraps_listener() {
        let error = DispatchError::new(
            "alpha.started",
            ListenerError::new("alpha.started", boxed("nope")),
        );
        assert_eq!(error.event(), "alpha.started");
        assert_eq!(error.cause().event(), "alpha.started");
        assert!(error.to_string().contains("nope"));
        assert!(error.source().is_some());
    }

    #[test]
    fn exit_code_maps() {
        let success = format!("{:?}", ExitCode::SUCCESS);
        let failure = format!("{:?}", ExitCode::FAILURE);
        assert_eq!(
            format!("{:?}", Outcome::Completed.into_exit_code()),
            success
        );
        assert_eq!(
            format!("{:?}", Outcome::ShutdownRequested.into_exit_code()),
            success
        );
        assert_eq!(
            format!(
                "{:?}",
                Outcome::Failed(KernelError::Run(vec![])).into_exit_code()
            ),
            failure
        );
    }

    #[test]
    fn outcome_exposes_error() {
        let outcome = Outcome::Failed(KernelError::Shutdown(vec![
            ShutdownError::deadline_exceeded("alpha"),
        ]));
        assert!(!outcome.is_success());
        assert!(outcome.error().is_some());
        assert!(Outcome::Completed.error().is_none());
        assert!(Outcome::ShutdownRequested.is_success());
    }
}
