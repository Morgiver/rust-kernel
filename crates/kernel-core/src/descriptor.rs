//! Descriptors and policies that a unit attaches to itself.
//!
//! Nothing here executes anything: these are plain, `Copy`-cheap declarations
//! that the assembling layer reads to decide *when* and *how long*. They carry
//! no runtime, no allocation and no interior mutability, which is what lets a
//! bundle declare its manifest as a `static`.
//!
//! The two families are:
//!
//! * **Descriptors** — [`BundleManifest`], [`ComponentDescriptor`],
//!   [`RunnableDescriptor`]: the bounds and the policy of one unit. A
//!   component or a runnable declares its name once, on its own trait, so a
//!   descriptor carries none.
//! * **Policies** — [`Lifetime`], [`Priority`], [`RestartPolicy`], [`Backoff`],
//!   [`ShutdownPolicy`], plus the [`Stage`] ladder that shutdown walks.

use core::fmt;
use core::time::Duration;

use crate::id::ContractRef;

/// How long a provided value lives, and therefore how often it is built.
///
/// ```
/// use kernel_core::Lifetime;
///
/// assert_eq!(Lifetime::default(), Lifetime::Shared);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Lifetime {
    /// Built at most once; every resolution hands back the same value.
    #[default]
    Shared,
    /// Built at most once per scope; a scope is one unit of work.
    Scoped,
    /// Built on every resolution; the caller owns the result.
    Factory,
}

impl Lifetime {
    /// Lower-case, stable name, suitable for a telemetry field.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Lifetime::Shared => "shared",
            Lifetime::Scoped => "scoped",
            Lifetime::Factory => "factory",
        }
    }
}

impl fmt::Display for Lifetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Explicit ordering weight; **the higher value runs first**.
///
/// The derived [`Ord`] is the plain numeric order, so a consumer that wants
/// "highest first" sorts in reverse. Ties are broken by registration order,
/// which is the consumer's job, not this type's.
///
/// ```
/// use kernel_core::Priority;
///
/// let mut order = [Priority(0), Priority(10), Priority(-5)];
/// order.sort_by(|a, b| b.cmp(a));
/// assert_eq!(order, [Priority(10), Priority(0), Priority(-5)]);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Priority(pub i32);

impl Priority {
    /// Runs before anything registered at [`Priority::NORMAL`].
    pub const HIGH: Priority = Priority(100);
    /// The default weight.
    pub const NORMAL: Priority = Priority(0);
    /// Runs after anything registered at [`Priority::NORMAL`].
    pub const LOW: Priority = Priority(-100);

    /// Wraps a raw weight.
    pub const fn new(value: i32) -> Self {
        Priority(value)
    }

    /// The raw weight.
    pub const fn get(&self) -> i32 {
        self.0
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The shutdown ladder, in the order it is climbed.
///
/// The ordering is load-bearing: the coordinator compares stages to decide
/// whether a transition moves forward, and it never moves backward.
///
/// ```
/// use kernel_core::Stage;
///
/// assert!(Stage::Running < Stage::Draining);
/// assert!(Stage::Draining < Stage::Stopping);
/// assert!(Stage::Stopping < Stage::Stopped);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stage {
    /// Normal operation; new work is accepted.
    #[default]
    Running,
    /// New work is refused, in-flight work continues.
    Draining,
    /// In-flight work must finish; the deadline is running.
    Stopping,
    /// Nothing is left to run.
    Stopped,
}

impl Stage {
    /// Lower-case, stable name, suitable for a telemetry field.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Stage::Running => "running",
            Stage::Draining => "draining",
            Stage::Stopping => "stopping",
            Stage::Stopped => "stopped",
        }
    }

    /// True once the ladder has been entered, i.e. anything but
    /// [`Stage::Running`].
    pub const fn is_shutting_down(&self) -> bool {
        !matches!(self, Stage::Running)
    }

    /// True for the last rung, from which there is no further transition.
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Stage::Stopped)
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a bundle says about itself before anything is built.
///
/// `requires` is deliberately redundant with the real graph: it buys a
/// high-level diagnostic ("this bundle needs a contract nobody provides")
/// before the graph walk produces a deeper, less readable error. A manifest
/// that lies is rejected during resolution.
///
/// Every builder is `const`, so a bundle declares its manifest once:
///
/// ```
/// use kernel_core::{BundleManifest, ContractRef};
///
/// trait Sink {}
///
/// static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Sink>()];
///
/// static MANIFEST: BundleManifest = BundleManifest::new("sample", "0.1.0")
///     .requires(&REQUIRES)
///     .after(&["other-bundle"]);
///
/// assert_eq!(MANIFEST.name, "sample");
/// assert_eq!(MANIFEST.requires.len(), 1);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BundleManifest {
    /// Unique, stable name of the bundle.
    pub name: &'static str,
    /// Version string, opaque to this crate.
    pub version: &'static str,
    /// Contracts this bundle needs someone else to provide.
    pub requires: &'static [ContractRef],
    /// Bundles that must be registered before this one. Rare — prefer
    /// expressing the need as a contract in `requires`.
    pub after: &'static [&'static str],
}

impl BundleManifest {
    /// A manifest that requires nothing and orders after nothing.
    pub const fn new(name: &'static str, version: &'static str) -> Self {
        BundleManifest {
            name,
            version,
            requires: &[],
            after: &[],
        }
    }

    /// Sets the contracts this bundle expects to find, replacing any previous
    /// list.
    pub const fn requires(self, requires: &'static [ContractRef]) -> Self {
        BundleManifest { requires, ..self }
    }

    /// Sets the bundles that must be registered first, replacing any previous
    /// list.
    pub const fn after(self, after: &'static [&'static str]) -> Self {
        BundleManifest { after, ..self }
    }
}

impl fmt::Display for BundleManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.name, self.version)
    }
}

/// Time bounds of one component.
///
/// It carries no name: a component declares its name once, on the trait it
/// implements, and that declaration is read where the concrete type is still
/// known. `None` on a timeout means "no bound of its own": the assembling layer
/// falls back to whatever global policy it holds.
///
/// ```
/// use core::time::Duration;
/// use kernel_core::ComponentDescriptor;
///
/// static DESCRIPTOR: ComponentDescriptor =
///     ComponentDescriptor::new().boot_timeout(Duration::from_secs(5));
///
/// assert_eq!(DESCRIPTOR.shutdown_timeout, None);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ComponentDescriptor {
    /// Upper bound on booting. Exceeding it is a boot failure.
    pub boot_timeout: Option<Duration>,
    /// Upper bound on shutting down. Exceeding it abandons the task and is
    /// recorded as an unclean stop.
    ///
    /// It shortens this component's own share of
    /// [`ShutdownPolicy::stop`](field@ShutdownPolicy::stop) — which is a
    /// per-unit budget, granted afresh when this component's shutdown begins —
    /// and never extends it.
    pub shutdown_timeout: Option<Duration>,
}

impl ComponentDescriptor {
    /// A descriptor with no bound of its own.
    pub const fn new() -> Self {
        ComponentDescriptor {
            boot_timeout: None,
            shutdown_timeout: None,
        }
    }

    /// Sets the boot bound.
    pub const fn boot_timeout(self, timeout: Duration) -> Self {
        ComponentDescriptor {
            boot_timeout: Some(timeout),
            ..self
        }
    }

    /// Sets the shutdown bound.
    pub const fn shutdown_timeout(self, timeout: Duration) -> Self {
        ComponentDescriptor {
            shutdown_timeout: Some(timeout),
            ..self
        }
    }
}

/// What the termination of a runnable means for everything else.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Criticality {
    /// Its termination stops everything. The safe default: a unit that stops
    /// unexpectedly should not be ignored.
    #[default]
    Essential,
    /// Its termination is recorded; the rest keeps running.
    Ancillary,
}

impl Criticality {
    /// Lower-case, stable name, suitable for a telemetry field.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Criticality::Essential => "essential",
            Criticality::Ancillary => "ancillary",
        }
    }
}

impl fmt::Display for Criticality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a failed runnable is started again, and how often.
///
/// A clean return is never a failure, so it never restarts; only an error does.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum RestartPolicy {
    /// One shot. A failure is reported, not retried.
    #[default]
    Never,
    /// Retry after a failure, up to `max_attempts` restarts.
    OnFailure {
        /// Number of restarts allowed, not counting the first run. Zero is
        /// equivalent to [`RestartPolicy::Never`].
        max_attempts: u32,
        /// Delay schedule between restarts.
        backoff: Backoff,
    },
}

impl RestartPolicy {
    /// Convenience constructor for the retrying variant.
    pub const fn on_failure(max_attempts: u32, backoff: Backoff) -> Self {
        RestartPolicy::OnFailure {
            max_attempts,
            backoff,
        }
    }

    /// Whether a restart is still allowed after `attempts` restarts have
    /// already been spent.
    pub const fn allows(&self, attempts: u32) -> bool {
        match self {
            RestartPolicy::Never => false,
            RestartPolicy::OnFailure { max_attempts, .. } => attempts < *max_attempts,
        }
    }
}

/// Delay schedule between two attempts.
///
/// Both variants are total functions of the attempt number: no clock, no
/// state, no allocation. Jitter is opt-in and takes its entropy from the
/// caller, because this crate has no dependency and must stay reproducible
/// under test.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Backoff {
    /// The same delay every time.
    Fixed(Duration),
    /// `initial * factor^attempt`, clamped to `max`.
    Exponential {
        /// Delay before the first restart.
        initial: Duration,
        /// Ceiling the schedule never exceeds.
        max: Duration,
        /// Growth per attempt. A non-finite or non-positive factor is treated
        /// as `1.0`, which degrades the schedule to a fixed delay rather than
        /// producing nonsense.
        factor: f64,
    },
}

impl Default for Backoff {
    /// One hundred milliseconds, doubling, capped at thirty seconds.
    fn default() -> Self {
        Backoff::Exponential {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            factor: 2.0,
        }
    }
}

impl Backoff {
    /// Delay before attempt number `attempt`, zero-based.
    ///
    /// Deterministic and saturating: an absurd attempt number yields `max`
    /// rather than overflowing or panicking.
    ///
    /// ```
    /// use core::time::Duration;
    /// use kernel_core::Backoff;
    ///
    /// let backoff = Backoff::Exponential {
    ///     initial: Duration::from_secs(1),
    ///     max: Duration::from_secs(30),
    ///     factor: 2.0,
    /// };
    ///
    /// assert_eq!(backoff.delay(0), Duration::from_secs(1));
    /// assert_eq!(backoff.delay(3), Duration::from_secs(8));
    /// assert_eq!(backoff.delay(u32::MAX), Duration::from_secs(30));
    /// ```
    pub fn delay(&self, attempt: u32) -> Duration {
        match *self {
            Backoff::Fixed(delay) => delay,
            Backoff::Exponential {
                initial,
                max,
                factor,
            } => {
                if attempt == 0 {
                    return initial.min(max);
                }
                let factor = if factor.is_finite() && factor > 0.0 {
                    factor
                } else {
                    1.0
                };
                let scaled = initial.as_secs_f64() * factor.powf(f64::from(attempt));
                // A non-finite or negative product means the schedule ran off
                // the end of the representable range: clamp, never panic.
                Duration::try_from_secs_f64(scaled).unwrap_or(max).min(max)
            }
        }
    }

    /// Spreads `delay` over `[delay / 2, delay]`, deterministically for a
    /// given `seed`.
    ///
    /// The caller supplies the entropy. Two calls with the same seed and the
    /// same delay always return the same value, so a test can pin it.
    ///
    /// ```
    /// use core::time::Duration;
    /// use kernel_core::Backoff;
    ///
    /// let delay = Duration::from_secs(8);
    /// let jittered = Backoff::with_jitter(delay, 42);
    ///
    /// assert!(jittered >= delay / 2 && jittered <= delay);
    /// assert_eq!(jittered, Backoff::with_jitter(delay, 42));
    /// ```
    pub fn with_jitter(delay: Duration, seed: u64) -> Duration {
        let total = delay.as_nanos();
        let floor = total / 2;
        // `span` is `total - floor`, so `floor + span == total` exactly, odd
        // nanosecond counts included.
        let span = total - floor;
        let offset = if span == 0 {
            0
        } else {
            u128::from(mix(seed)) % (span + 1)
        };
        nanos_to_duration(floor + offset)
    }
}

/// Global two-phase shutdown budget, overridable per unit by its descriptor.
///
/// # The budgets are per unit, not per phase
///
/// `drain` and `stop` are what ONE unit is allowed, granted afresh when that
/// unit's own shutdown begins. They are not a total for a phase to divide
/// among its units.
///
/// * Runnables stop concurrently, so per-unit and per-phase coincide for them:
///   their half of the shutdown costs `drain + stop` however many there are.
/// * Components are stopped one after another, and each is given `stop` from
///   the moment its own `shutdown` is called.
///
/// This is what makes the per-unit override mean something:
/// [`ComponentDescriptor::shutdown_timeout`] is per unit and shortens this
/// budget, which is only coherent if what it shortens is per unit too.
///
/// # What that costs
///
/// Worst case for a whole shutdown is `drain + stop + (stop × components)`,
/// and it grows with the number of components. The total is NOT bounded by
/// `drain + stop`, nor by [`total`](Self::total).
///
/// That cost buys one rule: a unit is never abandoned because another unit
/// overran. A single deadline shared across the component walk would let a
/// component that legitimately used two thirds of the budget leave its
/// neighbour a third, and the neighbour would be reported failed for a wait
/// that was not its own. A unit that wants a tighter bound declares one, and a
/// declared bound only ever shortens.
///
/// [`total`](Self::total) is `drain + stop`: what one unit may cost across
/// both stages, and what a unit declaring its own timeout is shortening.
///
/// [`ComponentDescriptor::shutdown_timeout`]: field@ComponentDescriptor::shutdown_timeout
///
/// ```
/// use core::time::Duration;
/// use kernel_core::ShutdownPolicy;
///
/// let policy = ShutdownPolicy::default();
/// assert_eq!(policy.drain, Duration::from_secs(10));
/// assert_eq!(policy.stop, Duration::from_secs(20));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShutdownPolicy {
    /// Budget for refusing new work while in-flight work continues.
    pub drain: Duration,
    /// Budget for letting in-flight work finish.
    pub stop: Duration,
}

impl ShutdownPolicy {
    /// The default budget: ten seconds draining, twenty seconds stopping.
    pub const DEFAULT: ShutdownPolicy =
        ShutdownPolicy::new(Duration::from_secs(10), Duration::from_secs(20));

    /// A budget with explicit phases.
    pub const fn new(drain: Duration, stop: Duration) -> Self {
        ShutdownPolicy { drain, stop }
    }

    /// Total time the two phases may take together, saturating.
    pub fn total(&self) -> Duration {
        self.drain.saturating_add(self.stop)
    }

    /// Budget for one stage: `None` for stages that are not timed.
    pub const fn budget(&self, stage: Stage) -> Option<Duration> {
        match stage {
            Stage::Draining => Some(self.drain),
            Stage::Stopping => Some(self.stop),
            Stage::Running | Stage::Stopped => None,
        }
    }
}

impl Default for ShutdownPolicy {
    fn default() -> Self {
        ShutdownPolicy::DEFAULT
    }
}

/// Criticality and time bounds of one runnable.
///
/// It carries no name, for the same reason [`ComponentDescriptor`] carries
/// none: the name is declared once, on the trait.
///
/// ```
/// use core::time::Duration;
/// use kernel_core::{Criticality, RunnableDescriptor};
///
/// static DESCRIPTOR: RunnableDescriptor = RunnableDescriptor::new()
///     .criticality(Criticality::Ancillary)
///     .drain_timeout(Duration::from_secs(2));
///
/// assert_eq!(DESCRIPTOR.criticality, Criticality::Ancillary);
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RunnableDescriptor {
    /// What its termination means for everything else.
    pub criticality: Criticality,
    /// Whether a failure is retried.
    pub restart: RestartPolicy,
    /// Bound on the draining phase; `None` defers to the global
    /// [`ShutdownPolicy`].
    pub drain_timeout: Option<Duration>,
    /// Bound on the stopping phase; `None` defers to the global
    /// [`ShutdownPolicy`].
    pub stop_timeout: Option<Duration>,
}

impl RunnableDescriptor {
    /// An essential, never-restarted runnable with no bound of its own.
    pub const fn new() -> Self {
        RunnableDescriptor {
            criticality: Criticality::Essential,
            restart: RestartPolicy::Never,
            drain_timeout: None,
            stop_timeout: None,
        }
    }

    /// Sets what its termination means.
    pub const fn criticality(self, criticality: Criticality) -> Self {
        RunnableDescriptor {
            criticality,
            ..self
        }
    }

    /// Sets the restart policy.
    pub const fn restart(self, restart: RestartPolicy) -> Self {
        RunnableDescriptor { restart, ..self }
    }

    /// Sets the draining bound.
    pub const fn drain_timeout(self, timeout: Duration) -> Self {
        RunnableDescriptor {
            drain_timeout: Some(timeout),
            ..self
        }
    }

    /// Sets the stopping bound.
    pub const fn stop_timeout(self, timeout: Duration) -> Self {
        RunnableDescriptor {
            stop_timeout: Some(timeout),
            ..self
        }
    }
}

impl Default for RunnableDescriptor {
    fn default() -> Self {
        RunnableDescriptor::new()
    }
}

/// SplitMix64 finalizer: a full-avalanche bijection on `u64`.
///
/// Chosen over a hand-rolled mixer because it is published, tested prior art
/// and needs no state. It is not a cryptographic hash and is not used as one.
const fn mix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Rebuilds a `Duration` from a nanosecond count that may exceed `u64`.
fn nanos_to_duration(nanos: u128) -> Duration {
    const NANOS_PER_SEC: u128 = 1_000_000_000;
    let secs = nanos / NANOS_PER_SEC;
    match u64::try_from(secs) {
        Ok(secs) => Duration::new(secs, (nanos % NANOS_PER_SEC) as u32),
        Err(_) => Duration::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait Sink {}

    #[test]
    fn stage_ordering() {
        assert!(Stage::Running < Stage::Draining);
        assert!(Stage::Draining < Stage::Stopping);
        assert!(Stage::Stopping < Stage::Stopped);

        let mut stages = [
            Stage::Stopped,
            Stage::Running,
            Stage::Stopping,
            Stage::Draining,
        ];
        stages.sort();
        assert_eq!(
            stages,
            [
                Stage::Running,
                Stage::Draining,
                Stage::Stopping,
                Stage::Stopped
            ]
        );

        assert!(!Stage::Running.is_shutting_down());
        assert!(Stage::Draining.is_shutting_down());
        assert!(Stage::Stopped.is_terminal());
        assert!(!Stage::Stopping.is_terminal());
    }

    #[test]
    fn defaults_are_sane() {
        assert_eq!(Lifetime::default(), Lifetime::Shared);
        assert_eq!(Priority::default(), Priority(0));
        assert_eq!(Priority::default(), Priority::NORMAL);
        assert_eq!(Stage::default(), Stage::Running);
        assert_eq!(Criticality::default(), Criticality::Essential);
        assert_eq!(RestartPolicy::default(), RestartPolicy::Never);

        let policy = ShutdownPolicy::default();
        assert_eq!(policy.drain, Duration::from_secs(10));
        assert_eq!(policy.stop, Duration::from_secs(20));
        assert_eq!(policy.total(), Duration::from_secs(30));
        assert_eq!(policy.budget(Stage::Draining), Some(policy.drain));
        assert_eq!(policy.budget(Stage::Stopping), Some(policy.stop));
        assert_eq!(policy.budget(Stage::Running), None);
        assert_eq!(policy.budget(Stage::Stopped), None);
    }

    #[test]
    fn priority_sorts_high_first() {
        let mut order = [Priority::LOW, Priority::HIGH, Priority::NORMAL];
        order.sort_by(|a, b| b.cmp(a));
        assert_eq!(order, [Priority::HIGH, Priority::NORMAL, Priority::LOW]);
        assert_eq!(Priority::new(7).get(), 7);
    }

    #[test]
    fn const_manifest_builds() {
        static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Sink>()];
        static MANIFEST: BundleManifest = BundleManifest::new("sample", "0.1.0")
            .requires(&REQUIRES)
            .after(&["earlier"]);

        assert_eq!(MANIFEST.name, "sample");
        assert_eq!(MANIFEST.version, "0.1.0");
        assert_eq!(MANIFEST.requires.len(), 1);
        assert_eq!(MANIFEST.after, ["earlier"]);
        assert_eq!(MANIFEST.to_string(), "sample@0.1.0");

        static BARE: BundleManifest = BundleManifest::new("bare", "0.0.0");
        assert!(BARE.requires.is_empty());
        assert!(BARE.after.is_empty());
    }

    #[test]
    fn const_descriptors_build() {
        static COMPONENT: ComponentDescriptor = ComponentDescriptor::new()
            .boot_timeout(Duration::from_secs(5))
            .shutdown_timeout(Duration::from_secs(1));
        assert_eq!(COMPONENT.boot_timeout, Some(Duration::from_secs(5)));
        assert_eq!(COMPONENT.shutdown_timeout, Some(Duration::from_secs(1)));
        assert_eq!(ComponentDescriptor::new().boot_timeout, None);
        assert_eq!(ComponentDescriptor::default(), ComponentDescriptor::new());

        static RUNNABLE: RunnableDescriptor = RunnableDescriptor::new()
            .criticality(Criticality::Ancillary)
            .restart(RestartPolicy::on_failure(
                3,
                Backoff::Fixed(Duration::from_secs(1)),
            ))
            .drain_timeout(Duration::from_secs(2))
            .stop_timeout(Duration::from_secs(4));
        assert_eq!(RUNNABLE.criticality, Criticality::Ancillary);
        assert_eq!(RUNNABLE.drain_timeout, Some(Duration::from_secs(2)));
        assert_eq!(RUNNABLE.stop_timeout, Some(Duration::from_secs(4)));

        let bare = RunnableDescriptor::new();
        assert_eq!(bare.criticality, Criticality::Essential);
        assert_eq!(bare.restart, RestartPolicy::Never);
        assert_eq!(bare, RunnableDescriptor::default());
    }

    #[test]
    fn restart_allows_counts() {
        assert!(!RestartPolicy::Never.allows(0));

        let policy = RestartPolicy::on_failure(2, Backoff::Fixed(Duration::ZERO));
        assert!(policy.allows(0));
        assert!(policy.allows(1));
        assert!(!policy.allows(2));
        assert!(!RestartPolicy::on_failure(0, Backoff::Fixed(Duration::ZERO)).allows(0));
    }

    #[test]
    fn fixed_delay_is_constant() {
        let backoff = Backoff::Fixed(Duration::from_millis(250));
        for attempt in [0, 1, 7, u32::MAX] {
            assert_eq!(backoff.delay(attempt), Duration::from_millis(250));
        }
    }

    #[test]
    fn exponential_grows_and_caps() {
        let backoff = Backoff::Exponential {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            factor: 2.0,
        };
        let expected = [1, 2, 4, 8, 16, 30, 30, 30];
        for (attempt, secs) in expected.iter().enumerate() {
            assert_eq!(
                backoff.delay(attempt as u32),
                Duration::from_secs(*secs),
                "attempt {attempt}"
            );
        }
    }

    #[test]
    fn delay_saturates_at_max() {
        let backoff = Backoff::Exponential {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            factor: 2.0,
        };
        assert_eq!(backoff.delay(u32::MAX), Duration::from_secs(30));
        assert_eq!(backoff.delay(u32::MAX - 1), Duration::from_secs(30));
        assert_eq!(backoff.delay(1_000_000), Duration::from_secs(30));
        assert_eq!(Backoff::default().delay(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn degenerate_schedules_are_safe() {
        // A non-finite or non-positive factor degrades to a fixed delay.
        for factor in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -2.0] {
            let backoff = Backoff::Exponential {
                initial: Duration::from_secs(2),
                max: Duration::from_secs(30),
                factor,
            };
            assert_eq!(backoff.delay(0), Duration::from_secs(2));
            assert_eq!(backoff.delay(5), Duration::from_secs(2));
            assert_eq!(backoff.delay(u32::MAX), Duration::from_secs(2));
        }

        // A ceiling below the initial delay clamps from the very first attempt.
        let clamped = Backoff::Exponential {
            initial: Duration::from_secs(10),
            max: Duration::from_secs(1),
            factor: 2.0,
        };
        assert_eq!(clamped.delay(0), Duration::from_secs(1));
        assert_eq!(clamped.delay(3), Duration::from_secs(1));

        // A zero initial delay stays zero however it is scaled.
        let zero = Backoff::Exponential {
            initial: Duration::ZERO,
            max: Duration::from_secs(30),
            factor: 2.0,
        };
        assert_eq!(zero.delay(9), Duration::ZERO);

        // A shrinking schedule is allowed and stays representable.
        let shrinking = Backoff::Exponential {
            initial: Duration::from_secs(8),
            max: Duration::from_secs(30),
            factor: 0.5,
        };
        assert_eq!(shrinking.delay(3), Duration::from_secs(1));
        assert_eq!(shrinking.delay(u32::MAX), Duration::ZERO);
    }

    #[test]
    fn jitter_within_bounds() {
        let delays = [
            Duration::from_millis(1),
            Duration::from_millis(333),
            Duration::from_secs(30),
            Duration::from_nanos(3),
            Duration::MAX,
        ];
        let seeds = [0u64, 1, 2, 7, 42, 1_000_003, u64::MAX / 2, u64::MAX];

        for delay in delays {
            for seed in seeds {
                let jittered = Backoff::with_jitter(delay, seed);
                assert!(jittered >= delay / 2, "{delay:?} {seed} {jittered:?}");
                assert!(jittered <= delay, "{delay:?} {seed} {jittered:?}");
            }
        }
    }

    #[test]
    fn jitter_is_deterministic() {
        let delay = Duration::from_secs(8);
        assert_eq!(
            Backoff::with_jitter(delay, 12_345),
            Backoff::with_jitter(delay, 12_345)
        );
        assert_eq!(Backoff::with_jitter(Duration::ZERO, 99), Duration::ZERO);

        // The seed actually moves the result across a reasonable sample.
        let distinct = (0..64)
            .map(|seed| Backoff::with_jitter(delay, seed))
            .collect::<std::collections::BTreeSet<_>>();
        assert!(distinct.len() > 1, "jitter ignored the seed");
    }
}
