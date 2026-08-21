//! Health reporting: a three-state verdict and the probe extension point.
//!
//! # Aggregation, not exposure
//!
//! The kernel *aggregates* health and never *exposes* it. It collects every
//! contributed [`HealthProbe`], awaits each [`HealthProbe::check`] and folds the
//! answers through [`Health::worst`] into a single verdict. It opens no socket,
//! serves no endpoint and picks no wire format: publishing the verdict — on a
//! port, in a file, through a signal — is the job of a bundle that reads the
//! aggregate.
//!
//! # Contributing a probe
//!
//! [`HealthProbe`] extends [`Extension`], so it is collected like any other
//! extension point: any bundle contributes a probe without the kernel knowing
//! any of them, and the kernel folds whatever it finds.

use crate::extension::Extension;
use crate::future::BoxFuture;

/// The verdict of a health check.
///
/// The three states are ordered by severity: `Up` is the least severe, then
/// `Degraded`, then `Down`. [`Health::worst`] folds two verdicts by keeping the
/// more severe one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Health {
    /// Fully operational.
    Up,
    /// Operational, but not nominally so: reduced capacity, a failing
    /// dependency that is being worked around, a backlog building up.
    Degraded {
        /// Human-readable explanation of the degradation.
        detail: String,
    },
    /// Not operational.
    Down {
        /// Human-readable explanation of the outage.
        detail: String,
    },
}

impl Health {
    /// Builds a [`Health::Degraded`] verdict from any string-like detail.
    pub fn degraded(detail: impl Into<String>) -> Self {
        Health::Degraded {
            detail: detail.into(),
        }
    }

    /// Builds a [`Health::Down`] verdict from any string-like detail.
    pub fn down(detail: impl Into<String>) -> Self {
        Health::Down {
            detail: detail.into(),
        }
    }

    /// Returns `true` only for [`Health::Up`].
    ///
    /// A degraded verdict is *not* up: callers that need to distinguish
    /// "serving, imperfectly" from "not serving" must match on the variant.
    pub fn is_up(&self) -> bool {
        matches!(self, Health::Up)
    }

    /// Returns the attached explanation, or `None` for [`Health::Up`].
    pub fn detail(&self) -> Option<&str> {
        match self {
            Health::Up => None,
            Health::Degraded { detail } | Health::Down { detail } => Some(detail),
        }
    }

    /// Severity rank; higher is worse. Private: the numbers are an
    /// implementation detail of the fold, not a public ordering.
    fn severity(&self) -> u8 {
        match self {
            Health::Up => 0,
            Health::Degraded { .. } => 1,
            Health::Down { .. } => 2,
        }
    }

    /// Folds two verdicts into one: `Down` beats `Degraded` beats `Up`.
    ///
    /// The operation is associative, and commutative with respect to severity.
    /// On a tie it keeps the left-hand argument, so folding a collection in
    /// registration order keeps the *first* detail seen at the winning
    /// severity — deterministic and reproducible.
    ///
    /// ```
    /// use kernel_core::Health;
    ///
    /// assert_eq!(Health::worst(Health::Up, Health::Up), Health::Up);
    /// assert_eq!(
    ///     Health::worst(Health::degraded("slow"), Health::down("gone")),
    ///     Health::down("gone"),
    /// );
    /// ```
    pub fn worst(a: Health, b: Health) -> Health {
        if b.severity() > a.severity() { b } else { a }
    }

    /// Folds a whole collection through [`Health::worst`], left to right.
    ///
    /// An empty collection is [`Health::Up`]: a system with no probe has
    /// nothing reporting trouble.
    ///
    /// ```
    /// use kernel_core::Health;
    ///
    /// let verdicts = [Health::Up, Health::degraded("slow"), Health::Up];
    /// assert_eq!(Health::worst_of(verdicts), Health::degraded("slow"));
    /// ```
    pub fn worst_of<I: IntoIterator<Item = Health>>(items: I) -> Health {
        items.into_iter().fold(Health::Up, Health::worst)
    }
}

impl Default for Health {
    /// A verdict nobody has contradicted yet is [`Health::Up`].
    fn default() -> Self {
        Health::Up
    }
}

/// Renders a verdict as one line, detail included.
///
/// The rendering is complete on its own — `up`, `degraded: <detail>`,
/// `down: <detail>` — and carries no name, no punctuation of its own and no
/// newline. That is deliberate: an aggregate that renders several verdicts
/// composes them as `{name}: {verdict}` lines and never matches on the
/// variant to recover the detail.
impl core::fmt::Display for Health {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Health::Up => f.write_str("up"),
            Health::Degraded { detail } => write!(f, "degraded: {detail}"),
            Health::Down { detail } => write!(f, "down: {detail}"),
        }
    }
}

/// A named health check contributed as an extension.
///
/// The kernel collects every probe, awaits each [`check`](HealthProbe::check)
/// and folds the results through [`Health::worst`]. It never publishes the
/// result itself.
///
/// A probe should be cheap and must not block: it answers about state it
/// already observes, it does not go and produce that state.
///
/// ```
/// use kernel_core::{BoxFuture, Extension, Health, HealthProbe};
///
/// struct SampleProbe {
///     ready: bool,
/// }
///
/// impl Extension for SampleProbe {}
///
/// impl HealthProbe for SampleProbe {
///     fn name(&self) -> &'static str {
///         "sample"
///     }
///
///     fn check<'a>(&'a self) -> BoxFuture<'a, Health> {
///         let verdict = if self.ready {
///             Health::Up
///         } else {
///             Health::down("not ready")
///         };
///         Box::pin(async move { verdict })
///     }
/// }
/// ```
pub trait HealthProbe: Extension {
    /// Stable identifier of this probe, used to attribute a verdict.
    fn name(&self) -> &'static str;

    /// Runs the check.
    fn check<'a>(&'a self) -> BoxFuture<'a, Health>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> [Health; 3] {
        [Health::Up, Health::degraded("d"), Health::down("x")]
    }

    #[test]
    fn worst_ranks_severity() {
        assert_eq!(
            Health::worst(Health::Up, Health::degraded("d")),
            Health::degraded("d")
        );
        assert_eq!(
            Health::worst(Health::degraded("d"), Health::down("x")),
            Health::down("x")
        );
        assert_eq!(
            Health::worst(Health::Up, Health::down("x")),
            Health::down("x")
        );
    }

    #[test]
    fn worst_is_associative() {
        for a in samples() {
            for b in samples() {
                for c in samples() {
                    let left = Health::worst(Health::worst(a.clone(), b.clone()), c.clone());
                    let right = Health::worst(a.clone(), Health::worst(b.clone(), c.clone()));
                    assert_eq!(left, right, "{a:?} {b:?} {c:?}");
                }
            }
        }
    }

    #[test]
    fn up_is_identity() {
        for a in samples() {
            assert_eq!(Health::worst(Health::Up, a.clone()), a);
            assert_eq!(Health::worst(a.clone(), Health::Up), a);
        }
    }

    #[test]
    fn tie_keeps_left() {
        assert_eq!(
            Health::worst(Health::down("first"), Health::down("second")),
            Health::down("first")
        );
        assert_eq!(
            Health::worst(Health::degraded("first"), Health::degraded("second")),
            Health::degraded("first")
        );
    }

    #[test]
    fn empty_fold_is_up() {
        assert_eq!(Health::worst_of([]), Health::Up);
        assert_eq!(Health::default(), Health::Up);
    }

    #[test]
    fn fold_keeps_first_worst() {
        let verdicts = [
            Health::Up,
            Health::down("one"),
            Health::degraded("two"),
            Health::down("three"),
        ];
        assert_eq!(Health::worst_of(verdicts), Health::down("one"));
    }

    #[test]
    fn is_up_and_detail() {
        assert!(Health::Up.is_up());
        assert!(!Health::degraded("d").is_up());
        assert!(!Health::down("x").is_up());
        assert_eq!(Health::Up.detail(), None);
        assert_eq!(Health::degraded("d").detail(), Some("d"));
        assert_eq!(Health::down("x").detail(), Some("x"));
    }

    #[test]
    fn display_reads_plainly() {
        assert_eq!(Health::Up.to_string(), "up");
        assert_eq!(Health::degraded("slow").to_string(), "degraded: slow");
        assert_eq!(Health::down("gone").to_string(), "down: gone");
    }

    #[test]
    fn composes_into_lines() {
        let verdicts = [
            ("first", Health::Up),
            ("second", Health::degraded("slow")),
            ("third", Health::down("gone")),
        ];
        let overall = Health::worst_of(verdicts.iter().map(|(_, v)| v.clone()));

        let mut rendered = overall.to_string();
        for (name, verdict) in &verdicts {
            rendered.push_str(&format!("\n  {name}: {verdict}"));
        }

        assert_eq!(
            rendered,
            "down: gone\n  first: up\n  second: degraded: slow\n  third: down: gone"
        );
    }

    #[test]
    fn probe_is_object_safe() {
        struct P;
        impl Extension for P {}
        impl HealthProbe for P {
            fn name(&self) -> &'static str {
                "p"
            }
            fn check<'a>(&'a self) -> BoxFuture<'a, Health> {
                Box::pin(async { Health::Up })
            }
        }
        let probes: Vec<Box<dyn HealthProbe>> = vec![Box::new(P)];
        assert_eq!(probes[0].name(), "p");
    }
}
