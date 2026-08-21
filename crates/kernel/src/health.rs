//! Aggregating the health probes the bundles contributed.
//!
//! The kernel aggregates and never exposes: serving this on a port is a
//! bundle's job. [`aggregate`] folds every contributed probe into one
//! [`HealthReport`] and stops there — it opens no socket, serves no endpoint
//! and picks no wire format. Publishing the report is the work of a bundle
//! that reads it.
//!
//! # One point, probes of every type
//!
//! An extension point is keyed by the type contributed to it, and two bundles
//! contribute probes of two unrelated types. [`Probe`] is the single type they
//! share: a bundle wraps its own probe in one and contributes that, so this
//! module collects the whole list without naming any of them.
//!
//! # No probe holds the report
//!
//! A probe answers about state it already observes, so it should return at
//! once. One that does not must not take the report down with it: every check
//! is capped at [`PROBE_TIMEOUT`], and a probe that has not answered by then is
//! reported [`Health::Down`] while the others report normally.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::fmt;

use kernel_core::{BoxFuture, Extension, Health, HealthProbe};
use tokio::time::timeout;

use crate::extension::ExtensionPoints;

/// How long a single probe may take before it is reported [`Health::Down`].
///
/// The cap is per probe, not per report: probes run concurrently, so a report
/// with any number of them takes at most this long.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// One contributed health probe, in the form the extension point collects.
///
/// The wrapper exists because an extension point is keyed by the contributed
/// type: without it, every probe type would be its own point and no aggregate
/// could reach them all. A bundle wraps whatever it implemented and contributes
/// the wrapper.
///
/// # Examples
///
/// ```
/// use kernel::core::{BoxFuture, Extension, Health, HealthProbe};
/// use kernel::health::Probe;
///
/// struct Sample;
///
/// impl Extension for Sample {}
///
/// impl HealthProbe for Sample {
///     fn name(&self) -> &'static str {
///         "sample"
///     }
///
///     fn check(&self) -> BoxFuture<'_, Health> {
///         Box::pin(async { Health::Up })
///     }
/// }
///
/// let probe = Probe::new(Sample);
/// assert_eq!(probe.get().name(), "sample");
/// ```
pub struct Probe(Box<dyn HealthProbe>);

impl Probe {
    /// Wraps a probe so that it can be contributed.
    pub fn new(probe: impl HealthProbe) -> Self {
        Self(Box::new(probe))
    }

    /// Borrows the wrapped probe.
    #[must_use]
    pub fn get(&self) -> &dyn HealthProbe {
        self.0.as_ref()
    }
}

impl Extension for Probe {}

impl fmt::Debug for Probe {
    /// Renders the probe's name, which is all of it that can be read without
    /// running the check.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Probe").field(&self.0.name()).finish()
    }
}

/// The health of the whole process, and of each probe that reported.
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// The worst verdict any probe returned.
    pub overall: Health,
    /// Each probe's name and verdict, in contribution order.
    pub probes: Vec<(&'static str, Health)>,
}

/// Runs every contributed probe concurrently and folds the verdicts.
///
/// An empty set of probes is [`Health::Up`]: nothing reported a problem.
pub async fn aggregate(points: &ExtensionPoints) -> HealthReport {
    let probes = points.collect::<Probe>();

    let names: Vec<&'static str> = probes.iter().map(|probe| probe.get().name()).collect();
    let checks: Vec<BoxFuture<'_, Health>> =
        probes.iter().map(|probe| bounded(probe.get())).collect();

    let verdicts = Concurrently::new(checks).await;
    let overall = Health::worst_of(verdicts.iter().cloned());

    HealthReport {
        overall,
        probes: names.into_iter().zip(verdicts).collect(),
    }
}

/// Caps one check at [`PROBE_TIMEOUT`], turning a probe that never answers into
/// a verdict that names it.
fn bounded(probe: &dyn HealthProbe) -> BoxFuture<'_, Health> {
    let name = probe.name();
    Box::pin(async move {
        timeout(PROBE_TIMEOUT, probe.check())
            .await
            .unwrap_or_else(|_| {
                Health::down(format!("{name} did not answer within {PROBE_TIMEOUT:?}"))
            })
    })
}

/// Drives every check on the calling task, all of them at once.
///
/// Spawning would be the ordinary way to run futures concurrently, and it is
/// not available here: a check borrows the probe it belongs to, which lives in
/// the collection the caller holds, so no check is `'static`. Polling them from
/// one future keeps the borrow and still leaves a slow probe unable to delay
/// the others.
struct Concurrently<'a> {
    /// Each check until it resolves, then `None`.
    pending: Vec<Option<BoxFuture<'a, Health>>>,
    /// Each verdict once its check resolved, in the same positions.
    verdicts: Vec<Option<Health>>,
}

impl<'a> Concurrently<'a> {
    fn new(checks: Vec<BoxFuture<'a, Health>>) -> Self {
        Self {
            verdicts: vec![None; checks.len()],
            pending: checks.into_iter().map(Some).collect(),
        }
    }
}

impl Future for Concurrently<'_> {
    /// The verdicts, in the order the checks were handed over.
    type Output = Vec<Health>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = self.get_mut();

        let mut waiting = false;
        for (slot, verdict) in state.pending.iter_mut().zip(state.verdicts.iter_mut()) {
            let Some(check) = slot.as_mut() else {
                continue;
            };
            match check.as_mut().poll(cx) {
                Poll::Ready(value) => {
                    *verdict = Some(value);
                    *slot = None;
                }
                Poll::Pending => waiting = true,
            }
        }

        if waiting {
            return Poll::Pending;
        }

        Poll::Ready(
            state
                .verdicts
                .iter_mut()
                // Infallible by construction: a slot is emptied only when its
                // verdict is written, and every slot is empty here.
                .map(|verdict| verdict.take().unwrap_or_default())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::future::pending;

    use kernel_core::ExtensionId;
    use tokio::time::{Instant, sleep};

    use crate::registry::ContributionEntry;

    /// Answers at once with the verdict it was built with.
    struct Fixed(&'static str, Health);

    impl Extension for Fixed {}

    impl HealthProbe for Fixed {
        fn name(&self) -> &'static str {
            self.0
        }

        fn check(&self) -> BoxFuture<'_, Health> {
            let verdict = self.1.clone();
            Box::pin(async move { verdict })
        }
    }

    /// Answers after a delay.
    struct Slow(&'static str, Duration);

    impl Extension for Slow {}

    impl HealthProbe for Slow {
        fn name(&self) -> &'static str {
            self.0
        }

        fn check(&self) -> BoxFuture<'_, Health> {
            let delay = self.1;
            Box::pin(async move {
                sleep(delay).await;
                Health::Up
            })
        }
    }

    /// Never answers at all.
    struct Deaf(&'static str);

    impl Extension for Deaf {}

    impl HealthProbe for Deaf {
        fn name(&self) -> &'static str {
            self.0
        }

        fn check(&self) -> BoxFuture<'_, Health> {
            Box::pin(pending())
        }
    }

    fn points(probes: Vec<Probe>) -> ExtensionPoints {
        let contributions = probes
            .into_iter()
            .enumerate()
            .map(|(index, probe)| ContributionEntry {
                extension: ExtensionId::of::<Probe>(),
                bundle: "first",
                order: u32::try_from(index).unwrap_or(u32::MAX),
                item: Box::new(probe),
            })
            .collect();

        ExtensionPoints::from_parts(vec![ExtensionId::of::<Probe>()], contributions)
    }

    #[tokio::test]
    async fn empty_set_is_up() {
        let report = aggregate(&points(Vec::new())).await;

        assert_eq!(report.overall, Health::Up);
        assert!(report.probes.is_empty());
    }

    #[tokio::test]
    async fn keeps_probe_order() {
        let report = aggregate(&points(vec![
            Probe::new(Fixed("one", Health::Up)),
            Probe::new(Fixed("two", Health::degraded("slow"))),
            Probe::new(Fixed("three", Health::Up)),
        ]))
        .await;

        let names: Vec<&str> = report.probes.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["one", "two", "three"]);
        assert_eq!(report.probes[1].1, Health::degraded("slow"));
    }

    #[tokio::test]
    async fn folds_worst_verdict() {
        let report = aggregate(&points(vec![
            Probe::new(Fixed("one", Health::Up)),
            Probe::new(Fixed("two", Health::degraded("slow"))),
            Probe::new(Fixed("three", Health::down("gone"))),
        ]))
        .await;

        assert_eq!(report.overall, Health::down("gone"));
        assert_eq!(report.probes[0].1, Health::Up);
    }

    // Three probes that each wait a second must cost one second, not three.
    #[tokio::test(start_paused = true)]
    async fn checks_run_concurrently() {
        let delay = Duration::from_secs(1);
        let started = Instant::now();

        let report = aggregate(&points(vec![
            Probe::new(Slow("one", delay)),
            Probe::new(Slow("two", delay)),
            Probe::new(Slow("three", delay)),
        ]))
        .await;

        assert!(started.elapsed() < delay * 2, "{:?}", started.elapsed());
        assert_eq!(report.overall, Health::Up);
        assert_eq!(report.probes.len(), 3);
    }

    // The one that matters: a probe that never returns must not hold the
    // report, and must be reported rather than omitted.
    #[tokio::test(start_paused = true)]
    async fn hanging_probe_is_down() {
        let report = aggregate(&points(vec![
            Probe::new(Fixed("one", Health::Up)),
            Probe::new(Deaf("two")),
        ]))
        .await;

        let detail = report.probes[1].1.detail();
        assert_eq!(report.probes[0].1, Health::Up);
        assert!(matches!(report.probes[1].1, Health::Down { .. }));
        assert!(detail.is_some_and(|detail| detail.contains("two")));
        assert!(matches!(report.overall, Health::Down { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_bounds_report() {
        let started = Instant::now();

        let report = aggregate(&points(vec![Probe::new(Deaf("one"))])).await;

        assert!(started.elapsed() >= PROBE_TIMEOUT);
        assert!(started.elapsed() < PROBE_TIMEOUT * 2);
        assert!(matches!(report.overall, Health::Down { .. }));
    }

    #[test]
    fn debug_names_probe() {
        let probe = Probe::new(Fixed("one", Health::Up));

        assert!(format!("{probe:?}").contains("one"));
    }
}
