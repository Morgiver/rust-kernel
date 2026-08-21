//! The application's own bundle: the two jobs no feature owns.
//!
//! * **Health.** Every feature contributes probes to a point the kernel
//!   declares. Folding them into one verdict is a judgement about the whole
//!   process, so it belongs to whoever assembled the process — not to `ledger`,
//!   which knows nothing of the desk, nor to `orders`, which knows nothing of
//!   the journal.
//! * **Lifecycle.** The kernel publishes its phase transitions as events. A
//!   feature has no reason to care that phase five started; an operator does,
//!   and the application is the operator's side of the program.
//!
//! # Why this is a bundle and not three lines in `main`
//!
//! Both jobs need a [`Registry`], and a `Registry` is handed to
//! [`Bundle::register`] and to nowhere else: [`KernelBuilder`] has `bundle`,
//! `config_source`, `telemetry`, `shutdown_policy` and `capture_signals`, and
//! no way to register a listener, a component or a contribution directly. So
//! the application writes one bundle of its own and lists it last — last
//! because ties in the boot order break on registration order, which is what
//! puts [`Vitals`] behind every component whose probe it is about to read.
//!
//! It is still an application-layer bundle, and it obeys the same rule as the
//! other three: it names no `*-bundle` crate. It happens to name none of the
//! `*-contracts` crates either, because health and lifecycle are the kernel's
//! vocabulary rather than this application's.
//!
//! [`KernelBuilder`]: kernel::KernelBuilder

use std::sync::Arc;

use kernel::core::{ComponentError, Health, ListenerError, RegisterError};
use kernel::{
    BootContext, BoxFuture, Bundle, BundleManifest, Component, ComponentDescriptor, Flow,
    HealthReport, Listener, ListenerContext, Priority, Probe, Provider, Registry, Running,
    ShutdownRequested,
};

/// The name this bundle registers under, and the prefix its output carries.
const NAME: &str = "app";

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

/// Reads every contributed probe once, as the last component to boot.
///
/// Once, and at that moment: phase four is over but phase five has not started,
/// so what it prints is the state each feature booted into, not the state it
/// settles at. A process that answers a health endpoint would read the probes
/// again on every request — see [`fold`] for why this one cannot.
///
/// It owns nothing and releases nothing, so it has no `shutdown`: the whole
/// component is one read of a table other bundles filled.
struct Vitals;

impl Component for Vitals {
    fn name() -> &'static str {
        "app.vitals"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            report(&fold(cx.collect::<Probe>()).await);
            Ok(())
        })
    }
}

/// Runs every probe and folds the verdicts into the kernel's own report type.
///
/// This is [`kernel::aggregate`] written out by hand, and it is written out
/// because that function cannot be called from here. It takes an
/// `&ExtensionPoints`, and no public accessor anywhere hands one out:
/// `Kernel` exposes `handle`, `container` and `run`; `Container` exposes
/// bindings, config, telemetry and the handle; `BootContext` exposes
/// [`collect`](BootContext::collect), which returns borrowed items and not the
/// table they came from. So the one publicly reachable form of the contributed
/// probes is a `Vec<&Probe>` inside a component's `boot`, and the fold has to
/// happen there.
///
/// What the hand-written version loses, and a reader should not copy: the
/// kernel's runs the checks concurrently and caps each one at
/// [`PROBE_TIMEOUT`](kernel::PROBE_TIMEOUT), so a probe that never answers
/// becomes a verdict naming it. This loop is sequential and unbounded — a deaf
/// probe hangs the boot until the component's boot timeout fires.
async fn fold(probes: Vec<&Probe>) -> HealthReport {
    let mut checked = Vec::with_capacity(probes.len());
    for probe in probes {
        checked.push((probe.get().name(), probe.get().check().await));
    }

    HealthReport {
        overall: Health::worst_of(checked.iter().map(|(_, verdict)| verdict.clone())),
        probes: checked,
    }
}

/// Prints one report, overall verdict first.
fn report(report: &HealthReport) {
    println!("[{NAME}] health: {}", verdict(&report.overall));
    for (probe, health) in &report.probes {
        println!("[{NAME}]   {probe}: {}", verdict(health));
    }
}

/// Renders one verdict on one line. `Health` has `Debug`, not `Display`.
fn verdict(health: &Health) -> String {
    match health {
        Health::Up => "up".to_owned(),
        Health::Degraded { detail } => format!("degraded — {detail}"),
        Health::Down { detail } => format!("down — {detail}"),
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Narrates the kernel's own phases.
///
/// One type, two events. Dispatch is indexed by the payload type, so the two
/// registrations below need a turbofish to say which impl each one means —
/// which is the type index made visible rather than a wart.
struct Watch;

impl Listener<Running> for Watch {
    fn on_event<'a>(
        &'a self,
        event: &'a mut Running,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            println!(
                "[{NAME}] running: {} runnable(s) started — press Ctrl-C to stop",
                event.runnables
            );
            Ok(Flow::Continue)
        })
    }
}

impl Listener<ShutdownRequested> for Watch {
    /// The one kernel event that is dispatched rather than emitted, so a
    /// listener may add context before the shutdown ladder moves. The note goes
    /// back to the kernel with the event; the line goes to whoever is watching
    /// the terminal.
    fn on_event<'a>(
        &'a self,
        event: &'a mut ShutdownRequested,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            println!("[{NAME}] stopping: {:?}", event.reason);
            event.notes.push("seen by the application".to_owned());
            Ok(Flow::Continue)
        })
    }
}

// ---------------------------------------------------------------------------
// The bundle
// ---------------------------------------------------------------------------

/// The application's own bundle. Listed last.
#[derive(Debug, Default)]
pub struct Console;

impl Bundle for Console {
    /// Requires nothing. It reads what other bundles contributed, and a
    /// contribution is not a contract: nothing here would fail if the other
    /// three registered no probe at all.
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new(NAME, "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        registry.component(Provider::from_value(Arc::new(Vitals)));

        registry.listen::<Running, _>(Watch, Priority::NORMAL);
        registry.listen::<ShutdownRequested, _>(Watch, Priority::NORMAL);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kernel::core::{Extension, HealthProbe};

    use super::*;

    /// A probe with a fixed verdict, so the fold can be exercised without a
    /// kernel.
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

    #[tokio::test]
    async fn fold_keeps_the_worst() {
        let probes = [
            Probe::new(Fixed("first", Health::Up)),
            Probe::new(Fixed("second", Health::degraded("a backlog"))),
        ];

        let report = fold(probes.iter().collect()).await;

        assert_eq!(report.overall, Health::degraded("a backlog"));
        assert_eq!(report.probes.len(), 2);
        assert_eq!(report.probes[0].0, "first");
    }

    #[tokio::test]
    async fn empty_fold_is_up() {
        assert_eq!(fold(Vec::new()).await.overall, Health::Up);
    }

    #[test]
    fn verdicts_read_plainly() {
        assert_eq!(verdict(&Health::Up), "up");
        assert_eq!(verdict(&Health::down("closed")), "down — closed");
    }
}
