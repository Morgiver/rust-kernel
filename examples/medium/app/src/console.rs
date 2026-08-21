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
//! no verb of its own. The seven verbs live on the registry and stay there.
//!
//! What the application does NOT need is a type and two trait methods to reach
//! that form. [`FnBundle`] is a bundle whose registration pass is a closure,
//! so the three lines below are three lines, listed last — last because ties
//! in the boot order break on registration order, which is what puts
//! [`Vitals`] behind every component whose probe it is about to read.
//!
//! It is still an application-layer bundle, and it obeys the same rule as the
//! other three: it names no `*-bundle` crate. It happens to name none of the
//! `*-contracts` crates either, because health and lifecycle are the kernel's
//! vocabulary rather than this application's.
//!
//! # What this bundle prints, and where a copier must stop
//!
//! Both jobs below end in `println!`, and both do it from inside a kernel
//! trait: [`Component::boot`] for the health line, [`Listener::on_event`] for
//! the two lifecycle lines. Writing to standard output blocks the executor
//! thread it runs on. It is acceptable here for one reason — this is the
//! application layer, the lines are the program's own output, and there are
//! four of them for the whole run.
//!
//! A component or listener that writes an unbounded number of lines, or writes
//! anywhere slower than a terminal, does not do this: it hands what it has to
//! something that owns the writing. The convention the features hold to is the
//! other one — a failure goes to telemetry, never to standard output.
//!
//! [`KernelBuilder`]: kernel::KernelBuilder
//! [`Bundle::register`]: kernel::Bundle::register
//! [`Registry`]: kernel::Registry

use std::sync::Arc;

use kernel::core::{ComponentError, ListenerError};
use kernel::{
    BootContext, BoxFuture, BundleManifest, Component, ComponentDescriptor, Flow, FnBundle,
    Listener, ListenerContext, Priority, Provider, Running, ShutdownRequested, aggregate,
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
/// settles at. A process that answers a health request would call
/// [`aggregate`] again on every one; nothing about it is tied to boot.
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

    /// One call, and no loop of this application's own.
    ///
    /// [`aggregate`] takes the table of contributed extension points, which
    /// [`BootContext::extensions`] hands out, and it does two things a
    /// hand-written fold does not: it runs the checks concurrently, and it caps
    /// each one at [`PROBE_TIMEOUT`](kernel::PROBE_TIMEOUT) so that a probe
    /// which never answers becomes a verdict naming it instead of a boot that
    /// hangs until the component's own timeout fires.
    ///
    /// The report renders itself, so there is no rendering here either.
    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            println!("[{NAME}] health: {}", aggregate(cx.extensions()).await);
            Ok(())
        })
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
///
/// It requires nothing: it reads what other bundles contributed, and a
/// contribution is not a contract, so nothing here would fail if the other
/// three registered no probe at all. The manifest is stated anyway, because a
/// version is what a diagnostic naming this bundle prints.
pub fn bundle() -> FnBundle {
    FnBundle::new(NAME, |registry| {
        registry.component(Provider::from_value(Arc::new(Vitals)));

        registry.listen::<Running, _>(Watch, Priority::NORMAL);
        registry.listen::<ShutdownRequested, _>(Watch, Priority::NORMAL);

        Ok(())
    })
    .manifest(BundleManifest::new(NAME, "0.1.0"))
}

#[cfg(test)]
mod tests {
    use kernel::Probe;
    use kernel::core::{Extension, Health, HealthProbe};

    use super::*;

    /// A probe with a fixed verdict, so the aggregate can be exercised without
    /// a kernel.
    ///
    /// Hand-written: `kernel-testkit` ships a recorder, a parking runnable and
    /// a lifecycle-logging component, and no health probe.
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

    /// The table the kernel would have filled, filled by hand.
    fn contributed(probes: [Probe; 2]) -> kernel::component::DetachedBoot {
        let [first, second] = probes;
        BootContext::builder()
            .with_contribution(first)
            .with_contribution(second)
            .build()
    }

    /// What this bundle contributes to the report is one call: the worst
    /// verdict wins, and every probe is named under it.
    #[tokio::test]
    async fn aggregate_keeps_the_worst() {
        let detached = contributed([
            Probe::new(Fixed("first", Health::Up)),
            Probe::new(Fixed("second", Health::degraded("a backlog"))),
        ]);

        let report = aggregate(detached.context().extensions()).await;

        assert_eq!(report.overall, Health::degraded("a backlog"));
        assert_eq!(
            report.to_string(),
            "degraded: a backlog\n  first: up\n  second: degraded: a backlog"
        );
    }

    /// Nothing contributed is still a verdict, and still one line.
    #[tokio::test]
    async fn empty_report_is_up() {
        let detached = BootContext::builder().build();

        let report = aggregate(detached.context().extensions()).await;

        assert_eq!(report.overall, Health::Up);
        assert_eq!(report.to_string(), "up");
    }
}
