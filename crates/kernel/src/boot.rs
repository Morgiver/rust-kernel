//! Phase four: instantiate the graph and boot the components.

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use kernel_core::error::BoxSource;
use kernel_core::{ComponentError, ComponentId, Level, Record, ShutdownPolicy, Telemetry};
use tokio::time::{Instant, timeout};

use crate::component::{BootContext, Component, ShutdownContext};
use crate::dispatcher::EventDispatcher;
use crate::events::{BootCompleted, BootStarted, ComponentBooted};
use crate::resolve::Resolved;
use crate::shutdown::ShutdownController;

/// Identity a failure is attributed to when it belongs to the graph rather than
/// to any registered component.
///
/// The shared table is instantiated before the first component boots, so a
/// provider that fails there has no component to blame. The index is out of
/// every registry's reach, which is what keeps this from colliding with a real
/// component.
const GRAPH: ComponentId = ComponentId::new("graph", u32::MAX);

/// Telemetry event name for a component that booted.
const BOOTED: &str = "kernel.component_booted";
/// Telemetry event name for the failure that ends phase four.
const FAILED: &str = "kernel.boot_failed";
/// Telemetry event name for a rollback shutdown that failed on its way out.
const ROLLBACK_FAILED: &str = "kernel.rollback_failed";

/// What phase four produced, in the order it actually happened.
///
/// The order is the OBSERVED one, not the planned one: shutdown reverses this
/// vector, so a boot that diverged from the plan still unwinds correctly.
pub struct Booted {
    /// Components that booted, in the order they booted.
    pub components: Vec<(ComponentId, Arc<dyn Component>)>,
}

impl core::fmt::Debug for Booted {
    /// Lists the identities only: `Component` carries no `Debug` supertrait,
    /// and requiring one would tax every implementor for the sake of a
    /// diagnostic.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Booted")
            .field(
                "components",
                &self.components.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Phase four failed, and what had to be unwound because of it.
#[derive(Debug)]
pub struct BootFailure {
    /// The component whose boot failed.
    pub component: ComponentId,
    /// Why it failed.
    pub source: ComponentError,
    /// Components stopped again, in the order they were stopped.
    pub rolled_back: Vec<ComponentId>,
}

/// A unit did not finish within the budget its descriptor gave it.
///
/// Private to this module: it is a cause, wrapped into the
/// [`ComponentError`] that names the unit, and never a type a caller matches
/// on.
#[derive(Debug)]
struct Overran {
    /// What the unit was doing.
    what: &'static str,
    /// The budget it was given.
    budget: Duration,
}

impl fmt::Display for Overran {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} did not return within {:?}", self.what, self.budget)
    }
}

impl std::error::Error for Overran {}

/// Instantiates every shared binding, then boots the components in plan order.
///
/// # Why every shared binding, and not only what the plan reaches
///
/// Sealing the container converts a late resolution into an error; it does not
/// prevent one. The design's ban on lazy resolution therefore holds only if
/// every `Shared` binding is already built when the seal lands. A `Shared`
/// binding reachable solely from the `requires` of a `Factory` or `Scoped`
/// provider is named by no component and no runnable, so a plan-driven boot
/// would skip it — and the first unit of work that built that factory would be
/// refused at run time, which is exactly the production failure the design
/// exists to prevent.
///
/// Instantiating the whole table is what makes the rule mechanical. It also
/// means a provider that fails only when run fails HERE, before anything
/// serves.
///
/// # The seal
///
/// A boot that reaches its end seals the container, and it is the sweep above
/// that earns the right to: by then every `Shared` value exists, so the seal
/// forbids nothing that was ever going to succeed. Sealing is idempotent, so a
/// caller that seals again after this returns changes nothing. A boot that
/// fails leaves the container open — rollback still resolves through it.
///
/// # The budget the rollback runs on
///
/// A boot that fails unwinds what it booted, and it does so on `policy` — the
/// budget the kernel was configured with — not on a constant of its own. The
/// policy is a parameter rather than a field of [`Resolved`] because phase
/// three neither has it nor needs it: it is the smaller of the two changes,
/// and it keeps the graph free of a value that only the driver reads.
pub(crate) async fn boot(
    resolved: &Resolved,
    policy: ShutdownPolicy,
) -> Result<Booted, BootFailure> {
    let started = Instant::now();
    let container = &resolved.container;
    let dispatcher: &EventDispatcher = &resolved.dispatcher;
    let telemetry = Arc::clone(container.telemetry());

    dispatcher.emit(BootStarted {
        components: resolved.plan.components.len(),
    });

    // The whole table, not the reachable part of it. See above.
    if let Err(error) = container.instantiate_shared().await {
        let source = ComponentError::new(GRAPH, Box::new(error));
        record_failure(&telemetry, GRAPH, &source);
        return Err(BootFailure {
            component: GRAPH,
            source,
            rolled_back: Vec::new(),
        });
    }

    let mut booted = Booted {
        components: Vec::new(),
    };
    let cx = BootContext::new(container, dispatcher, &resolved.extensions);

    for id in resolved.plan.components.iter().copied() {
        let entry = &resolved.components[id.index() as usize];

        let component = match (entry.build)(container).await {
            Ok(component) => component,
            Err(error) => {
                let source = ComponentError::new(id, Box::new(error));
                record_failure(&telemetry, id, &source);
                return Err(unwind(resolved, policy, booted, id, source).await);
            }
        };

        let at = Instant::now();
        let budget = component.descriptor().boot_timeout;
        if let Err(source) = bounded(budget, "boot", id, component.boot(&cx)).await {
            record_failure(&telemetry, id, &source);
            return Err(unwind(resolved, policy, booted, id, source).await);
        }
        let elapsed = at.elapsed();

        // Recorded only once the component is up, so the observed order is the
        // order of facts and not of intentions.
        booted.components.push((id, component));
        telemetry.record(
            Record::new(Level::Info, BOOTED)
                .with("component", id.name())
                .with("elapsed", elapsed),
        );
        dispatcher.emit(ComponentBooted {
            component: id,
            elapsed,
        });
    }

    container.seal();
    dispatcher.emit(BootCompleted {
        elapsed: started.elapsed(),
    });

    Ok(booted)
}

/// Stops the components of a partial boot, in the reverse of the observed order.
///
/// # Why it takes a context
///
/// A component is stopped through [`Component::shutdown`], which is handed a
/// [`ShutdownContext`]; the components alone cannot produce one. The caller
/// owns the shutdown ladder — phase six holds the real one, and [`boot`] opens
/// a stopping ladder of its own for the rollback it drives — so the context is
/// passed in rather than invented here.
///
/// A shutdown that fails is recorded and the walk continues: the component
/// still appears in the returned vector, because "was stopped" is what the
/// vector reports and an unclean stop is still a stop.
///
/// # The budget is per component, not per walk
///
/// `stop` is the [`ShutdownPolicy`]'s stop budget, and each component gets it
/// afresh, counted from the moment its own `shutdown` is called. One deadline
/// for the whole walk would charge a component for its neighbour's wait: two
/// components that each want two thirds of the budget are each within it, yet
/// the second would be cut having done nothing wrong. A unit is never
/// abandoned because another unit overran, so the walk costs the sum of the
/// per-component budgets in the worst case, and the kernel still never blocks
/// indefinitely.
pub(crate) async fn rollback(
    booted: Booted,
    cx: &ShutdownContext<'_>,
    stop: Duration,
) -> Vec<ComponentId> {
    let telemetry = Arc::clone(cx.telemetry());
    let mut stopped = Vec::with_capacity(booted.components.len());

    for (id, component) in booted.components.into_iter().rev() {
        let budget = allowance(component.descriptor().shutdown_timeout, stop);
        if let Err(error) = bounded(Some(budget), "shutdown", id, component.shutdown(cx)).await {
            telemetry.record(
                Record::new(Level::Error, ROLLBACK_FAILED)
                    .with("component", id.name())
                    .with("error", error.to_string()),
            );
        }
        stopped.push(id);
    }

    stopped
}

/// Runs one lifecycle call under an optional budget.
///
/// Elapsing drops the future rather than detaching it: a component that
/// overran its boot must not go on booting behind the kernel's back, and the
/// only way to guarantee that for a borrowed future is to stop polling it.
async fn bounded(
    budget: Option<Duration>,
    what: &'static str,
    id: ComponentId,
    call: impl Future<Output = Result<(), ComponentError>>,
) -> Result<(), ComponentError> {
    let Some(budget) = budget else {
        return call.await;
    };
    match timeout(budget, call).await {
        Ok(result) => result,
        Err(_) => Err(ComponentError::new(
            id,
            Box::new(Overran { what, budget }) as BoxSource,
        )),
    }
}

/// One component's own stop budget: the tighter of what it declared and what
/// the policy allows.
///
/// A descriptor shortens the policy's budget; it never extends it, so a
/// component asking for more than the policy grants gets the policy's figure.
fn allowance(declared: Option<Duration>, stop: Duration) -> Duration {
    declared.map_or(stop, |declared| declared.min(stop))
}

/// Rolls the partial boot back and assembles the failure it produced.
///
/// The ladder is opened here, at [`Stage::Stopping`], because nothing is
/// running yet: a boot that never reached phase five has no in-flight work to
/// drain. It is opened on the *configured* `policy`, so a kernel given a short
/// budget unwinds a failed boot on that budget and not on a default one.
///
/// [`Stage::Stopping`]: kernel_core::Stage::Stopping
async fn unwind(
    resolved: &Resolved,
    policy: ShutdownPolicy,
    booted: Booted,
    component: ComponentId,
    source: ComponentError,
) -> BootFailure {
    let (controller, shutdown) = ShutdownController::new(policy);
    controller.begin_stopping();
    let cx = ShutdownContext::new(&resolved.container, &resolved.dispatcher, &shutdown);
    let rolled_back = rollback(booted, &cx, policy.stop).await;
    controller.finish();

    BootFailure {
        component,
        source,
        rolled_back,
    }
}

/// Records the failure that ends phase four.
fn record_failure(telemetry: &Arc<dyn Telemetry>, id: ComponentId, source: &ComponentError) {
    telemetry.record(
        Record::new(Level::Error, FAILED)
            .with("component", id.name())
            .with("error", source.to_string()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use kernel_core::{
        BoxFuture, BuildError, ComponentDescriptor, ConfigTree, ContractRef, Event, Flow, Lifetime,
        ListenerError, Priority, RecordingTelemetry,
    };

    use crate::dispatcher::{Listener, ListenerContext};
    use crate::provider::Provider;
    use crate::registry::Registry;
    use crate::resolve::resolve;

    /// Names the test components answer to, indexed by their const parameter.
    const NAMES: [&str; 4] = ["alpha", "beta", "gamma", "delta"];

    /// Shared, ordered record of what happened.
    #[derive(Clone, Default)]
    struct Trace(Arc<Mutex<Vec<String>>>);

    impl Trace {
        fn push(&self, entry: String) {
            self.0.lock().expect("trace").push(entry);
        }

        fn entries(&self) -> Vec<String> {
            self.0.lock().expect("trace").clone()
        }
    }

    /// What a lifecycle call does when it is reached.
    #[derive(Clone, Copy)]
    enum Step {
        /// Return successfully.
        Pass,
        /// Return a failure.
        Fail,
        /// Wait, then return successfully.
        Wait(Duration),
    }

    /// A component whose behaviour on both lifecycle calls is data.
    ///
    /// The const parameter is what makes each one a distinct type, which is
    /// what a registry needs: a component is bound under its own type, so two
    /// components of one type would be one ambiguous binding.
    struct Unit<const N: usize> {
        trace: Trace,
        booting: Step,
        stopping: Step,
        budget: Option<Duration>,
    }

    impl<const N: usize> Unit<N> {
        fn new(trace: &Trace) -> Self {
            Self {
                trace: trace.clone(),
                booting: Step::Pass,
                stopping: Step::Pass,
                budget: None,
            }
        }

        fn booting(mut self, step: Step) -> Self {
            self.booting = step;
            self
        }

        fn stopping(mut self, step: Step) -> Self {
            self.stopping = step;
            self
        }

        fn budget(mut self, budget: Duration) -> Self {
            self.budget = Some(budget);
            self
        }

        fn id() -> ComponentId {
            ComponentId::new(NAMES[N], u32::try_from(N).expect("small"))
        }

        async fn step(&self, step: Step, what: &'static str) -> Result<(), ComponentError> {
            if let Step::Wait(delay) = step {
                tokio::time::sleep(delay).await;
            }
            // After the wait, so a call that was dropped at its deadline leaves
            // no trace at all.
            self.trace.push(format!("{what}:{}", NAMES[N]));
            match step {
                Step::Fail => Err(ComponentError::new(Self::id(), "refused".into())),
                _ => Ok(()),
            }
        }
    }

    impl<const N: usize> Component for Unit<N> {
        fn name() -> &'static str {
            NAMES[N]
        }

        fn descriptor(&self) -> ComponentDescriptor {
            let descriptor = ComponentDescriptor::new();
            match self.budget {
                Some(budget) => descriptor.boot_timeout(budget),
                None => descriptor,
            }
        }

        fn boot<'a>(
            &'a self,
            _cx: &'a BootContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(self.step(self.booting, "boot"))
        }

        fn shutdown<'a>(
            &'a self,
            _cx: &'a ShutdownContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(self.step(self.stopping, "stop"))
        }
    }

    /// A contract nothing in the plan names.
    trait Buried: Send + Sync + 'static {}

    struct Deep;

    impl Buried for Deep {}

    /// A contract bound per resolution, which is what keeps `Buried` out of
    /// every plan: a factory is built by whoever asks, and nobody asks here.
    trait PerCall: Send + Sync + 'static {}

    struct Once;

    impl PerCall for Once {}

    fn registry(telemetry: &RecordingTelemetry) -> Registry {
        Registry::new(
            Arc::new(ConfigTree::empty()),
            Arc::new(telemetry.clone()) as Arc<dyn Telemetry>,
        )
    }

    /// The plan's identities, which are what `Booted` and `BootFailure` carry.
    ///
    /// A component is identified by the name the registry recorded for it, not
    /// by the one its descriptor answers with, so the expectations below are
    /// written against the plan rather than against a literal.
    fn planned(resolved: &Resolved) -> Vec<ComponentId> {
        resolved.plan.components.clone()
    }

    #[tokio::test]
    async fn boots_in_plan_order() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let mut registry = registry(&sink);
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));
        registry.component(Provider::from_value(Arc::new(Unit::<1>::new(&trace))));
        registry.component(Provider::from_value(Arc::new(Unit::<2>::new(&trace))));

        let resolved = resolve(registry, &[]).expect("resolve");
        let booted = boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect("boot");

        assert_eq!(trace.entries(), ["boot:alpha", "boot:beta", "boot:gamma"]);
        let observed: Vec<ComponentId> = booted.components.iter().map(|(id, _)| *id).collect();
        assert_eq!(observed, planned(&resolved));
        assert!(format!("{booted:?}").contains("alpha"));
    }

    #[tokio::test]
    async fn builds_unreached_binding() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let built = Arc::new(AtomicUsize::new(0));
        let mut registry = registry(&sink);

        // Reachable from the `requires` of a factory and from nowhere else, so
        // no component and no runnable names it and no plan mentions it.
        let counter = Arc::clone(&built);
        registry.provide::<dyn Buried>(Provider::from_fn(move |_container| {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(Arc::new(Deep) as Arc<dyn Buried>)
            })
        }));
        registry.provide::<dyn PerCall>(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    container
                        .get::<dyn Buried>()
                        .await
                        .map_err(|error| BuildError::new("per-call", Box::new(error)))?;
                    Ok(Arc::new(Once) as Arc<dyn PerCall>)
                })
            })
            .lifetime(Lifetime::Factory)
            .requires([ContractRef::of::<dyn Buried>()]),
        );
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));

        let resolved = resolve(registry, &[]).expect("resolve");
        assert!(!resolved.plan.components.is_empty());

        boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect("boot");

        // Built by phase four, before the seal, though nothing in the plan
        // reaches it.
        assert_eq!(built.load(Ordering::Relaxed), 1);

        resolved.container.seal();
        assert!(resolved.container.is_sealed());
        assert!(resolved.container.get::<dyn Buried>().await.is_ok());
        // And the factory that needs it still builds, sealed or not.
        assert!(resolved.container.get::<dyn PerCall>().await.is_ok());
        assert_eq!(built.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn boot_seals_container() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let mut registry = registry(&sink);
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));

        let resolved = resolve(registry, &[]).expect("resolve");
        assert!(!resolved.container.is_sealed());

        boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect("boot");

        assert!(resolved.container.is_sealed());
    }

    #[tokio::test]
    async fn provider_failure_stops_boot() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let mut registry = registry(&sink);
        registry.provide::<dyn Buried>(Provider::from_fn(|_container| {
            Box::pin(async { Err(BuildError::new("buried", "unavailable".into())) })
        }));
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));

        let resolved = resolve(registry, &[]).expect("resolve");
        let failure = boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect_err("boot fails");

        assert_eq!(failure.component, GRAPH);
        assert!(failure.rolled_back.is_empty());
        assert!(failure.source.to_string().contains("unavailable"));
        // Nothing booted: the sweep runs first.
        assert!(trace.entries().is_empty());
        assert!(sink.contains(FAILED));
    }

    #[tokio::test]
    async fn rolls_back_in_reverse() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let mut registry = registry(&sink);
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));
        registry.component(Provider::from_value(Arc::new(Unit::<1>::new(&trace))));
        registry.component(Provider::from_value(Arc::new(
            Unit::<2>::new(&trace).booting(Step::Fail),
        )));
        registry.component(Provider::from_value(Arc::new(Unit::<3>::new(&trace))));

        let resolved = resolve(registry, &[]).expect("resolve");
        let plan = planned(&resolved);
        let failure = boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect_err("boot fails");

        assert_eq!(failure.component, plan[2]);
        // Reverse of what was observed, which is a strict prefix of the plan:
        // reversing the plan would have tried to stop `delta` and `gamma`.
        assert_eq!(failure.rolled_back, [plan[1], plan[0]]);
        assert_eq!(
            trace.entries(),
            [
                "boot:alpha",
                "boot:beta",
                "boot:gamma",
                "stop:beta",
                "stop:alpha"
            ]
        );
    }

    #[tokio::test]
    async fn rollback_survives_failure() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let mut registry = registry(&sink);
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));
        registry.component(Provider::from_value(Arc::new(
            Unit::<1>::new(&trace).stopping(Step::Fail),
        )));
        registry.component(Provider::from_value(Arc::new(
            Unit::<2>::new(&trace).booting(Step::Fail),
        )));

        let resolved = resolve(registry, &[]).expect("resolve");
        let plan = planned(&resolved);
        let failure = boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect_err("boot fails");

        // `beta` refused to stop, and `alpha` was stopped all the same.
        assert_eq!(failure.rolled_back, [plan[1], plan[0]]);
        assert!(
            trace
                .entries()
                .ends_with(&["stop:beta".into(), "stop:alpha".into()])
        );
        assert!(sink.contains(ROLLBACK_FAILED));
    }

    #[tokio::test(start_paused = true)]
    async fn boot_timeout_fails() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let mut registry = registry(&sink);
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));
        registry.component(Provider::from_value(Arc::new(
            Unit::<1>::new(&trace)
                .booting(Step::Wait(Duration::from_secs(60)))
                .budget(Duration::from_secs(1)),
        )));

        let resolved = resolve(registry, &[]).expect("resolve");
        let plan = planned(&resolved);
        let failure = boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect_err("boot fails");

        assert_eq!(failure.component, plan[1]);
        assert!(failure.source.to_string().contains("boot did not return"));
        assert_eq!(failure.rolled_back, [plan[0]]);

        // The overrunning future was dropped, not detached: it never reached
        // the line past its wait, however long the runtime goes on.
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!(trace.entries(), ["boot:alpha", "stop:alpha"]);
    }

    #[tokio::test(start_paused = true)]
    async fn rollback_bounds_shutdown() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let mut registry = registry(&sink);
        registry.component(Provider::from_value(Arc::new(
            Unit::<0>::new(&trace).stopping(Step::Wait(Duration::from_secs(600))),
        )));
        registry.component(Provider::from_value(Arc::new(
            Unit::<1>::new(&trace).booting(Step::Fail),
        )));

        let resolved = resolve(registry, &[]).expect("resolve");
        let plan = planned(&resolved);
        let failure = boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect_err("boot fails");

        // The deaf component is abandoned at the ladder's deadline and still
        // counts as stopped; the kernel does not wait ten minutes for it.
        assert_eq!(failure.rolled_back, [plan[0]]);
        assert_eq!(trace.entries(), ["boot:alpha", "boot:beta"]);
        assert!(sink.contains(ROLLBACK_FAILED));
    }

    #[tokio::test]
    async fn empty_plan_boots() {
        let sink = RecordingTelemetry::new();
        let resolved = resolve(registry(&sink), &[]).expect("resolve");

        let booted = boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect("boot");

        assert!(booted.components.is_empty());
        assert!(resolved.container.is_sealed());
    }

    /// Records its tag whenever the event it watches arrives.
    struct Tap<E> {
        tag: &'static str,
        seen: Arc<Mutex<Vec<&'static str>>>,
        event: core::marker::PhantomData<fn() -> E>,
    }

    impl<E: Event> Tap<E> {
        fn new(tag: &'static str, seen: &Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                tag,
                seen: Arc::clone(seen),
                event: core::marker::PhantomData,
            }
        }
    }

    impl<E: Event> Listener<E> for Tap<E> {
        fn on_event<'a>(
            &'a self,
            _event: &'a mut E,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async move {
                self.seen.lock().expect("seen").push(self.tag);
                Ok(Flow::Continue)
            })
        }
    }

    #[tokio::test]
    async fn emits_boot_events() {
        let trace = Trace::default();
        let sink = RecordingTelemetry::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut registry = registry(&sink);
        registry.listen(Tap::<BootStarted>::new("started", &seen), Priority::NORMAL);
        registry.listen(
            Tap::<ComponentBooted>::new("booted", &seen),
            Priority::NORMAL,
        );
        registry.listen(
            Tap::<BootCompleted>::new("completed", &seen),
            Priority::NORMAL,
        );
        registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&trace))));

        let resolved = resolve(registry, &[]).expect("resolve");
        boot(&resolved, ShutdownPolicy::DEFAULT)
            .await
            .expect("boot");

        // `emit` hands each event to a task of its own, so let those run.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        let seen = seen.lock().expect("seen").clone();
        assert_eq!(seen.len(), 3);
        assert!(seen.contains(&"started"));
        assert!(seen.contains(&"booted"));
        assert!(seen.contains(&"completed"));
    }
}
