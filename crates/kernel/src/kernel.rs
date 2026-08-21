//! The state machine itself, and the builder that produces it.
//!
//! # Two halves, and the line between them
//!
//! [`KernelBuilder::build`] runs phases one to three — configure, register,
//! resolve — and instantiates nothing at all. [`Kernel::run`] runs phases four
//! to seven — boot, run, shutdown, terminated — and is the only half that
//! builds an object or starts a task. The line between them is the design's
//! central rule: every graph error appears by phase three, so a kernel that
//! exists is a kernel whose graph is closed.
//!
//! # Where each lifecycle event is published
//!
//! The dispatcher is built by phase three, so nothing can be published before
//! it exists. [`BundleRegistered`] and [`GraphResolved`] are therefore emitted
//! at the end of `build`, which is the earliest moment a listener can be
//! reached at all; everything from [`BootStarted`](crate::events::BootStarted)
//! onwards is emitted as it happens. [`ShutdownRequested`] alone is
//! *dispatched*: a listener may add notes to it, and it is awaited before the
//! ladder moves so those notes reach whoever reads the request.

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use kernel_core::{
    ConfigSource, ContractId, KernelError, Level, NoopTelemetry, Outcome, Record, RunError,
    RunErrorKind, RunnableId, ShutdownError, ShutdownPolicy, Telemetry,
};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::boot::{Booted, RolledBack, boot, rollback};
use crate::bundle::Bundle;
use crate::component::ShutdownContext;
use crate::config::ConfigChain;
use crate::container::Container;
use crate::dispatcher::EventDispatcher;
use crate::events::{
    BundleRegistered, Draining, GraphResolved, Running, ShutdownReason, ShutdownRequested, Stopped,
    Stopping,
};
use crate::health::Probe;
use crate::registry::Registry;
use crate::resolve::{Resolved, resolve};
use crate::runnable::Runnable;
use crate::shutdown::{KernelHandle, Shutdown, ShutdownController};
use crate::supervisor::Supervisor;

/// Telemetry event name for the end of phase one.
const CONFIGURED: &str = "kernel.configured";
/// Telemetry event name for the end of phase two.
const REGISTERED: &str = "kernel.registered";
/// Telemetry event name for the end of phase three.
const RESOLVED: &str = "kernel.resolved";
/// Telemetry event name for a phase of the build that failed.
const BUILD_FAILED: &str = "kernel.build_failed";
/// Telemetry event name for the start of phase five.
const RUNNING: &str = "kernel.running";
/// Telemetry event name for the first rung of the ladder.
const DRAINING: &str = "kernel.draining";
/// Telemetry event name for the second rung of the ladder.
const STOPPING: &str = "kernel.stopping";
/// Telemetry event name for the request that started phase six.
const REQUESTED: &str = "kernel.shutdown_requested";
/// Telemetry event name for the end of phase seven.
const STOPPED: &str = "kernel.stopped";
/// Telemetry event name for a listener that failed on the shutdown request.
const LISTENER_FAILED: &str = "kernel.request_listener_failed";
/// Telemetry event name for a shutdown request that outlived the drain budget.
const REQUEST_OVERRAN: &str = "kernel.request_overran";
/// Telemetry event name for a runnable that could not be resolved at start.
const START_FAILED: &str = "kernel.runnable_start_failed";
/// Telemetry event name for detached emissions that outlived the stop.
const EMISSIONS_LEFT: &str = "kernel.emissions_left";

/// Unit a failure of the kernel's own driver is attributed to.
///
/// [`ShutdownError`] names its unit with a string, and the driver is not a
/// registered unit: it is the thing that drives them.
const KERNEL: &str = "kernel";

/// Cause an essential runnable that left the others behind is reported under.
///
/// It returned cleanly, so it produced no error of its own; the ending is the
/// failure, and it needs a sentence rather than a source.
const ESSENTIAL_LEFT: &str = "essential runnable returned while others were still running";

/// The registry hook `kernel-testkit` installs; see `KernelBuilder::hook`.
#[cfg(feature = "testing")]
type RegistryHook = Box<dyn FnOnce(&mut Registry) + Send>;

/// Assembles a kernel: phases one to three, and nothing else.
///
/// `build` runs the whole validation and instantiates nothing. If it returns
/// `Ok`, the configuration is valid, every contract is satisfied, the graph has
/// no cycle — and nothing has run yet.
pub struct KernelBuilder {
    /// Configuration sources, in the order they were appended.
    sources: ConfigChain,
    /// Bundles, in declaration order — which is registration order.
    bundles: Vec<Box<dyn Bundle>>,
    /// Where the kernel's own records go.
    telemetry: Arc<dyn Telemetry>,
    /// The drain and stop budgets phase six runs on.
    policy: ShutdownPolicy,
    /// Whether the kernel installs its own stop-signal handler.
    capture_signals: bool,
    /// Ran after every bundle has registered and before phase three.
    ///
    /// The hook `kernel-testkit` substitutes bindings through. It exists only
    /// under the `testing` feature, which `ci/check-testing-feature.sh`
    /// refuses on any non-dev dependency edge — so in a production graph this
    /// field, and the hook that installs it, are not compiled at all.
    ///
    /// Within `cargo test` of a crate that dev-depends on `kernel-testkit`,
    /// cargo unifies the feature across the whole build and the hook IS
    /// reachable from any test there, without naming a `kernel-testkit` type.
    /// The substitution API still lives on `kernel_testkit::TestBuilder`,
    /// which is what keeps a substitution in the phase order; the type does
    /// not, and cannot, hold the boundary on its own.
    #[cfg(feature = "testing")]
    hook: Option<RegistryHook>,
}

impl KernelBuilder {
    /// A builder with no sources, no bundles, and telemetry disabled.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: ConfigChain::new(),
            bundles: Vec::new(),
            telemetry: Arc::new(NoopTelemetry),
            policy: ShutdownPolicy::DEFAULT,
            capture_signals: true,
            #[cfg(feature = "testing")]
            hook: None,
        }
    }

    /// Installs the registry hook. Not public API; see the field.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn __register_hook(mut self, hook: RegistryHook) -> Self {
        self.hook = Some(hook);
        self
    }

    /// Appends a configuration source. Later sources win, leaf by leaf.
    #[must_use]
    pub fn config_source(mut self, source: impl ConfigSource) -> Self {
        self.sources.push(source);
        self
    }

    /// Appends a bundle. Declaration order is registration order, and it is the
    /// only tie-break in the topological sort.
    #[must_use]
    pub fn bundle(mut self, bundle: impl Bundle) -> Self {
        self.bundles.push(Box::new(bundle));
        self
    }

    /// Where the kernel's own records go. Defaults to a no-op sink.
    #[must_use]
    pub fn telemetry(mut self, telemetry: Arc<dyn Telemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// The drain and stop budgets. A descriptor may shorten them per unit.
    #[must_use]
    pub fn shutdown_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Whether the kernel installs its own stop-signal handler. On by default.
    #[must_use]
    pub fn capture_signals(mut self, enabled: bool) -> Self {
        self.capture_signals = enabled;
        self
    }

    /// Runs phases one to three. Aggregates every failure of each phase.
    ///
    /// Each phase reports **all** of its failures at once; the first phase that
    /// fails stops the sequence. There is nothing to learn from resolving a
    /// graph that was built from a configuration which did not load, and a
    /// second wave of errors caused by the first would bury it.
    ///
    /// # Errors
    ///
    /// [`KernelError::Config`] if any source failed to load,
    /// [`KernelError::Register`] if any bundle failed to register, and
    /// [`KernelError::Resolve`] if the graph does not close. Nothing has been
    /// instantiated in any of the three cases.
    pub async fn build(self) -> Result<Kernel, KernelError> {
        let telemetry = Arc::clone(&self.telemetry);

        // Phase one.
        let tree = match self.sources.load() {
            Ok(tree) => tree,
            Err(errors) => {
                return Err(refused(
                    &telemetry,
                    "configure",
                    KernelError::Config(errors),
                ));
            }
        };
        telemetry.record(
            Record::new(Level::Info, CONFIGURED).with("sources", count(self.sources.len())),
        );

        // Phase two.
        let mut registry = Registry::new(Arc::new(tree), Arc::clone(&telemetry));
        // The kernel's own extension point. No bundle can declare it, and a
        // contribution to a point nobody declared is a phase-three error — so
        // without this line every contributed probe would be refused.
        registry.declare_extension_point::<Probe>();

        let mut manifests = Vec::with_capacity(self.bundles.len());
        let mut failures = Vec::new();
        for bundle in &self.bundles {
            let manifest = bundle.manifest();
            // Attribution is what the kernel knows, not what a bundle claims.
            registry.enter_bundle(manifest.name);
            if let Err(error) = bundle.register(&mut registry) {
                failures.push(error);
            }
            manifests.push(manifest);
        }
        if !failures.is_empty() {
            return Err(refused(
                &telemetry,
                "register",
                KernelError::Register(failures),
            ));
        }

        // After every bundle, before phase three: a substitution must be seen
        // by the graph validation, not smuggled past it.
        #[cfg(feature = "testing")]
        if let Some(hook) = self.hook {
            registry.enter_bundle("<substituted>");
            hook(&mut registry);
        }
        telemetry
            .record(Record::new(Level::Info, REGISTERED).with("bundles", count(manifests.len())));

        // Phase three.
        let resolved = match resolve(registry, &manifests) {
            Ok(resolved) => resolved,
            Err(errors) => {
                return Err(refused(&telemetry, "resolve", KernelError::Resolve(errors)));
            }
        };
        telemetry.record(
            Record::new(Level::Info, RESOLVED)
                .with("components", count(resolved.plan.components.len()))
                .with("runnables", count(resolved.plan.runnables.len())),
        );

        // Phase two's notifications, published at the first moment a listener
        // can be reached: the dispatcher is what phase three just built.
        for manifest in &manifests {
            resolved.dispatcher.emit(BundleRegistered {
                bundle: manifest.name,
            });
        }
        resolved.dispatcher.emit(GraphResolved {
            bindings: bindings_of(&resolved),
            components: resolved.plan.components.len(),
            runnables: resolved.plan.runnables.len(),
        });

        Ok(Kernel {
            resolved,
            policy: self.policy,
            capture_signals: self.capture_signals,
        })
    }
}

impl Default for KernelBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for KernelBuilder {
    /// Counts what was declared: neither a source nor a bundle carries a
    /// `Debug` bound, and requiring one would tax every implementor for the
    /// sake of a diagnostic.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelBuilder")
            .field("sources", &self.sources.len())
            .field("bundles", &self.bundles.len())
            .field("policy", &self.policy)
            .field("capture_signals", &self.capture_signals)
            .finish_non_exhaustive()
    }
}

/// A validated graph, ready to boot.
pub struct Kernel {
    /// The graph phase three closed.
    resolved: Resolved,
    /// The budgets phase six runs on.
    policy: ShutdownPolicy,
    /// Whether phase five installs a stop-signal handler.
    capture_signals: bool,
}

impl Kernel {
    /// Starts a builder.
    #[must_use]
    pub fn builder() -> KernelBuilder {
        KernelBuilder::new()
    }

    /// A handle that asks this kernel to stop. Clonable, and resolvable from
    /// the container so any component can reach it.
    ///
    /// It is the very handle every [`BootContext`](crate::BootContext) and
    /// [`RunContext`](crate::RunContext) hands out, and the one
    /// [`Container::handle`] answers with: a unit that asks for a stop and a
    /// caller that asks for one are asking the same object.
    #[must_use]
    pub fn handle(&self) -> KernelHandle {
        self.resolved.container.handle()
    }

    /// The dispatcher phase three built, for a caller that wants to reach the
    /// listeners without running the kernel.
    ///
    /// It is the very dispatcher every phase publishes through, so an event
    /// emitted here reaches the same table a component's would. Its main use
    /// outside a run is [`EventDispatcher::settle`], which resolves once every
    /// detached emission has finished walking that table: without this
    /// accessor it is reachable only from inside a boot, run or shutdown
    /// context, and a caller that emitted before running had no way to wait
    /// for what it emitted.
    #[must_use]
    pub fn dispatcher(&self) -> &Arc<EventDispatcher> {
        &self.resolved.dispatcher
    }

    /// The container, for a caller that wants to reach into the graph before
    /// running.
    ///
    /// Nothing is built yet: resolving through it here builds the value early,
    /// which is exactly what phase four is for. Reach in to read the
    /// configuration or the telemetry sink, not to pre-build the graph.
    #[must_use]
    pub fn container(&self) -> &Container {
        &self.resolved.container
    }

    /// Runs phases four to seven.
    ///
    /// # What the shutdown budget costs at worst
    ///
    /// Phase six stops the runnables first and the components second, and
    /// [`ShutdownPolicy`]'s `drain` and `stop` are PER-UNIT budgets rather
    /// than budgets for a phase. The runnables stop concurrently, so their
    /// half costs `drain + stop` however many there are. The components are
    /// stopped one after another and each gets `stop` afresh, counted from the
    /// moment its own `shutdown` is called.
    ///
    /// The shutdown request is dispatched before either half, and that wait is
    /// capped at `drain`: it is where the drain begins, and an awaited dispatch
    /// hands a listener the whole process until it returns.
    ///
    /// The detached emissions are awaited twice — once before the components
    /// go down, once after the last event of the run — and each wait is capped
    /// at `stop` as well.
    ///
    /// Worst case for a whole shutdown is therefore
    /// `2 × drain + stop + (stop × components) + 2 × stop` — it grows with the
    /// number of components, and that is the price, stated rather than hidden.
    /// What it buys is the rule that a unit is never abandoned because another
    /// unit overran. A shared deadline would let one slow component consume the
    /// walk's budget and have its neighbour cut for it, which reports a failure
    /// against a unit that did nothing wrong.
    ///
    /// A component that declares a
    /// [`shutdown_timeout`](kernel_core::ComponentDescriptor::shutdown_timeout)
    /// shortens its own budget and never extends it, so bounding the total
    /// more tightly is in the components' own hands.
    ///
    /// # Dropping this future does not skip the stop
    ///
    /// Phases four to seven are driven on their own task; this future only
    /// awaits it. Dropping it REQUESTS a stop rather than skipping one, and the
    /// task carries it through. What the caller loses by dropping is the
    /// [`Outcome`], not the release of resources — so selecting over `run` is
    /// safe, as long as the runtime outlives the drop.
    ///
    /// # The wait after this one belongs to the caller
    ///
    /// Every rung above is bounded, and this returning means the ladder is
    /// over. It does NOT mean every task is gone: a runnable that overran its
    /// budget was aborted, recorded as `runnable.abandoned`, and never waited
    /// for — and an abort is only observed at an await point, so one that never
    /// reaches one goes on occupying a worker thread.
    ///
    /// The kernel does not own the runtime and cannot end it. A multi-threaded
    /// `Runtime` joins every worker thread in its own `Drop`, with no bound, so
    /// an application that ends on that drop — which is what `#[tokio::main]`
    /// does — can complete this whole ladder, report a successful [`Outcome`],
    /// and never exit. Tear the runtime down with a bound instead:
    ///
    /// ```no_run
    /// # use core::time::Duration;
    /// # fn build() -> kernel::KernelBuilder { kernel::Kernel::builder() }
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .enable_all()
    ///     .build()
    ///     .expect("a runtime");
    /// let outcome = runtime.block_on(async {
    ///     build().build().await.expect("a graph").run().await
    /// });
    /// runtime.shutdown_timeout(Duration::from_secs(5));
    /// # let _ = outcome;
    /// ```
    ///
    /// Both examples in this repository end that way, and
    /// `crates/kernel/tests/exit.rs` holds the bound against a runnable built
    /// to defeat everything except it.
    pub async fn run(self) -> Outcome {
        // Armed before the task is spawned, so a drop between the two still
        // reaches the kernel: the handle is the same object the driver watches.
        let mut stopper = StopOnDrop(Some(self.handle()));
        let driver = tokio::spawn(drive(self));

        // Dropping a `JoinHandle` detaches its task, it does not cancel it —
        // which is what makes the claim above true rather than hopeful.
        let ended = driver.await;
        stopper.disarm();

        match ended {
            Ok(outcome) => outcome,
            // The driver itself came apart. Nothing else can report it, so it
            // is reported here rather than propagated as an unwind.
            Err(join) => Outcome::Failed(KernelError::Shutdown(vec![ShutdownError::failed(
                KERNEL,
                join.to_string().into(),
            )])),
        }
    }
}

impl fmt::Debug for Kernel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Kernel")
            .field("components", &self.resolved.plan.components.len())
            .field("runnables", &self.resolved.plan.runnables.len())
            .field("policy", &self.policy)
            .field("capture_signals", &self.capture_signals)
            .finish_non_exhaustive()
    }
}

/// Asks the kernel to stop unless it is disarmed first.
///
/// This is the whole of the "dropping `run` requests a stop" guarantee: the
/// guard is dropped with the future, whether the future was awaited to the end
/// or abandoned halfway.
struct StopOnDrop(Option<KernelHandle>);

impl StopOnDrop {
    /// Gives up the request, for a run that ended on its own.
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.shutdown();
        }
    }
}

/// Phases four to seven, from a boot to an [`Outcome`].
///
/// Owns everything it needs, so that it can be spawned and outlive the future
/// that awaited it.
async fn drive(kernel: Kernel) -> Outcome {
    let Kernel {
        resolved,
        policy,
        capture_signals,
    } = kernel;
    let telemetry = Arc::clone(resolved.container.telemetry());
    let dispatcher = Arc::clone(&resolved.dispatcher);

    // Phase four. `boot` instantiates the whole shared table, boots the
    // components in plan order, seals the container, and rolls back on its own
    // if it fails — so a failure here has already been unwound.
    let booted = match boot(&resolved, policy).await {
        Ok(booted) => booted,
        Err(failure) => {
            return Outcome::Failed(KernelError::Boot {
                component: failure.component,
                source: failure.source,
                rolled_back: failure.rolled_back,
            });
        }
    };

    // One ladder for the whole shutdown, held behind an `Arc` because a signal
    // handler moves it from a task of its own.
    let (controller, shutdown) = ShutdownController::new(policy);
    let controller = Arc::new(controller);

    // Phase five.
    let units = match units(&resolved).await {
        Ok(units) => units,
        Err(error) => {
            telemetry.record(
                Record::new(Level::Error, START_FAILED)
                    .with("runnable", error.runnable().name())
                    .with("error", error.to_string()),
            );
            return give_up(&resolved, booted, &controller, policy, error).await;
        }
    };

    let started = units.len();
    let mut supervisor = Supervisor::start(
        units,
        resolved.container.clone(),
        Arc::clone(&dispatcher),
        shutdown.clone(),
        resolved.container.handle(),
    );
    telemetry.record(Record::new(Level::Info, RUNNING).with("runnables", count(started)));
    dispatcher.emit(Running { runnables: started });

    let signals = signals::capture(
        capture_signals,
        Arc::clone(&controller),
        Arc::clone(&telemetry),
    );
    let ladder = announce(
        shutdown.clone(),
        Arc::clone(&dispatcher),
        Arc::clone(&telemetry),
    );

    let reason = supervisor.watch(&shutdown).await;
    // Read before the stop, because it is the state at the instant the reason
    // was decided that tells a completion from a lost essential unit.
    let still_running = supervisor.live();

    // Phase six. The signal watcher has done its work — a second signal is not
    // a reason to start a second shutdown.
    if let Some(task) = signals {
        task.abort();
    }

    // Dispatched, not emitted: a listener may enrich the request, and the
    // ladder does not move until every one of them has been heard.
    //
    // Bounded like every other rung, and on the drain budget, because this is
    // where the drain begins: an awaited dispatch hands a third-party listener
    // the whole process, and section 13 promises the kernel never blocks
    // indefinitely. Elapsing drops the walk rather than detaching it — a
    // listener cut here must not go on enriching a request nobody is reading —
    // and the ladder continues with whatever notes were gathered before the
    // deadline.
    let mut request = ShutdownRequested {
        reason: reason.clone(),
        notes: Vec::new(),
    };
    match timeout(policy.drain, dispatcher.dispatch(&mut request)).await {
        Ok(Ok(_)) => {}
        // A broken listener does not get to hold up a shutdown.
        Ok(Err(error)) => telemetry.record(
            Record::new(Level::Error, LISTENER_FAILED)
                .with("reason", named(&reason))
                .with("error", error.to_string()),
        ),
        // Neither does a listener that never returns.
        Err(_) => telemetry.record(
            Record::new(Level::Warn, REQUEST_OVERRAN)
                .with("reason", named(&reason))
                .with("budget", policy.drain),
        ),
    }
    telemetry.record(
        Record::new(Level::Info, REQUESTED)
            .with("reason", named(&reason))
            .with("notes", count(request.notes.len())),
    );

    // Runnables first, components second, each in the reverse of the order it
    // actually booted in. The supervisor drives the ladder itself, so this is
    // the only place either rung is moved during a normal stop.
    let errors = supervisor.stop(&controller).await;

    // Before the components go down. A notification a runnable emitted on its
    // way out reaches listeners that resolve through the container, and a
    // listener that resolves a component after `rollback` reaches one that has
    // already been shut down.
    settle(&dispatcher, policy.stop, &telemetry).await;

    // The components get a stop budget of their own rather than whatever the
    // runnables left behind, and each component gets one rather than sharing
    // the walk's. Sharing a deadline makes units compete: a runnable that spent
    // the whole budget handed the components zero, and one slow component
    // handed its neighbour whatever was left. Either way a unit is cut for a
    // reason that has nothing to do with it. The second ladder is opened at
    // `Stopping` — nothing of the components' is in flight to drain — and the
    // walk charges `policy.stop` per component. See `rollback`.
    let (components, watcher) = ShutdownController::new(policy);
    components.begin_stopping();
    let cx = ShutdownContext::new(&resolved.container, &dispatcher, &watcher);
    let RolledBack {
        stopped,
        failures: unstopped,
    } = rollback(booted, &cx, policy.stop).await;
    components.finish();

    // Phase seven.
    controller.finish();
    // Resolves at once: the ladder is past both rungs by now. Awaiting rather
    // than aborting is what guarantees both events were published.
    let _ = ladder.await;

    let abandoned = errors
        .iter()
        .filter(|error| matches!(error.kind(), RunErrorKind::DeadlineExceeded))
        .count();
    // Every abnormal ending of the whole run, restarts included — which is why
    // it is no longer called `errors`. An ancillary runnable that failed and
    // was restarted is in this total, already reported as `runnable.failed`
    // seconds earlier, and a successful exit whose last line read `errors=2`
    // sent an operator hunting for a failure that had been handled.
    let failures = errors.len();
    let refused_to_stop = unstopped.len();
    let outcome = outcome_of(reason, errors, still_running, unstopped);
    // What nothing recovered from: the failures the outcome itself carries.
    // Zero on every successful exit, by construction.
    let unhandled = unhandled_of(&outcome);
    telemetry.record(
        Record::new(Level::Info, STOPPED)
            .with("components", count(stopped.len()))
            .with("unstopped", count(refused_to_stop))
            .with("abandoned", count(abandoned))
            .with("unhandled", count(unhandled))
            .with("run_failures", count(failures)),
    );
    dispatcher.emit(Stopped {
        abandoned,
        unhandled,
        run_failures: failures,
    });

    // The last emission of the run, waited for rather than left to race the end
    // of the process.
    settle(&dispatcher, policy.stop, &telemetry).await;

    outcome
}

/// Waits for the detached emissions to reach their listeners, and gives up
/// after `budget`.
///
/// [`EventDispatcher::emit`] returns before its listeners run, so without this
/// every notification emitted late in a run is a race against the end of the
/// process — one the notification loses often enough that no test can assert on
/// it. Awaiting the emissions here is what makes the second dispatch mode
/// deliverable rather than best-effort.
///
/// The wait is bounded like every other rung of the ladder: an emission is a
/// notification, and a listener that never returns must not hold the process
/// down. An overrun is recorded with what is still outstanding, and the stop
/// continues.
async fn settle(dispatcher: &EventDispatcher, budget: Duration, telemetry: &Arc<dyn Telemetry>) {
    if timeout(budget, dispatcher.settle()).await.is_err() {
        telemetry.record(
            Record::new(Level::Warn, EMISSIONS_LEFT)
                .with("in_flight", count(dispatcher.in_flight())),
        );
    }
}

/// How many failures the outcome carries — the ones nothing recovered from.
///
/// This is what an operator reads on the last line of a run, so it counts what
/// is still open rather than everything that ever went wrong. A run that ended
/// well carries none; one that failed carries the errors that made it fail.
fn unhandled_of(outcome: &Outcome) -> usize {
    match outcome {
        Outcome::Failed(KernelError::Run(errors)) => errors.len(),
        Outcome::Failed(KernelError::Shutdown(errors)) => errors.len(),
        // No other aggregate reaches phase seven, and a failure that did would
        // still be one failure rather than none.
        Outcome::Failed(_) => 1,
        Outcome::Completed | Outcome::ShutdownRequested => 0,
    }
}

/// Resolves every runnable the plan names, in plan order.
///
/// Phase four instantiated the whole shared table, and a runnable is a shared
/// binding, so this reads values that already exist rather than building any.
/// A failure is therefore a failure of the graph — but it is reported against
/// the runnable's identity, because that is what a reader can act on.
async fn units(
    resolved: &Resolved,
) -> Result<Vec<(RunnableId, ContractId, Arc<dyn Runnable>)>, RunError> {
    let mut units = Vec::with_capacity(resolved.plan.runnables.len());

    for id in resolved.plan.runnables.iter().copied() {
        let entry = &resolved.runnables[id.index() as usize];
        let unit = (entry.build)(&resolved.container)
            .await
            .map_err(|error| RunError::failed(id, Box::new(error)))?;
        // The contract this runnable's own binding answers: what frames the
        // container its `run` resolves through.
        units.push((id, entry.contract, unit));
    }

    Ok(units)
}

/// Stops the components a failed start left booted, and reports the failure.
///
/// The ladder goes straight to [`Stage::Stopping`]: nothing ever ran, so there
/// is no in-flight work to drain. No lifecycle event is published either — a
/// kernel that never reached phase five never entered phase six.
///
/// [`Stage::Stopping`]: kernel_core::Stage::Stopping
async fn give_up(
    resolved: &Resolved,
    booted: Booted,
    controller: &ShutdownController,
    policy: ShutdownPolicy,
    error: RunError,
) -> Outcome {
    // Phase three's own notifications may still be walking their tables; they
    // are waited for here for the same reason as on the normal ladder, before
    // the components a listener might resolve are stopped.
    let telemetry = Arc::clone(resolved.container.telemetry());
    settle(&resolved.dispatcher, policy.stop, &telemetry).await;

    controller.begin_stopping();
    let watcher = controller.watcher();
    let cx = ShutdownContext::new(&resolved.container, &resolved.dispatcher, &watcher);
    // The stop failures are recorded by `rollback` and go no further: this
    // outcome is already a failure, and the start that failed is its cause.
    let _ = rollback(booted, &cx, policy.stop).await;
    controller.finish();

    Outcome::Failed(KernelError::Run(vec![error]))
}

/// Publishes each rung of the ladder at the moment it is reached.
///
/// The supervisor moves both rungs from inside its own stop, so the kernel
/// cannot emit them inline without emitting them at the wrong moment. Watching
/// the ladder reports where it actually is rather than where the caller assumes
/// it to be.
fn announce(
    shutdown: Shutdown,
    dispatcher: Arc<EventDispatcher>,
    telemetry: Arc<dyn Telemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        shutdown.draining().await;
        telemetry.record(Record::new(Level::Info, DRAINING));
        dispatcher.emit(Draining);

        shutdown.stopping().await;
        telemetry.record(Record::new(Level::Info, STOPPING));
        dispatcher.emit(Stopping);
    })
}

/// What the whole run ended as: phase five, then the stop it was given.
fn outcome_of(
    reason: ShutdownReason,
    errors: Vec<RunError>,
    still_running: usize,
    unstopped: Vec<ShutdownError>,
) -> Outcome {
    let outcome = run_outcome(reason, errors, still_running);
    // Section 12: a component that fails to stop never holds the walk up, and
    // it influences the exit code. A run that already failed keeps the failure
    // that ended it as its cause — the stop failures are on the telemetry
    // stream either way — but a run that ended well and left a component
    // unstopped did not end well.
    if unstopped.is_empty() || !outcome.is_success() {
        return outcome;
    }
    Outcome::Failed(KernelError::Shutdown(unstopped))
}

/// What phase five alone ended as, before the stop is taken into account.
///
/// The reason decides, not the error list: an ancillary runnable that failed
/// and was restarted is in that list, and a process that ran for a week must
/// not exit non-zero because of a blip it recovered from. Design section 12
/// makes exactly one thing in phase five fatal — an essential runnable ending —
/// and that is what this reports, with the errors that runnable produced.
///
/// # The two cases a clean essential return can be
///
/// `still_running` is how many runnables were alive at the instant the reason
/// was decided, and it is what tells the two apart:
///
/// * **None left.** Every runnable returned on its own, the last of them the
///   essential one. That is the batch-shaped kernel finishing its work, which
///   is exactly what [`Outcome::Completed`] describes, and it exits zero.
/// * **Some still running.** The process lost the unit that defined it while
///   the others still had work to do. Nothing failed, so there is no cause to
///   name — but a run that ended without its essential unit is not a
///   completion, and it exits non-zero with the ending named as the failure.
///
/// An essential runnable that ended on a failure of its own is fatal either
/// way, and that failure is the cause the outcome carries.
fn run_outcome(reason: ShutdownReason, errors: Vec<RunError>, still_running: usize) -> Outcome {
    match reason {
        ShutdownReason::Completed => Outcome::Completed,
        ShutdownReason::Signal | ShutdownReason::Programmatic => Outcome::ShutdownRequested,
        ShutdownReason::EssentialFinished(id) => {
            let fatal: Vec<RunError> = errors
                .into_iter()
                .filter(|error| error.runnable() == id)
                .collect();
            match (fatal.is_empty(), still_running) {
                (true, 0) => Outcome::Completed,
                (true, _) => Outcome::Failed(KernelError::Run(vec![RunError::failed(
                    id,
                    ESSENTIAL_LEFT.to_owned().into(),
                )])),
                (false, _) => Outcome::Failed(KernelError::Run(fatal)),
            }
        }
    }
}

/// Records the phase that refused to build, and hands the error back.
fn refused(telemetry: &Arc<dyn Telemetry>, phase: &'static str, error: KernelError) -> KernelError {
    telemetry.record(
        Record::new(Level::Error, BUILD_FAILED)
            .with("phase", phase)
            .with("error", error.to_string()),
    );
    error
}

/// How many bindings the container holds, every lifetime counted.
fn bindings_of(resolved: &Resolved) -> usize {
    resolved.container.binding_count()
}

/// A count, in the only integer shape a record carries.
fn count(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The name a record reports a shutdown reason under.
fn named(reason: &ShutdownReason) -> &'static str {
    match reason {
        ShutdownReason::Signal => "signal",
        ShutdownReason::Programmatic => "programmatic",
        ShutdownReason::EssentialFinished(_) => "essential_finished",
        ShutdownReason::Completed => "completed",
    }
}

/// Capturing the process stop signal.
///
/// Behind the `signals` feature *and* [`KernelBuilder::capture_signals`]: the
/// feature decides whether the runtime's signal support is compiled in at all,
/// the builder decides whether this kernel is the thing that listens. An
/// application that installs its own handler turns the second off and keeps the
/// first; a build with `--no-default-features` has neither, and every other
/// trigger still works.
#[cfg(feature = "signals")]
mod signals {
    use std::sync::Arc;

    use kernel_core::{Level, Record, Telemetry};
    use tokio::task::JoinHandle;

    use crate::shutdown::ShutdownController;

    /// Telemetry event name for a captured stop signal.
    ///
    /// Local to this module: a build without signal support never records it.
    const SIGNAL: &str = "kernel.signal_received";

    /// Watches for a stop signal, and moves the ladder when one arrives.
    ///
    /// Moving the ladder rather than asking the handle is what makes the
    /// difference visible: the supervisor reports
    /// [`ShutdownReason::Signal`](crate::events::ShutdownReason::Signal) for a
    /// ladder that moved under it, and
    /// [`Programmatic`](crate::events::ShutdownReason::Programmatic) for a
    /// handle that was asked.
    pub(super) fn capture(
        enabled: bool,
        controller: Arc<ShutdownController>,
        telemetry: Arc<dyn Telemetry>,
    ) -> Option<JoinHandle<()>> {
        if !enabled {
            return None;
        }

        Some(tokio::spawn(async move {
            arrived().await;
            telemetry.record(Record::new(Level::Warn, SIGNAL));
            controller.begin_draining();
        }))
    }

    /// Resolves on the first stop signal the platform can deliver.
    async fn arrived() {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            // A handler that cannot be installed is not a reason to take the
            // process down: the other triggers still work.
            let Ok(mut terminate) = signal(SignalKind::terminate()) else {
                let _ = tokio::signal::ctrl_c().await;
                return;
            };

            tokio::select! {
                _ = terminate.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }

        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// Signal capture, compiled out.
///
/// The builder's flag is still read and still means what it says; there is
/// simply no signal support to install, so nothing listens for one.
#[cfg(not(feature = "signals"))]
mod signals {
    use std::sync::Arc;

    use kernel_core::Telemetry;
    use tokio::task::JoinHandle;

    use crate::shutdown::ShutdownController;

    /// Always `None`: this build has no signal support.
    pub(super) fn capture(
        _enabled: bool,
        _controller: Arc<ShutdownController>,
        _telemetry: Arc<dyn Telemetry>,
    ) -> Option<JoinHandle<()>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::time::Duration;
    use std::sync::Mutex;

    use kernel_core::{
        Backoff, BoxFuture, BundleManifest, ComponentDescriptor, ComponentError, ComponentId,
        ConfigError, ConfigTree, ContractRef, Criticality, Event, FieldValue, Flow, Health,
        HealthProbe, ListenerError, Priority, RecordingTelemetry, RegisterError, RestartPolicy,
        RunnableDescriptor,
    };

    use crate::component::{BootContext, Component};
    use crate::config::MemorySource;
    use crate::dispatcher::{Listener, ListenerContext};
    use crate::events::BootStarted;
    use crate::provider::Provider;
    use crate::runnable::RunContext;

    /// Names the test components answer to, indexed by their const parameter.
    const NAMES: [&str; 3] = ["alpha", "beta", "gamma"];

    /// Budgets short enough that a paused clock walks the whole ladder in one
    /// step, and long enough that nothing is abandoned by accident.
    fn brief() -> ShutdownPolicy {
        ShutdownPolicy::new(Duration::from_secs(1), Duration::from_secs(2))
    }

    /// Shared, ordered record of what happened, readable from outside the
    /// kernel — which is the only place a dropped `run` can be observed from.
    #[derive(Clone, Default)]
    struct Trace(Arc<Mutex<Vec<String>>>);

    impl Trace {
        fn push(&self, entry: impl Into<String>) {
            self.0.lock().expect("trace").push(entry.into());
        }

        fn entries(&self) -> Vec<String> {
            self.0.lock().expect("trace").clone()
        }

        fn has(&self, entry: &str) -> bool {
            self.entries().iter().any(|seen| seen == entry)
        }

        fn count(&self, entry: &str) -> usize {
            self.entries().iter().filter(|seen| *seen == entry).count()
        }
    }

    /// A bundle that registers whatever it was built with.
    struct Parts<F>(&'static str, F);

    impl<F> Bundle for Parts<F>
    where
        F: Fn(&mut Registry) -> Result<(), RegisterError> + Send + Sync + 'static,
    {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new(self.0, "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            (self.1)(registry)
        }
    }

    /// A bundle whose registration always fails.
    struct Broken(&'static str);

    impl Bundle for Broken {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new(self.0, "0.1.0")
        }

        fn register(&self, _registry: &mut Registry) -> Result<(), RegisterError> {
            Err(RegisterError::new(self.0, "refused".into()))
        }
    }

    /// A source that never loads.
    struct Unreadable(&'static str);

    impl kernel_core::ConfigSource for Unreadable {
        fn name(&self) -> &'static str {
            self.0
        }

        fn load(&self) -> Result<ConfigTree, ConfigError> {
            Err(ConfigError::source_error(self.0, "unreadable".into()))
        }
    }

    /// A component that traces both of its lifecycle calls.
    ///
    /// The const parameter is what makes each one a distinct type, which is
    /// what a registry needs: a component is bound under its own type.
    struct Unit<const N: usize> {
        trace: Trace,
        fails: bool,
    }

    impl<const N: usize> Unit<N> {
        fn new(trace: &Trace) -> Self {
            Self {
                trace: trace.clone(),
                fails: false,
            }
        }

        fn failing(trace: &Trace) -> Self {
            Self {
                trace: trace.clone(),
                fails: true,
            }
        }
    }

    impl<const N: usize> Component for Unit<N> {
        fn name() -> &'static str {
            NAMES[N]
        }

        fn descriptor(&self) -> ComponentDescriptor {
            ComponentDescriptor::new()
        }

        fn boot<'a>(
            &'a self,
            _cx: &'a BootContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async move {
                self.trace.push(format!("boot:{}", NAMES[N]));
                if self.fails {
                    let id = ComponentId::new(NAMES[N], u32::try_from(N).expect("small"));
                    return Err(ComponentError::new(id, "refused".into()));
                }
                Ok(())
            })
        }

        fn shutdown<'a>(
            &'a self,
            _cx: &'a ShutdownContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async move {
                self.trace.push(format!("stop:{}", NAMES[N]));
                Ok(())
            })
        }
    }

    /// Waits for the stop, which is what a well-behaved runnable does.
    struct Waiter(RunnableDescriptor, Trace);

    impl Runnable for Waiter {
        fn name() -> &'static str {
            "waiter"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                self.1.push("run");
                cx.shutdown().stopping().await;
                self.1.push("returned");
                Ok(())
            })
        }
    }

    /// Returns as soon as it starts, with the result it was built with.
    struct Prompt(RunnableDescriptor, bool);

    impl Runnable for Prompt {
        fn name() -> &'static str {
            "prompt"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            let ok = self.1;
            Box::pin(async move {
                if ok {
                    Ok(())
                } else {
                    Err(RunError::failed(cx.id(), "gave up".into()))
                }
            })
        }
    }

    /// Fails its first run, then waits for the stop.
    struct Flaky(RunnableDescriptor, Trace);

    impl Runnable for Flaky {
        fn name() -> &'static str {
            "flaky"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                self.1.push("start");
                if self.1.count("start") == 1 {
                    return Err(RunError::failed(cx.id(), "first try".into()));
                }
                cx.shutdown().stopping().await;
                Ok(())
            })
        }
    }

    /// Ignores its shutdown token entirely — what the contract forbids, and
    /// what the kernel has to survive anyway.
    struct Deaf(RunnableDescriptor);

    impl Runnable for Deaf {
        fn name() -> &'static str {
            "deaf"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            })
        }
    }

    /// Emits an event of its own just before returning, which is the last
    /// thing a well-behaved runnable does and the case a detached emission used
    /// to lose.
    struct Parting(RunnableDescriptor);

    impl Runnable for Parting {
        fn name() -> &'static str {
            "parting"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            self.0
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                cx.shutdown().stopping().await;
                cx.dispatcher().emit(Parted);
                Ok(())
            })
        }
    }

    /// What [`Parting`] emits.
    #[derive(Debug)]
    struct Parted;

    impl Event for Parted {
        const NAME: &'static str = "test.parted";
    }

    /// Records its tag, but not before the clock has moved.
    ///
    /// A listener that finishes in one poll is delivered by the scheduler's
    /// goodwill as often as by design, which is what made the old behaviour
    /// impossible to assert on either way. This one cannot finish unless
    /// something waits for it.
    struct Late(Trace);

    impl Listener<Parted> for Late {
        fn on_event<'a>(
            &'a self,
            _event: &'a mut Parted,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                self.0.push("parted");
                Ok(Flow::Continue)
            })
        }
    }

    /// A probe, to prove the kernel declares the point that collects them.
    struct Sample;

    impl kernel_core::Extension for Sample {}

    impl HealthProbe for Sample {
        fn name(&self) -> &'static str {
            "sample"
        }

        fn check(&self) -> BoxFuture<'_, Health> {
            Box::pin(async { Health::Up })
        }
    }

    /// Records its tag whenever the event it watches arrives.
    struct Tap<E> {
        tag: &'static str,
        trace: Trace,
        event: core::marker::PhantomData<fn() -> E>,
    }

    impl<E: Event> Tap<E> {
        fn new(tag: &'static str, trace: &Trace) -> Self {
            Self {
                tag,
                trace: trace.clone(),
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
                self.trace.push(self.tag);
                Ok(Flow::Continue)
            })
        }
    }

    /// Adds a note to the request, which is why it is dispatched and not
    /// emitted.
    struct Annotate;

    impl Listener<ShutdownRequested> for Annotate {
        fn on_event<'a>(
            &'a self,
            event: &'a mut ShutdownRequested,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async move {
                event.notes.push("noted".to_owned());
                Ok(Flow::Continue)
            })
        }
    }

    fn essential() -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn ancillary() -> RunnableDescriptor {
        RunnableDescriptor::new().criticality(Criticality::Ancillary)
    }

    /// A builder over one bundle, with a recording sink and short budgets.
    fn builder<F>(sink: &RecordingTelemetry, register: F) -> KernelBuilder
    where
        F: Fn(&mut Registry) -> Result<(), RegisterError> + Send + Sync + 'static,
    {
        Kernel::builder()
            .telemetry(Arc::new(sink.clone()) as Arc<dyn Telemetry>)
            .shutdown_policy(brief())
            .capture_signals(false)
            .bundle(Parts("unit", register))
    }

    /// The value of one field of the first record under `event`.
    fn field(sink: &RecordingTelemetry, event: &str, key: &str) -> Option<FieldValue> {
        sink.records()
            .into_iter()
            .find(|record| record.event == event)
            .and_then(|record| record.field(key).cloned())
    }

    #[tokio::test]
    async fn builds_without_instantiating() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&inner))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        // Phase three closed the graph and phase four has not run.
        assert!(!kernel.container().is_sealed());
        assert!(trace.entries().is_empty());
        assert!(sink.contains(RESOLVED));
    }

    #[tokio::test]
    async fn config_failure_stops_build() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let error = Kernel::builder()
            .telemetry(Arc::new(sink.clone()) as Arc<dyn Telemetry>)
            .config_source(Unreadable("first"))
            .config_source(Unreadable("second"))
            .config_source(MemorySource::new(ConfigTree::empty()))
            .bundle(Parts("unit", move |_registry: &mut Registry| {
                inner.push("registered");
                Ok(())
            }))
            .build()
            .await
            .expect_err("build fails");

        match error {
            // Both failing sources, not the first alone.
            KernelError::Config(errors) => assert_eq!(errors.len(), 2),
            other => panic!("unexpected error: {other}"),
        }
        // Phase two never ran: there is nothing to learn from registering
        // against a configuration that did not load.
        assert!(trace.entries().is_empty());
        assert!(sink.contains(BUILD_FAILED));
    }

    #[tokio::test]
    async fn register_failures_aggregate() {
        let sink = RecordingTelemetry::new();

        let error = Kernel::builder()
            .telemetry(Arc::new(sink.clone()) as Arc<dyn Telemetry>)
            .bundle(Broken("first"))
            .bundle(Broken("second"))
            .build()
            .await
            .expect_err("build fails");

        match error {
            KernelError::Register(errors) => {
                assert_eq!(errors.len(), 2);
                assert_eq!(errors[0].bundle(), "first");
                assert_eq!(errors[1].bundle(), "second");
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(!sink.contains(RESOLVED));
    }

    #[tokio::test]
    async fn resolve_failure_reports() {
        trait Absent: Send + Sync + 'static {}

        let sink = RecordingTelemetry::new();
        let error = builder(&sink, |registry| {
            registry.provide::<dyn Absent>(
                Provider::from_fn(|_container| Box::pin(async { unreachable!() }))
                    .requires([ContractRef::of::<dyn Absent>()]),
            );
            Ok(())
        })
        .build()
        .await
        .expect_err("build fails");

        // A self-requirement is a cycle, and the graph never closes.
        assert!(matches!(error, KernelError::Resolve(errors) if !errors.is_empty()));
    }

    #[tokio::test]
    async fn counts_every_binding() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        trait Spare: Send + Sync + 'static {}
        struct Idle;
        impl Spare for Idle {}

        /// Keeps the count the event carried, readable after the emission.
        struct Seen(Arc<AtomicUsize>);

        impl Listener<GraphResolved> for Seen {
            fn on_event<'a>(
                &'a self,
                event: &'a mut GraphResolved,
                _cx: &'a ListenerContext<'a>,
            ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
                self.0.store(event.bindings, Ordering::Relaxed);
                Box::pin(async { Ok(Flow::Continue) })
            }
        }

        let sink = RecordingTelemetry::new();
        let seen = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&seen);

        let kernel = builder(&sink, move |registry| {
            // Named by no component and by no runnable: a count taken from the
            // plan alone never sees it.
            registry.provide::<dyn Spare>(Provider::from_value(Arc::new(Idle) as Arc<dyn Spare>));
            registry.listen(Seen(Arc::clone(&counted)), Priority::NORMAL);
            Ok(())
        })
        .build()
        .await
        .expect("build");

        // Phase three's own notifications are detached; this waits for them
        // instead of yielding a plausible number of times and hoping.
        kernel.resolved.dispatcher.settle().await;

        assert_eq!(
            seen.load(Ordering::Relaxed),
            kernel.container().binding_count()
        );
        // The plan is empty, so a count taken from it would have reported none.
        assert!(seen.load(Ordering::Relaxed) >= 1);
        assert_eq!(
            field(&sink, RESOLVED, "components"),
            Some(FieldValue::Int(0))
        );
        assert_eq!(
            field(&sink, RESOLVED, "runnables"),
            Some(FieldValue::Int(0))
        );
    }

    #[tokio::test]
    async fn declares_probe_point() {
        let sink = RecordingTelemetry::new();

        // Contributing to a point nobody declared is a phase-three error, so
        // this only builds because the kernel declared the point itself.
        let built = builder(&sink, |registry| {
            registry.contribute(Probe::new(Sample));
            Ok(())
        })
        .build()
        .await;

        assert!(built.is_ok());
    }

    #[tokio::test]
    async fn handle_reaches_container() {
        let sink = RecordingTelemetry::new();
        let kernel = builder(&sink, |_registry| Ok(()))
            .build()
            .await
            .expect("build");

        // The handle a component resolves and the handle the caller holds are
        // one object.
        assert!(!kernel.container().handle().is_shutting_down());
        kernel.handle().shutdown();
        assert!(kernel.container().handle().is_shutting_down());
    }

    #[tokio::test(start_paused = true)]
    async fn handle_stops_the_kernel() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&inner))));
            registry.runnable(Provider::from_value(Arc::new(Waiter(
                essential(),
                inner.clone(),
            ))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let handle = kernel.handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            handle.shutdown();
        });

        let outcome = kernel.run().await;

        assert!(matches!(outcome, Outcome::ShutdownRequested));
        assert!(outcome.is_success());
        assert_eq!(
            trace.entries(),
            ["boot:alpha", "run", "returned", "stop:alpha"]
        );
        assert!(sink.contains(STOPPED));
    }

    #[tokio::test(start_paused = true)]
    async fn drop_requests_stop() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&inner))));
            registry.runnable(Provider::from_value(Arc::new(Waiter(
                essential(),
                inner.clone(),
            ))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let outcome = tokio::select! {
            outcome = kernel.run() => Some(outcome),
            () = tokio::time::sleep(Duration::from_millis(50)) => None,
        };

        // The future lost the race and was dropped with it.
        assert!(outcome.is_none());
        assert!(trace.has("run"));

        // The task it left behind carries the stop to the end, and the proof is
        // read from outside the kernel entirely.
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(trace.has("returned"));
        assert!(trace.has("stop:alpha"));
        assert!(sink.contains(STOPPED));
    }

    #[tokio::test(start_paused = true)]
    async fn no_runnable_completes() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&inner))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let outcome = kernel.run().await;

        // A graph with nothing running in it is a program that has finished.
        assert!(matches!(outcome, Outcome::Completed));
        assert_eq!(trace.entries(), ["boot:alpha", "stop:alpha"]);
    }

    #[tokio::test(start_paused = true)]
    async fn essential_end_stops_kernel() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&inner))));
            registry.runnable(Provider::from_value(Arc::new(Prompt(essential(), true))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let outcome = kernel.run().await;

        // The only runnable there was, and it returned on its own: every
        // runnable finished, which is a completion. The other case — an
        // essential one leaving others behind — is the integration suite's
        // `essential_end_stops_kernel`.
        assert!(matches!(outcome, Outcome::Completed));
        assert_eq!(trace.entries(), ["boot:alpha", "stop:alpha"]);
        assert_eq!(
            field(&sink, REQUESTED, "reason"),
            Some(FieldValue::from("essential_finished"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn essential_failure_is_fatal() {
        let sink = RecordingTelemetry::new();
        let kernel = builder(&sink, |registry| {
            registry.runnable(Provider::from_value(Arc::new(Prompt(essential(), false))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let outcome = kernel.run().await;

        match outcome.error() {
            Some(KernelError::Run(errors)) => assert_eq!(errors.len(), 1),
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert!(!outcome.is_success());
        // The other half of `stop_counts_unhandled`: nothing recovered from
        // this one, and the last line of the run says so.
        assert_eq!(field(&sink, STOPPED, "unhandled"), Some(FieldValue::Int(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn ancillary_failure_restarts() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.runnable(Provider::from_value(Arc::new(Flaky(
                ancillary().restart(RestartPolicy::on_failure(
                    2,
                    Backoff::Fixed(Duration::from_millis(10)),
                )),
                inner.clone(),
            ))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let handle = kernel.handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            handle.shutdown();
        });

        let outcome = kernel.run().await;

        // The failure did not end the run, and it was started again.
        assert!(matches!(outcome, Outcome::ShutdownRequested));
        assert_eq!(trace.count("start"), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn deaf_runnable_is_abandoned() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&inner))));
            registry.runnable(Provider::from_value(Arc::new(Deaf(ancillary()))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let handle = kernel.handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            handle.shutdown();
        });

        // The whole point: this returns at all.
        let outcome = kernel.run().await;

        assert!(matches!(outcome, Outcome::ShutdownRequested));
        assert_eq!(field(&sink, STOPPED, "abandoned"), Some(FieldValue::Int(1)));
        // The components were still stopped, after the runnable was dropped.
        assert!(trace.has("stop:alpha"));
    }

    #[tokio::test(start_paused = true)]
    async fn boot_failure_rolls_back() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.component(Provider::from_value(Arc::new(Unit::<0>::new(&inner))));
            registry.component(Provider::from_value(Arc::new(Unit::<1>::failing(&inner))));
            registry.component(Provider::from_value(Arc::new(Unit::<2>::new(&inner))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let outcome = kernel.run().await;

        match outcome.error() {
            Some(KernelError::Boot { rolled_back, .. }) => assert_eq!(rolled_back.len(), 1),
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert!(!outcome.is_success());
        // The third component never booted, and the first was stopped again.
        assert_eq!(trace.entries(), ["boot:alpha", "boot:beta", "stop:alpha"]);
    }

    #[tokio::test(start_paused = true)]
    async fn publishes_every_phase() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.listen(
                Tap::<BundleRegistered>::new("registered", &inner),
                Priority::NORMAL,
            );
            registry.listen(
                Tap::<GraphResolved>::new("resolved", &inner),
                Priority::NORMAL,
            );
            registry.listen(Tap::<BootStarted>::new("booting", &inner), Priority::NORMAL);
            registry.listen(Tap::<Running>::new("running", &inner), Priority::NORMAL);
            registry.listen(Tap::<Draining>::new("draining", &inner), Priority::NORMAL);
            registry.listen(Tap::<Stopping>::new("stopping", &inner), Priority::NORMAL);
            registry.listen(Tap::<Stopped>::new("stopped", &inner), Priority::NORMAL);
            registry.listen(Annotate, Priority::NORMAL);
            registry.runnable(Provider::from_value(Arc::new(Prompt(essential(), true))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        // No yielding: `run` does not return until the emissions have settled,
        // `stopped` being the last of them.
        let outcome = kernel.run().await;

        assert!(outcome.is_success());
        for tag in [
            "registered",
            "resolved",
            "booting",
            "running",
            "draining",
            "stopping",
            "stopped",
        ] {
            assert!(trace.has(tag), "missing {tag}");
        }
        // The request is dispatched, so a listener's note is in it.
        assert_eq!(field(&sink, REQUESTED, "notes"), Some(FieldValue::Int(1)));
    }

    // The gap: `emit` spawned a task nothing joined, so an event emitted on
    // the way out raced the end of the run and was lost as often as not. The
    // ladder waits for it now, and this asserts on it with no yielding at all.
    #[tokio::test(start_paused = true)]
    async fn late_emit_reaches_listener() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.listen(Late(inner.clone()), Priority::NORMAL);
            registry.runnable(Provider::from_value(Arc::new(Parting(essential()))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let handle = kernel.handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            handle.shutdown();
        });

        let outcome = kernel.run().await;

        assert!(outcome.is_success());
        assert!(trace.has("parted"));
    }

    // A run that recovered from an ancillary failure must not sign off with a
    // count that reads like a failure: what nothing handled is its own field,
    // and the total is named for what it holds.
    #[tokio::test(start_paused = true)]
    async fn stop_counts_unhandled() {
        let sink = RecordingTelemetry::new();
        let trace = Trace::default();
        let inner = trace.clone();

        let kernel = builder(&sink, move |registry| {
            registry.runnable(Provider::from_value(Arc::new(Flaky(
                ancillary().restart(RestartPolicy::on_failure(
                    2,
                    Backoff::Fixed(Duration::from_millis(10)),
                )),
                inner.clone(),
            ))));
            Ok(())
        })
        .build()
        .await
        .expect("build");

        let handle = kernel.handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            handle.shutdown();
        });

        let outcome = kernel.run().await;

        // The failure happened, was reported when it happened, and was
        // recovered from: the exit is clean and says so.
        assert!(outcome.is_success());
        assert_eq!(trace.count("start"), 2);
        assert_eq!(field(&sink, STOPPED, "unhandled"), Some(FieldValue::Int(0)));
        assert_eq!(
            field(&sink, STOPPED, "run_failures"),
            Some(FieldValue::Int(1))
        );
        assert_eq!(field(&sink, STOPPED, "errors"), None);
    }

    #[test]
    fn builder_defaults_are_stated() {
        let builder = KernelBuilder::default();

        assert_eq!(builder.sources.len(), 0);
        assert!(builder.bundles.is_empty());
        assert_eq!(builder.policy, ShutdownPolicy::DEFAULT);
        assert!(builder.capture_signals);
        assert!(format!("{builder:?}").contains("capture_signals"));
    }
}
