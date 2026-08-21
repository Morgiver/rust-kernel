//! Building a kernel for a test, and driving it from one.

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use kernel::dispatcher::{Listener, ListenerContext};
use kernel::{
    Bundle, Component, Container, Kernel, KernelBuilder, KernelHandle, Provider, Registry,
    Runnable, Running,
};
use kernel_core::{
    BoxFuture, ConfigSource, Flow, KernelError, ListenerError, Outcome, Priority,
    RecordingTelemetry, ShutdownError, ShutdownPolicy, Telemetry,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::doubles::Parking;

/// The budgets a test kernel shuts down on.
///
/// Short on purpose: a test that reaches a shutdown deadline should report a
/// failure in the time a test takes, not in the time a production stop is
/// allowed. A test that needs the production numbers states them with
/// [`TestBuilder::shutdown_policy`].
const TEST_POLICY: ShutdownPolicy =
    ShutdownPolicy::new(Duration::from_millis(50), Duration::from_millis(100));

/// Unit a failure of the harness's own driver task is attributed to.
const DRIVER: &str = "kernel";

/// How long [`TestHarness::wait_for_record`] waits before giving up.
///
/// Long enough for a supervisor working through a backoff, short enough that a
/// kernel which never records what the test asked for fails in the time a test
/// takes. A test with a paused clock never spends it: the poll's sleep advances
/// the clock instead of the wall.
const PATIENCE: Duration = Duration::from_secs(5);

/// How often [`TestHarness::wait_for_record`] looks again.
const POLL: Duration = Duration::from_millis(5);

/// One registration a substitution performs, held until phase two runs it.
///
/// `FnOnce`: a substitution moves the double it registers, and the hook runs
/// exactly once.
type Substitution = Box<dyn FnOnce(&mut Registry) + Send>;

/// Assembles a kernel for a test, and is the ONLY place a binding can be
/// substituted.
///
/// Distinct from [`kernel::KernelBuilder`] on purpose: substitution is not a
/// method someone can reach by accident from production code, it is a method on
/// a type production code does not depend on.
///
/// Differences from the production builder, all deliberate: signals are never
/// captured, the shutdown budgets are short, and telemetry is recorded and
/// readable before the kernel is built.
pub struct TestBuilder {
    /// The production builder, driven by delegation: a test kernel must go
    /// through the same three phases as the real one, not through a second
    /// assembly path that could disagree with it.
    builder: KernelBuilder,
    /// The sink handed to the builder, kept so a test can read it early.
    telemetry: Arc<RecordingTelemetry>,
    /// The substitutions, in the order they were declared. They run after every
    /// bundle has registered and before phase three, so the graph validation
    /// sees them.
    substitutions: Vec<Substitution>,
    /// Whether a [`Parking`] runnable is registered to hold the graph open.
    ///
    /// A flag rather than one more entry in `substitutions`, so that asking
    /// twice registers one runnable instead of two bindings of one contract.
    keep_running: bool,
}

impl TestBuilder {
    /// A builder with no bundles, no signal capture and short budgets.
    #[must_use]
    pub fn new() -> Self {
        let telemetry = Arc::new(RecordingTelemetry::new());
        Self {
            builder: KernelBuilder::new()
                .telemetry(Arc::clone(&telemetry) as Arc<dyn Telemetry>)
                .capture_signals(false)
                .shutdown_policy(TEST_POLICY),
            telemetry,
            substitutions: Vec::new(),
            keep_running: false,
        }
    }

    /// Holds the kernel open even when nothing in the graph runs.
    ///
    /// A kernel whose runnables have all ended has nothing left to wait for,
    /// and a kernel with no runnable at all is that case from the start: phase
    /// five publishes [`Running`] and the stop is requested in the same breath,
    /// so by the time [`start`](Self::start) hands a harness back the
    /// components have been shut down and the container hands out objects
    /// nobody can use any more.
    ///
    /// That is correct for a program — an object graph with nothing running in
    /// it is a program that exits — and it makes the commonest bundle shape,
    /// a component plus the contract it answers, impossible to drive from a
    /// test. This registers a [`Parking`] runnable, which returns when the
    /// shutdown token fires and does nothing else, so the graph stays open
    /// until the test asks for the stop.
    ///
    /// Explicit, and never implied by anything else here: a harness that
    /// decided on its own when a kernel stops would make the test agree with a
    /// kernel nobody runs. A bundle that owns a runnable of its own does not
    /// need it, and asking for it twice registers one runnable.
    #[must_use]
    pub fn keep_running(mut self) -> Self {
        self.keep_running = true;
        self
    }

    /// Appends a bundle, exactly as the production builder would.
    #[must_use]
    pub fn bundle(mut self, bundle: impl Bundle) -> Self {
        self.builder = self.builder.bundle(bundle);
        self
    }

    /// Appends a configuration source.
    #[must_use]
    pub fn config_source(mut self, source: impl ConfigSource) -> Self {
        self.builder = self.builder.config_source(source);
        self
    }

    /// Overrides the shutdown budgets.
    #[must_use]
    pub fn shutdown_policy(mut self, policy: ShutdownPolicy) -> Self {
        self.builder = self.builder.shutdown_policy(policy);
        self
    }

    /// Replaces the implementation of a contract with a double.
    ///
    /// # Replace, or add
    ///
    /// A contract a bundle in this graph already binds is REPLACED: the double
    /// takes that binding's place, rank and default position, and the
    /// implementation the bundle registered is gone. A contract nobody binds is
    /// ADDED, which is the round trip
    /// [`missing_contracts`](crate::missing_contracts) describes — one double
    /// per unsatisfied line.
    ///
    /// Both are one call because both are the same sentence: after it, the
    /// contract resolves to the double. Which of the two happened is a fact
    /// about the bundles under test, not about the test.
    ///
    /// Replacing is not smuggling: the substitutions are recorded before phase
    /// three, so a graph a double leaves open is refused here exactly as the
    /// real assembly would refuse it.
    ///
    /// # Nature is kept
    ///
    /// The substitution KEEPS THE NATURE of what it replaces: standing in for a
    /// component leaves it a component, still booted and still stopped by the
    /// kernel. A double that skipped the lifecycle would make the test agree
    /// with a kernel nobody runs.
    ///
    /// This verb stands in for a contract the kernel resolves and never drives
    /// — the nature of a service. A double for a unit the kernel drives goes
    /// through [`substitute_component`](Self::substitute_component) or
    /// [`substitute_runnable`](Self::substitute_runnable), which register it as
    /// what it stands in for; a double registered by this verb alone would be
    /// resolvable and never booted.
    #[must_use]
    pub fn substitute<C: ?Sized + Send + Sync + 'static>(mut self, double: Arc<C>) -> Self {
        self.substitutions.push(Box::new(move |registry| {
            registry.__replace(Provider::from_value(double));
        }));
        self
    }

    /// Replaces a named implementation, or adds one under that name.
    ///
    /// A name is part of a contract's identity, so this replaces the binding
    /// recorded under that name and never the default one. See
    /// [`substitute`](Self::substitute) for when each half applies.
    #[must_use]
    pub fn substitute_named<C: ?Sized + Send + Sync + 'static>(
        mut self,
        name: &'static str,
        double: Arc<C>,
    ) -> Self {
        self.substitutions.push(Box::new(move |registry| {
            registry.__replace_named(name, Provider::from_value(double));
        }));
        self
    }

    /// Stands in for a component, and stays one.
    ///
    /// Registered through the very verb a bundle registers a component with, so
    /// the double is in the boot order, booted in dependency order, sealed with
    /// everything else and stopped in reverse — the nature of what it replaces,
    /// kept mechanically rather than remembered.
    ///
    /// It binds the double under its own concrete type, which is what the
    /// kernel drives. A component double that must also answer a contract the
    /// graph asks for is passed to [`substitute`](Self::substitute) as well,
    /// with the same [`Arc`]: both bindings then hand out one object. The two
    /// bindings are two entries, so a unit that requires the contract is
    /// ordered against the value binding and not against this one — a double
    /// whose boot has to precede its consumer's is the one case where that
    /// matters, and the ordering has to be stated on the consumer.
    ///
    /// Replace or add, like [`substitute`](Self::substitute): a component a
    /// bundle already registered under this type is replaced, both halves of
    /// it — the binding the container resolves and the entry the kernel boots
    /// — and one nobody registered is added.
    #[must_use]
    pub fn substitute_component<T: Component>(mut self, double: Arc<T>) -> Self {
        self.substitutions.push(Box::new(move |registry| {
            registry.__replace_component(Provider::from_value(double));
        }));
        self
    }

    /// Stands in for a runnable, and stays one.
    ///
    /// The counterpart of [`substitute_component`](Self::substitute_component)
    /// for the other unit the kernel drives: the double is started by the
    /// supervisor, watched, and stopped on the shutdown ladder.
    #[must_use]
    pub fn substitute_runnable<T: Runnable>(mut self, double: Arc<T>) -> Self {
        self.substitutions.push(Box::new(move |registry| {
            registry.__replace_runnable(Provider::from_value(double));
        }));
        self
    }

    /// The telemetry sink, readable before and after the build.
    #[must_use]
    pub fn telemetry(&self) -> Arc<RecordingTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// Runs phases one to three, like the production builder.
    ///
    /// # Errors
    ///
    /// Whatever [`KernelBuilder::build`] reports. A substituted graph is
    /// validated like any other: the substitutions are recorded before phase
    /// three, so one that leaves the graph open fails here exactly as the real
    /// assembly would.
    pub async fn build(self) -> Result<Kernel, KernelError> {
        self.hooked().build().await
    }

    /// Builds and starts, returning a harness that drives the rest.
    ///
    /// Returns once the kernel has started its runnables, so a test that asks
    /// the harness a question next is asking a running kernel. A kernel that
    /// ended before it got that far — a boot failure — is reported here rather
    /// than left for [`TestHarness::wait`] to explain.
    ///
    /// # Errors
    ///
    /// Whatever [`build`](Self::build) reports, plus the error of a run that
    /// failed before reaching phase five.
    pub async fn start(mut self) -> Result<TestHarness, KernelError> {
        // Phase five publishes `Running`; a listener on it is the kernel's own
        // answer to "are you up", rather than a delay this crate would have to
        // guess the length of.
        let (started, mut is_started) = watch::channel(false);
        self.substitutions.push(Box::new(move |registry| {
            registry.listen::<Running, _>(StartedListener { started }, Priority::NORMAL);
        }));

        let telemetry = self.telemetry();
        let kernel = self.build().await?;
        let container = kernel.container().clone();
        let handle = kernel.handle();

        let (ended, mut is_ended) = watch::channel(false);
        let task = tokio::spawn(async move {
            let outcome = kernel.run().await;
            // Set before the task returns, so a waiter released by it can join
            // the task without racing it.
            ended.send_replace(true);
            outcome
        });

        tokio::select! {
            _ = is_started.wait_for(|started| *started) => {}
            _ = is_ended.wait_for(|ended| *ended) => {}
        }

        // A kernel that ended without ever reaching phase five did not start.
        // One that reached it and ended anyway did, and the outcome is the
        // harness's to report.
        let run = if *is_started.borrow() {
            Run::Live(task)
        } else {
            match join(task).await {
                Outcome::Failed(error) => return Err(error),
                outcome => Run::Ended(outcome),
            }
        };

        Ok(TestHarness {
            handle,
            container,
            telemetry,
            run,
        })
    }

    /// The production builder with every substitution installed as its hook.
    ///
    /// One hook for the whole list: the kernel offers a single one, and running
    /// them in declaration order there is what puts them after every bundle and
    /// before phase three.
    fn hooked(self) -> KernelBuilder {
        let substitutions = self.substitutions;
        let keep_running = self.keep_running;
        self.builder
            .__register_hook(Box::new(move |registry: &mut Registry| {
                for substitution in substitutions {
                    substitution(registry);
                }
                // Last, so that a runnable the test substituted is registered
                // under its own order and this one only ever joins them.
                if keep_running {
                    registry.runnable(Provider::from_value(Arc::new(Parking)));
                }
            }))
    }
}

impl Default for TestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Raises the flag when the kernel publishes [`Running`].
struct StartedListener {
    /// The flag [`TestBuilder::start`] waits on.
    started: watch::Sender<bool>,
}

impl Listener<Running> for StartedListener {
    fn on_event<'a>(
        &'a self,
        _event: &'a mut Running,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            // `send_replace` rather than `send`: a caller that gave up waiting
            // has dropped the receiver, and that is not a failure to report.
            self.started.send_replace(true);
            Ok(Flow::Continue)
        })
    }
}

/// The run, whether it is still going or already over.
enum Run {
    /// Phases four to seven, still on their task.
    Live(JoinHandle<Outcome>),
    /// The run ended before the harness was handed over.
    Ended(Outcome),
}

/// Awaits the driver task and turns its failure into an outcome.
///
/// A panic inside the kernel is re-raised rather than summarised: the test that
/// caused it should read the panic it caused, at the assertion it came from.
async fn join(task: JoinHandle<Outcome>) -> Outcome {
    match task.await {
        Ok(outcome) => outcome,
        Err(joined) if joined.is_panic() => std::panic::resume_unwind(joined.into_panic()),
        Err(joined) => Outcome::Failed(KernelError::Shutdown(vec![ShutdownError::failed(
            DRIVER,
            joined.to_string().into(),
        )])),
    }
}

/// Drives a running kernel from a test.
pub struct TestHarness {
    /// Asks the kernel to stop.
    handle: KernelHandle,
    /// The container the running kernel resolves through.
    container: Container,
    /// The sink the kernel and its units record into.
    telemetry: Arc<RecordingTelemetry>,
    /// Phases four to seven.
    run: Run,
}

impl TestHarness {
    /// A handle onto the running kernel.
    #[must_use]
    pub fn handle(&self) -> KernelHandle {
        self.handle.clone()
    }

    /// The container, so a test can resolve what the bundles provided.
    #[must_use]
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// The telemetry sink.
    #[must_use]
    pub fn telemetry(&self) -> Arc<RecordingTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// Asks the kernel to stop and waits for it.
    pub async fn stop(self) -> Outcome {
        self.handle.shutdown();
        self.wait().await
    }

    /// Waits for the kernel to stop on its own.
    ///
    /// A test that expects an essential runnable to end the process waits here
    /// rather than asking for a stop.
    pub async fn wait(self) -> Outcome {
        match self.run {
            Run::Live(task) => join(task).await,
            Run::Ended(outcome) => outcome,
        }
    }

    /// Whether phases four to seven are still on their task.
    ///
    /// `false` once the run has returned, whether it stopped on its own or was
    /// asked to. A kernel with no runnable is already `false` when the harness
    /// is handed over, which is what
    /// [`TestBuilder::keep_running`](TestBuilder::keep_running) exists to
    /// change — asking this is how a test states which of the two it expected
    /// instead of inferring it from a resolution that happened to succeed.
    #[must_use]
    pub fn is_running(&self) -> bool {
        match &self.run {
            Run::Live(task) => !task.is_finished(),
            Run::Ended(_) => false,
        }
    }

    /// Waits until `event` has been recorded at least `count` times, and
    /// returns how many were.
    ///
    /// Telemetry is what a running kernel says about itself, and most of what
    /// it says arrives on a task the test does not hold: a restart, a listener
    /// failure, a phase transition. Reading the sink once therefore reads it
    /// too early, and every test that needs one of those facts would otherwise
    /// write the same poll by hand.
    ///
    /// It gives up after a fixed patience and returns the count it saw, so a
    /// test asserts on a number rather than on a timeout: `assert_eq!` on the
    /// result names both what was expected and what arrived.
    pub async fn wait_for_record(&self, event: &str, count: usize) -> usize {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let seen = self
                .telemetry
                .records()
                .iter()
                .filter(|record| record.event == event)
                .count();
            if seen >= count || tokio::time::Instant::now() >= deadline {
                return seen;
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

impl fmt::Debug for TestHarness {
    /// Says whether the run is still going and how much the sink holds. Neither
    /// the container nor the handle carries anything a test could act on here,
    /// and the kernel's own `Debug` counts what it drives.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestHarness")
            .field("running", &self.is_running())
            .field("records", &self.telemetry.records().len())
            .finish_non_exhaustive()
    }
}
