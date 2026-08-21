//! Building a kernel for a test, and driving it from one.

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
        }
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
            registry.provide(Provider::from_value(double));
        }));
        self
    }

    /// Replaces a named implementation.
    #[must_use]
    pub fn substitute_named<C: ?Sized + Send + Sync + 'static>(
        mut self,
        name: &'static str,
        double: Arc<C>,
    ) -> Self {
        self.substitutions.push(Box::new(move |registry| {
            registry.provide_named(name, Provider::from_value(double));
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
    #[must_use]
    pub fn substitute_component<T: Component>(mut self, double: Arc<T>) -> Self {
        self.substitutions.push(Box::new(move |registry| {
            registry.component(Provider::from_value(double));
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
            registry.runnable(Provider::from_value(double));
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
        self.builder
            .__register_hook(Box::new(move |registry: &mut Registry| {
                for substitution in substitutions {
                    substitution(registry);
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
}
