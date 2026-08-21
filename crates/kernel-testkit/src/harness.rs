//! Building a kernel for a test, and driving it from one.
//!
//! Three sizes, and a test takes the smallest one that answers its question.
//! [`TestHarness`] drives a running kernel and reports what it recorded, on the
//! way past and at the end. [`Registered`] stops after phase three and reads
//! back what one registration pass declared, which is what a bundle's own test
//! suite asks about. [`container`] keeps only the container, for a test whose
//! whole question is whether a binding resolves.

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use kernel::dispatcher::{EventDispatcher, Listener, ListenerContext};
use kernel::{
    Bundle, Component, Container, FnBundle, Kernel, KernelBuilder, KernelHandle, Provider,
    Registry, Runnable, Running,
};
use kernel_core::{
    BoxFuture, ConfigSource, ContainerError, Event, FieldValue, Flow, KernelError, ListenerError,
    Outcome, Priority, Record, RecordingTelemetry, RegisterError, ShutdownError, ShutdownPolicy,
    Telemetry,
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

/// How long [`TestHarness::wait_for`] waits before giving up.
///
/// Long enough for a supervisor working through a backoff, short enough that a
/// kernel which never records what the test asked for fails in the time a test
/// takes. A test with a paused clock never spends it: the poll's sleep advances
/// the clock instead of the wall.
const PATIENCE: Duration = Duration::from_secs(5);

/// How often [`TestHarness::wait_for`] looks again.
const POLL: Duration = Duration::from_millis(5);

/// The record phase three writes, read by [`Registered::components`] and
/// [`Registered::runnables`].
const RESOLVED: &str = "kernel.resolved";

/// The bundle name [`container`] registers its pass under.
const FIXTURE: &str = "fixture";

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

    /// Runs phases one to three and hands back what they recorded.
    ///
    /// The counterpart of [`start`](Self::start) for a test that has nothing to
    /// drive: the graph is assembled and validated, and nothing is booted, run
    /// or stopped. It is what a bundle's own test suite asks for — see
    /// [`Registered`].
    ///
    /// # Errors
    ///
    /// Whatever [`build`](Self::build) reports.
    pub async fn registered(self) -> Result<Registered, KernelError> {
        let telemetry = self.telemetry();
        let kernel = self.build().await?;
        Ok(Registered { kernel, telemetry })
    }

    /// Runs phases one to three and hands back the container alone.
    ///
    /// Shorthand for [`registered`](Self::registered) followed by
    /// [`Registered::into_container`], for the case where the bindings are all
    /// the test wants. See [`container`] for the one-closure form.
    ///
    /// # Errors
    ///
    /// Whatever [`build`](Self::build) reports.
    pub async fn container(self) -> Result<Container, KernelError> {
        Ok(self.registered().await?.into_container())
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
    ///
    /// The outcome alone. A test that also reads what the stop recorded —
    /// `runnable.abandoned` and `kernel.stopped` are written while this call is
    /// running, and this call is what consumes the harness — asks
    /// [`stopped`](Self::stopped) for both together.
    pub async fn stop(self) -> Outcome {
        self.stopped().await.into_outcome()
    }

    /// Asks the kernel to stop, and reports the outcome with the records.
    ///
    /// The last rungs of the ladder are where the interesting lines are
    /// written, and every one of them arrives after the harness has been
    /// consumed. [`Ended`] carries the sink out with the outcome, so reading
    /// them does not depend on the test having cloned
    /// [`telemetry`](Self::telemetry) beforehand.
    pub async fn stopped(self) -> Ended {
        self.handle.shutdown();
        self.waited().await
    }

    /// Waits for the kernel to stop on its own.
    ///
    /// A test that expects an essential runnable to end the process waits here
    /// rather than asking for a stop. [`waited`](Self::waited) is the same
    /// wait, reporting the records with the outcome.
    pub async fn wait(self) -> Outcome {
        self.waited().await.into_outcome()
    }

    /// Waits for the kernel to stop on its own, and reports the records with
    /// the outcome.
    ///
    /// The counterpart of [`stopped`](Self::stopped) for a run that ends
    /// without being asked to.
    pub async fn waited(self) -> Ended {
        let telemetry = Arc::clone(&self.telemetry);
        let outcome = match self.run {
            Run::Live(task) => join(task).await,
            Run::Ended(outcome) => outcome,
        };
        Ended { outcome, telemetry }
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
        self.wait_for(&RecordMatch::event(event), count).await
    }

    /// Waits until `count` records match, and returns how many did.
    ///
    /// The event name is rarely the fact under test: a restart is told apart
    /// from another restart by which runnable it names, and a stop by how many
    /// units it abandoned. [`RecordMatch`] constrains the fields as well, so a
    /// test that cares about one of them waits here instead of writing the
    /// bounded poll again by hand.
    ///
    /// Same patience and same report as
    /// [`wait_for_record`](Self::wait_for_record): it gives up after a fixed
    /// budget and hands back the count it saw, so the assertion names a number
    /// rather than a timeout.
    pub async fn wait_for(&self, query: &RecordMatch, count: usize) -> usize {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let seen = query.count(&self.telemetry.records());
            if seen >= count || tokio::time::Instant::now() >= deadline {
                return seen;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Everything recorded so far, in emission order.
    #[must_use]
    pub fn records(&self) -> Vec<Record> {
        self.telemetry.records()
    }

    /// How many records match, read once and without waiting.
    #[must_use]
    pub fn count(&self, query: &RecordMatch) -> usize {
        query.count(&self.telemetry.records())
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

/// What a record has to look like for a test to be waiting for it.
///
/// [`TestHarness::wait_for_record`] matches the event name and nothing else,
/// which answers "did a runnable restart" and not "did *this* runnable
/// restart". Every fact a kernel reports about a named unit is in a field —
/// `runnable`, `component`, `attempt`, `abandoned` — so a query that cannot
/// read one cannot be used for the questions worth asking.
///
/// A query with no field constraint matches on the event alone, so this is a
/// superset of what the name-only helper does and never a second mechanism.
///
/// # Examples
///
/// ```
/// use kernel_core::{Level, Record};
/// use kernel_testkit::RecordMatch;
///
/// let record = Record::new(Level::Error, "runnable.abandoned")
///     .with("runnable", "reader")
///     .with("restarts", 2i64);
///
/// assert!(RecordMatch::event("runnable.abandoned").matches(&record));
/// assert!(RecordMatch::event("runnable.abandoned").with("runnable", "reader").matches(&record));
/// assert!(!RecordMatch::event("runnable.abandoned").with("runnable", "writer").matches(&record));
/// assert!(RecordMatch::field("restarts", 2i64).matches(&record));
/// ```
#[derive(Debug, Clone)]
pub struct RecordMatch {
    /// The event name the record must carry, if the query names one.
    event: Option<String>,
    /// The fields the record must carry, all of them, value included.
    fields: Vec<(String, FieldValue)>,
}

impl RecordMatch {
    /// Records carrying this event name.
    #[must_use]
    pub fn event(event: &str) -> Self {
        Self {
            event: Some(event.to_owned()),
            fields: Vec::new(),
        }
    }

    /// Records carrying this field, whatever event they name.
    ///
    /// For a fact reported under more than one event name — a unit named by
    /// `runnable` in half a dozen of them — where constraining the name would
    /// narrow the question rather than sharpen it.
    #[must_use]
    pub fn field(key: &str, value: impl Into<FieldValue>) -> Self {
        Self {
            event: None,
            fields: vec![(key.to_owned(), value.into())],
        }
    }

    /// Narrows the query by one more field.
    ///
    /// Every constraint has to hold: two calls ask for a record carrying both
    /// fields, not for one carrying either.
    #[must_use]
    pub fn with(mut self, key: &str, value: impl Into<FieldValue>) -> Self {
        self.fields.push((key.to_owned(), value.into()));
        self
    }

    /// Whether `record` satisfies every constraint.
    #[must_use]
    pub fn matches(&self, record: &Record) -> bool {
        if let Some(event) = &self.event
            && record.event != event.as_str()
        {
            return false;
        }
        self.fields
            .iter()
            .all(|(key, value)| record.field(key) == Some(value))
    }

    /// How many of `records` satisfy every constraint.
    #[must_use]
    pub fn count(&self, records: &[Record]) -> usize {
        records.iter().filter(|record| self.matches(record)).count()
    }

    /// The first record that satisfies every constraint.
    #[must_use]
    pub fn first<'a>(&self, records: &'a [Record]) -> Option<&'a Record> {
        records.iter().find(|record| self.matches(record))
    }
}

/// A run that is over, with everything it recorded.
///
/// The records that say the most about a stop are written *during* it:
/// `runnable.abandoned` as the supervisor gives up on a task, `kernel.stopped`
/// as phase seven closes. Both arrive inside the call that consumes the
/// harness, so a test holding only an [`Outcome`] cannot reach them and a test
/// that forgot to clone [`TestHarness::telemetry`] beforehand has lost them.
/// This carries the two out together.
pub struct Ended {
    /// How the run ended.
    outcome: Outcome,
    /// The sink the whole run recorded into, the stop included.
    telemetry: Arc<RecordingTelemetry>,
}

impl Ended {
    /// How the run ended.
    #[must_use]
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// The outcome alone, for a test that is done with the records.
    #[must_use]
    pub fn into_outcome(self) -> Outcome {
        self.outcome
    }

    /// Whether the run ended without failure.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.outcome.is_success()
    }

    /// The sink, still shared with whatever else holds it.
    #[must_use]
    pub fn telemetry(&self) -> Arc<RecordingTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// Everything the run recorded, in emission order.
    #[must_use]
    pub fn records(&self) -> Vec<Record> {
        self.telemetry.records()
    }

    /// How many records match.
    #[must_use]
    pub fn count(&self, query: &RecordMatch) -> usize {
        query.count(&self.telemetry.records())
    }

    /// Whether at least one record matches.
    #[must_use]
    pub fn contains(&self, query: &RecordMatch) -> bool {
        self.count(query) > 0
    }

    /// The first record that matches, cloned out of the sink.
    #[must_use]
    pub fn find(&self, query: &RecordMatch) -> Option<Record> {
        query.first(&self.telemetry.records()).cloned()
    }
}

impl fmt::Debug for Ended {
    /// The outcome and how much was recorded. The sink itself is what
    /// [`records`](Self::records) is for, and rendering it here would bury the
    /// outcome under a run's worth of lines.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ended")
            .field("outcome", &self.outcome)
            .field("records", &self.telemetry.len())
            .finish_non_exhaustive()
    }
}

/// A registration pass that ran, and what it recorded.
///
/// [`Registry`] has no public constructor, and it should not have one: a
/// registry that never reaches phase three records declarations nobody
/// validated. So a bundle crate cannot call its own `register` — the one
/// method it exists to implement — without an application to run it from, and
/// its most load-bearing claims ("this contract is bound twice, and the
/// unnamed one is the default") are observable only by starting a program.
///
/// This runs the pass through the real phases one to three and stops there:
/// the graph is assembled and validated, and nothing is booted, started or
/// stopped. What the pass recorded is then read back through the container the
/// validation produced and the dispatcher it built.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use kernel::core::{BundleManifest, RegisterError};
/// use kernel::{Bundle, Provider, Registry};
/// use kernel_testkit::Registered;
///
/// trait Sink: Send + Sync + 'static {}
/// struct Plain;
/// impl Sink for Plain {}
///
/// struct Twice;
///
/// impl Bundle for Twice {
///     fn manifest(&self) -> BundleManifest {
///         BundleManifest::new("twice", "0.1.0")
///     }
///
///     fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
///         registry.provide(Provider::from_value(Arc::new(Plain) as Arc<dyn Sink>));
///         registry.provide_named(
///             "spare",
///             Provider::from_value(Arc::new(Plain) as Arc<dyn Sink>),
///         );
///         Ok(())
///     }
/// }
///
/// # async fn example() {
/// let registered = Registered::of(Twice).await.expect("registered");
/// assert_eq!(registered.bindings::<dyn Sink>().await, 2);
/// assert!(!registered.defaults_to::<dyn Sink>("spare").await);
/// # }
/// ```
pub struct Registered {
    /// Phases one to three, kept so the container and the dispatcher it built
    /// outlive the call that produced them.
    kernel: Kernel,
    /// The sink phases one to three recorded into.
    telemetry: Arc<RecordingTelemetry>,
}

impl Registered {
    /// Runs `bundle`'s registration pass, alone and with no configuration.
    ///
    /// A bundle that reads configuration in `register`, or that needs a double
    /// for a contract it does not bind itself, goes through
    /// [`TestBuilder::registered`] instead — this is the short form of exactly
    /// that call. [`missing_contracts`](crate::missing_contracts) names the
    /// doubles a bundle needs.
    ///
    /// # Errors
    ///
    /// Whatever kept the assembly from reaching the end of phase three: a
    /// `register` that failed, or a graph that does not close.
    pub async fn of(bundle: impl Bundle) -> Result<Self, KernelError> {
        TestBuilder::new().bundle(bundle).registered().await
    }

    /// The container phase three produced.
    #[must_use]
    pub fn container(&self) -> &Container {
        self.kernel.container()
    }

    /// The container alone, outliving everything else here.
    ///
    /// The bindings survive; the dispatcher does not, so a test that also asks
    /// [`listeners`](Self::listeners) keeps the whole thing instead.
    #[must_use]
    pub fn into_container(self) -> Container {
        self.kernel.container().clone()
    }

    /// The dispatcher phase three built from the listeners the pass recorded.
    #[must_use]
    pub fn dispatcher(&self) -> &Arc<EventDispatcher> {
        self.kernel.dispatcher()
    }

    /// The sink phases one to three recorded into.
    #[must_use]
    pub fn telemetry(&self) -> Arc<RecordingTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// Everything the assembly recorded, in emission order.
    #[must_use]
    pub fn records(&self) -> Vec<Record> {
        self.telemetry.records()
    }

    /// The implementation the pass bound under no name.
    ///
    /// # Errors
    ///
    /// [`ContainerError::NotProvided`] when the pass bound no default for `C`,
    /// or whatever the binding's own build reported.
    pub async fn get<C: ?Sized + Send + Sync + 'static>(&self) -> Result<Arc<C>, ContainerError> {
        self.container().get::<C>().await
    }

    /// The implementation the pass bound under `name`.
    ///
    /// # Errors
    ///
    /// [`ContainerError::NotProvided`] when the pass bound nothing under that
    /// name, or whatever the binding's own build reported.
    pub async fn get_named<C: ?Sized + Send + Sync + 'static>(
        &self,
        name: &'static str,
    ) -> Result<Arc<C>, ContainerError> {
        self.container().get_named::<C>(name).await
    }

    /// How many implementations of `C` the pass bound, named and unnamed alike.
    ///
    /// Zero for a contract nobody bound: "no implementation" is an answer.
    pub async fn bindings<C: ?Sized + Send + Sync + 'static>(&self) -> usize {
        self.container()
            .get_all::<C>()
            .await
            .map_or(0, |values| values.len())
    }

    /// Whether the binding recorded under `name` is the one an unnamed
    /// resolution hands back.
    ///
    /// The claim a bundle makes when it binds a contract twice and states which
    /// half is the default. It compares the two resolved values by pointer, so
    /// it answers for a [`Lifetime::Shared`](kernel_core::Lifetime::Shared)
    /// binding — the default, and the only lifetime for which "the same
    /// implementation" is a fact about identity rather than about equality. A
    /// contract with no default binding, or none under `name`, is `false`.
    pub async fn defaults_to<C: ?Sized + Send + Sync + 'static>(&self, name: &'static str) -> bool {
        let (Ok(default), Ok(named)) = (self.get::<C>().await, self.get_named::<C>(name).await)
        else {
            return false;
        };
        Arc::ptr_eq(&default, &named)
    }

    /// How many listeners the pass registered for `E`.
    #[must_use]
    pub fn listeners<E: Event>(&self) -> usize {
        self.dispatcher().listener_count::<E>()
    }

    /// How many components the pass registered.
    #[must_use]
    pub fn components(&self) -> usize {
        self.resolved("components")
    }

    /// How many runnables the pass registered.
    #[must_use]
    pub fn runnables(&self) -> usize {
        self.resolved("runnables")
    }

    /// One count off the record phase three writes about the plan it built.
    ///
    /// Read from the sink rather than from the plan, which the kernel keeps to
    /// itself: the record is the kernel's own published statement of what it is
    /// about to drive, so a test reading it reads what an operator would.
    fn resolved(&self, key: &str) -> usize {
        let records = self.telemetry.records();
        let Some(record) = RecordMatch::event(RESOLVED).first(&records) else {
            return 0;
        };
        match record.field(key) {
            Some(FieldValue::Int(count)) => usize::try_from(*count).unwrap_or(0),
            _ => 0,
        }
    }
}

impl fmt::Debug for Registered {
    /// Counts what the pass recorded. The kernel's own `Debug` renders the
    /// plan, and the container's renders the table.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registered")
            .field("components", &self.components())
            .field("runnables", &self.runnables())
            .field("records", &self.telemetry.len())
            .finish_non_exhaustive()
    }
}

/// A container over one registration pass, with no kernel to drive.
///
/// [`Container`] has no public constructor — phase three is the only thing that
/// may build one, which is what keeps a container from existing over a graph
/// nobody validated. A test that wants one binding to resolve therefore had to
/// assemble a whole kernel, start it, and remember to stop it, for the sake of
/// a single `get`.
///
/// This is that, in one call: the closure is the registration pass a
/// [`Bundle`] would have written, it runs through the real phases one to three,
/// and the container it produced comes back. Nothing is booted, started or
/// stopped, so the bindings are unsealed and a
/// [`Lifetime::Scoped`](kernel_core::Lifetime::Scoped) value resolves from a
/// [`Container::scope`] exactly as it would in a unit of work.
///
/// # Errors
///
/// Whatever kept the assembly from reaching the end of phase three: a pass that
/// returned [`RegisterError`], or a graph that does not close.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use kernel::Provider;
/// use kernel::core::Lifetime;
/// use kernel_testkit::container;
///
/// trait Sink: Send + Sync + 'static {}
/// struct Plain;
/// impl Sink for Plain {}
///
/// # async fn example() {
/// let container = container(|registry| {
///     registry.provide(
///         Provider::from_value(Arc::new(Plain) as Arc<dyn Sink>).lifetime(Lifetime::Scoped),
///     );
///     Ok(())
/// })
/// .await
/// .expect("a container");
///
/// let scope = container.scope();
/// assert!(scope.get::<dyn Sink>().await.is_ok());
/// # }
/// ```
pub async fn container<F>(register: F) -> Result<Container, KernelError>
where
    F: Fn(&mut Registry) -> Result<(), RegisterError> + Send + Sync + 'static,
{
    TestBuilder::new()
        .bundle(FnBundle::new(FIXTURE, register))
        .container()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    use kernel::{BootContext, RunContext};
    use kernel_core::{
        BundleManifest, ComponentDescriptor, ComponentError, Lifetime, RunError, RunnableDescriptor,
    };

    /// The contract the fixtures bind.
    trait Marker: Send + Sync + 'static {
        /// Which of the two implementations this is.
        fn tag(&self) -> &'static str;
    }

    /// One implementation, told apart by the tag it carries.
    struct Tagged(&'static str);

    impl Marker for Tagged {
        fn tag(&self) -> &'static str {
            self.0
        }
    }

    /// A component, so the pass has one to count.
    struct Idle;

    impl Component for Idle {
        fn name() -> &'static str {
            "idle"
        }

        fn descriptor(&self) -> ComponentDescriptor {
            ComponentDescriptor::new()
        }

        fn boot<'a>(
            &'a self,
            _cx: &'a BootContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// A runnable that never observes the stop, so the ladder abandons it.
    struct Deaf;

    impl Runnable for Deaf {
        fn name() -> &'static str {
            "deaf"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            RunnableDescriptor::new()
        }

        fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            })
        }
    }

    /// A listener the pass records, so the dispatcher has one to count.
    struct Watch;

    impl Listener<Running> for Watch {
        fn on_event<'a>(
            &'a self,
            _event: &'a mut Running,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async { Ok(Flow::Continue) })
        }
    }

    /// A bundle that binds one contract twice, one of them named, and states
    /// that the unnamed one is the default.
    struct Twice;

    impl Bundle for Twice {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("twice", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), kernel_core::RegisterError> {
            registry.provide(Provider::from_value(
                Arc::new(Tagged("plain")) as Arc<dyn Marker>
            ));
            registry.provide_named(
                "spare",
                Provider::from_value(Arc::new(Tagged("spare")) as Arc<dyn Marker>),
            );
            registry.component(Provider::from_value(Arc::new(Idle)));
            registry.listen::<Running, _>(Watch, Priority::NORMAL);
            Ok(())
        }
    }

    /// One binding, one container, no kernel to start and none to stop. The
    /// scoped value is the case the container is wanted for: it needs a unit of
    /// work, and nothing else here needs a run.
    #[tokio::test]
    async fn container_holds_one_binding() {
        let container = container(|registry| {
            registry.provide(
                Provider::from_value(Arc::new(Tagged("scoped")) as Arc<dyn Marker>)
                    .lifetime(Lifetime::Scoped),
            );
            Ok(())
        })
        .await
        .expect("a container");

        assert!(matches!(
            container.get::<dyn Marker>().await,
            Err(ContainerError::NoScope { .. })
        ));

        let scope = container.scope();
        let first = scope.get::<dyn Marker>().await.expect("scoped");
        let second = scope.get::<dyn Marker>().await.expect("scoped");
        assert_eq!(first.tag(), "scoped");
        assert!(Arc::ptr_eq(&first, &second));
    }

    /// A pass that does not close is refused here exactly as an application
    /// would refuse it: the container never exists over an unvalidated graph.
    #[tokio::test]
    async fn container_refuses_open_graph() {
        let error = container(|registry| {
            registry.provide(
                Provider::from_value(Arc::new(Tagged("plain")) as Arc<dyn Marker>)
                    .requires([kernel_core::ContractRef::of::<dyn Send>()]),
            );
            Ok(())
        })
        .await
        .expect_err("an open graph");

        assert!(matches!(error, KernelError::Resolve(_)));
    }

    /// A bundle's own claim about its bindings, read without an application:
    /// two implementations, one of them named, and the unnamed one is what an
    /// unnamed resolution returns.
    #[tokio::test]
    async fn bundle_binds_twice() {
        let registered = Registered::of(Twice).await.expect("registered");

        assert_eq!(registered.bindings::<dyn Marker>().await, 2);
        assert_eq!(
            registered.get::<dyn Marker>().await.expect("default").tag(),
            "plain"
        );
        assert_eq!(
            registered
                .get_named::<dyn Marker>("spare")
                .await
                .expect("named")
                .tag(),
            "spare"
        );
        assert!(!registered.defaults_to::<dyn Marker>("spare").await);
        assert_eq!(registered.components(), 1);
        assert_eq!(registered.runnables(), 0);
        assert_eq!(registered.listeners::<Running>(), 1);
    }

    /// The named binding that claims the default position is the one the
    /// unnamed resolution returns, and `defaults_to` says so.
    #[tokio::test]
    async fn named_binding_is_default() {
        let registered = TestBuilder::new()
            .bundle(FnBundle::new("defaulted", |registry| {
                let binding = registry.provide_named(
                    "spare",
                    Provider::from_value(Arc::new(Tagged("spare")) as Arc<dyn Marker>),
                );
                let _ = binding.as_default();
                Ok(())
            }))
            .registered()
            .await
            .expect("registered");

        assert!(registered.defaults_to::<dyn Marker>("spare").await);
        assert!(!registered.defaults_to::<dyn Marker>("absent").await);
    }

    /// The fact under test is in a field, and the event name alone cannot tell
    /// the two apart: one runnable started, not two.
    #[tokio::test(start_paused = true)]
    async fn waits_on_a_field() {
        let harness = TestBuilder::new()
            .keep_running()
            .start()
            .await
            .expect("start");

        let one = RecordMatch::event("kernel.running").with("runnables", 1i64);
        let two = RecordMatch::event("kernel.running").with("runnables", 2i64);
        assert_eq!(harness.wait_for(&one, 1).await, 1);
        assert_eq!(harness.wait_for(&two, 1).await, 0);
        assert_eq!(harness.count(&one), 1);
        assert!(!harness.records().is_empty());

        harness.stop().await;
    }

    /// A query with no event name matches the field wherever it is reported.
    #[tokio::test(start_paused = true)]
    async fn matches_field_alone() {
        let harness = TestBuilder::new()
            .keep_running()
            .start()
            .await
            .expect("start");

        assert!(
            harness
                .wait_for(&RecordMatch::field("runnables", 1i64), 1)
                .await
                >= 1
        );

        harness.stop().await;
    }

    /// The records that describe a stop are written during it. The outcome and
    /// the sink come back together, so reading them does not depend on the test
    /// having cloned the telemetry before it asked for the stop.
    #[tokio::test(start_paused = true)]
    async fn stop_reports_records() {
        let harness = TestBuilder::new()
            .substitute_runnable(Arc::new(Deaf))
            .start()
            .await
            .expect("start");

        assert!(!harness.telemetry().contains("runnable.abandoned"));

        let ended = harness.stopped().await;

        assert!(ended.contains(&RecordMatch::event("runnable.abandoned").with("runnable", "deaf")));
        assert!(
            !ended.contains(&RecordMatch::event("runnable.abandoned").with("runnable", "idle"))
        );
        assert_eq!(
            ended.count(&RecordMatch::event("kernel.stopped").with("abandoned", 1i64)),
            1
        );
        assert_eq!(
            ended
                .find(&RecordMatch::event("kernel.stopped"))
                .and_then(|record| record.field("abandoned").cloned()),
            Some(FieldValue::Int(1))
        );
        assert!(!ended.records().is_empty());
        assert!(format!("{ended:?}").starts_with("Ended {"));
        let _ = ended.into_outcome();
    }

    /// A run that ends on its own reports the same pair.
    #[tokio::test(start_paused = true)]
    async fn wait_reports_records() {
        let harness = TestBuilder::new().start().await.expect("start");
        let ended = harness.waited().await;

        assert!(ended.is_success());
        assert!(ended.outcome().is_success());
        assert!(ended.contains(&RecordMatch::event("kernel.stopped")));
        assert_eq!(ended.telemetry().len(), ended.records().len());
    }
}
