//! What a whole kernel does, driven through the public API alone.
//!
//! Every test here assembles a bundle, builds a kernel from it and runs it.
//! Nothing reaches into the crate's internals: what a unit did is read back
//! from a journal the units themselves write into, from the telemetry sink the
//! builder was given, or from the [`Outcome`] the run returned.
//!
//! Time is paused in every test that waits. A shutdown ladder measured in
//! milliseconds is still a ladder, and a suite that actually sleeps is a suite
//! nobody runs.

use core::future::pending;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::{Arc, Mutex};

use kernel::core::{
    Backoff, BundleManifest, ComponentDescriptor, ComponentError, ComponentId, Criticality, Flow,
    KernelError, Lifetime, ListenerError, Outcome, Priority, RecordingTelemetry, RegisterError,
    RestartPolicy, RunError, RunnableDescriptor, ShutdownPolicy,
};
use kernel::events::ShutdownRequested;
use kernel::{
    BootContext, BoxFuture, Bundle, Component, ContractRef, FnBundle, Kernel, Listener,
    ListenerContext, Provider, Registry, RunContext, Runnable, Running, ShutdownContext, Stopped,
};

// --------------------------------------------------------------------------
// The journal
// --------------------------------------------------------------------------

/// An ordered record of what the units did, shared by every unit in one test.
///
/// Order is the whole point: a boot order, an unwind order and a stop order are
/// all claims about sequence, and a counter cannot express one.
#[derive(Clone, Default)]
struct Journal(Arc<Mutex<Vec<String>>>);

impl Journal {
    /// Appends one entry.
    fn note(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(entry.into());
    }

    /// A snapshot, in the order the entries were noted.
    fn entries(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Whether an entry was noted.
    fn saw(&self, entry: &str) -> bool {
        self.entries().iter().any(|noted| noted == entry)
    }

    /// Where an entry sits in the sequence.
    fn at(&self, entry: &str) -> Option<usize> {
        self.entries().iter().position(|noted| noted == entry)
    }
}

// --------------------------------------------------------------------------
// Contracts nothing in the plan names
// --------------------------------------------------------------------------

/// A contract no component and no runnable is registered under.
trait Surface: Send + Sync + 'static {}

/// A contract built once per unit of work, out of a [`Surface`].
trait Sink: Send + Sync + 'static {}

/// The one implementation both contracts are bound to.
struct Plain;

impl Surface for Plain {}
impl Sink for Plain {}

// --------------------------------------------------------------------------
// Components
// --------------------------------------------------------------------------

/// What a component does when it is told to boot.
#[derive(Clone, Copy)]
enum OnBoot {
    /// Boots and stays up.
    Ready,
    /// Refuses, which is what makes the rest of phase four unwind.
    Refuse,
}

/// Everything a test component does, behind one type per contract.
///
/// The kernel binds a component under its own concrete type, so two components
/// of one type would be two bindings claiming one contract. The behaviour lives
/// here and the [`components!`] macro gives it as many distinct types as a test
/// needs.
struct Part {
    /// The name the journal writes; the same one the type declares.
    label: &'static str,
    /// Where the calls are recorded.
    journal: Journal,
    /// What `boot` does.
    on_boot: OnBoot,
    /// How long `shutdown` awaits before it notes anything.
    on_stop: Option<Duration>,
    /// Whether `shutdown` answers with a failure of its own.
    refuses_stop: bool,
}

impl Part {
    /// A component that boots and stays up.
    fn ready(label: &'static str, journal: &Journal) -> Self {
        Self {
            label,
            journal: journal.clone(),
            on_boot: OnBoot::Ready,
            on_stop: None,
            refuses_stop: false,
        }
    }

    /// A component that refuses to boot.
    fn refusing(label: &'static str, journal: &Journal) -> Self {
        Self {
            label,
            journal: journal.clone(),
            on_boot: OnBoot::Refuse,
            on_stop: None,
            refuses_stop: false,
        }
    }

    /// A component that boots, stays up, and then fails to stop.
    fn refusing_stop(mut self) -> Self {
        self.refuses_stop = true;
        self
    }

    /// A component whose `shutdown` really awaits, which is what a zero budget
    /// abandons and a budget of its own lets finish.
    fn unhurried(mut self, delay: Duration) -> Self {
        self.on_stop = Some(delay);
        self
    }

    /// Notes the call, then answers as configured.
    async fn boot(&self) -> Result<(), ComponentError> {
        self.journal.note(format!("boot {}", self.label));
        match self.on_boot {
            OnBoot::Ready => Ok(()),
            OnBoot::Refuse => Err(ComponentError::new(
                ComponentId::new(self.label, 0),
                "refused to boot".to_owned().into(),
            )),
        }
    }

    /// Notes that the component was told to stop, after whatever it awaits.
    ///
    /// The note comes last on purpose: a call dropped at its deadline leaves no
    /// entry at all, so the journal reports completion rather than arrival.
    async fn stop(&self) -> Result<(), ComponentError> {
        if let Some(delay) = self.on_stop {
            tokio::time::sleep(delay).await;
        }
        self.journal.note(format!("stop {}", self.label));
        if self.refuses_stop {
            return Err(ComponentError::new(
                ComponentId::new(self.label, 0),
                "refused to stop".to_owned().into(),
            ));
        }
        Ok(())
    }
}

/// Gives [`Part`] one distinct component type per name.
macro_rules! components {
    ($($ty:ident => $label:literal),+ $(,)?) => {$(
        /// A test component; the type exists so that this one has a contract
        /// of its own.
        struct $ty(Part);

        impl Component for $ty {
            fn name() -> &'static str {
                $label
            }

            fn descriptor(&self) -> ComponentDescriptor {
                ComponentDescriptor::new()
            }

            fn boot<'a>(
                &'a self,
                _cx: &'a BootContext<'a>,
            ) -> BoxFuture<'a, Result<(), ComponentError>> {
                Box::pin(self.0.boot())
            }

            fn shutdown<'a>(
                &'a self,
                _cx: &'a ShutdownContext<'a>,
            ) -> BoxFuture<'a, Result<(), ComponentError>> {
                Box::pin(self.0.stop())
            }
        }
    )+};
}

components!(First => "first", Second => "second", Third => "third");

// --------------------------------------------------------------------------
// Runnables
// --------------------------------------------------------------------------

/// What a runnable does once it has started.
#[derive(Clone, Copy)]
enum Work {
    /// Waits for the stop token, then returns cleanly.
    Wait,
    /// Asks the kernel to stop, then waits for the token.
    Request,
    /// Returns at once, which is what an essential runnable's end means.
    Finish,
    /// Returns a failure at once.
    Fail,
    /// Fails its first attempt, then asks the kernel to stop.
    FailThenRequest,
    /// Never returns, and never looks at the token.
    Deaf,
    /// Resolves the bindings the plan never named, then asks for a stop.
    Reach,
}

/// Everything a test runnable does, behind one type per contract.
struct Job {
    /// Criticality and the budgets this runnable may shorten.
    descriptor: RunnableDescriptor,
    /// Where the starts and the ends are recorded.
    journal: Journal,
    /// What `run` does.
    work: Work,
    /// How many times `run` has been entered, restarts included.
    attempts: Arc<AtomicUsize>,
}

impl Job {
    /// A job with a counter of its own.
    fn new(descriptor: RunnableDescriptor, journal: &Journal, work: Work) -> Self {
        Self {
            descriptor,
            journal: journal.clone(),
            work,
            attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A job whose attempts the test counts too.
    fn counting(mut self, attempts: &Arc<AtomicUsize>) -> Self {
        self.attempts = Arc::clone(attempts);
        self
    }

    /// One run, from the attempt it is up to.
    async fn run(&self, cx: RunContext) -> Result<(), RunError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        // The name the kernel actually attributes this run to.
        let name = cx.id().name();
        self.journal.note(format!("run {name} #{attempt}"));

        match self.work {
            Work::Wait => cx.shutdown().stopping().await,
            Work::Request => {
                cx.handle().shutdown();
                cx.shutdown().stopping().await;
            }
            Work::Finish => {}
            Work::Fail => {
                return Err(RunError::failed(cx.id(), "refused".to_owned().into()));
            }
            Work::FailThenRequest => {
                if attempt == 1 {
                    return Err(RunError::failed(
                        cx.id(),
                        "first attempt refused".to_owned().into(),
                    ));
                }
                cx.handle().shutdown();
                cx.shutdown().stopping().await;
            }
            Work::Deaf => pending::<()>().await,
            Work::Reach => {
                if cx.container().is_sealed() {
                    self.journal.note("sealed");
                }
                if cx.container().get::<dyn Surface>().await.is_ok() {
                    self.journal.note("surface reached");
                }
                let scope = cx.container().scope();
                if scope.container().get::<dyn Sink>().await.is_ok() {
                    self.journal.note("sink reached");
                }
                cx.handle().shutdown();
                cx.shutdown().stopping().await;
            }
        }

        self.journal.note(format!("end {name}"));
        Ok(())
    }
}

/// Gives [`Job`] one distinct runnable type per role.
macro_rules! runnables {
    ($($ty:ident => $label:literal),+ $(,)?) => {$(
        /// A test runnable; the type exists so that this one has a contract of
        /// its own.
        struct $ty(Job);

        impl Runnable for $ty {
            fn name() -> &'static str {
                $label
            }

            fn descriptor(&self) -> RunnableDescriptor {
                self.0.descriptor
            }

            fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
                Box::pin(async move { self.0.run(cx).await })
            }
        }
    )+};
}

runnables!(
    Caller => "caller",
    Waiter => "waiter",
    Closer => "closer",
    Flapper => "flapper",
    Deafened => "deafened",
    Reacher => "reacher",
    Refuser => "refuser",
);

// --------------------------------------------------------------------------
// Listeners
// --------------------------------------------------------------------------

/// Records what the last event of the run carried.
///
/// [`Stopped`] is emitted rather than dispatched, so the run settles its
/// detached emissions before returning and this has already been called by the
/// time the outcome is read.
struct Ending(Arc<Mutex<Option<Stopped>>>);

impl Listener<Stopped> for Ending {
    fn on_event<'a>(
        &'a self,
        event: &'a mut Stopped,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(event.clone());
            Ok(Flow::Continue)
        })
    }
}

/// Notes that phase five was reached, and what it reported.
struct Heard(Journal);

impl Listener<Running> for Heard {
    fn on_event<'a>(
        &'a self,
        event: &'a mut Running,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            self.0.note(format!("running {}", event.runnables));
            Ok(Flow::Continue)
        })
    }
}

/// Adds a note to the shutdown request, and records that it was heard.
///
/// [`ShutdownRequested`] is the one event that is dispatched rather than
/// emitted, so this listener runs *before* the ladder moves — which is what the
/// journal positions assert.
struct Witness(Journal);

impl Listener<ShutdownRequested> for Witness {
    fn on_event<'a>(
        &'a self,
        event: &'a mut ShutdownRequested,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            self.0.note("request heard");
            event.notes.push("witnessed".to_owned());
            Ok(Flow::Continue)
        })
    }
}

/// Hears the shutdown request and never answers.
struct Deaf;

impl Listener<ShutdownRequested> for Deaf {
    fn on_event<'a>(
        &'a self,
        _event: &'a mut ShutdownRequested,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(pending())
    }
}

// --------------------------------------------------------------------------
// Assembly
// --------------------------------------------------------------------------

/// A bundle whose whole registration pass is supplied by the test.
struct Assembly {
    /// The name the manifest publishes.
    name: &'static str,
    /// The registration pass itself.
    fill: Box<dyn Fn(&mut Registry) + Send + Sync>,
}

impl Bundle for Assembly {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new(self.name, "0.0.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        (self.fill)(registry);
        Ok(())
    }
}

/// Wraps a registration closure into a bundle.
fn assembly(name: &'static str, fill: impl Fn(&mut Registry) + Send + Sync + 'static) -> Assembly {
    Assembly {
        name,
        fill: Box::new(fill),
    }
}

/// The budget every test runs on: short, and stated rather than inherited.
const BUDGET: ShutdownPolicy =
    ShutdownPolicy::new(Duration::from_millis(50), Duration::from_millis(50));

/// Builds a kernel out of one bundle. Signals are off: a test process is not
/// the kernel's to interrupt.
async fn built(sink: &RecordingTelemetry, bundle: Assembly) -> Kernel {
    Kernel::builder()
        .telemetry(Arc::new(sink.clone()))
        .shutdown_policy(BUDGET)
        .capture_signals(false)
        .bundle(bundle)
        .build()
        .await
        .expect("the graph must close")
}

/// An ancillary runnable that is never restarted.
fn ancillary() -> RunnableDescriptor {
    RunnableDescriptor::new().criticality(Criticality::Ancillary)
}

/// The integer a recorded event carried under `key`.
fn recorded(sink: &RecordingTelemetry, event: &str, key: &str) -> Option<i64> {
    sink.records()
        .iter()
        .find(|record| record.event == event)
        .and_then(|record| record.int(key))
}

// --------------------------------------------------------------------------
// The tests
// --------------------------------------------------------------------------

/// The whole sequence, in one assertion: components boot in plan order, the
/// runnable runs, and everything stops in the reverse of the order it booted.
#[tokio::test(start_paused = true)]
async fn boots_runs_and_stops() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("ordered", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.component(Provider::from_value(Arc::new(Second(Part::ready(
                "second", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Caller(Job::new(
                ancillary(),
                &journal,
                Work::Request,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert_eq!(
        journal.entries(),
        [
            "boot first",
            "boot second",
            "run caller #1",
            "end caller",
            "stop second",
            "stop first",
        ]
    );
}

/// An essential runnable returning ends the run, and takes the ancillary one
/// down with it. It returned cleanly, but it returned while the other runnable
/// still had work to do: the process lost the unit that defined it before the
/// rest were done, which is not a completion and does not exit zero.
///
/// The other case — every runnable returning on its own, the essential one last
/// — is `batch_run_completes`.
#[tokio::test(start_paused = true)]
async fn essential_end_stops_kernel() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("essential", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Waiter(Job::new(
                ancillary(),
                &journal,
                Work::Wait,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Closer(Job::new(
                RunnableDescriptor::new(),
                &journal,
                Work::Finish,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(!outcome.is_success(), "{outcome:?}");
    let Some(KernelError::Run(errors)) = outcome.error() else {
        panic!("expected a run failure, got {outcome:?}");
    };
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].runnable().name(), "closer");
    assert!(journal.saw("end closer"));
    assert!(journal.saw("end waiter"));
    assert!(journal.saw("stop first"));
}

/// The other half of the same rule: a kernel whose every runnable returns on
/// its own has finished the work it was assembled to do, essential unit
/// included, and it exits zero.
#[tokio::test(start_paused = true)]
async fn batch_run_completes() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("batch", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Closer(Job::new(
                RunnableDescriptor::new(),
                &journal,
                Work::Finish,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(matches!(outcome, Outcome::Completed), "{outcome:?}");
    assert!(journal.saw("end closer"));
    assert!(journal.saw("stop first"));
}

/// An ancillary runnable that fails is restarted, and the kernel it failed
/// The container a runnable resolves through is framed by that runnable's own
/// `requires`.
///
/// Undeclared, the resolution panics in debug builds and the panic ends that
/// runnable's task — which is what the supervisor reports. Declared, it is the
/// ordinary resolution `unreached_binding_is_built` pins.
#[cfg(debug_assertions)]
#[tokio::test(start_paused = true)]
async fn run_guard_refuses_undeclared() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("greedy", move |registry| {
            registry.provide(Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>));
            // No `requires`, and `Work::Reach` resolves `Surface` all the same.
            registry.runnable(Provider::from_value(Arc::new(Reacher(Job::new(
                ancillary(),
                &journal,
                Work::Reach,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Caller(Job::new(
                ancillary(),
                &journal,
                Work::Request,
            )))));
        })
    };

    let _ = built(&sink, bundle).await.run().await;

    assert!(sink.contains("runnable.failed"));
    assert!(!journal.saw("surface reached"));
}

/// Section 12: a component that fails to stop influences the exit code.
///
/// Nothing else went wrong — the run was asked to stop and it stopped — so
/// without this the process exits zero while a component is still holding
/// whatever it was told to release.
#[tokio::test(start_paused = true)]
async fn failed_stop_reaches_outcome() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("stubborn", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(
                Part::ready("first", &journal).refusing_stop(),
            ))));
            registry.runnable(Provider::from_value(Arc::new(Caller(Job::new(
                ancillary(),
                &journal,
                Work::Request,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(!outcome.is_success(), "{outcome:?}");
    match outcome.error() {
        Some(KernelError::Shutdown(errors)) => {
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].unit(), "first");
            assert!(errors[0].to_string().contains("refused to stop"));
        }
        other => panic!("expected a shutdown failure, got {other:?}"),
    }
    assert!(journal.saw("stop first"));
}

/// inside of is still there to stop later: the outcome is the stop that was
/// asked for, not the failure that was recovered from.
#[tokio::test(start_paused = true)]
async fn ancillary_failure_restarts() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let bundle = {
        let journal = journal.clone();
        let attempts = Arc::clone(&attempts);
        assembly("flapping", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Flapper(
                Job::new(
                    ancillary().restart(RestartPolicy::on_failure(
                        3,
                        Backoff::Fixed(Duration::from_millis(10)),
                    )),
                    &journal,
                    Work::FailThenRequest,
                )
                .counting(&attempts),
            ))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(sink.contains("runnable.restarted"));
    assert!(journal.saw("run flapper #2"));
    assert!(journal.saw("stop first"));
}

/// A component that refuses to boot unwinds the ones before it, in the reverse
/// of the order they were observed to boot in — and never unwinds itself.
#[tokio::test]
async fn boot_failure_rolls_back() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("refusing", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.component(Provider::from_value(Arc::new(Second(Part::ready(
                "second", &journal,
            )))));
            registry.component(Provider::from_value(Arc::new(Third(Part::refusing(
                "third", &journal,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(!outcome.is_success(), "{outcome:?}");
    let Some(KernelError::Boot { rolled_back, .. }) = outcome.error() else {
        panic!("expected a boot failure, got {outcome:?}");
    };
    assert_eq!(rolled_back.len(), 2);
    assert_eq!(
        journal.entries(),
        [
            "boot first",
            "boot second",
            "boot third",
            "stop second",
            "stop first",
        ]
    );
}

/// The hostile case for the ban on lazy resolution: a `Shared` binding no
/// component and no runnable names is still built before the seal, so the first
/// unit of work that reaches it through a `Scoped` provider is served rather
/// than refused.
#[tokio::test(start_paused = true)]
async fn unreached_binding_is_built() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let builds = Arc::new(AtomicUsize::new(0));
    let bundle = {
        let journal = journal.clone();
        let builds = Arc::clone(&builds);
        assembly("unreached", move |registry| {
            let counter = Arc::clone(&builds);
            let noted = journal.clone();
            registry.provide(Provider::from_fn(move |_container| {
                let counter = Arc::clone(&counter);
                let noted = noted.clone();
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    noted.note("build surface");
                    Ok(Arc::new(Plain) as Arc<dyn Surface>)
                })
            }));

            let noted = journal.clone();
            registry.provide(
                Provider::from_fn(move |container| {
                    let noted = noted.clone();
                    Box::pin(async move {
                        let reached = container.get::<dyn Surface>().await.is_ok();
                        noted.note(if reached {
                            "sink built from surface"
                        } else {
                            "sink built blind"
                        });
                        Ok(Arc::new(Plain) as Arc<dyn Sink>)
                    })
                })
                .lifetime(Lifetime::Scoped)
                .requires([ContractRef::of::<dyn Surface>()]),
            );

            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            // Declared because it is resolved from `run`, not only from a
            // build: the debug guard reads the same `requires` at both moments.
            // `Sink` is resolved inside the scope this runnable opens, so it is
            // declared where a unit of work declares — the guard reads that
            // list on the scope's own container.
            registry.runnable(
                Provider::from_value(Arc::new(Reacher(Job::new(
                    ancillary(),
                    &journal,
                    Work::Reach,
                ))))
                .requires([ContractRef::of::<dyn Surface>()])
                .requires_scoped([ContractRef::of::<dyn Sink>()]),
            );
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(
        journal.entries().first().map(String::as_str),
        Some("build surface")
    );
    assert!(journal.at("build surface") < journal.at("boot first"));
    assert!(journal.saw("sealed"));
    assert!(journal.saw("surface reached"));
    assert!(journal.saw("sink built from surface"));
    assert!(journal.saw("sink reached"));
}

/// Dropping the future `run` returned does not skip the stop: the driver is on
/// a task of its own, the drop asks it to stop, and it carries the request
/// through. What the caller loses is the outcome, not the release.
#[tokio::test(start_paused = true)]
async fn dropped_run_still_stops() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("dropped", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Waiter(Job::new(
                ancillary(),
                &journal,
                Work::Wait,
            )))));
        })
    };
    let kernel = built(&sink, bundle).await;

    {
        let mut run = Box::pin(kernel.run());
        tokio::select! {
            outcome = &mut run => panic!("the run must not end on its own: {outcome:?}"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        assert!(journal.saw("run waiter #1"));
        assert!(!journal.saw("stop first"));
    }

    // Long enough for the whole ladder the dropped run left behind.
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(journal.saw("end waiter"));
    assert!(journal.saw("stop first"));
    assert!(sink.contains("kernel.stopped"));
}

/// A runnable that ignores its token is abandoned when its budget runs out, and
/// the kernel stops anyway. The one thing this kernel promises never to do is
/// wait for it.
#[tokio::test(start_paused = true)]
async fn deaf_runnable_is_abandoned() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("deaf", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Deafened(Job::new(
                ancillary()
                    .drain_timeout(Duration::from_millis(10))
                    .stop_timeout(Duration::from_millis(10)),
                &journal,
                Work::Deaf,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Caller(Job::new(
                ancillary(),
                &journal,
                Work::Request,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert!(journal.saw("run deafened #1"));
    assert!(!journal.saw("end deafened"));
    assert!(sink.contains("runnable.abandoned"));
    assert_eq!(recorded(&sink, "kernel.stopped", "abandoned"), Some(1));
    assert!(journal.saw("stop first"));
}

/// The components' stop budget is their own, not what the runnables left.
///
/// The deaf runnable ignores its token and burns the drain budget and the stop
/// budget in full, so under one shared deadline the components would begin their
/// phase with nothing left and this one — which really awaits — would be
/// abandoned without ever noting a thing. It gets a fresh stop budget instead,
/// and finishes inside it.
#[tokio::test(start_paused = true)]
async fn component_gets_own_budget() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    // Two thirds of the stop budget: too long to fit in what the runnables
    // leave, short enough to fit in a budget of its own.
    let unhurried = Duration::from_millis(30);
    let bundle = {
        let journal = journal.clone();
        assembly("unhurried", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(
                Part::ready("first", &journal).unhurried(unhurried),
            ))));
            registry.runnable(Provider::from_value(Arc::new(Deafened(Job::new(
                ancillary(),
                &journal,
                Work::Deaf,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Caller(Job::new(
                ancillary(),
                &journal,
                Work::Request,
            )))));
        })
    };

    let started = tokio::time::Instant::now();
    let outcome = built(&sink, bundle).await.run().await;
    let elapsed = started.elapsed();

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    // The runnables spent both budgets before the component was even called.
    assert!(sink.contains("runnable.abandoned"));
    assert!(elapsed >= BUDGET.total() + unhurried, "{elapsed:?}");
    // And it ran to the end rather than being dropped at a deadline of zero.
    assert!(journal.saw("stop first"));
}

/// Section 13: the kernel never blocks indefinitely, the shutdown request
/// included.
///
/// The dispatch of [`ShutdownRequested`] is awaited, so a listener that never
/// returns would hold the whole process on the one rung nothing else bounds.
/// It is cut at the drain budget, the overrun is recorded, and the ladder goes
/// on down.
#[tokio::test(start_paused = true)]
async fn deaf_request_listener_is_cut() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("unanswered", move |registry| {
            registry.listen(Deaf, Priority::HIGH);
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Caller(Job::new(
                ancillary(),
                &journal,
                Work::Request,
            )))));
        })
    };

    // The outer bound is what turns an unbounded rung into a failed test
    // rather than a hung one.
    let outcome = tokio::time::timeout(Duration::from_secs(60), built(&sink, bundle).await.run())
        .await
        .expect("a deaf request listener must not hold the kernel");

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert!(sink.contains("kernel.request_overran"));
    // The rungs below the request were still walked.
    assert!(journal.saw("stop first"));
    assert!(sink.contains("kernel.stopped"));
}

/// The shutdown request reaches its listeners before the ladder moves, and the
/// notes they add reach whoever reads the request.
#[tokio::test(start_paused = true)]
async fn request_reaches_listeners() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("witnessed", move |registry| {
            registry.listen(Witness(journal.clone()), Priority::NORMAL);
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Caller(Job::new(
                ancillary(),
                &journal,
                Work::Request,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert!(journal.at("request heard") < journal.at("end caller"));
    assert!(journal.at("request heard") < journal.at("stop first"));
    assert_eq!(
        recorded(&sink, "kernel.shutdown_requested", "notes"),
        Some(1)
    );
}

/// An essential runnable ends the run whatever its result: the clean return is
/// `essential_end_stops_kernel`, this is the other half. The failure is the
/// cause the outcome names, the components still stop, and the process exits
/// non-zero.
#[tokio::test(start_paused = true)]
async fn essential_failure_stops_kernel() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly("refused", move |registry| {
            registry.component(Provider::from_value(Arc::new(First(Part::ready(
                "first", &journal,
            )))));
            registry.component(Provider::from_value(Arc::new(Second(Part::ready(
                "second", &journal,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Waiter(Job::new(
                ancillary(),
                &journal,
                Work::Wait,
            )))));
            registry.runnable(Provider::from_value(Arc::new(Refuser(Job::new(
                RunnableDescriptor::new(),
                &journal,
                Work::Fail,
            )))));
        })
    };

    let outcome = built(&sink, bundle).await.run().await;

    assert!(!outcome.is_success(), "{outcome:?}");
    let Some(KernelError::Run(errors)) = outcome.error() else {
        panic!("expected a run failure, got {outcome:?}");
    };
    assert_eq!(errors.len(), 1, "{errors:?}");
    // The ancillary one was told to stop, and both components unwound.
    assert!(journal.saw("end waiter"));
    assert_eq!(
        journal.entries().last().map(String::as_str),
        Some("stop first")
    );
    assert!(journal.at("stop second") < journal.at("stop first"));
    // The error blames the name the unit declares, not the Rust type path: the
    // registry stamps `Runnable::name` into the id, and every record and every
    // error carries that one name from there on.
    assert_eq!(errors[0].runnable().name(), "refuser");
}

// --------------------------------------------------------------------------
// Registering, listening and settling without a bundle of one's own
// --------------------------------------------------------------------------

/// An application registers a listener without authoring a bundle type.
///
/// The seven verbs live on the [`Registry`], and a `Registry` reaches
/// [`Bundle::register`] and nowhere else. [`FnBundle`] is that form with a
/// closure in it, so the application writes the one line it has instead of a
/// type and two trait methods.
#[tokio::test(start_paused = true)]
async fn listener_without_a_bundle() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();

    let heard = journal.clone();
    let kernel = Kernel::builder()
        .telemetry(Arc::new(sink.clone()))
        .shutdown_policy(BUDGET)
        .capture_signals(false)
        .bundle(FnBundle::new("app", move |registry| {
            registry.listen::<Running, _>(Heard(heard.clone()), Priority::NORMAL);
            Ok(())
        }))
        .build()
        .await
        .expect("a graph with one listener in it closes");

    let outcome = kernel.run().await;

    assert!(outcome.is_success(), "{outcome:?}");
    assert!(journal.saw("running 0"), "{:?}", journal.entries());
}

/// The last event of the run carries the same three counts the telemetry line
/// does, so a listener and an operator reading `kernel.stopped` agree.
///
/// The run below fails once and recovers: `run_failures` counts the ending,
/// `unhandled` counts what nothing recovered from, and reading either one for
/// the other is the confusion the split exists to prevent. A listener that
/// could read only `abandoned` could not tell the two apart at all.
#[tokio::test(start_paused = true)]
async fn stopped_carries_counts() {
    let journal = Journal::default();
    let seen = Arc::new(Mutex::new(None));
    let sink = RecordingTelemetry::new();
    let attempts = Arc::new(AtomicUsize::new(0));

    let flapping = {
        let journal = journal.clone();
        let attempts = Arc::clone(&attempts);
        assembly("flapping", move |registry| {
            registry.runnable(Provider::from_value(Arc::new(Flapper(
                Job::new(
                    ancillary().restart(RestartPolicy::on_failure(
                        3,
                        Backoff::Fixed(Duration::from_millis(10)),
                    )),
                    &journal,
                    Work::FailThenRequest,
                )
                .counting(&attempts),
            ))));
        })
    };

    let kept = Arc::clone(&seen);
    let kernel = Kernel::builder()
        .telemetry(Arc::new(sink.clone()))
        .shutdown_policy(BUDGET)
        .capture_signals(false)
        .bundle(flapping)
        .bundle(FnBundle::new("app", move |registry| {
            registry.listen::<Stopped, _>(Ending(Arc::clone(&kept)), Priority::NORMAL);
            Ok(())
        }))
        .build()
        .await
        .expect("the graph closes");

    let outcome = kernel.run().await;
    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");

    let event = seen
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .expect("the stop was published before the run returned");
    assert_eq!(event.abandoned, 0);
    // The failure was recovered from, so it is a failure of the run and not one
    // of the outcome.
    assert_eq!(event.run_failures, 1);
    assert_eq!(event.unhandled, 0);
    assert_eq!(
        recorded(&sink, "kernel.stopped", "run_failures"),
        Some(i64::try_from(event.run_failures).expect("a count fits"))
    );
    assert_eq!(
        recorded(&sink, "kernel.stopped", "unhandled"),
        Some(i64::try_from(event.unhandled).expect("a count fits"))
    );
}

/// A caller that emits before running can wait for what it emitted.
///
/// [`EventDispatcher::settle`] is what makes a detached emission deliverable
/// rather than best-effort, and without an accessor on [`Kernel`] it was
/// reachable only from inside a boot, run or shutdown context.
///
/// [`EventDispatcher::settle`]: kernel::EventDispatcher::settle
#[tokio::test(start_paused = true)]
async fn dispatcher_settles_emissions() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();

    let heard = journal.clone();
    let kernel = Kernel::builder()
        .telemetry(Arc::new(sink.clone()))
        .shutdown_policy(BUDGET)
        .capture_signals(false)
        .bundle(FnBundle::new("app", move |registry| {
            registry.listen::<Running, _>(Heard(heard.clone()), Priority::NORMAL);
            Ok(())
        }))
        .build()
        .await
        .expect("the graph closes");

    kernel.dispatcher().emit(Running { runnables: 7 });
    kernel.dispatcher().settle().await;

    assert!(journal.saw("running 7"), "{:?}", journal.entries());
}

/// A listener is callable with no kernel behind it, like a component and a
/// runnable before it.
///
/// This file is a separate crate, so a context the kernel builds internally is
/// out of reach here: [`ListenerContext::detached`] is what makes the call
/// writable at all.
#[tokio::test]
async fn detached_listener_runs() {
    let journal = Journal::default();
    let detached = ListenerContext::detached();

    let flow = Heard(journal.clone())
        .on_event(&mut Running { runnables: 2 }, &detached.context())
        .await
        .expect("the listener handled the event");

    assert!(matches!(flow, Flow::Continue));
    assert!(journal.saw("running 2"));
    // The container it was given is empty, and it is a real one: a listener
    // that resolves is told what is missing rather than panicking.
    assert!(detached.container().get::<Journal>().await.is_err());
}

// --------------------------------------------------------------------------
// The scope guard, through a real supervisor
// --------------------------------------------------------------------------

/// Resolves a `Scoped` binding it did not declare, inside the scope it opens.
struct Greedy;

impl Runnable for Greedy {
    fn name() -> &'static str {
        "greedy"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        ancillary()
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            let scope = cx.container().scope();
            let _ = scope.get::<dyn Sink>().await;
            Ok(())
        })
    }
}

/// The scope a runnable opens is framed by what the runnable declared its unit
/// of work resolves, on the container the supervisor actually hands it.
///
/// `container/mod.rs` proves the frame over a fixture. This proves the
/// production path carries it: `for_unit`, `RunContext`, `scope()`.
#[cfg(debug_assertions)]
#[tokio::test(start_paused = true)]
async fn scope_guard_bites_under_the_kernel() {
    let sink = RecordingTelemetry::new();
    let bundle = assembly("greedy-scope", |registry| {
        registry.provide(
            Provider::from_value(Arc::new(Plain) as Arc<dyn Sink>).lifetime(Lifetime::Scoped),
        );
        registry.provide_named(
            "other",
            Provider::from_value(Arc::new(Plain) as Arc<dyn Sink>).lifetime(Lifetime::Scoped),
        );
        // Declares one scoped need and resolves the other.
        registry.runnable(
            Provider::from_value(Arc::new(Greedy))
                .requires_scoped([ContractRef::named::<dyn Sink>("other")]),
        );
    });

    let outcome = built(&sink, bundle).await.run().await;

    assert!(outcome.is_success(), "{outcome:?}");
    let failed = sink
        .records()
        .into_iter()
        .find(|record| record.event == "runnable.failed")
        .expect("the guard panicked and the supervisor filed it");
    let error = failed.str("cause").expect("the failure names its cause");
    assert!(error.contains("requires_scoped"), "{error}");
}
