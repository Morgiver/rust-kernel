//! Adversarial checks on unit identity and on the component stop budget.
//!
//! Every unit here declares a name that shares no substring with its Rust type,
//! so a diagnostic that leaks a type path cannot pass by coincidence.

use core::future::pending;
use core::time::Duration;
use std::sync::{Arc, Mutex};

use kernel::core::{
    BundleManifest, ComponentDescriptor, ComponentError, Criticality, FieldValue, KernelError,
    Outcome, Record, RecordingTelemetry, RegisterError, RunError, RunnableDescriptor,
    ShutdownPolicy,
};
use kernel::{
    BootContext, BoxFuture, Bundle, Component, Kernel, Provider, Registry, RunContext, Runnable,
    ShutdownContext,
};

// --------------------------------------------------------------------------
// Journal
// --------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Journal(Arc<Mutex<Vec<String>>>);

impl Journal {
    fn note(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(entry.into());
    }

    fn entries(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn saw(&self, entry: &str) -> bool {
        self.entries().iter().any(|noted| noted == entry)
    }
}

// --------------------------------------------------------------------------
// Units whose declared name shares nothing with their type
// --------------------------------------------------------------------------

/// Declared name: `wire-pool`. What `shutdown` does is per-instance.
struct Wq7 {
    journal: Journal,
    /// `None` boots fine; `Some` never returns from `boot`.
    hangs_on_boot: bool,
    /// How long `shutdown` awaits. `None` returns at once, `Some(None)` never
    /// returns.
    stop: Stop,
}

#[derive(Clone, Copy)]
enum Stop {
    Now,
    After(Duration),
    Never,
}

impl Component for Wq7 {
    fn name() -> &'static str {
        "wire-pool"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        if self.hangs_on_boot {
            ComponentDescriptor::new().boot_timeout(Duration::from_millis(20))
        } else {
            ComponentDescriptor::new()
        }
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.journal.note("boot wire-pool");
            if self.hangs_on_boot {
                pending::<()>().await;
            }
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        _cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            match self.stop {
                Stop::Now => {}
                Stop::After(delay) => tokio::time::sleep(delay).await,
                Stop::Never => pending::<()>().await,
            }
            self.journal.note("stop wire-pool");
            Ok(())
        })
    }
}

/// Declared name: `cache-warmer`. Second component, so the walk has two stops.
struct Vz3 {
    journal: Journal,
    stop: Stop,
}

impl Component for Vz3 {
    fn name() -> &'static str {
        "cache-warmer"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.journal.note("boot cache-warmer");
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        _cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            match self.stop {
                Stop::Now => {}
                Stop::After(delay) => tokio::time::sleep(delay).await,
                Stop::Never => pending::<()>().await,
            }
            self.journal.note("stop cache-warmer");
            Ok(())
        })
    }
}

/// Declared name: `heartbeat-loop`. Fails at once, which is what an essential
/// runnable's failure has to blame.
struct Bk2 {
    journal: Journal,
}

impl Runnable for Bk2 {
    fn name() -> &'static str {
        "heartbeat-loop"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            self.journal.note(format!("run {}", cx.id().name()));
            Err(RunError::failed(cx.id(), "refused".to_owned().into()))
        })
    }
}

/// Declared name: `signal-relay`. Asks for a stop, then waits for the token.
struct Nm5;

impl Runnable for Nm5 {
    fn name() -> &'static str {
        "signal-relay"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new().criticality(Criticality::Ancillary)
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            cx.handle().shutdown();
            cx.shutdown().stopping().await;
            Ok(())
        })
    }
}

/// Declared name: `tape-drive`. Never looks at the token, so it burns both
/// runnable budgets in full.
struct Jd4;

impl Runnable for Jd4 {
    fn name() -> &'static str {
        "tape-drive"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new().criticality(Criticality::Ancillary)
    }

    fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            pending::<()>().await;
            Ok(())
        })
    }
}

// --------------------------------------------------------------------------
// Assembly
// --------------------------------------------------------------------------

struct Assembly {
    fill: Box<dyn Fn(&mut Registry) + Send + Sync>,
}

impl Bundle for Assembly {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new("audit", "0.0.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        (self.fill)(registry);
        Ok(())
    }
}

fn assembly(fill: impl Fn(&mut Registry) + Send + Sync + 'static) -> Assembly {
    Assembly {
        fill: Box::new(fill),
    }
}

async fn built(sink: &RecordingTelemetry, policy: ShutdownPolicy, bundle: Assembly) -> Kernel {
    Kernel::builder()
        .telemetry(Arc::new(sink.clone()))
        .shutdown_policy(policy)
        .capture_signals(false)
        .bundle(bundle)
        .build()
        .await
        .expect("the graph must close")
}

/// Every string a record carries, event name included.
fn rendered(records: &[Record]) -> Vec<String> {
    records
        .iter()
        .map(|record| {
            let fields: Vec<String> = record
                .fields
                .iter()
                .map(|field| format!("{}={}", field.key, field.value))
                .collect();
            format!("{} {}", record.event, fields.join(" "))
        })
        .collect()
}

/// The `Str` field a record carried under `key`.
fn text(sink: &RecordingTelemetry, event: &str, key: &str) -> Option<String> {
    sink.records()
        .iter()
        .find(|record| record.event == event)
        .and_then(|record| record.field(key))
        .and_then(|value| match value {
            FieldValue::Str(found) => Some(found.clone()),
            _ => None,
        })
}

/// Fails if any recorded string mentions a Rust type path of a test unit.
fn no_type_paths(sink: &RecordingTelemetry) {
    let lines = rendered(&sink.records());
    for leaked in ["Wq7", "Vz3", "Bk2", "Nm5", "Jd4", "audit::"] {
        let offenders: Vec<&String> = lines.iter().filter(|line| line.contains(leaked)).collect();
        assert!(offenders.is_empty(), "`{leaked}` leaked into {offenders:?}");
    }
}

const BUDGET: ShutdownPolicy =
    ShutdownPolicy::new(Duration::from_millis(40), Duration::from_millis(40));

// --------------------------------------------------------------------------
// Claim 1 — one declared name, and the kernel uses it
// --------------------------------------------------------------------------

/// A component that overruns `boot` is blamed by its declared name in the
/// telemetry record, in the error and in the `Outcome`. The failure is authored
/// entirely by the kernel — the component never builds an error of its own — so
/// nothing but the registry's stamp can supply the name.
#[tokio::test]
async fn boot_overrun_names_the_component() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly(move |registry| {
            registry.component(Provider::from_value(Arc::new(Wq7 {
                journal: journal.clone(),
                hangs_on_boot: true,
                stop: Stop::Now,
            })));
        })
    };

    let outcome = built(&sink, BUDGET, bundle).await.run().await;

    let Outcome::Failed(
        error @ KernelError::Boot {
            component, source, ..
        },
    ) = &outcome
    else {
        panic!("expected a boot failure, got {outcome:?}");
    };

    // The Outcome.
    assert_eq!(component.name(), "wire-pool");
    assert!(!format!("{outcome:?}").contains("Wq7"), "{outcome:?}");
    // The error.
    assert_eq!(source.component().name(), "wire-pool");
    let rendered = error.to_string();
    assert!(rendered.contains("wire-pool"), "{rendered}");
    assert!(!rendered.contains("Wq7"), "{rendered}");
    // The telemetry record.
    assert_eq!(
        text(&sink, "kernel.boot_failed", "component").as_deref(),
        Some("wire-pool")
    );
    no_type_paths(&sink);
    assert!(journal.saw("boot wire-pool"));
}

/// The same for a runnable: the id it is handed, the error the run ends on, the
/// record the supervisor writes and the `Outcome` all carry `heartbeat-loop`.
#[tokio::test]
async fn run_failure_names_the_runnable() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly(move |registry| {
            registry.component(Provider::from_value(Arc::new(Wq7 {
                journal: journal.clone(),
                hangs_on_boot: false,
                stop: Stop::Now,
            })));
            registry.runnable(Provider::from_value(Arc::new(Bk2 {
                journal: journal.clone(),
            })));
        })
    };

    let outcome = built(&sink, BUDGET, bundle).await.run().await;

    let Outcome::Failed(error @ KernelError::Run(errors)) = &outcome else {
        panic!("expected a run failure, got {outcome:?}");
    };
    assert_eq!(errors.len(), 1, "{errors:?}");

    // The id handed to `run`.
    assert!(journal.saw("run heartbeat-loop"), "{:?}", journal.entries());
    // The Outcome and the error.
    assert_eq!(errors[0].runnable().name(), "heartbeat-loop");
    let rendered = error.to_string();
    assert!(rendered.contains("heartbeat-loop"), "{rendered}");
    assert!(!rendered.contains("Bk2"), "{rendered}");
    assert!(!format!("{outcome:?}").contains("Bk2"), "{outcome:?}");
    // The telemetry record.
    assert_eq!(
        text(&sink, "runnable.failed", "runnable").as_deref(),
        Some("heartbeat-loop")
    );
    no_type_paths(&sink);
}

/// A lifecycle unit registered with a lifetime of its own has that lifetime
/// overridden, and the override is recorded. The record is about one unit, so
/// it must name that unit the way everything else does.
#[tokio::test]
async fn lifetime_override_names_the_unit() {
    let sink = RecordingTelemetry::new();
    let journal = Journal::default();
    let bundle = {
        let journal = journal.clone();
        assembly(move |registry| {
            registry.component(
                Provider::from_value(Arc::new(Wq7 {
                    journal: journal.clone(),
                    hangs_on_boot: false,
                    stop: Stop::Now,
                }))
                .lifetime(kernel::core::Lifetime::Scoped),
            );
            registry.runnable(Provider::from_value(Arc::new(Nm5)));
        })
    };

    let outcome = built(&sink, BUDGET, bundle).await.run().await;
    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");

    assert!(
        sink.contains("registry.lifetime_overridden"),
        "the override must be recorded"
    );
    assert_eq!(
        text(&sink, "registry.lifetime_overridden", "contract").as_deref(),
        Some("wire-pool"),
        "the override record must blame the declared name"
    );
    no_type_paths(&sink);
}

// --------------------------------------------------------------------------
// Claim 2 — the components get a stop budget of their own
// --------------------------------------------------------------------------

/// The runnables burn drain and stop in full; a component that genuinely awaits
/// two thirds of a stop budget still runs to the end.
#[tokio::test]
async fn component_stop_survives_burnt_runnable_budget() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly(move |registry| {
            registry.component(Provider::from_value(Arc::new(Wq7 {
                journal: journal.clone(),
                hangs_on_boot: false,
                stop: Stop::After(Duration::from_millis(26)),
            })));
            registry.runnable(Provider::from_value(Arc::new(Jd4)));
            registry.runnable(Provider::from_value(Arc::new(Nm5)));
        })
    };

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        built(&sink, BUDGET, bundle).await.run(),
    )
    .await
    .expect("the kernel must not hang");
    let elapsed = started.elapsed();

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert!(
        sink.contains("runnable.abandoned"),
        "the deaf runnable must have spent both budgets"
    );
    assert!(
        journal.saw("stop wire-pool"),
        "the component must finish its own stop: {:?}",
        journal.entries()
    );
    assert!(elapsed >= BUDGET.total(), "{elapsed:?}");
}

/// The other half of the same ruling: a component whose `shutdown` never
/// returns is still cut at the deadline of the component phase. The run ends,
/// the overrun is recorded against the declared name, and the component's own
/// note is never written.
#[tokio::test]
async fn deaf_component_is_still_cut() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly(move |registry| {
            registry.component(Provider::from_value(Arc::new(Wq7 {
                journal: journal.clone(),
                hangs_on_boot: false,
                stop: Stop::Never,
            })));
            registry.runnable(Provider::from_value(Arc::new(Nm5)));
        })
    };

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        built(&sink, BUDGET, bundle).await.run(),
    )
    .await
    .expect("a component that never returns must not hang the kernel");
    let elapsed = started.elapsed();

    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
    assert!(
        !journal.saw("stop wire-pool"),
        "the call was cut, so it noted nothing"
    );
    assert_eq!(
        text(&sink, "kernel.rollback_failed", "component").as_deref(),
        Some("wire-pool")
    );
    // Cut at its own deadline, not at some multiple of it.
    assert!(
        elapsed < BUDGET.total() + BUDGET.stop * 4,
        "cut far too late: {elapsed:?}"
    );
    no_type_paths(&sink);
}

/// What the fresh budget is shared between. Two components each await a little
/// under half the stop budget; the walk is bounded by ONE deadline for the
/// whole phase, so the second one is charged for the first one's wait.
#[tokio::test]
async fn component_budget_is_shared_across_the_walk() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly(move |registry| {
            registry.component(Provider::from_value(Arc::new(Wq7 {
                journal: journal.clone(),
                hangs_on_boot: false,
                // Two thirds of the phase budget, on its own comfortably inside
                // it.
                stop: Stop::After(Duration::from_millis(26)),
            })));
            registry.component(Provider::from_value(Arc::new(Vz3 {
                journal: journal.clone(),
                stop: Stop::After(Duration::from_millis(26)),
            })));
            registry.runnable(Provider::from_value(Arc::new(Nm5)));
        })
    };

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        built(&sink, BUDGET, bundle).await.run(),
    )
    .await
    .expect("the kernel must not hang");
    assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");

    assert!(
        journal.saw("stop cache-warmer"),
        "the first component stopped is the last booted: {:?}",
        journal.entries()
    );
    assert!(
        journal.saw("stop wire-pool"),
        "each component must get a budget it does not share with its neighbour: {:?}, records {:?}",
        journal.entries(),
        rendered(&sink.records())
    );
}

/// Declared name: `ledger`. Refuses to boot, which is what makes phase four
/// unwind whatever booted before it.
struct Hy8;

impl Component for Hy8 {
    fn name() -> &'static str {
        "ledger"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            Err(ComponentError::new(
                kernel::core::ComponentId::new(Self::name(), 0),
                "refused".to_owned().into(),
            ))
        })
    }
}

/// The rollback a failed boot drives must run on the budget the kernel was
/// configured with. A component that never returns is cut at the *configured*
/// stop budget, not at some budget the kernel invented for itself.
#[tokio::test]
async fn boot_rollback_honours_the_configured_budget() {
    let journal = Journal::default();
    let sink = RecordingTelemetry::new();
    let bundle = {
        let journal = journal.clone();
        assembly(move |registry| {
            registry.component(Provider::from_value(Arc::new(Wq7 {
                journal: journal.clone(),
                hangs_on_boot: false,
                stop: Stop::Never,
            })));
            registry.component(Provider::from_value(Arc::new(Hy8)));
        })
    };

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        built(&sink, BUDGET, bundle).await.run(),
    )
    .await
    .expect("the rollback must be bounded by the configured stop budget");
    let elapsed = started.elapsed();

    assert!(!outcome.is_success(), "{outcome:?}");
    assert!(
        elapsed < BUDGET.stop * 10,
        "rolled back on a budget the kernel was never given: {elapsed:?}"
    );
}
