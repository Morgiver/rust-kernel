//! Substitution: the round trip from a missing contract to a bundle that boots,
//! and the rule that a double keeps the nature of what it stands in for.

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use kernel::core::{
    BoxFuture, BuildError, BundleManifest, ComponentDescriptor, ComponentError, ContractRef,
    KernelError, RegisterError, ResolveError, RunError, RunnableDescriptor,
};
use kernel::{BootContext, Component, Provider, RunContext, Runnable, ShutdownContext};
use kernel_testkit::{FnBundle, TestBuilder, missing_contracts};

// --------------------------------------------------------------------------
// The contracts a bundle under test asks somebody else for
// --------------------------------------------------------------------------

/// Answers with a number.
trait Counter: Send + Sync + 'static {
    /// The number.
    fn value(&self) -> u64;
}

/// Keeps what it is given.
trait Sink: Send + Sync + 'static {
    /// Keeps one value.
    fn keep(&self, value: u64);
}

/// The two contracts the bundle under test needs and does not provide.
static NEEDED: [ContractRef; 2] = [
    ContractRef::of::<dyn Counter>(),
    ContractRef::of::<dyn Sink>(),
];

// --------------------------------------------------------------------------
// The bundle under test
// --------------------------------------------------------------------------

/// A component that uses both contracts, so both are declared and both are
/// really resolved — the debug-build container refuses an undeclared one.
struct Joiner {
    counter: Arc<dyn Counter>,
    sink: Arc<dyn Sink>,
}

impl Component for Joiner {
    fn name() -> &'static str {
        "joiner"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.sink.keep(self.counter.value());
            Ok(())
        })
    }
}

/// A bundle that stands alone only once both contracts are answered.
fn joining() -> FnBundle {
    FnBundle::new("joining", |registry| {
        registry.component(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    let counter = container
                        .get::<dyn Counter>()
                        .await
                        .map_err(|error| BuildError::new("joiner", Box::new(error)))?;
                    let sink = container
                        .get::<dyn Sink>()
                        .await
                        .map_err(|error| BuildError::new("joiner", Box::new(error)))?;
                    Ok(Arc::new(Joiner { counter, sink }))
                })
            })
            .requires(NEEDED.iter().copied()),
        );
        Ok(())
    })
    .manifest(BundleManifest::new("joining", "0.1.0").requires(&NEEDED))
}

// --------------------------------------------------------------------------
// The doubles
// --------------------------------------------------------------------------

struct Fixed;

impl Counter for Fixed {
    fn value(&self) -> u64 {
        7
    }
}

#[derive(Default)]
struct Kept {
    values: Mutex<Vec<u64>>,
}

impl Kept {
    fn values(&self) -> Vec<u64> {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Sink for Kept {
    fn keep(&self, value: u64) {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(value);
    }
}

/// A component double that records the lifecycle it was given.
#[derive(Default)]
struct Watched {
    booted: AtomicBool,
    stopped: AtomicBool,
}

impl Watched {
    fn booted(&self) -> bool {
        self.booted.load(Ordering::Acquire)
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl Component for Watched {
    fn name() -> &'static str {
        "watched"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.booted.store(true, Ordering::Release);
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        _cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.stopped.store(true, Ordering::Release);
            Ok(())
        })
    }
}

/// A runnable that holds the kernel up until the ladder reaches stopping.
struct Idle;

impl Runnable for Idle {
    fn name() -> &'static str {
        "idle"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            cx.shutdown().stopping().await;
            Ok(())
        })
    }
}

/// The same, but it says whether the supervisor ever started it.
#[derive(Default)]
struct Ticked {
    started: AtomicBool,
}

impl Ticked {
    fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
}

impl Runnable for Ticked {
    fn name() -> &'static str {
        "ticked"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            self.started.store(true, Ordering::Release);
            cx.shutdown().stopping().await;
            Ok(())
        })
    }
}

// --------------------------------------------------------------------------
// The round trip
// --------------------------------------------------------------------------

// What phase three reports as unsatisfied is the list of doubles to write, and
// writing exactly that list is what makes the bundle stand alone.
#[tokio::test(start_paused = true)]
async fn missing_then_boots() {
    let missing = missing_contracts(joining()).expect("the bundle registers cleanly");
    assert_eq!(
        missing,
        [
            ContractRef::of::<dyn Counter>(),
            ContractRef::of::<dyn Sink>()
        ]
    );

    let sink = Arc::new(Kept::default());
    let harness = TestBuilder::new()
        .bundle(joining())
        .substitute(Arc::new(Fixed) as Arc<dyn Counter>)
        .substitute(Arc::clone(&sink) as Arc<dyn Sink>)
        .start()
        .await
        .expect("the substituted bundle stands alone");

    let outcome = harness.stop().await;

    assert!(outcome.is_success(), "outcome: {outcome:?}");
    assert_eq!(sink.values(), [7]);
}

// A bundle that asks nobody for anything has nothing to stub.
#[test]
fn standing_alone_needs_nothing() {
    let alone = FnBundle::new("alone", |registry| {
        registry.provide(Provider::from_value(Arc::new(Fixed) as Arc<dyn Counter>));
        Ok(())
    });

    assert!(
        missing_contracts(alone)
            .expect("the bundle registers cleanly")
            .is_empty()
    );
}

// A bundle that never reaches phase three has no list of doubles to offer, and
// must not be reported as standing alone. `missing_contracts` builds with no
// configuration source, so a bundle that reads configuration in `register` lands
// here — which is the ordinary bundle, not a corner case.
#[test]
fn register_failure_is_reported() {
    let reads_config = FnBundle::new("reads-config", |registry| {
        let _: u64 = registry
            .config("absent")
            .map_err(|error| RegisterError::new("reads-config", Box::new(error)))?;
        Ok(())
    });

    let error = missing_contracts(reads_config).expect_err("registration failed");

    let KernelError::Register(errors) = error else {
        panic!("expected a registration failure, got {error:?}");
    };
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].bundle(), "reads-config");
}

// Half a substitution leaves the graph open, and phase three says so — the
// doubles go through the validation, not around it.
#[tokio::test(start_paused = true)]
async fn half_substituted_refused() {
    let error = TestBuilder::new()
        .bundle(joining())
        .substitute(Arc::new(Fixed) as Arc<dyn Counter>)
        .build()
        .await
        .expect_err("one contract is still unanswered");

    let KernelError::Resolve(errors) = error else {
        panic!("expected a resolution failure, got {error:?}");
    };
    assert!(errors.iter().any(|error| matches!(
        error,
        ResolveError::MissingContract { contract, .. } if *contract == ContractRef::of::<dyn Sink>()
    )));
}

// The rule this crate is worth having: standing in for a component leaves it a
// component. The kernel boots the double and the kernel stops it.
#[tokio::test(start_paused = true)]
async fn double_keeps_lifecycle() {
    let double = Arc::new(Watched::default());

    let harness = TestBuilder::new()
        .substitute_component(Arc::clone(&double))
        .substitute_runnable(Arc::new(Idle))
        .start()
        .await
        .expect("start");

    assert!(double.booted(), "the kernel did not boot the double");
    assert!(!double.stopped());

    let outcome = harness.stop().await;

    assert!(double.stopped(), "the kernel did not stop the double");
    assert!(outcome.is_success(), "outcome: {outcome:?}");
}

// A double that stands in for a contract AND is a component answers both with
// one object: the binding the graph resolves and the unit the kernel drives are
// the same value.
#[tokio::test(start_paused = true)]
async fn one_double_two_roles() {
    let double = Arc::new(Standing::default());

    let harness = TestBuilder::new()
        .bundle(joining())
        .substitute(Arc::new(Fixed) as Arc<dyn Counter>)
        .substitute(Arc::clone(&double) as Arc<dyn Sink>)
        .substitute_component(Arc::clone(&double))
        .start()
        .await
        .expect("start");

    let resolved = harness
        .container()
        .get::<dyn Sink>()
        .await
        .expect("the contract resolves");
    assert!(Arc::ptr_eq(
        &(Arc::clone(&double) as Arc<dyn Sink>),
        &resolved
    ));
    assert!(double.booted());

    harness.stop().await;

    assert!(double.stopped());
    assert_eq!(double.values(), [7]);
}

/// A double that is both the implementation of a contract and a component.
#[derive(Default)]
struct Standing {
    lifecycle: Watched,
    values: Mutex<Vec<u64>>,
}

impl Standing {
    fn booted(&self) -> bool {
        self.lifecycle.booted()
    }

    fn stopped(&self) -> bool {
        self.lifecycle.stopped()
    }

    fn values(&self) -> Vec<u64> {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Sink for Standing {
    fn keep(&self, value: u64) {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(value);
    }
}

impl Component for Standing {
    fn name() -> &'static str {
        "standing"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        self.lifecycle.boot(cx)
    }

    fn shutdown<'a>(
        &'a self,
        cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        self.lifecycle.shutdown(cx)
    }
}

// A substituted name is reachable under that name and nowhere else.
#[tokio::test(start_paused = true)]
async fn named_double_is_named() {
    let harness = TestBuilder::new()
        .substitute_named("secondary", Arc::new(Fixed) as Arc<dyn Counter>)
        .start()
        .await
        .expect("start");

    let named = harness
        .container()
        .get_named::<dyn Counter>("secondary")
        .await
        .expect("the named binding resolves");
    assert_eq!(named.value(), 7);
    assert!(harness.container().get::<dyn Counter>().await.is_err());

    harness.stop().await;
}

// A named binding a bundle already recorded is replaced under that name, and
// the default position is left alone: a name is part of a contract's identity,
// so the double stands in for exactly the binding that carries it.
#[tokio::test(start_paused = true)]
async fn named_double_replaces_binding() {
    let harness = TestBuilder::new()
        .bundle(FnBundle::new("provider", |registry| {
            registry.provide(Provider::from_value(Arc::new(Real) as Arc<dyn Counter>));
            registry.provide_named(
                "secondary",
                Provider::from_value(Arc::new(Real) as Arc<dyn Counter>),
            );
            Ok(())
        }))
        .substitute_named("secondary", Arc::new(Fixed) as Arc<dyn Counter>)
        .start()
        .await
        .expect("the double stands in for the named binding");

    let named = harness
        .container()
        .get_named::<dyn Counter>("secondary")
        .await
        .expect("the named binding resolves");
    assert_eq!(named.value(), 7);
    // The unnamed binding was not touched by a substitution that named one.
    let default = harness
        .container()
        .get::<dyn Counter>()
        .await
        .expect("the default binding resolves");
    assert_eq!(default.value(), 1);

    harness.stop().await;
}

// Standing in for a component a bundle already registered replaces both halves
// of it: the binding the container resolves and the entry the kernel boots. The
// bundle's own component is never built and never booted, and the graph still
// holds exactly one component.
#[tokio::test(start_paused = true)]
async fn component_double_replaces_unit() {
    let real = Arc::new(Watched::default());
    let double = Arc::new(Watched::default());

    let registered = Arc::clone(&real);
    let harness = TestBuilder::new()
        .bundle(FnBundle::new("owner", move |registry| {
            registry.component(Provider::from_value(Arc::clone(&registered)));
            Ok(())
        }))
        .substitute_component(Arc::clone(&double))
        .substitute_runnable(Arc::new(Idle))
        .start()
        .await
        .expect("the double stands in for the bundle's own component");

    assert!(double.booted(), "the kernel did not boot the double");
    assert!(!real.booted(), "the replaced component was booted anyway");

    let resolved = harness
        .container()
        .get::<Watched>()
        .await
        .expect("the component resolves");
    assert!(Arc::ptr_eq(&double, &resolved));

    harness.stop().await;

    assert!(double.stopped());
    assert!(!real.stopped());
}

// The same for the other unit the kernel drives: the double is the runnable the
// supervisor starts, and the one the bundle registered is not started at all.
#[tokio::test(start_paused = true)]
async fn runnable_double_replaces_unit() {
    let real = Arc::new(Ticked::default());
    let double = Arc::new(Ticked::default());

    let registered = Arc::clone(&real);
    let harness = TestBuilder::new()
        .bundle(FnBundle::new("owner", move |registry| {
            registry.runnable(Provider::from_value(Arc::clone(&registered)));
            Ok(())
        }))
        .substitute_runnable(Arc::clone(&double))
        .start()
        .await
        .expect("the double stands in for the bundle's own runnable");

    assert!(double.started(), "the supervisor did not start the double");
    assert!(!real.started(), "the replaced runnable was started anyway");

    harness.stop().await;
}

// `RegisterError` is reachable from a closure bundle, and the kernel attributes
// it to the name the closure was given.
#[tokio::test(start_paused = true)]
async fn closure_failure_is_attributed() {
    let error = TestBuilder::new()
        .bundle(FnBundle::new("broken", |_registry| {
            Err(RegisterError::new("broken", "deliberate".to_owned().into()))
        }))
        .build()
        .await
        .expect_err("the bundle refused to register");

    let KernelError::Register(errors) = error else {
        panic!("expected a registration failure, got {error:?}");
    };
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].bundle(), "broken");
}

// --------------------------------------------------------------------------
// Substituting something the bundle already provides
// --------------------------------------------------------------------------

/// The other `Counter`, the one the bundle provides for itself.
struct Real;

impl Counter for Real {
    fn value(&self) -> u64 {
        1
    }
}

/// The same bundle, but it answers `Counter` itself and needs only a sink.
fn self_counting() -> FnBundle {
    FnBundle::new("self-counting", |registry| {
        registry.provide(Provider::from_value(Arc::new(Real) as Arc<dyn Counter>));
        registry.component(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    let counter = container
                        .get::<dyn Counter>()
                        .await
                        .map_err(|error| BuildError::new("joiner", Box::new(error)))?;
                    let sink = container
                        .get::<dyn Sink>()
                        .await
                        .map_err(|error| BuildError::new("joiner", Box::new(error)))?;
                    Ok(Arc::new(Joiner { counter, sink }))
                })
            })
            .requires(NEEDED.iter().copied()),
        );
        Ok(())
    })
    .manifest(BundleManifest::new("self-counting", "0.1.0").requires(&NEEDED))
}

// The verb is called `substitute` and its doc says it "replaces the
// implementation of a contract". A contract the bundle already binds is the
// only case in which there is anything to replace, and it is the case a test
// that swaps a real dependency for a double is written for.
#[tokio::test]
async fn substitute_replaces_a_bound_contract() {
    let kept = Arc::new(Kept::default());

    let harness = TestBuilder::new()
        .bundle(self_counting())
        .substitute(Arc::new(Fixed) as Arc<dyn Counter>)
        .substitute(Arc::clone(&kept) as Arc<dyn Sink>)
        .start()
        .await
        .expect("the double stands in for the bundle's own binding");

    harness.stop().await;

    assert_eq!(
        kept.values(),
        [7],
        "the real counter answered, not the double"
    );
}
