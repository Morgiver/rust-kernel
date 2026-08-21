//! Driving a kernel from a test: the builder's defaults, the harness's ends,
//! and the event log.

use std::collections::BTreeMap;
use std::sync::Arc;

use kernel::core::{
    BoxFuture, ComponentDescriptor, ComponentError, Outcome, Priority, RunError, RunnableDescriptor,
};
use kernel::core::{ConfigNode, ConfigTree, Scalar};
use kernel::{
    BootContext, Component, ComponentBooted, MemorySource, Provider, RunContext, Runnable,
};
use kernel_testkit::{EventLog, FnBundle, TestBuilder};

/// A component that does nothing but be booted, under a name of its own.
struct First;

/// The second one, so the log has an order to report.
struct Second;

macro_rules! plain {
    ($ty:ty, $name:literal) => {
        impl Component for $ty {
            fn name() -> &'static str {
                $name
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
    };
}

plain!(First, "first");
plain!(Second, "second");

/// A runnable that returns the moment it is started.
struct Prompt;

impl Runnable for Prompt {
    fn name() -> &'static str {
        "prompt"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A runnable that stays up until the ladder reaches stopping.
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

// The sink is readable before the kernel exists, which is the point of handing
// it to the builder rather than to the kernel.
#[tokio::test(start_paused = true)]
async fn telemetry_before_build() {
    let builder = TestBuilder::new();
    let telemetry = builder.telemetry();
    assert!(telemetry.is_empty());

    let kernel = builder.build().await.expect("build");

    assert!(telemetry.contains("kernel.resolved"));
    assert!(Arc::ptr_eq(
        kernel.container().telemetry(),
        &(telemetry as Arc<dyn kernel::Telemetry>)
    ));
}

/// A two-level tree with one scalar leaf.
fn tree(outer: &str, inner: &str, value: i64) -> ConfigTree {
    ConfigTree::from_node(ConfigNode::Map(BTreeMap::from([(
        outer.to_owned(),
        ConfigNode::Map(BTreeMap::from([(
            inner.to_owned(),
            ConfigNode::Scalar(Scalar::Int(value)),
        )])),
    )])))
}

// A configuration source reaches the container the same way it would in
// production.
#[tokio::test(start_paused = true)]
async fn config_source_reaches_container() {
    let harness = TestBuilder::new()
        .config_source(MemorySource::new(tree("alpha", "beta", 3)))
        .start()
        .await
        .expect("start");

    assert!(harness.container().config().get("alpha.beta").is_some());

    harness.stop().await;
}

// `stop` asks and waits; the reason is a programmatic request, which is a
// success.
#[tokio::test(start_paused = true)]
async fn stop_requests_and_waits() {
    let harness = TestBuilder::new()
        .substitute_runnable(Arc::new(Idle))
        .start()
        .await
        .expect("start");

    let outcome = harness.stop().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested));
}

// `wait` is for a kernel that ends on its own: every runnable returned, so the
// run is a completion nobody asked for.
#[tokio::test(start_paused = true)]
async fn wait_lets_it_end() {
    let harness = TestBuilder::new()
        .substitute_runnable(Arc::new(Prompt))
        .start()
        .await
        .expect("start");

    let outcome = harness.wait().await;

    assert!(
        matches!(outcome, Outcome::Completed),
        "outcome: {outcome:?}"
    );
}

// The handle the harness hands out is the handle the kernel watches.
#[tokio::test(start_paused = true)]
async fn handle_stops_the_kernel() {
    let harness = TestBuilder::new()
        .substitute_runnable(Arc::new(Idle))
        .start()
        .await
        .expect("start");

    harness.handle().shutdown();
    let outcome = harness.wait().await;

    assert!(matches!(outcome, Outcome::ShutdownRequested));
}

// The log keeps every event of its type, in the order they were dispatched,
// and the copy the registry took and the copy the test kept are one log.
#[tokio::test(start_paused = true)]
async fn log_records_in_order() {
    let log: EventLog<ComponentBooted> = EventLog::new();
    let registered = log.clone();

    let harness = TestBuilder::new()
        .bundle(FnBundle::new("two", move |registry| {
            registry.component(Provider::from_value(Arc::new(First)));
            registry.component(Provider::from_value(Arc::new(Second)));
            registry.listen::<ComponentBooted, _>(registered.clone(), Priority::NORMAL);
            Ok(())
        }))
        .substitute_runnable(Arc::new(Idle))
        .start()
        .await
        .expect("start");

    harness.stop().await;
    tokio::task::yield_now().await;

    let names: Vec<&'static str> = log
        .events()
        .iter()
        .map(|event| event.component.name())
        .collect();
    assert_eq!(names, ["first", "second"]);
    assert_eq!(log.len(), 2);
    assert!(!log.is_empty());

    log.clear();
    assert!(log.is_empty());
}

// An empty log is empty, and a log nobody dispatched to stays that way.
#[test]
fn empty_log_is_empty() {
    let log: EventLog<ComponentBooted> = EventLog::default();

    assert!(log.is_empty());
    assert_eq!(log.len(), 0);
    assert!(log.events().is_empty());
}

// A closure bundle reports the name it was given, and the manifest it was given
// when it was given one.
#[tokio::test(start_paused = true)]
async fn closure_bundle_names_itself() {
    use kernel::Bundle;
    use kernel::core::BundleManifest;

    let plain = FnBundle::new("plain", |_registry| Ok(()));
    assert_eq!(Bundle::manifest(&plain).name, "plain");

    let stated =
        FnBundle::new("plain", |_registry| Ok(())).manifest(BundleManifest::new("stated", "2.0.0"));
    assert_eq!(Bundle::manifest(&stated).version, "2.0.0");

    let harness = TestBuilder::new()
        .bundle(stated)
        .start()
        .await
        .expect("start");
    assert!(harness.telemetry().contains("kernel.registered"));
    harness.stop().await;
}
