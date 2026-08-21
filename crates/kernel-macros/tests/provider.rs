//! `#[provider]` — what the expansion resolves, and what it declares.
//!
//! The load-bearing pair is [`generated_list_holds`] and
//! [`hand_written_list_drifts`]. They register the *same* build against the
//! same graph; one declares its requirement from the signature, the other by
//! hand and wrongly. The container's debug guard fires on the second and stays
//! silent on the first, which is the whole claim this attribute makes.

use std::fmt;
use std::sync::Arc;

use kernel::core::config::ConfigNode;
use kernel::core::{BundleManifest, ConfigTree, ContractRef, RegisterError};
use kernel::{Bundle, Kernel, Lifetime, MemorySource, Provider, Registry};
use kernel_macros::provider;

trait Sink: Send + Sync + 'static {
    fn mark(&self) -> u8;
}

trait Surface: Send + Sync + 'static {
    fn mark(&self) -> u8;
}

struct Plain(u8);

impl Sink for Plain {
    fn mark(&self) -> u8 {
        self.0
    }
}

struct Layered {
    under: Arc<dyn Sink>,
    extra: u8,
}

impl Surface for Layered {
    fn mark(&self) -> u8 {
        self.under.mark() + self.extra
    }
}

/// The constructor the whole file turns on: one dependency, taken as a
/// parameter, which is what the attribute reads `requires` from.
#[provider]
async fn layered(under: Arc<dyn Sink>) -> Arc<dyn Surface> {
    Arc::new(Layered { under, extra: 1 }) as Arc<dyn Surface>
}

/// The same build, with the declaration written by hand — and drifted: the
/// parameter was added later and the list was not.
fn layered_by_hand() -> Provider<dyn Surface> {
    Provider::from_fn(|container| {
        Box::pin(async move {
            let under = container.get::<dyn Sink>().await.expect("bound");
            Ok(Arc::new(Layered { under, extra: 1 }) as Arc<dyn Surface>)
        })
    })
}

struct Fixture {
    hand_written: bool,
}

impl Bundle for Fixture {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new("fixture", "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        registry.provide(Provider::from_value(Arc::new(Plain(7)) as Arc<dyn Sink>));
        if self.hand_written {
            registry.provide(layered_by_hand());
        } else {
            registry.provide(layered());
        }
        Ok(())
    }
}

async fn resolve_surface(hand_written: bool) -> Arc<dyn Surface> {
    let kernel = Kernel::builder()
        .capture_signals(false)
        .bundle(Fixture { hand_written })
        .build()
        .await
        .expect("the graph closes");

    kernel
        .container()
        .get::<dyn Surface>()
        .await
        .expect("bound")
}

#[test]
fn derives_the_requirement() {
    let provider = layered();

    assert_eq!(provider.requires, vec![ContractRef::of::<dyn Sink>()]);
    assert_eq!(provider.lifetime, Lifetime::Shared);
}

#[tokio::test]
async fn generated_list_holds() {
    let surface = resolve_surface(false).await;

    assert_eq!(surface.mark(), 8);
}

#[tokio::test]
#[should_panic(expected = "did not declare in `requires`")]
async fn hand_written_list_drifts() {
    // Phase three sees nothing wrong: every binding this graph names exists.
    // Only the run-time guard notices, and only when the build actually runs.
    let _ = resolve_surface(true).await;
}

// --------------------------------------------------------------------------
// The rest of the parameter and return shapes.
// --------------------------------------------------------------------------

trait Probe: Send + Sync + 'static {
    fn mark(&self) -> u8;
}

struct Spot(u8);

impl Probe for Spot {
    fn mark(&self) -> u8 {
        self.0
    }
}

struct Summed(u8);

impl Surface for Summed {
    fn mark(&self) -> u8 {
        self.0
    }
}

#[provider]
async fn from_named(#[named("secondary")] other: Arc<dyn Sink>) -> Arc<dyn Surface> {
    Arc::new(Summed(other.mark())) as Arc<dyn Surface>
}

#[provider]
async fn from_all(every: Vec<Arc<dyn Probe>>) -> Arc<dyn Surface> {
    Arc::new(Summed(every.iter().map(|probe| probe.mark()).sum())) as Arc<dyn Surface>
}

#[provider]
fn from_config(config: &ConfigTree) -> Arc<dyn Surface> {
    let depth = config
        .get("depth")
        .and_then(|node| node.as_i64().ok())
        .unwrap_or(0);
    Arc::new(Summed(depth as u8)) as Arc<dyn Surface>
}

/// A failure the kernel has never heard of, wrapped by the expansion.
#[derive(Debug)]
struct Refused;

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("refused")
    }
}

impl std::error::Error for Refused {}

#[provider]
async fn fallible(under: Arc<dyn Sink>) -> Result<Arc<dyn Surface>, Refused> {
    if under.mark() == 0 {
        return Err(Refused);
    }
    Ok(Arc::new(Summed(under.mark())) as Arc<dyn Surface>)
}

/// A constructor that already speaks the kernel's failure type: the expansion
/// hands it straight back rather than wrapping it a second time.
#[provider]
async fn refuses() -> Result<Arc<dyn Surface>, kernel::core::BuildError> {
    Err(kernel::core::BuildError::new("test", Box::new(Refused)))
}

struct Shapes;

impl Bundle for Shapes {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new("shapes", "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        registry.provide(Provider::from_value(Arc::new(Plain(0)) as Arc<dyn Sink>));
        registry.provide_named(
            "secondary",
            Provider::from_value(Arc::new(Plain(3)) as Arc<dyn Sink>),
        );
        registry.provide(Provider::from_value(Arc::new(Spot(4)) as Arc<dyn Probe>));
        registry.provide_named(
            "more",
            Provider::from_value(Arc::new(Spot(5)) as Arc<dyn Probe>),
        );
        registry.provide(from_named());
        Ok(())
    }
}

#[test]
fn named_parameter_declared() {
    let provider = from_named();

    assert_eq!(
        provider.requires,
        vec![ContractRef::named::<dyn Sink>("secondary")]
    );
}

#[test]
fn collection_declares_once() {
    let provider = from_all();

    assert_eq!(provider.requires, vec![ContractRef::of::<dyn Probe>()]);
}

#[test]
fn config_declares_nothing() {
    let provider = from_config();

    assert!(provider.requires.is_empty());
}

#[tokio::test]
async fn named_parameter_resolves() {
    let kernel = Kernel::builder()
        .capture_signals(false)
        .bundle(Shapes)
        .build()
        .await
        .expect("the graph closes");

    let surface = kernel
        .container()
        .get::<dyn Surface>()
        .await
        .expect("bound");

    assert_eq!(surface.mark(), 3);
}

#[tokio::test]
async fn collection_takes_every_binding() {
    struct Collected;

    impl Bundle for Collected {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("collected", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            registry.provide(Provider::from_value(Arc::new(Spot(4)) as Arc<dyn Probe>));
            registry.provide_named(
                "more",
                Provider::from_value(Arc::new(Spot(5)) as Arc<dyn Probe>),
            );
            registry.provide(from_all());
            Ok(())
        }
    }

    let kernel = Kernel::builder()
        .capture_signals(false)
        .bundle(Collected)
        .build()
        .await
        .expect("the graph closes");

    let surface = kernel
        .container()
        .get::<dyn Surface>()
        .await
        .expect("bound");

    assert_eq!(surface.mark(), 9);
}

#[tokio::test]
async fn foreign_error_is_wrapped() {
    struct Failing;

    impl Bundle for Failing {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("failing", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            registry.provide(Provider::from_value(Arc::new(Plain(0)) as Arc<dyn Sink>));
            registry.provide(fallible());
            Ok(())
        }
    }

    let kernel = Kernel::builder()
        .capture_signals(false)
        .bundle(Failing)
        .build()
        .await
        .expect("the graph closes");

    let Err(error) = kernel.container().get::<dyn Surface>().await else {
        panic!("the constructor refuses");
    };

    let rendered = format!("{error}");
    assert!(rendered.contains("refused"), "{rendered}");
    assert!(rendered.contains("Surface"), "{rendered}");
}

#[tokio::test]
async fn kernel_error_passes_through() {
    struct Refusing;

    impl Bundle for Refusing {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("refusing", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            registry.provide(refuses());
            Ok(())
        }
    }

    let kernel = Kernel::builder()
        .capture_signals(false)
        .bundle(Refusing)
        .build()
        .await
        .expect("the graph closes");

    let Err(error) = kernel.container().get::<dyn Surface>().await else {
        panic!("the constructor refuses");
    };

    let rendered = format!("{error}");
    assert_eq!(rendered.matches("failed to build").count(), 1, "{rendered}");
}

#[tokio::test]
async fn config_parameter_reads_tree() {
    struct Configured;

    impl Bundle for Configured {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("configured", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            registry.provide(from_config());
            Ok(())
        }
    }

    let mut tree = ConfigTree::empty();
    tree.insert("depth", ConfigNode::from(6_i64))
        .expect("insert");

    let kernel = Kernel::builder()
        .capture_signals(false)
        .config_source(MemorySource::new(tree))
        .bundle(Configured)
        .build()
        .await
        .expect("the graph closes");

    let surface = kernel
        .container()
        .get::<dyn Surface>()
        .await
        .expect("bound");

    assert_eq!(surface.mark(), 6);
}

#[tokio::test]
async fn lifetime_stays_adjustable() {
    // The attribute returns a plain `Provider`, so the builder verbs still
    // apply: nothing about the expansion is a closed shape.
    let provider = layered().lifetime(Lifetime::Factory);

    assert_eq!(provider.lifetime, Lifetime::Factory);
}
