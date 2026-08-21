//! The list of doubles to write, asked of a bundle that reads configuration.
//!
//! `missing_contracts` resolves with no source at all, so a bundle that reads a
//! key in `register` fails there and has no list to give. That is the shape
//! most bundles have, which makes it the shape the list is worth most for:
//! [`missing_contracts_with`] is the same question asked with the sources the
//! bundle needs to get as far as phase three.

use std::sync::Arc;

use kernel::core::{
    BoxFuture, BuildError, ComponentDescriptor, ComponentError, ConfigNode, ConfigTree,
    ContractRef, KernelError, RegisterError,
};
use kernel::{BootContext, Component, MemorySource, Provider};
use kernel_testkit::{FnBundle, missing_contracts, missing_contracts_with};

/// The key both bundles read in `register`, with no fallback of their own.
const KEY: &str = "settings.size";

/// The contract `reader` needs and nobody else here provides.
trait Surface: Send + Sync + 'static {
    /// Says it was reached, so the need is a real resolution and not a claim.
    fn touch(&self);
}

/// The one implementation, used only by the bundle that stands alone.
struct Plain;

impl Surface for Plain {
    fn touch(&self) {}
}

/// A component that resolves the contract, so the requirement is declared and
/// exercised rather than merely written down.
struct Holder {
    /// What the provider resolved for it.
    surface: Arc<dyn Surface>,
}

impl Component for Holder {
    fn name() -> &'static str {
        "holder"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.surface.touch();
            Ok(())
        })
    }
}

/// Reads `KEY` in `register`, then needs `dyn Surface` from somebody else.
fn reader() -> FnBundle {
    FnBundle::new("reader", |registry| {
        registry
            .config::<i64>(KEY)
            .map_err(|error| RegisterError::new("reader", Box::new(error)))?;
        registry.component(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    let surface = container
                        .get::<dyn Surface>()
                        .await
                        .map_err(|error| BuildError::new("holder", Box::new(error)))?;
                    Ok(Arc::new(Holder { surface }))
                })
            })
            .requires([ContractRef::of::<dyn Surface>()]),
        );
        Ok(())
    })
}

/// Reads `KEY` too, and answers its own need.
fn lonely() -> FnBundle {
    FnBundle::new("lonely", |registry| {
        registry
            .config::<i64>(KEY)
            .map_err(|error| RegisterError::new("lonely", Box::new(error)))?;
        registry.provide(Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>));
        Ok(())
    })
}

/// A source carrying `KEY` as `node`.
fn source(name: &'static str, node: ConfigNode) -> MemorySource {
    let mut tree = ConfigTree::empty();
    tree.insert(KEY, node)
        .expect("a literal path cannot collide");
    MemorySource::named(name, tree)
}

/// A source that satisfies the read.
fn good() -> MemorySource {
    source("good", ConfigNode::from(4_i64))
}

/// A source that carries the key with a type the read refuses.
fn bad() -> MemorySource {
    source("bad", ConfigNode::from("not a number"))
}

// With no source, the bundle never registers, so there is no list — and the
// refusal names the bundle rather than answering the empty list, which would
// have read as "this one stands alone".
#[test]
fn no_source_never_lists() {
    let error = missing_contracts(reader()).expect_err("nothing to read the key from");

    assert!(matches!(error, KernelError::Register(_)), "{error:?}");
    assert!(error.to_string().contains("reader"), "{error}");
}

// Given what it reads, the same bundle reaches phase three and answers the
// list it was always going to answer.
#[test]
fn sources_reach_register() {
    let missing = missing_contracts_with(reader(), [good()]).expect("phase three is reached");

    assert_eq!(missing, [ContractRef::of::<dyn Surface>()]);
}

// The other end of the same list: a bundle that answers its own need reports
// nothing missing, and the configuration read is no longer what stops it.
#[test]
fn configured_bundle_stands_alone() {
    let missing = missing_contracts_with(lonely(), [good()]).expect("phase three is reached");

    assert_eq!(missing, []);
}

// Sources are appended in iteration order and the later one wins, exactly as
// the production builder appends them: the same pair in the other order fails.
#[test]
fn later_source_wins() {
    let missing = missing_contracts_with(reader(), [bad(), good()]).expect("`good` overrides");
    assert_eq!(missing, [ContractRef::of::<dyn Surface>()]);

    let error = missing_contracts_with(reader(), [good(), bad()]).expect_err("`bad` overrides");
    assert!(matches!(error, KernelError::Register(_)), "{error:?}");
}

// An empty list of sources is the no-source case, spelled out: the two entry
// points are one mechanism.
#[test]
fn empty_sources_match_none() {
    let error = missing_contracts_with(reader(), Vec::<MemorySource>::new())
        .expect_err("nothing to read the key from");

    assert!(matches!(error, KernelError::Register(_)), "{error:?}");
}
