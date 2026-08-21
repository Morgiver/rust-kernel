//! What the expansions put in the user's namespace.
//!
//! The crate lint level here is the kernel's own: a bundle that denies
//! `missing_docs` must be able to use every macro without adding an `allow`.
//! That only holds if the attributes written on a constructor — the doc comment
//! above all — move to the item the attribute generates, and if visibility
//! moves with them.

#![deny(missing_docs)]

use std::sync::Arc;

use kernel::Provider;
use kernel_macros::{FromConfig, provider};

/// A contract, so the provider has something to bind.
pub trait Surface: Send + Sync + 'static {
    /// A value to read back.
    fn mark(&self) -> u8;
}

struct Plain(u8);

impl Surface for Plain {
    fn mark(&self) -> u8 {
        self.0
    }
}

/// Binds the surface. This comment is what the generated function inherits.
#[provider]
pub async fn surface() -> Arc<dyn Surface> {
    Arc::new(Plain(5)) as Arc<dyn Surface>
}

/// A struct that reads itself out of a node.
#[derive(FromConfig)]
pub struct Settings {
    /// How deep to go.
    pub depth: u32,
}

#[test]
fn documented_provider_compiles() {
    let provider: Provider<dyn Surface> = surface();

    assert!(provider.requires.is_empty());
}

#[tokio::test]
async fn provider_builds_the_value() {
    let container = kernel::Kernel::builder()
        .capture_signals(false)
        .build()
        .await
        .expect("the graph closes");
    let built = (surface().build)(container.container())
        .await
        .expect("build");

    assert_eq!(built.mark(), 5);
}
