//! Registers the ledger. The only crate of the three an application names.
//!
//! This is the wiring, and it is meant to be short: everything it registers is
//! defined somewhere else, and everything it reads is read once. Six lines of
//! `register` decide who provides [`Ledger`], which object the kernel drives,
//! and what the health report is told.
//!
//! # What it depends on, and what it must never depend on
//!
//! `ledger-contracts` — the vocabulary — and `ledger-component` — this
//! feature's own implementation. Not `orders-bundle`, not `audit-bundle`, and
//! not any other `*-bundle` crate: a bundle reaches another feature through
//! that feature's contracts crate and the container, or it does not reach it
//! at all. `ci/check-bundle-graph.sh` walks the resolved dependency graph and
//! fails on any `*-bundle` → `*-bundle` edge, so the rule is a build failure
//! rather than a convention.
//!
//! Note which way the arrows point. Nothing depends on this crate except the
//! application, so replacing the ledger means writing a different `*-bundle`
//! and changing one line of `main` — the consumers, which name only
//! `ledger-contracts`, do not even recompile.
//!
//! # What it registers
//!
//! * an extension point, [`OpeningNote`], declared here so anyone may
//!   contribute to it and read by [`Book`] as it boots;
//! * a component, so the kernel opens the journal before anything uses it and
//!   flushes it before the process exits;
//! * a provider for `dyn Ledger` that resolves *that same component*, so the
//!   object the kernel booted is the object callers get;
//! * a health probe, wrapped in the kernel's [`Probe`] so that probes of
//!   unrelated types share one extension point.
//!
//! # Configuration
//!
//! Read under the prefix [`NAME`], in `register`, before anything exists. The
//! reader has no fallbacks — every default belongs in the application's own
//! configuration source, stated once — so an assembly that supplies none of
//! this is refused in phase two, naming the leaf it could not find:
//!
//! ```text
//! ledger.batch         int       entries buffered before an append commits them
//! ledger.signing_key   string    read as a Secret; never rendered anywhere
//! ledger.flush_timeout duration  "2s"; becomes the component's shutdown timeout
//! ```

use std::sync::Arc;

use kernel::core::{BundleManifest, RegisterError};
use kernel::health::Probe;
use kernel::{Bundle, Provider, Registry};
use ledger_component::{Book, BookProbe, Settings};
use ledger_contracts::{Ledger, OpeningNote};

/// The name this bundle publishes, the prefix it reads its configuration
/// under, and the name every registration diagnostic blames.
pub const NAME: &str = "ledger";

/// The manifest this bundle answers with.
///
/// `requires` is empty and honest: the ledger asks nothing of the other
/// features. A lie in either direction is caught in phase three — a contract
/// listed here and provided by nobody is reported by name, before the graph
/// walk, and a contract used without being listed is reported by the walk
/// itself.
static MANIFEST: BundleManifest = BundleManifest::new(NAME, "0.1.0");

/// Registers the ledger.
///
/// Constructed by the application and by nothing else:
///
/// ```no_run
/// # async fn assemble() {
/// use kernel::Kernel;
/// use ledger_bundle::LedgerBundle;
///
/// let builder = Kernel::builder().bundle(LedgerBundle::default());
/// # let _ = builder;
/// # }
/// ```
#[derive(Debug, Default)]
pub struct LedgerBundle;

impl LedgerBundle {
    /// A bundle with nothing to configure.
    ///
    /// Everything this feature can be told is told through the configuration
    /// tree, so the constructor takes no arguments. A bundle that needed
    /// something the tree cannot carry — a handle the application already owns
    /// — would take it here.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Bundle for LedgerBundle {
    fn manifest(&self) -> BundleManifest {
        MANIFEST
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        let settings: Settings = registry
            .config(NAME)
            .map_err(|error| RegisterError::new(NAME, Box::new(error)))?;

        // Declared before anything can be collected. Contributing to a point
        // nobody declared is a phase-three error, which is why declaring is
        // the bundle's job and not the component's: the component is not there
        // yet.
        registry.declare_extension_point::<OpeningNote>();

        // One object, three roles. It is built here rather than by a provider
        // closure because the probe below has to hold the same `Arc` — a
        // second instance would report on a book nobody writes to.
        let book = Arc::new(Book::new(settings));

        // As a component: the kernel boots it, stops it, and enforces the
        // timeouts its descriptor declares. `also` binds the same object under
        // the contract everyone else names — an alias, not a second provider:
        // it resolves the component's own binding and widens the very `Arc` that
        // comes back, so `container.get::<dyn Ledger>()` and the kernel's boot
        // walk reach one object. The one resolution it performs is declared for
        // it, so phase three orders it and the debug guard passes.
        registry
            .component(Provider::from_value(Arc::clone(&book)))
            .also(|book: Arc<Book>| book as Arc<dyn Ledger>);

        // As a health probe. The kernel declares the `Probe` point itself, so
        // this contributes to a point no application had to open, and the
        // aggregate reads it without ever naming `BookProbe`.
        registry.contribute(Probe::new(BookProbe::new(book)));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_names_the_bundle() {
        let manifest = LedgerBundle::new().manifest();

        assert_eq!(manifest.name, NAME);
        assert!(manifest.requires.is_empty());
        assert!(manifest.after.is_empty());
    }

    // `register` itself is not tested here. A `Registry` cannot be built
    // outside the `kernel` crate — its constructor is crate-private — so the
    // only way to exercise a `Bundle` is to hand it to a builder, and neither
    // `kernel-testkit` nor a runtime is a dev-dependency of this crate. What
    // `register` wires is covered where it can be: the store, its probe and
    // its configuration reader are tested in `ledger-component`, and the
    // assembled graph is exercised by the application crate.
}
