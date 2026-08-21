//! The unit of composition: one crate, one manifest, one registration pass.
//!
//! A bundle is the only thing an application assembles. It does not build
//! anything, own anything, or run anything — it *fills in a form*. Phase two
//! walks the bundles in declaration order and hands each one the same
//! [`Registry`]; phase three reads what they wrote and decides whether the
//! whole makes a graph.
//!
//! # Why registration is deaf
//!
//! [`Bundle::register`] is synchronous, receives no container, and has no way
//! to reach another bundle. That is not an oversight and it is not a
//! limitation to be worked around later: when bundle A registers, bundle B may
//! not have registered yet, so anything A could learn about B would depend on
//! declaration order. A design where registration can observe registration has
//! no deterministic answer to "what does the graph look like right now" — the
//! answer changes with the order the application happened to list its bundles
//! in.
//!
//! Splitting one phase into two removes the question. Phase two is *pure
//! declaration*, where order buys nothing; phase three sees every declaration
//! at once and can therefore report every graph error at once, rather than the
//! first one a partial view happened to expose. Anything a bundle wants from
//! another bundle it asks for as a contract, and receives in phase four when
//! the container exists.

use kernel_core::{BundleManifest, RegisterError};

use crate::registry::Registry;

/// A unit of composition the kernel can register.
///
/// One implemented bundle is one crate. It contributes a [`BundleManifest`]
/// describing what it is and what it needs, and a single registration pass
/// that writes into the [`Registry`].
///
/// The trait is deliberately tiny. A bundle has no boot hook, no shutdown
/// hook and no run loop of its own — those belong to the
/// [`Component`](crate::component::Component)s and
/// [`Runnable`](crate::runnable::Runnable)s it registers, which the kernel
/// drives individually and can report on individually. A bundle-level lifecycle
/// would be a second, coarser lifecycle running alongside the real one.
///
/// # Registration sees nothing
///
/// `register` is synchronous, gets no container, and cannot reach another
/// bundle: when this bundle registers, the next one may not exist yet. Declare
/// what is needed as a contract in [`manifest`](Self::manifest) or in a
/// [`Provider`](crate::provider::Provider)'s requirements, and receive it from
/// the container once the graph has been validated.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use kernel::core::{BundleManifest, RegisterError};
/// use kernel::{Bundle, Provider, Registry};
///
/// trait Surface: Send + Sync + 'static {}
///
/// struct Plain;
/// impl Surface for Plain {}
///
/// struct Example;
///
/// impl Bundle for Example {
///     fn manifest(&self) -> BundleManifest {
///         BundleManifest::new("example", "0.1.0")
///     }
///
///     fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
///         registry.provide(Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>));
///         Ok(())
///     }
/// }
/// ```
pub trait Bundle: Send + Sync + 'static {
    /// What this bundle is, and what it needs someone else to provide.
    ///
    /// Read once per bundle in phase two, and again in phase three: a
    /// `requires` entry no binding satisfies is a graph error attributed to
    /// this bundle by name, reported before the graph walk so the diagnostic
    /// reads as a missing dependency rather than as a deep resolution failure.
    /// A manifest that claims less than the bundle actually registers is
    /// rejected too — a decorative manifest is worse than none.
    fn manifest(&self) -> BundleManifest;

    /// Writes this bundle's declarations into the registry.
    ///
    /// Called once, in declaration order, with no container and no view of any
    /// other bundle. Returning [`RegisterError`] aborts phase two for this
    /// bundle; every bundle's failure is collected before the kernel gives up,
    /// so one broken bundle does not hide the next.
    ///
    /// Registration must not block: it is the only phase that runs
    /// synchronously, and work done here is work done before anything has been
    /// validated. Build nothing, connect to nothing, read no file that is not
    /// already in the configuration tree.
    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kernel_core::{
        BundleManifest, ConfigTree, ContractRef, NoopTelemetry, RegisterError, Telemetry,
    };

    use super::Bundle;
    use crate::provider::Provider;
    use crate::registry::Registry;

    trait Surface: Send + Sync + 'static {}

    struct Plain;

    impl Surface for Plain {}

    struct Simple;

    impl Bundle for Simple {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("simple", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            registry.provide(Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>));
            Ok(())
        }
    }

    struct Demanding;

    impl Bundle for Demanding {
        fn manifest(&self) -> BundleManifest {
            static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Surface>()];

            BundleManifest::new("demanding", "0.1.0")
                .requires(&REQUIRES)
                .after(&["simple"])
        }

        fn register(&self, _registry: &mut Registry) -> Result<(), RegisterError> {
            Ok(())
        }
    }

    struct Broken;

    impl Bundle for Broken {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("broken", "0.1.0")
        }

        fn register(&self, _registry: &mut Registry) -> Result<(), RegisterError> {
            Err(RegisterError::new("broken", "deliberate".to_owned().into()))
        }
    }

    fn registry() -> Registry {
        Registry::new(
            Arc::new(ConfigTree::empty()),
            Arc::new(NoopTelemetry) as Arc<dyn Telemetry>,
        )
    }

    #[test]
    fn registers_into_registry() {
        let mut registry = registry();

        Simple.register(&mut registry).expect("register");

        assert_eq!(registry.into_parts().bindings.len(), 1);
    }

    #[test]
    fn manifest_carries_declarations() {
        let manifest = Demanding.manifest();

        assert_eq!(manifest.name, "demanding");
        assert_eq!(manifest.version, "0.1.0");
        assert_eq!(manifest.requires.len(), 1);
        assert_eq!(manifest.after, &["simple"]);
    }

    #[test]
    fn failure_is_attributed() {
        let error = Broken.register(&mut registry()).expect_err("must fail");

        assert_eq!(error.bundle(), "broken");
    }

    // The kernel keeps bundles as trait objects: a bundle that is not
    // dyn-compatible cannot be registered at all.
    #[test]
    fn is_dyn_compatible() {
        let bundles: Vec<Box<dyn Bundle>> = vec![Box::new(Simple), Box::new(Demanding)];
        let mut registry = registry();

        for bundle in &bundles {
            bundle.register(&mut registry).expect("register");
        }

        let names: Vec<&'static str> = bundles.iter().map(|b| b.manifest().name).collect();
        assert_eq!(names, ["simple", "demanding"]);
    }

    // Two bundles in one pass see the same registry and nothing of each other;
    // what distinguishes their entries is attribution the kernel adds, not
    // anything either bundle said.
    #[test]
    fn passes_are_independent() {
        let mut registry = registry();

        registry.enter_bundle("simple");
        Simple.register(&mut registry).expect("register");
        registry.enter_bundle("second");
        Simple.register(&mut registry).expect("register");

        let bundles: Vec<&'static str> = registry
            .into_parts()
            .bindings
            .iter()
            .map(|entry| entry.bundle)
            .collect();
        assert_eq!(bundles, ["simple", "second"]);
    }
}
