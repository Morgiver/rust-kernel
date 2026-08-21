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
//!
//! # What a bundle serves
//!
//! The kernel aggregates health and never publishes it: opening a port and
//! picking a wire format are a bundle's decisions, so a bundle is what serves
//! the report. Two pieces make that writable without a timer or an aggregation
//! loop of the bundle's own:
//!
//! * [`BootContext::extensions`](crate::component::BootContext::extensions)
//!   hands a booting component the whole contribution table as an `Arc`, so it
//!   can be kept and read again on every request — which
//!   [`collect`](crate::component::BootContext::collect), whose items are
//!   borrowed for the boot call, cannot be. The kept table is what
//!   [`aggregate`](crate::health::aggregate) takes, and aggregation is
//!   concurrent and bounded per probe, so one deaf probe becomes one verdict
//!   rather than a stalled report.
//! * [`Shutdown::sleep_until_draining`](crate::shutdown::Shutdown::sleep_until_draining)
//!   is the wait a periodic runnable needs: it returns when the period passed
//!   or when the ladder moved, and says which. A bundle that polls therefore
//!   needs no timer and no `select!` of its own.

use core::fmt;

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

/// Version a closure-built bundle reports until its author states another one.
///
/// A bundle assembled from a closure has no release to name: it is written
/// where it is used rather than distributed, so a zero version is more honest
/// than borrowing the application's.
const UNVERSIONED: &str = "0.0.0";

/// The registration pass, as [`FnBundle`] keeps it.
///
/// `Fn` rather than `FnOnce`: [`Bundle::register`] takes `&self`, and one
/// bundle value may be registered by two kernels.
type RegisterFn = Box<dyn Fn(&mut Registry) -> Result<(), RegisterError> + Send + Sync>;

/// A [`Bundle`] whose registration pass is a closure.
///
/// The seven registration verbs live on [`Registry`], and a `Registry` is
/// handed to [`Bundle::register`] and to nowhere else. An application that
/// needs one line of registration — a listener on a lifecycle event, one
/// binding, one contribution — would otherwise have to declare a type and two
/// trait methods to reach the form it fills in. This is that form, reachable
/// from a closure.
///
/// It is not a shortcut around the phase order: the closure runs in phase two,
/// in declaration order, against the same registry every other bundle writes
/// into, and phase three reads what it wrote like anything else.
///
/// # Examples
///
/// ```
/// # use kernel::core::{BoxFuture, Flow, ListenerError, Priority};
/// # use kernel::dispatcher::{Listener, ListenerContext};
/// # use kernel::{FnBundle, Kernel, Running};
/// # struct Watch;
/// # impl Listener<Running> for Watch {
/// #     fn on_event<'a>(
/// #         &'a self,
/// #         _event: &'a mut Running,
/// #         _cx: &'a ListenerContext<'a>,
/// #     ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
/// #         Box::pin(async { Ok(Flow::Continue) })
/// #     }
/// # }
/// let builder = Kernel::builder().bundle(FnBundle::new("app", |registry| {
///     registry.listen::<Running, _>(Watch, Priority::NORMAL);
///     Ok(())
/// }));
/// ```
pub struct FnBundle {
    /// What this bundle reports in phase two and phase three.
    manifest: BundleManifest,
    /// What it writes into the registry when the kernel asks.
    register: RegisterFn,
}

impl FnBundle {
    /// A bundle with the given name that runs `register` when the kernel asks.
    pub fn new<F>(name: &'static str, register: F) -> Self
    where
        F: Fn(&mut Registry) -> Result<(), RegisterError> + Send + Sync + 'static,
    {
        Self {
            manifest: BundleManifest::new(name, UNVERSIONED),
            register: Box::new(register),
        }
    }

    /// Overrides the manifest this bundle reports.
    ///
    /// The default manifest declares no requirement and no ordering, which is
    /// what an application that registers one listener wants; a bundle that
    /// states what it needs, or that must be ordered against another, states
    /// it here.
    #[must_use]
    pub fn manifest(mut self, manifest: BundleManifest) -> Self {
        self.manifest = manifest;
        self
    }
}

impl Bundle for FnBundle {
    fn manifest(&self) -> BundleManifest {
        self.manifest
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        (self.register)(registry)
    }
}

impl fmt::Debug for FnBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FnBundle")
            .field("manifest", &self.manifest.name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::time::Duration;
    use std::sync::{Arc, Mutex, OnceLock};

    use kernel_core::{
        BoxFuture, BundleManifest, ComponentDescriptor, ComponentError, ConfigTree, ContractRef,
        Criticality, Extension, Health, HealthProbe, NoopTelemetry, Outcome, RegisterError,
        RunError, RunnableDescriptor, ShutdownPolicy, Telemetry,
    };

    use super::Bundle;
    use crate::component::{BootContext, Component};
    use crate::extension::ExtensionPoints;
    use crate::health::{Probe, aggregate};
    use crate::kernel::Kernel;
    use crate::provider::Provider;
    use crate::registry::Registry;
    use crate::runnable::{RunContext, Runnable};

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

    // ----------------------------------------------------------------------
    // What a bundle can serve
    // ----------------------------------------------------------------------

    /// Answers at once, with the verdict it was built with.
    struct Ready;

    impl Extension for Ready {}

    impl HealthProbe for Ready {
        fn name(&self) -> &'static str {
            "ready"
        }

        fn check(&self) -> BoxFuture<'_, Health> {
            Box::pin(async { Health::Up })
        }
    }

    /// What the component hands its runnable, and what the test reads.
    #[derive(Default)]
    struct Board {
        points: OnceLock<Arc<ExtensionPoints>>,
        reports: AtomicUsize,
        overall: Mutex<Option<Health>>,
    }

    /// Keeps the contribution table for as long as the process lives.
    struct Vitals(Arc<Board>);

    impl Component for Vitals {
        fn name() -> &'static str {
            "vitals"
        }

        fn descriptor(&self) -> ComponentDescriptor {
            ComponentDescriptor::new()
        }

        fn boot<'a>(
            &'a self,
            cx: &'a BootContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async move {
                let _ = self.0.points.set(Arc::clone(cx.extensions()));
                Ok(())
            })
        }
    }

    /// Reports health on a period, with no timer and no `select!` of its own.
    struct Pulse {
        board: Arc<Board>,
        every: Duration,
    }

    impl Runnable for Pulse {
        fn name() -> &'static str {
            "pulse"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            RunnableDescriptor::new().criticality(Criticality::Ancillary)
        }

        fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async move {
                let points = Arc::clone(self.board.points.get().expect("vitals booted first"));

                loop {
                    let report = aggregate(&points).await;
                    *self.board.overall.lock().expect("not poisoned") = Some(report.overall);
                    let taken = self.board.reports.fetch_add(1, Ordering::Relaxed) + 1;

                    if taken == 3 {
                        // A unit asks for a stop; it does not drive one.
                        cx.handle().shutdown();
                    }

                    if !cx
                        .shutdown()
                        .sleep_until_draining(self.every)
                        .await
                        .is_elapsed()
                    {
                        return Ok(());
                    }
                }
            })
        }
    }

    /// One bundle: a point, a probe, the component that keeps the table and
    /// the runnable that serves it.
    struct Serving(Arc<Board>);

    impl Bundle for Serving {
        fn manifest(&self) -> BundleManifest {
            BundleManifest::new("serving", "0.1.0")
        }

        fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
            registry.declare_extension_point::<Probe>();
            registry.contribute(Probe::new(Ready));

            let board = Arc::clone(&self.0);
            registry.component(Provider::from_value(Arc::new(Vitals(board))));

            let board = Arc::clone(&self.0);
            registry.runnable(Provider::from_value(Arc::new(Pulse {
                board,
                every: Duration::from_millis(50),
            })));
            Ok(())
        }
    }

    /// The whole of section 14 in one run: the kernel aggregates, the bundle
    /// serves, and the loop that serves it needs neither a timer nor a race
    /// written by hand.
    #[tokio::test(start_paused = true)]
    async fn bundle_serves_health() {
        let board = Arc::new(Board::default());
        let kernel = Kernel::builder()
            .capture_signals(false)
            .shutdown_policy(ShutdownPolicy::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
            ))
            .bundle(Serving(Arc::clone(&board)))
            .build()
            .await
            .expect("the graph closes");

        let outcome = kernel.run().await;

        assert!(matches!(outcome, Outcome::ShutdownRequested), "{outcome:?}");
        assert_eq!(board.reports.load(Ordering::Relaxed), 3);
        assert_eq!(
            *board.overall.lock().expect("not poisoned"),
            Some(Health::Up)
        );
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
