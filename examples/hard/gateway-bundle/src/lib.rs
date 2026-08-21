//! Registers the door and the accept loop. The only gateway crate an
//! application names.
//!
//! Wiring, and nothing else: everything registered below is defined in
//! `gateway-component`, everything it promises is defined in
//! `gateway-contracts`, and this file decides only who provides what, under
//! which configuration prefix.
//!
//! # What it depends on, and what it must never depend on
//!
//! `gateway-contracts` — the vocabulary — and `gateway-component` — this
//! feature's own implementation. Not `worker-bundle`, and not any other
//! `*-bundle` crate: a bundle reaches another feature through that feature's
//! contracts crate and the container, or it does not reach it at all.
//! `ci/check-bundle-graph.sh` walks the resolved dependency graph and fails the
//! build on any `*-bundle` → `*-bundle` edge, so the rule is a build failure
//! rather than a convention.
//!
//! Read that as a constraint on this file: the handler is named exactly once,
//! as `dyn Handler`, and no concrete type of whichever feature implements it
//! appears anywhere below. If one were wanted, the rule would be working.
//!
//! # What it registers, and why each is the kind of thing it is
//!
//! | registered as | what | why that kind |
//! |---|---|---|
//! | component | [`Doorway`] | a resource: bound once, released once, ordered by the boot graph |
//! | runnable | [`Acceptor`] | it must watch the ladder, and only a runnable can |
//! | scoped binding | [`Visit`] | one per request, resolved rather than threaded through calls |
//! | contribution | [`DoorwayProbe`] | health belongs to whatever owns the resource |
//!
//! The first two rows are the whole lesson of this example. A component is
//! handed its shutdown context only after every runnable has already stopped,
//! so it never observes `Draining` and cannot express "refuse new work, finish
//! held work". The accept loop has to express exactly that, so the accept loop
//! is a runnable — see the [`gateway_component`] module documentation for the
//! long form.
//!
//! # Configuration
//!
//! Read under the prefix [`NAME`], during registration, before anything
//! exists.
//!
//! ```text
//! gateway.address       string    address to bind; "127.0.0.1:0" by default
//! gateway.read_timeout  duration  "5s"; how long a silent connection is held
//! ```
//!
//! Both are optional: an absent key reads as `None` and the default written in
//! `gateway-component` applies, while a key that is *present* and holds the
//! wrong kind of value still fails the registration, with the path in the
//! message.
//!
//! # Why this bundle re-exports three types
//!
//! A bundle crate normally exports one thing, itself. This one also re-exports
//! [`Doorway`], [`Acceptor`] and [`Visit`], because an application that binds
//! port zero cannot know its own address until after boot, and the object that
//! publishes it is the component. Rather than making every application depend
//! on this feature's implementation crate to ask one question, the question is
//! reachable from the crate the application already names.

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::sync::Arc;

pub use gateway_component::{Acceptor, Doorway, Tally, Visit};

use gateway_component::{DoorwayProbe, Settings};
use gateway_contracts::Handler;
use kernel::core::{BuildError, BundleManifest, ConfigError, RegisterError};
use kernel::health::Probe;
use kernel::{Bundle, ContractRef, Lifetime, Provider, Registry};

/// The name this bundle publishes, the prefix it reads its configuration
/// under, and the name every registration diagnostic blames.
pub const NAME: &str = "gateway";

/// The manifest this bundle answers with.
///
/// One requirement, and it is the only thing this feature asks of the rest of
/// the process: somebody must answer requests. Which crate does is not stated
/// here and is not knowable from here.
static MANIFEST: BundleManifest =
    BundleManifest::new(NAME, "0.1.0").requires(&[ContractRef::of::<dyn Handler>()]);

/// Registers the gateway.
///
/// Constructed by the application and by nothing else:
///
/// ```no_run
/// # async fn assemble() {
/// use gateway_bundle::GatewayBundle;
/// use kernel::Kernel;
///
/// let builder = Kernel::builder().bundle(GatewayBundle::new());
/// # let _ = builder;
/// # }
/// ```
#[derive(Debug, Default)]
pub struct GatewayBundle;

impl GatewayBundle {
    /// A bundle with nothing to configure.
    ///
    /// Everything this feature can be told is told through the configuration
    /// tree, so the constructor takes no arguments.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Reads the two keys, each optional.
    fn settings(registry: &Registry) -> Result<Settings, ConfigError> {
        let fallback = Settings::default();
        Ok(Settings {
            address: registry
                .config::<Option<String>>("gateway.address")?
                .unwrap_or(fallback.address),
            read_timeout: registry
                .config::<Option<Duration>>("gateway.read_timeout")?
                .unwrap_or(fallback.read_timeout),
        })
    }
}

impl Bundle for GatewayBundle {
    fn manifest(&self) -> BundleManifest {
        MANIFEST
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        let settings =
            Self::settings(registry).map_err(|error| RegisterError::new(NAME, Box::new(error)))?;
        let read_timeout = settings.read_timeout;

        // One object, two roles. Built here rather than by a provider closure
        // because the probe below has to hold the same `Arc`: a second door
        // would report on a socket nobody is accepting on.
        let doorway = Arc::new(Doorway::new(settings));

        // As a component: the kernel binds it before anything runs and releases
        // it after everything has stopped. `registry.component` also binds it
        // as a contract, so `container.get::<Doorway>()` reaches the very
        // object the kernel booted — which is how an application learns the
        // address port zero actually granted.
        registry.component(Provider::from_value(Arc::clone(&doorway)));

        // As a health probe, contributed to the point the kernel declares.
        // Down the instant the acceptor closes the door, which is a whole
        // drain window before the process exits: that is when traffic should
        // stop being sent here.
        registry.contribute(Probe::new(DoorwayProbe::new(Arc::clone(&doorway))));

        // One visit per unit of work. The counter lives in the closure, so it
        // survives every build and numbers the visits in order.
        //
        // `Scoped` is what makes two concurrent requests two different visits
        // and one request's two resolutions the same one. Resolved outside a
        // scope it fails, and that failure is correct: there is no unit of work
        // to attach it to.
        let visits = Arc::new(AtomicU64::new(0));
        registry.provide(
            Provider::from_fn(move |_container| {
                let visits = Arc::clone(&visits);
                Box::pin(async move {
                    Ok(Arc::new(Visit::new(
                        visits.fetch_add(1, Ordering::Relaxed) + 1,
                    )))
                })
            })
            .lifetime(Lifetime::Scoped),
        );

        // As the runnable: the accept loop, which is the only unit in this
        // feature that can see the ladder move.
        //
        // Both resolutions are declared. Nothing can look inside the closure,
        // so phase three checks this list and the container checks it again in
        // debug builds — a provider that resolves something it did not declare
        // panics rather than working by accident.
        //
        // `Visit` is deliberately absent from the list, and it is not an
        // oversight: it is `Scoped`, and a requirer that is not itself `Scoped`
        // declaring a `Scoped` requirement is a phase-three `LifetimeConflict`.
        // The acceptor resolves it inside the scope it opens per request, where
        // there is a unit of work for it to belong to.
        registry.runnable(
            Provider::from_fn(move |container| {
                Box::pin(async move {
                    let doorway = container
                        .get::<Doorway>()
                        .await
                        .map_err(|error| BuildError::new("Acceptor", Box::new(error)))?;
                    let handler = container
                        .get::<dyn Handler>()
                        .await
                        .map_err(|error| BuildError::new("Acceptor", Box::new(error)))?;
                    Ok(Arc::new(Acceptor::new(doorway, handler, read_timeout)))
                })
            })
            .requires([
                ContractRef::of::<Doorway>(),
                ContractRef::of::<dyn Handler>(),
            ]),
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use core::future::Future;
    use core::task::{Context, Poll, Waker};

    use gateway_contracts::{HandlerError, Reply, Request};
    use kernel::core::{BoxFuture, KernelError, ResolveError};
    use kernel::{FnBundle, Kernel};

    use super::*;

    /// How many polls a build may take before the test gives up on it.
    ///
    /// Building a graph reads declarations and calls no provider, so nothing
    /// in it can park. The bound is what turns a regression that *does* park
    /// into a failing test instead of a wedged suite: this crate declares no
    /// runtime, so there is no timer to cut a hanging future short.
    const POLLS: usize = 1024;

    /// Runs a future that must not park.
    fn drive<T>(future: impl Future<Output = T>) -> T {
        let mut future = Box::pin(future);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..POLLS {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
        }
        panic!("the future parked, and this crate has no runtime to park on");
    }

    /// Somebody who answers, standing in for the whole worker feature.
    ///
    /// Written against `dyn Handler` and nothing else. It names no type of the
    /// crate that implements the contract for real — there is none to name
    /// from here, which is the isolation rule doing its job.
    struct Answering;

    impl Handler for Answering {
        fn handle(
            self: Arc<Self>,
            request: Request,
        ) -> BoxFuture<'static, Result<Reply, HandlerError>> {
            Box::pin(async move { Ok(Reply::new(request.line)) })
        }
    }

    /// The other half of the graph: whoever answers.
    fn answering() -> FnBundle {
        FnBundle::new("answering", |registry| {
            registry.provide(Provider::from_value(Arc::new(Answering) as Arc<dyn Handler>));
            Ok(())
        })
    }

    /// A kernel over this bundle plus `bundles`.
    fn built(with_handler: bool) -> Result<Kernel, KernelError> {
        let mut builder = Kernel::builder()
            .capture_signals(false)
            .bundle(GatewayBundle::new());
        if with_handler {
            builder = builder.bundle(answering());
        }
        drive(builder.build())
    }

    #[test]
    fn manifest_names_the_handler() {
        let manifest = GatewayBundle::new().manifest();

        assert_eq!(manifest.name, NAME);
        assert_eq!(manifest.requires, [ContractRef::of::<dyn Handler>()]);
        // No `after`: nothing here depends on the order the application listed
        // its bundles in. The contracts order the boot.
        assert!(manifest.after.is_empty());
    }

    #[test]
    fn graph_closes_with_a_handler() {
        let kernel = built(true).expect("the graph closes");

        // Bound, and reachable by the name the kernel booted it under: this is
        // the path an application uses to read back the address port zero
        // granted.
        let doorway = drive(kernel.container().get::<Doorway>()).expect("the door is bound");
        assert!(!doorway.is_open(), "nothing is bound before boot");
        assert_eq!(doorway.address(), None);
    }

    #[test]
    fn graph_refuses_without_a_handler() {
        let error = built(false).expect_err("nobody answers requests");

        let KernelError::Resolve(errors) = error else {
            panic!("a missing contract is a phase-three error: {error:?}");
        };
        assert!(
            errors
                .iter()
                .any(|error| matches!(error, ResolveError::MissingContract { .. })),
            "the missing handler is reported before anything is built: {errors:?}"
        );
    }

    /// Two units of work reach two visits; one unit of work reaches one.
    ///
    /// This is what a scope *is*, and it is asserted here rather than only
    /// through a socket because it holds with no socket, no runtime and no
    /// request: the lifetime is a property of the binding.
    #[test]
    fn visits_are_per_scope() {
        let kernel = built(true).expect("the graph closes");
        let container = kernel.container().clone();

        let first = container.scope();
        let second = container.scope();

        let one = drive(first.get::<Visit>()).expect("a scope has a visit");
        let again = drive(first.get::<Visit>()).expect("the same scope answers again");
        let other = drive(second.get::<Visit>()).expect("the other scope has its own");

        assert!(Arc::ptr_eq(&one, &again), "one unit of work, one visit");
        assert!(!Arc::ptr_eq(&one, &other), "two units of work, two visits");
        assert_ne!(one.id(), other.id(), "and they are numbered apart");

        // The count the acceptor puts on the wire: it reads two because the
        // second resolution found the same object, not a second one.
        assert_eq!(one.reach(), 1);
        assert_eq!(again.reach(), 2);
        assert_eq!(other.reach(), 1);
    }

    #[test]
    fn visit_needs_a_scope() {
        let kernel = built(true).expect("the graph closes");

        drive(kernel.container().get::<Visit>())
            .expect_err("a visit outside a unit of work belongs to nothing");
    }
}
