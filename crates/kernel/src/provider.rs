//! How a contract gets a value, and how that binding is adjusted afterwards.
//!
//! A [`Provider`] pairs three things: the [`Lifetime`] that decides how often
//! the value is built, the list of contracts the build will resolve, and the
//! build itself. The list is *declarative* because the language offers no
//! introspection — nothing can look inside the closure and see what it will
//! ask the container for. The debug-build guard in the container is what keeps
//! the declaration honest: resolving something that was never declared panics
//! while a provider is building.
//!
//! Every binding verb hands back a [`Binding`], so the entry that was just
//! recorded can still be adjusted — currently only by claiming the default
//! position for its contract. [`listen`](crate::Registry::listen) binds
//! nothing, so it hands back a [`Listening`](crate::Listening) instead, on
//! which the adjustment is the contracts the listener will resolve.

use core::fmt;
use core::marker::PhantomData;
use std::sync::Arc;

use kernel_core::{BoxFuture, BuildError, ContractRef, Lifetime};

use crate::container::Container;

/// Builds the value of a contract, given the container it is being built in.
///
/// The closure is handed the container so that a provider can resolve what it
/// depends on. Everything it resolves must appear in [`Provider::requires`]:
/// debug builds panic on an undeclared resolution.
///
/// The returned future borrows the container for the duration of the build,
/// which is what lets a provider await a dependency without cloning anything.
pub type BuildFn<C> = Box<
    dyn for<'a> Fn(&'a Container) -> BoxFuture<'a, Result<Arc<C>, BuildError>>
        + Send
        + Sync
        + 'static,
>;

/// Everything the container needs in order to bind one contract.
///
/// `C` is usually a trait object type (`dyn Something`), which is why it is
/// `?Sized`. It is `Send + Sync + 'static` because everything the container
/// keeps is shared across tasks and outlives every borrow of it.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use kernel::{Lifetime, Provider};
/// use kernel::core::ContractRef;
///
/// trait Surface: Send + Sync + 'static {}
/// trait Sink: Send + Sync + 'static {}
///
/// struct Plain;
/// impl Surface for Plain {}
///
/// let provider = Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>)
///     .lifetime(Lifetime::Scoped)
///     .requires([ContractRef::of::<dyn Sink>()]);
///
/// assert_eq!(provider.lifetime, Lifetime::Scoped);
/// assert_eq!(provider.requires.len(), 1);
/// ```
pub struct Provider<C: ?Sized + Send + Sync + 'static> {
    /// How often the value is built and how long it is kept.
    pub lifetime: Lifetime,
    /// The contracts `build` will resolve, declared up front.
    ///
    /// Phase three checks that every entry is satisfied by some binding, and
    /// builds the dependency graph from them. Debug builds check that `build`
    /// resolves nothing beyond this list.
    pub requires: Vec<ContractRef>,
    /// The build itself.
    pub build: BuildFn<C>,
}

impl<C: ?Sized + Send + Sync + 'static> Provider<C> {
    /// A provider that runs `build` to produce the value.
    ///
    /// The result is [`Lifetime::Shared`] and declares no requirement; use
    /// [`lifetime`](Self::lifetime) and [`requires`](Self::requires) to change
    /// either.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use kernel::{Lifetime, Provider};
    ///
    /// trait Surface: Send + Sync + 'static {}
    ///
    /// struct Plain;
    /// impl Surface for Plain {}
    ///
    /// let provider: Provider<dyn Surface> = Provider::from_fn(|_container| {
    ///     Box::pin(async { Ok(Arc::new(Plain) as Arc<dyn Surface>) })
    /// });
    ///
    /// assert_eq!(provider.lifetime, Lifetime::Shared);
    /// assert!(provider.requires.is_empty());
    /// ```
    pub fn from_fn<F>(build: F) -> Self
    where
        F: for<'a> Fn(&'a Container) -> BoxFuture<'a, Result<Arc<C>, BuildError>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            lifetime: Lifetime::Shared,
            requires: Vec::new(),
            build: Box::new(build),
        }
    }

    /// A provider for a value that already exists.
    ///
    /// The result is [`Lifetime::Shared`] and declares no requirement. Every
    /// resolution hands back a clone of the same `Arc`, so the lifetime is not
    /// a claim the container has to enforce — it is simply true.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use kernel::Provider;
    ///
    /// trait Surface: Send + Sync + 'static {}
    ///
    /// struct Plain;
    /// impl Surface for Plain {}
    ///
    /// let provider = Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>);
    /// assert!(provider.requires.is_empty());
    /// ```
    #[must_use]
    pub fn from_value(value: Arc<C>) -> Self {
        Self::from_fn(move |_container| {
            let value = Arc::clone(&value);
            Box::pin(async move { Ok(value) })
        })
    }

    /// Sets how often the value is built.
    #[must_use]
    pub fn lifetime(mut self, lifetime: Lifetime) -> Self {
        self.lifetime = lifetime;
        self
    }

    /// Adds declared dependencies.
    ///
    /// The entries are *appended*, never replaced, so a provider assembled in
    /// several steps cannot silently lose a declaration it already made.
    ///
    /// A provider that calls [`Container::get_all`] declares the contract
    /// itself — `ContractRef::of::<C>()` — once, and that covers every
    /// implementation the collection will hand back.
    #[must_use]
    pub fn requires(mut self, requires: impl IntoIterator<Item = ContractRef>) -> Self {
        self.requires.extend(requires);
        self
    }
}

impl<C: ?Sized + Send + Sync + 'static> fmt::Debug for Provider<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Provider")
            .field("lifetime", &self.lifetime)
            .field("requires", &self.requires)
            .finish_non_exhaustive()
    }
}

/// Returned by every registration verb so the binding can be adjusted.
///
/// The borrow ties the handle to the registry that produced it, so a binding
/// can only be adjusted before the next registration happens — there is no way
/// to keep one around and reach back into a registry that has moved on.
pub struct Binding<'r, C: ?Sized + Send + Sync + 'static> {
    is_default: &'r mut bool,
    contract: PhantomData<fn() -> Arc<C>>,
}

impl<'r, C: ?Sized + Send + Sync + 'static> Binding<'r, C> {
    /// Wraps the recorded entry's default flag.
    ///
    /// Registration verbs call this; nothing else can build a `Binding`.
    pub(crate) fn new(is_default: &'r mut bool) -> Self {
        Self {
            is_default,
            contract: PhantomData,
        }
    }

    /// Make this the implementation `get` returns when no name is asked for.
    ///
    /// A named binding is otherwise reachable only through
    /// [`Container::get_named`]. Claiming the default position twice for one
    /// contract is a phase-three error, not a silent overwrite.
    #[must_use]
    pub fn as_default(self) -> Self {
        *self.is_default = true;
        self
    }
}

impl<C: ?Sized + Send + Sync + 'static> fmt::Debug for Binding<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Binding")
            .field("is_default", &*self.is_default)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait Surface: Send + Sync + 'static {
        fn mark(&self) -> u8;
    }

    struct Plain(u8);

    impl Surface for Plain {
        fn mark(&self) -> u8 {
            self.0
        }
    }

    trait Sink: Send + Sync + 'static {}

    #[test]
    fn defaults_to_shared() {
        let provider = Provider::from_value(Arc::new(Plain(1)) as Arc<dyn Surface>);

        assert_eq!(provider.lifetime, Lifetime::Shared);
        assert!(provider.requires.is_empty());
    }

    #[test]
    fn builders_apply() {
        let provider = Provider::from_value(Arc::new(Plain(1)) as Arc<dyn Surface>)
            .lifetime(Lifetime::Factory)
            .requires([ContractRef::of::<dyn Sink>()])
            .requires([ContractRef::named::<dyn Sink>("secondary")]);

        assert_eq!(provider.lifetime, Lifetime::Factory);
        assert_eq!(provider.requires.len(), 2);
        assert_eq!(provider.requires[1].name(), Some("secondary"));
    }

    #[tokio::test]
    async fn value_provider_shares() {
        let value = Arc::new(Plain(7)) as Arc<dyn Surface>;
        let provider = Provider::from_value(Arc::clone(&value));
        let container = Container::new(
            Vec::new(),
            Arc::new(kernel_core::ConfigTree::empty()),
            Arc::new(kernel_core::NoopTelemetry),
            crate::shutdown::KernelHandle::detached(),
        );

        let built = (provider.build)(&container).await.expect("build");

        assert_eq!(built.mark(), 7);
        assert!(Arc::ptr_eq(&built, &value));
    }

    #[test]
    fn as_default_sets_flag() {
        let mut flag = false;
        {
            let binding = Binding::<dyn Surface>::new(&mut flag).as_default();
            assert!(format!("{binding:?}").contains("true"));
        }
        assert!(flag);
    }

    #[test]
    fn debug_omits_the_closure() {
        let provider = Provider::from_value(Arc::new(Plain(1)) as Arc<dyn Surface>);
        let rendered = format!("{provider:?}");

        assert!(rendered.contains("Provider"));
        assert!(rendered.contains("Shared"));
    }
}
