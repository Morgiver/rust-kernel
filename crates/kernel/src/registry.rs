//! The form the bundles fill in during phase two.
//!
//! Everything that ever enters the kernel enters through [`Registry`], and the
//! list of ways in is closed: seven registration verbs, two read-only
//! accessors. The closure is the point — see the type documentation.
//!
//! # The registry detects nothing
//!
//! It is a recorder, not a validator. Two bundles that both claim the default
//! implementation of one contract are both recorded, and phase three reports
//! them together with every other graph error in the same run. A registry that
//! failed on the second registration would surface exactly one collision per
//! run, so an application with four of them would need four runs to learn what
//! one run could have told it. Diagnostics that arrive one at a time are the
//! defect this split exists to avoid, and refusing to detect anything here is
//! what makes the phase-three aggregate possible.
//!
//! # Attribution
//!
//! Every entry carries the bundle that produced it and the rank it was
//! recorded under, neither of which the bundle supplies. The kernel enters a
//! bundle before calling [`Bundle::register`](crate::Bundle), and from there
//! on every entry stamps itself. A bundle cannot misattribute
//! its own declarations, and a phase-three diagnostic can name the bundle at
//! fault rather than the contract that happened to be reached first.

use core::any::TypeId;
use core::fmt;
use std::sync::Arc;

use kernel_core::error::ConfigErrorKind;
use kernel_core::{
    BoxFuture, BuildError, ConfigError, ConfigNode, ConfigTree, ContainerError, ContractId,
    ContractRef, Event, Extension, ExtensionId, FromConfig, Level, Lifetime, Priority, Record,
    Scalar, Telemetry,
};

use crate::component::Component;
use crate::container::erased::erase_build;
use crate::container::{BindingEntry, Container};
use crate::dispatcher::{ErasedListener, Listener, erase_listener};
use crate::provider::{Binding, Provider};
use crate::runnable::Runnable;

/// The bundle an entry is attributed to before any bundle has entered.
///
/// Only reachable when a registry is driven directly rather than by the
/// kernel, which always calls [`Registry::enter_bundle`] first.
const UNATTRIBUTED: &str = "<unattributed>";

/// An empty section, so that an absent prefix can still be offered to
/// [`FromConfig`] — which is how `Option<T>` reads as `None` rather than as a
/// failure.
static ABSENT: ConfigNode = ConfigNode::Scalar(Scalar::Null);

/// Resolves a lifecycle-managed unit through the binding it was recorded with.
///
/// `D` is the erased form of the unit — a trait object the kernel drives.
pub(crate) type UnitBuild<D> =
    Arc<dyn for<'a> Fn(&'a Container) -> BoxFuture<'a, Result<D, BuildError>> + Send + Sync>;

/// One lifecycle-managed unit, as the registry records it.
///
/// The unit is *not* stored: `build` resolves it from the container, through
/// the very binding [`Registry::component`] or [`Registry::runnable`]
/// registered alongside this entry. That indirection is what guarantees the
/// unit the kernel drives and the value a contract resolves to are the same
/// object.
pub(crate) struct UnitEntry<D> {
    /// The name the unit declares on its own trait, read at registration and
    /// carried unchanged into the id every diagnostic blames.
    pub name: &'static str,
    /// The bundle that registered it.
    pub bundle: &'static str,
    /// The binding this unit resolves through.
    pub contract: ContractId,
    /// Registration rank across the whole registry.
    pub order: u32,
    /// Resolves the unit and erases it to the trait object the kernel drives.
    pub build: UnitBuild<D>,
}

impl<D> fmt::Debug for UnitEntry<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnitEntry")
            .field("name", &self.name)
            .field("bundle", &self.bundle)
            .field("order", &self.order)
            .finish_non_exhaustive()
    }
}

/// One listener, as the registry records it.
///
/// `event` is the key the dispatcher's table is built on; `event_name` exists
/// only so that a diagnostic can name the event without a type map.
pub(crate) struct ListenerEntry {
    /// The event type this listener is registered for.
    pub event: TypeId,
    /// That event's declared name.
    pub event_name: &'static str,
    /// The bundle that registered it.
    pub bundle: &'static str,
    /// Higher runs first; ties break on `order`.
    pub priority: Priority,
    /// Registration rank across the whole registry.
    pub order: u32,
    /// The listener, with its event type erased.
    pub call: ErasedListener,
}

impl fmt::Debug for ListenerEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenerEntry")
            .field("event_name", &self.event_name)
            .field("bundle", &self.bundle)
            .field("priority", &self.priority)
            .field("order", &self.order)
            .finish_non_exhaustive()
    }
}

/// One contribution to an extension point, as the registry records it.
///
/// The item is erased here and restored by
/// [`ExtensionPoints`](crate::extension::ExtensionPoints); the registry never
/// looks inside it, and never checks that the point it names was declared —
/// that is a phase-three error, reported with all the others.
pub(crate) struct ContributionEntry {
    /// The extension point contributed to.
    pub extension: ExtensionId,
    /// The bundle that contributed.
    pub bundle: &'static str,
    /// Registration rank across the whole registry.
    pub order: u32,
    /// The contributed item.
    pub item: Box<dyn core::any::Any + Send + Sync>,
}

impl fmt::Debug for ContributionEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContributionEntry")
            .field("extension", &self.extension)
            .field("bundle", &self.bundle)
            .field("order", &self.order)
            .finish_non_exhaustive()
    }
}

/// Everything phase two recorded, handed to phase three in one piece.
pub(crate) struct RegistryParts {
    /// Contract bindings, in registration order.
    pub bindings: Vec<BindingEntry>,
    /// Components, in registration order.
    pub components: Vec<UnitEntry<Arc<dyn Component>>>,
    /// Runnables, in registration order.
    pub runnables: Vec<UnitEntry<Arc<dyn Runnable>>>,
    /// Listeners, in registration order.
    pub listeners: Vec<ListenerEntry>,
    /// Extension points somebody declared.
    pub declared_points: Vec<ExtensionId>,
    /// Contributions, in registration order.
    pub contributions: Vec<ContributionEntry>,
    /// The configuration tree, frozen at the end of phase one.
    pub config: Arc<ConfigTree>,
    /// The telemetry sink.
    pub telemetry: Arc<dyn Telemetry>,
}

impl fmt::Debug for RegistryParts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryParts")
            .field("bindings", &self.bindings.len())
            .field("components", &self.components.len())
            .field("runnables", &self.runnables.len())
            .field("listeners", &self.listeners.len())
            .field("declared_points", &self.declared_points.len())
            .field("contributions", &self.contributions.len())
            .finish_non_exhaustive()
    }
}

/// The single, closed door into the kernel.
///
/// # Seven verbs, and there is no eighth
///
/// A bundle may register exactly seven kinds of thing:
///
/// | verb | what it records |
/// |---|---|
/// | [`provide`](Self::provide) | a contract binding |
/// | [`provide_named`](Self::provide_named) | a contract binding under a name |
/// | [`component`](Self::component) | a unit the kernel boots and shuts down |
/// | [`runnable`](Self::runnable) | a unit the kernel runs and supervises |
/// | [`listen`](Self::listen) | a listener for one event type |
/// | [`declare_extension_point`](Self::declare_extension_point) | a point others may contribute to |
/// | [`contribute`](Self::contribute) | an item for such a point |
///
/// The list has no "etc.". It is not a starting point that grows as needs
/// appear: it is the definition of what the kernel is, expressed as the only
/// surface that can put anything into it. A request for an eighth verb is an
/// architecture decision — a claim that the kernel's responsibilities are not
/// what this list says they are — and it is taken as one, not merged as an
/// addition. Whatever the eighth verb would have recorded is a bundle's
/// business, reachable through a contract like everything else.
///
/// Two read-only accessors sit alongside them —
/// [`config`](Self::config) and [`telemetry`](Self::telemetry). They record
/// nothing, take `&self`, and give a bundle what it needs to build its
/// declarations; they are not part of the closed surface because they do not
/// open it.
///
/// # It records; it does not judge
///
/// No verb fails. Duplicates, contradictions and contributions to points
/// nobody declared are all recorded faithfully and reported together by phase
/// three. See the module documentation for why that is the whole point.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use kernel::{Provider, Registry};
///
/// trait Surface: Send + Sync + 'static {}
///
/// struct Plain;
/// impl Surface for Plain {}
///
/// # fn example(registry: &mut Registry) {
/// registry.provide(Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>));
/// registry
///     .provide_named("secondary", Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>))
///     .as_default();
/// # }
/// ```
pub struct Registry {
    bundle: &'static str,
    rank: u32,
    bindings: Vec<BindingEntry>,
    components: Vec<UnitEntry<Arc<dyn Component>>>,
    runnables: Vec<UnitEntry<Arc<dyn Runnable>>>,
    listeners: Vec<ListenerEntry>,
    declared_points: Vec<ExtensionId>,
    contributions: Vec<ContributionEntry>,
    config: Arc<ConfigTree>,
    telemetry: Arc<dyn Telemetry>,
}

impl Registry {
    // ----------------------------------------------------------------------
    // The seven verbs
    // ----------------------------------------------------------------------

    /// Binds `C` to a provider, under no name.
    ///
    /// This is the binding [`Container::get`] returns. Binding one contract
    /// twice this way is recorded, not refused: phase three reports it as an
    /// ambiguity naming both bundles.
    pub fn provide<C: ?Sized + Send + Sync + 'static>(
        &mut self,
        provider: Provider<C>,
    ) -> Binding<'_, C> {
        let order = self.next_rank();
        self.push_binding(
            ContractId::of::<C>(),
            ContractRef::of::<C>(),
            provider,
            order,
        );
        self.last_binding()
    }

    /// Binds `C` to a provider under `name`.
    ///
    /// The name is part of the contract's identity, so a named binding is
    /// reachable through [`Container::get_named`], which never falls back to
    /// the unnamed binding. It joins that binding in [`Container::get_all`],
    /// and it reaches [`Container::get`] only by claiming the default position
    /// with [`Binding::as_default`].
    pub fn provide_named<C: ?Sized + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
        provider: Provider<C>,
    ) -> Binding<'_, C> {
        let order = self.next_rank();
        self.push_binding(
            ContractId::named::<C>(name),
            ContractRef::named::<C>(name),
            provider,
            order,
        );
        self.last_binding()
    }

    /// Registers a unit whose lifecycle the kernel owns.
    ///
    /// This does *two* things. It binds `T` as a contract, so that
    /// `container.get::<T>()` resolves it like anything else, and it records a
    /// component entry whose build resolves *that same binding*. The kernel
    /// therefore boots the object the container hands out, not a second one
    /// built alongside it — going through the shared table is what makes "one
    /// instance" a mechanical consequence rather than a convention.
    ///
    /// The binding is forced to [`Lifetime::Shared`] for the same reason: a
    /// component the kernel boots once but the container rebuilds per scope
    /// would be two different objects with one name. A provider that asked for
    /// another lifetime has it overridden, and the override is recorded to
    /// telemetry rather than applied silently.
    ///
    /// A component that must keep running belongs in
    /// [`runnable`](Self::runnable): boot has a deadline.
    pub fn component<T: Component>(&mut self, provider: Provider<T>) -> Binding<'_, T> {
        let order = self.next_rank();
        let provider = self.force_shared(T::name(), provider);
        self.push_binding(
            ContractId::of::<T>(),
            ContractRef::of::<T>(),
            provider,
            order,
        );
        self.components.push(UnitEntry {
            name: T::name(),
            bundle: self.bundle,
            contract: ContractId::of::<T>(),
            order,
            build: Arc::new(resolve_component::<T>),
        });
        self.last_binding()
    }

    /// Registers a long-running unit the kernel starts and supervises.
    ///
    /// Identical in mechanism to [`component`](Self::component) — a `Shared`
    /// binding plus an entry that resolves it — so the supervised task and the
    /// resolvable contract are one object. What differs is what the kernel does
    /// with it: runnables start after every component has booted, are never
    /// ordered against each other, and are expected to run until the shutdown
    /// token fires.
    pub fn runnable<T: Runnable>(&mut self, provider: Provider<T>) -> Binding<'_, T> {
        let order = self.next_rank();
        let provider = self.force_shared(T::name(), provider);
        self.push_binding(
            ContractId::of::<T>(),
            ContractRef::of::<T>(),
            provider,
            order,
        );
        self.runnables.push(UnitEntry {
            name: T::name(),
            bundle: self.bundle,
            contract: ContractId::of::<T>(),
            order,
            build: Arc::new(resolve_runnable::<T>),
        });
        self.last_binding()
    }

    /// Registers a listener for one event type.
    ///
    /// Listeners run highest `priority` first, ties broken by registration
    /// order, so the walk is the same on every run. The table is built once in
    /// phase three and is immutable afterwards: this is the only moment at
    /// which a listener can be added, and there is no dynamic counterpart.
    ///
    /// The listener is stored with its event type erased, which is why it
    /// returns nothing — there is no binding to adjust.
    pub fn listen<E: Event, L: Listener<E>>(&mut self, listener: L, priority: Priority) {
        let order = self.next_rank();
        self.listeners.push(ListenerEntry {
            event: TypeId::of::<E>(),
            event_name: E::NAME,
            bundle: self.bundle,
            priority,
            order,
            call: erase_listener::<E, L>(listener),
        });
    }

    /// Opens an extension point of type `X`.
    ///
    /// Declaring says only that contributions of this type are expected and
    /// will be collected. A point nobody contributes to is valid and collects
    /// an empty vector; a *contribution* to a point nobody declared is a
    /// phase-three error, because it is almost always a typo that would
    /// otherwise vanish without trace.
    ///
    /// Declaring the same point twice is harmless and recorded as such.
    pub fn declare_extension_point<X: Extension>(&mut self) {
        self.declared_points.push(ExtensionId::of::<X>());
    }

    /// Contributes one item to the extension point of type `X`.
    ///
    /// Contributions are collected in bundle registration order, so two runs of
    /// the same program produce the same sequence. The item is owned by the
    /// kernel from here on and handed out by borrow.
    pub fn contribute<X: Extension>(&mut self, item: X) {
        let order = self.next_rank();
        self.contributions.push(ContributionEntry {
            extension: ExtensionId::of::<X>(),
            bundle: self.bundle,
            order,
            item: Box::new(item),
        });
    }

    // ----------------------------------------------------------------------
    // The two accessors
    // ----------------------------------------------------------------------

    /// Reads a typed section of the configuration.
    ///
    /// `prefix` is a dotted path; an empty prefix reads the whole tree. The
    /// tree is frozen before phase two starts, so this answers the same thing
    /// however often it is called.
    ///
    /// An absent section is [`ConfigErrorKind::Missing`] naming `prefix` —
    /// except for a type that accepts absence, such as `Option<T>`, which
    /// reads as `None`. Errors from inside the section are re-rooted under
    /// `prefix`, so the path in the error is the path in the file.
    pub fn config<T: FromConfig>(&self, prefix: &str) -> Result<T, ConfigError> {
        match self.config.get(prefix) {
            Some(node) => T::from_config(node).map_err(|error| under(prefix, error)),
            None => T::from_config(&ABSENT).map_err(|_| ConfigError::missing(prefix)),
        }
    }

    /// The telemetry sink every unit reports through.
    #[must_use]
    pub fn telemetry(&self) -> &Arc<dyn Telemetry> {
        &self.telemetry
    }

    // ----------------------------------------------------------------------
    // Kernel side — not part of the bundle-facing surface
    // ----------------------------------------------------------------------

    /// An empty registry over a frozen configuration tree.
    pub(crate) fn new(config: Arc<ConfigTree>, telemetry: Arc<dyn Telemetry>) -> Self {
        Self {
            bundle: UNATTRIBUTED,
            rank: 0,
            bindings: Vec::new(),
            components: Vec::new(),
            runnables: Vec::new(),
            listeners: Vec::new(),
            declared_points: Vec::new(),
            contributions: Vec::new(),
            config,
            telemetry,
        }
    }

    /// Attributes every following entry to `name`.
    ///
    /// Called by the kernel before each `Bundle::register`, so that attribution is something the kernel knows rather than something a
    /// bundle claims. A bundle cannot call this, cannot see it, and cannot get
    /// it wrong.
    pub(crate) fn enter_bundle(&mut self, name: &'static str) {
        self.bundle = name;
    }

    /// Hands everything recorded to phase three.
    pub(crate) fn into_parts(self) -> RegistryParts {
        RegistryParts {
            bindings: self.bindings,
            components: self.components,
            runnables: self.runnables,
            listeners: self.listeners,
            declared_points: self.declared_points,
            contributions: self.contributions,
            config: self.config,
            telemetry: self.telemetry,
        }
    }

    // ----------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------

    /// The next registration rank, shared by every kind of entry.
    ///
    /// One counter rather than one per kind, so that two entries of different
    /// kinds can still be ordered against each other — which is what phase
    /// three needs to break a tie between a component and the binding it
    /// resolves through.
    fn next_rank(&mut self) -> u32 {
        let rank = self.rank;
        self.rank = self.rank.saturating_add(1);
        rank
    }

    /// Records a binding, attributed and ranked.
    fn push_binding<C: ?Sized + Send + Sync + 'static>(
        &mut self,
        id: ContractId,
        contract: ContractRef,
        provider: Provider<C>,
        order: u32,
    ) {
        self.bindings.push(BindingEntry {
            id,
            contract,
            bundle: self.bundle,
            lifetime: provider.lifetime,
            requires: provider.requires,
            build: erase_build(provider.build),
            is_default: false,
            order,
        });
    }

    /// A handle on the binding just recorded.
    fn last_binding<C: ?Sized + Send + Sync + 'static>(&mut self) -> Binding<'_, C> {
        let entry = self
            .bindings
            .last_mut()
            .expect("a binding was recorded immediately before this call");
        Binding::new(&mut entry.is_default)
    }

    /// Forces a lifecycle-managed unit's binding to [`Lifetime::Shared`],
    /// reporting the override rather than performing it silently.
    ///
    /// `name` is the unit's declared name — `Component::name` or
    /// `Runnable::name` — because the record is about one unit, and a unit is
    /// named by what it declares and never by its Rust type path.
    fn force_shared<T: Send + Sync + 'static>(
        &self,
        name: &'static str,
        provider: Provider<T>,
    ) -> Provider<T> {
        if provider.lifetime != Lifetime::Shared {
            self.telemetry.record(
                Record::new(Level::Warn, "registry.lifetime_overridden")
                    .with("contract", name)
                    .with("requested", provider.lifetime.to_string()),
            );
        }
        provider.lifetime(Lifetime::Shared)
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("bundle", &self.bundle)
            .field("bindings", &self.bindings.len())
            .field("components", &self.components.len())
            .field("runnables", &self.runnables.len())
            .field("listeners", &self.listeners.len())
            .field("declared_points", &self.declared_points.len())
            .field("contributions", &self.contributions.len())
            .finish_non_exhaustive()
    }
}

/// Resolves a component through its own binding.
fn resolve_component<T: Component>(
    container: &Container,
) -> BoxFuture<'_, Result<Arc<dyn Component>, BuildError>> {
    Box::pin(async move {
        let unit = container
            .get::<T>()
            .await
            .map_err(|error| failed(T::name(), error))?;
        Ok(unit as Arc<dyn Component>)
    })
}

/// Resolves a runnable through its own binding.
fn resolve_runnable<T: Runnable>(
    container: &Container,
) -> BoxFuture<'_, Result<Arc<dyn Runnable>, BuildError>> {
    Box::pin(async move {
        let unit = container
            .get::<T>()
            .await
            .map_err(|error| failed(T::name(), error))?;
        Ok(unit as Arc<dyn Runnable>)
    })
}

/// Names the unit whose resolution failed, by its declared name.
///
/// `name` comes from `Component::name` or `Runnable::name`: both call sites
/// still know the concrete type, so the one declared identity is in reach and
/// the Rust type path is never the answer.
///
/// The [`ContainerError`] it wraps names the *contract* by its type, which is
/// a different question with a different answer — see the note on
/// [`ContainerError`].
fn failed(name: &'static str, error: ContainerError) -> BuildError {
    BuildError::new(name, Box::new(error))
}

/// Re-roots `error` under `prefix`, so a nested failure reports where it is.
///
/// A [`ConfigErrorKind::Source`] carries a foreign cause that cannot be
/// rebuilt, so it passes through untouched.
fn under(prefix: &str, error: ConfigError) -> ConfigError {
    if prefix.is_empty() {
        return error;
    }
    let path = if error.path().is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix}.{}", error.path())
    };
    let rebuilt = match error.kind() {
        ConfigErrorKind::Missing => Some(ConfigError::missing(path)),
        ConfigErrorKind::TypeMismatch { expected, found } => {
            Some(ConfigError::type_mismatch(path, expected, found))
        }
        ConfigErrorKind::Invalid(detail) => Some(ConfigError::invalid(path, detail.clone())),
        ConfigErrorKind::Source(_) => None,
    };
    rebuilt.unwrap_or(error)
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use kernel_core::{
        ComponentDescriptor, ComponentError, Flow, ListenerError, NoopTelemetry,
        RecordingTelemetry, RunError, RunnableDescriptor,
    };

    use super::*;
    use crate::component::{BootContext, ShutdownContext};
    use crate::dispatcher::ListenerContext;
    use crate::runnable::RunContext;
    use crate::shutdown::KernelHandle;

    // ------------------------------------------------------------------
    // Neutral placeholders
    // ------------------------------------------------------------------

    trait Surface: Send + Sync + 'static {}

    struct Plain;

    impl Surface for Plain {}

    struct Unit;

    impl Component for Unit {
        fn name() -> &'static str {
            "unit"
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

        fn shutdown<'a>(
            &'a self,
            _cx: &'a ShutdownContext<'a>,
        ) -> BoxFuture<'a, Result<(), ComponentError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct Worker;

    impl Runnable for Worker {
        fn name() -> &'static str {
            "worker"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            RunnableDescriptor::new()
        }

        fn run(self: Arc<Self>, _cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct Signal;

    impl Event for Signal {
        const NAME: &'static str = "signal";
    }

    struct Watcher;

    impl Listener<Signal> for Watcher {
        fn on_event<'a>(
            &'a self,
            _event: &'a mut Signal,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async { Ok(Flow::Continue) })
        }
    }

    struct Item;

    impl Extension for Item {}

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    fn registry() -> Registry {
        Registry::new(
            Arc::new(ConfigTree::empty()),
            Arc::new(NoopTelemetry) as Arc<dyn Telemetry>,
        )
    }

    fn surface() -> Provider<dyn Surface> {
        Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>)
    }

    fn container(parts: RegistryParts) -> Container {
        Container::new(
            parts.bindings,
            parts.config,
            parts.telemetry,
            KernelHandle::detached(),
        )
    }

    // ------------------------------------------------------------------
    // The registry records and does not judge
    // ------------------------------------------------------------------

    // The load-bearing rule: a duplicate is DATA, not an error. Phase three
    // reports every collision in one run; a registry that refused here would
    // report one per run.
    #[test]
    fn accepts_duplicate_default() {
        let mut registry = registry();

        registry.enter_bundle("first");
        registry.provide(surface());
        registry.enter_bundle("second");
        registry.provide(surface());

        let parts = registry.into_parts();
        assert_eq!(parts.bindings.len(), 2);
        assert_eq!(parts.bindings[0].bundle, "first");
        assert_eq!(parts.bindings[1].bundle, "second");
        assert_eq!(parts.bindings[0].id, parts.bindings[1].id);
    }

    #[test]
    fn accepts_duplicate_claims() {
        let mut registry = registry();

        let _ = registry.provide(surface()).as_default();
        let _ = registry.provide_named("secondary", surface()).as_default();

        let parts = registry.into_parts();
        assert!(parts.bindings.iter().all(|entry| entry.is_default));
    }

    #[test]
    fn accepts_undeclared_contribution() {
        let mut registry = registry();

        registry.contribute(Item);

        assert_eq!(registry.into_parts().contributions.len(), 1);
    }

    // ------------------------------------------------------------------
    // Attribution and order
    // ------------------------------------------------------------------

    #[test]
    fn stamps_entering_bundle() {
        let mut registry = registry();

        registry.enter_bundle("first");
        registry.provide(surface());
        registry.listen(Watcher, Priority(0));
        registry.enter_bundle("second");
        registry.contribute(Item);
        registry.component::<Unit>(Provider::from_value(Arc::new(Unit)));

        let parts = registry.into_parts();
        assert_eq!(parts.bindings[0].bundle, "first");
        assert_eq!(parts.listeners[0].bundle, "first");
        assert_eq!(parts.contributions[0].bundle, "second");
        assert_eq!(parts.components[0].bundle, "second");
    }

    #[test]
    fn defaults_to_unattributed() {
        let mut registry = registry();

        registry.provide(surface());

        assert_eq!(registry.into_parts().bindings[0].bundle, UNATTRIBUTED);
    }

    #[test]
    fn ranks_across_kinds() {
        let mut registry = registry();

        registry.provide(surface());
        registry.contribute(Item);
        registry.provide_named("secondary", surface());
        registry.listen(Watcher, Priority(0));

        let parts = registry.into_parts();
        assert_eq!(parts.bindings[0].order, 0);
        assert_eq!(parts.contributions[0].order, 1);
        assert_eq!(parts.bindings[1].order, 2);
        assert_eq!(parts.listeners[0].order, 3);
    }

    #[test]
    fn keeps_registration_order() {
        let mut registry = registry();

        for _ in 0..4 {
            registry.provide(surface());
        }

        let marks: Vec<u32> = registry
            .into_parts()
            .bindings
            .iter()
            .map(|entry| entry.order)
            .collect();
        assert_eq!(marks, [0, 1, 2, 3]);
    }

    // ------------------------------------------------------------------
    // Naming and defaults
    // ------------------------------------------------------------------

    #[test]
    fn named_binding_keeps_name() {
        let mut registry = registry();

        registry.provide_named("secondary", surface());

        let parts = registry.into_parts();
        assert_eq!(parts.bindings[0].id.name, Some("secondary"));
        assert_eq!(parts.bindings[0].contract.name(), Some("secondary"));
    }

    #[test]
    fn binding_is_not_default() {
        let mut registry = registry();

        registry.provide(surface());

        assert!(!registry.into_parts().bindings[0].is_default);
    }

    #[test]
    fn as_default_marks_entry() {
        let mut registry = registry();

        let _ = registry.provide_named("secondary", surface()).as_default();

        assert!(registry.into_parts().bindings[0].is_default);
    }

    // ------------------------------------------------------------------
    // Units resolve through their own binding
    // ------------------------------------------------------------------

    fn counter() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    // The whole reason the component entry holds a build rather than a value:
    // resolving it must not produce a second object.
    #[tokio::test]
    async fn component_builds_once() {
        let built = counter();
        let handle = Arc::clone(&built);
        let mut registry = registry();
        registry.component::<Unit>(Provider::from_fn(move |_container| {
            let handle = Arc::clone(&handle);
            Box::pin(async move {
                handle.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(Unit))
            })
        }));

        let parts = registry.into_parts();
        let build = Arc::clone(&parts.components[0].build);
        // The entry carries the declared name, not the Rust type path.
        assert_eq!(parts.components[0].name, Unit::name());
        let container = container(parts);

        let direct = container.get::<Unit>().await.expect("get");
        let erased = build(&container).await.expect("build");

        assert_eq!(built.load(Ordering::SeqCst), 1);
        assert_eq!(erased.descriptor(), direct.descriptor());
    }

    #[tokio::test]
    async fn runnable_builds_once() {
        let built = counter();
        let handle = Arc::clone(&built);
        let mut registry = registry();
        registry.runnable::<Worker>(Provider::from_fn(move |_container| {
            let handle = Arc::clone(&handle);
            Box::pin(async move {
                handle.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(Worker))
            })
        }));

        let parts = registry.into_parts();
        let build = Arc::clone(&parts.runnables[0].build);
        // The entry carries the declared name, not the Rust type path.
        assert_eq!(parts.runnables[0].name, Worker::name());
        let container = container(parts);

        let direct = container.get::<Worker>().await.expect("get");
        let erased = build(&container).await.expect("build");

        assert_eq!(built.load(Ordering::SeqCst), 1);
        assert_eq!(erased.descriptor(), direct.descriptor());
    }

    #[test]
    fn unit_points_at_binding() {
        let mut registry = registry();

        registry.component::<Unit>(Provider::from_value(Arc::new(Unit)));

        let parts = registry.into_parts();
        assert_eq!(parts.components[0].contract, parts.bindings[0].id);
        assert_eq!(parts.components[0].order, parts.bindings[0].order);
    }

    #[test]
    fn unit_binding_is_shared() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let mut registry = Registry::new(
            Arc::new(ConfigTree::empty()),
            Arc::clone(&telemetry) as Arc<dyn Telemetry>,
        );

        registry
            .component::<Unit>(Provider::from_value(Arc::new(Unit)).lifetime(Lifetime::Factory));

        assert_eq!(registry.into_parts().bindings[0].lifetime, Lifetime::Shared);
        assert!(telemetry.contains("registry.lifetime_overridden"));
    }

    #[tokio::test]
    async fn unit_failure_names_unit() {
        let mut registry = registry();
        registry.component::<Unit>(Provider::from_fn(|_container| {
            Box::pin(async { Err(BuildError::new("unit", "deliberate".to_owned().into())) })
        }));

        let parts = registry.into_parts();
        let build = Arc::clone(&parts.components[0].build);
        let container = container(parts);

        let Err(error) = build(&container).await else {
            panic!("resolution must fail");
        };
        // The declared name, not the Rust type path.
        assert_eq!(error.contract(), Unit::name());
        assert!(!error.contract().contains("Unit"), "{error}");
    }

    // ------------------------------------------------------------------
    // Events and extension points
    // ------------------------------------------------------------------

    #[test]
    fn listener_records_event() {
        let mut registry = registry();

        registry.listen(Watcher, Priority(7));

        let parts = registry.into_parts();
        assert_eq!(parts.listeners[0].event, TypeId::of::<Signal>());
        assert_eq!(parts.listeners[0].event_name, "signal");
        assert_eq!(parts.listeners[0].priority, Priority(7));
    }

    #[test]
    fn declares_extension_point() {
        let mut registry = registry();

        registry.declare_extension_point::<Item>();
        registry.declare_extension_point::<Item>();

        let parts = registry.into_parts();
        assert_eq!(parts.declared_points, [ExtensionId::of::<Item>(); 2]);
        assert!(parts.contributions.is_empty());
    }

    #[test]
    fn contributions_keep_order() {
        let mut registry = registry();

        registry.declare_extension_point::<Item>();
        registry.enter_bundle("first");
        registry.contribute(Item);
        registry.enter_bundle("second");
        registry.contribute(Item);

        let parts = registry.into_parts();
        assert_eq!(parts.contributions.len(), 2);
        assert!(parts.contributions[0].order < parts.contributions[1].order);
        assert_eq!(parts.contributions[0].extension, ExtensionId::of::<Item>());
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    fn configured() -> Registry {
        let mut tree = ConfigTree::empty();
        tree.insert("section.count", ConfigNode::from(3_i64))
            .expect("insert");
        Registry::new(
            Arc::new(tree),
            Arc::new(NoopTelemetry) as Arc<dyn Telemetry>,
        )
    }

    #[test]
    fn config_reads_section() {
        assert_eq!(
            configured().config::<i64>("section.count").expect("read"),
            3
        );
    }

    #[test]
    fn config_absent_is_missing() {
        let error = configured().config::<i64>("absent").expect_err("must fail");

        assert_eq!(error.path(), "absent");
        assert!(matches!(error.kind(), ConfigErrorKind::Missing));
    }

    #[test]
    fn absent_config_is_none() {
        assert_eq!(
            configured().config::<Option<i64>>("absent").expect("read"),
            None
        );
    }

    // A leaf error arrives path-less; the accessor is what knows where the
    // section came from, so it is what fills the path in.
    #[test]
    fn config_error_carries_prefix() {
        let error = configured()
            .config::<i64>("section")
            .expect_err("must fail");

        assert_eq!(error.path(), "section");
        assert!(matches!(
            error.kind(),
            ConfigErrorKind::TypeMismatch {
                expected: "int",
                ..
            }
        ));
    }

    #[test]
    fn telemetry_is_readable() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let registry = Registry::new(
            Arc::new(ConfigTree::empty()),
            Arc::clone(&telemetry) as Arc<dyn Telemetry>,
        );

        registry
            .telemetry()
            .record(Record::new(Level::Info, "probe"));

        assert!(telemetry.contains("probe"));
    }

    #[test]
    fn parts_carry_the_accessors() {
        let parts = configured().into_parts();

        assert!(parts.config.get("section.count").is_some());
        parts.telemetry.record(Record::new(Level::Info, "probe"));
    }
}
