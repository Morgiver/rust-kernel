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
use core::marker::PhantomData;
use std::sync::Arc;

use kernel_core::{
    BoxFuture, BuildError, ConfigError, ConfigTree, ContainerError, ContractId, ContractRef, Event,
    Extension, ExtensionId, FromConfig, Level, Lifetime, Priority, Record, Telemetry,
};

use crate::component::Component;
use crate::container::erased::erase_build;
use crate::container::{BindingEntry, Container};
use crate::dispatcher::{ErasedListener, Listener, erase_listener};
use crate::provider::{Binding, BuildFn, Provider};
use crate::runnable::Runnable;

/// The bundle an entry is attributed to before any bundle has entered.
///
/// Only reachable when a registry is driven directly rather than by the
/// kernel, which always calls [`Registry::enter_bundle`] first.
const UNATTRIBUTED: &str = "<unattributed>";

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
    /// The contracts this listener resolves while handling the event.
    ///
    /// Declared through [`Listening::requires`], and checked by phase three
    /// exactly as a provider's are. Nothing can read them off the listener
    /// itself, so an undeclared resolution is a resolution phase three never
    /// sees.
    pub requires: Vec<ContractRef>,
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
            .field("requires", &self.requires)
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
/// The list is the BUNDLE-facing surface, and the closure is about what a
/// bundle can do. A test assembling a graph reaches four replacement
/// affordances that no bundle can: they are `#[doc(hidden)]`, gated by the
/// `testing` feature `ci/check-testing-feature.sh` refuses on every non-dev
/// dependency edge, and reachable only from the hook that runs after every
/// bundle has registered. They add no kind of thing to the kernel — they
/// overwrite one of these seven — so they are not an eighth verb.
///
/// Nor is [`Binding::also`], which is reachable from a bundle and is not a
/// verb either. It records no new thing: it names a second contract answered by
/// the object one of these seven just recorded, and hands back the very `Arc`
/// that binding hands back. What a verb does is put an object into the kernel,
/// and `also` puts none in.
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
    /// A listener that resolves anything from the container while handling its
    /// event declares it on the returned [`Listening`]. That declaration is
    /// what phase three checks, and it is the only way a listener's needs can
    /// be checked at all: the listener is stored with its event type erased,
    /// and nothing can look inside it to see what it will ask for. A listener
    /// that resolves nothing declares nothing and ignores the handle.
    ///
    /// # Examples
    ///
    /// ```
    /// use kernel::core::{ContractRef, Priority};
    /// # use kernel::core::{BoxFuture, Event, Flow, ListenerError};
    /// # use kernel::dispatcher::{Listener, ListenerContext};
    /// # use kernel::Registry;
    /// # trait Sink: Send + Sync + 'static {}
    /// # struct Signal;
    /// # impl Event for Signal {
    /// #     const NAME: &'static str = "signal";
    /// # }
    /// # struct Watcher;
    /// # impl Listener<Signal> for Watcher {
    /// #     fn on_event<'a>(
    /// #         &'a self,
    /// #         _event: &'a mut Signal,
    /// #         _cx: &'a ListenerContext<'a>,
    /// #     ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
    /// #         Box::pin(async { Ok(Flow::Continue) })
    /// #     }
    /// # }
    /// # fn example(registry: &mut Registry) {
    /// registry
    ///     .listen(Watcher, Priority::NORMAL)
    ///     .requires([ContractRef::of::<dyn Sink>()]);
    /// # }
    /// ```
    pub fn listen<E: Event, L: Listener<E>>(
        &mut self,
        listener: L,
        priority: Priority,
    ) -> Listening<'_, E> {
        let order = self.next_rank();
        self.listeners.push(ListenerEntry {
            event: TypeId::of::<E>(),
            event_name: E::NAME,
            bundle: self.bundle,
            requires: Vec::new(),
            priority,
            order,
            call: erase_listener::<E, L>(listener),
        });
        let entry = self
            .listeners
            .last_mut()
            .expect("a listener was recorded immediately before this call");
        Listening::new(&mut entry.requires)
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
    // Replacement — not a verb, and not bundle-facing
    // ----------------------------------------------------------------------

    /// Replaces the binding recorded for `C`, or records one if there is none.
    ///
    /// Not public API, and not an eighth verb: the closed list above is the
    /// BUNDLE-facing surface, and none of these four is reachable from a
    /// bundle. They exist under the `testing` feature, which
    /// `ci/check-testing-feature.sh` refuses on every non-dev dependency edge,
    /// and they are reached from the hook that runs after every bundle has
    /// registered and before phase three — which is what keeps a replacement
    /// in the phase order and in front of the graph validation.
    ///
    /// A bundle cannot replace anything, and that is the rule this does not
    /// touch: two bundles claiming one contract is still an ambiguity phase
    /// three reports. Replacement is the assembler's word over the assembly,
    /// and only a test assembles this way.
    ///
    /// The replacement takes the place of what it replaced — the same
    /// registration rank, the same default position, the same name — so the
    /// boot order a graph had is the boot order it keeps. What changes is the
    /// bundle it is attributed to, the lifetime, the declared requirements and
    /// the build.
    ///
    /// Matching is on the contract id, name included: a binding recorded under
    /// a name is replaced by [`__replace_named`](Self::__replace_named) and
    /// never by this, even when it claimed the default position.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn __replace<C: ?Sized + Send + Sync + 'static>(&mut self, provider: Provider<C>) {
        self.replace_binding(ContractId::of::<C>(), ContractRef::of::<C>(), provider);
    }

    /// Replaces the binding recorded for `C` under `name`, or records one.
    ///
    /// See [`__replace`](Self::__replace) for what replacement means and why
    /// it is not a verb.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn __replace_named<C: ?Sized + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
        provider: Provider<C>,
    ) {
        self.replace_binding(
            ContractId::named::<C>(name),
            ContractRef::named::<C>(name),
            provider,
        );
    }

    /// Replaces the component recorded for `T`, or records one.
    ///
    /// Both halves of what [`component`](Self::component) records are covered:
    /// the binding the container resolves and the entry the kernel boots. A
    /// `T` that was bound but never registered as a component gains the entry
    /// here, so the double is driven whatever the graph did before it.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn __replace_component<T: Component>(&mut self, provider: Provider<T>) {
        let provider = self.force_shared(T::name(), provider);
        let contract = ContractId::of::<T>();
        self.replace_binding(contract, ContractRef::of::<T>(), provider);

        let bundle = self.bundle;
        if let Some(entry) = self
            .components
            .iter_mut()
            .find(|entry| entry.contract == contract)
        {
            entry.bundle = bundle;
            return;
        }
        let order = self.next_rank();
        self.components.push(UnitEntry {
            name: T::name(),
            bundle,
            contract,
            order,
            build: Arc::new(resolve_component::<T>),
        });
    }

    /// Replaces the runnable recorded for `T`, or records one.
    ///
    /// The counterpart of
    /// [`__replace_component`](Self::__replace_component) for the other unit
    /// the kernel drives.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn __replace_runnable<T: Runnable>(&mut self, provider: Provider<T>) {
        let provider = self.force_shared(T::name(), provider);
        let contract = ContractId::of::<T>();
        self.replace_binding(contract, ContractRef::of::<T>(), provider);

        let bundle = self.bundle;
        if let Some(entry) = self
            .runnables
            .iter_mut()
            .find(|entry| entry.contract == contract)
        {
            entry.bundle = bundle;
            return;
        }
        let order = self.next_rank();
        self.runnables.push(UnitEntry {
            name: T::name(),
            bundle,
            contract,
            order,
            build: Arc::new(resolve_runnable::<T>),
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
    /// An absent section is [`Missing`](kernel_core::error::ConfigErrorKind::Missing)
    /// naming `prefix` —
    /// except for a type that accepts absence, such as `Option<T>`, which
    /// reads as `None`. Errors from inside the section are re-rooted under
    /// `prefix`, so the path in the error is the path in the file.
    pub fn config<T: FromConfig>(&self, prefix: &str) -> Result<T, ConfigError> {
        self.config.root().field(prefix)
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
            requires_scoped: provider.requires_scoped,
            build: erase_build(provider.build),
            is_default: false,
            order,
        });
    }

    /// Overwrites the binding recorded under `id`, or records one.
    ///
    /// `is_default` and `order` survive: a replacement stands where what it
    /// replaced stood, so a graph that resolved in one order still does.
    #[cfg(feature = "testing")]
    fn replace_binding<C: ?Sized + Send + Sync + 'static>(
        &mut self,
        id: ContractId,
        contract: ContractRef,
        provider: Provider<C>,
    ) {
        let bundle = self.bundle;
        if let Some(entry) = self.bindings.iter_mut().find(|entry| entry.id == id) {
            entry.bundle = bundle;
            entry.lifetime = provider.lifetime;
            entry.requires = provider.requires;
            entry.requires_scoped = provider.requires_scoped;
            entry.build = erase_build(provider.build);
            return;
        }
        let order = self.next_rank();
        self.push_binding(id, contract, provider, order);
    }

    /// A handle on the binding just recorded.
    fn last_binding<C: ?Sized + Send + Sync + 'static>(&mut self) -> Binding<'_, C> {
        let index = self
            .bindings
            .len()
            .checked_sub(1)
            .expect("a binding was recorded immediately before this call");
        Binding::new(&mut self.bindings, index)
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

// --------------------------------------------------------------------------
// One instance, several contracts
// --------------------------------------------------------------------------

/// Aliasing: a second contract answered by the binding just recorded.
///
/// This lives on the handle a binding verb already returns rather than in the
/// list of verbs, and deliberately: the closed list of seven is what a bundle
/// may *record*, and an alias records no new thing. It names a contract the
/// object recorded a moment ago also answers.
impl<C: ?Sized + Send + Sync + 'static> Binding<'_, C> {
    /// Answers `A` with the very object this binding provides.
    ///
    /// One unit that implements two contracts — the concrete type the kernel
    /// boots and the abstraction its collaborators resolve — is registered
    /// once and reached under both names. `cast` is the widening the language
    /// cannot perform on its own: `|unit| unit as Arc<dyn Other>`.
    ///
    /// # What the alias is
    ///
    /// An entry that resolves the binding it aliases and widens the result. It
    /// therefore:
    ///
    /// * hands back the **same** `Arc` the aliased contract hands back, so
    ///   "one instance" is mechanical and not a convention two providers keep;
    /// * carries the aliased binding's [`Lifetime`], so a `Scoped` unit stays
    ///   one value per scope under both contracts;
    /// * declares the aliased contract as its requirement, so phase three
    ///   orders it after what it aliases and the debug guard is satisfied;
    /// * takes no registration rank of its own — it stands at the rank of the
    ///   binding it aliases, because it is not a second registration.
    ///
    /// A `Factory` binding is rebuilt on every resolution by definition, so
    /// there the alias hands back a fresh value like any other resolution of
    /// it.
    ///
    /// The alias is unnamed, which is what makes it the implementation
    /// [`Container::get`] returns for `A`. Two aliases claiming one contract,
    /// or an alias colliding with an ordinary binding, is
    /// [`DuplicateDefault`](kernel_core::ResolveError::DuplicateDefault) —
    /// reported by phase three with everything else, never a silent overwrite.
    ///
    /// Aliases chain, and so does [`as_default`](Binding::as_default): the
    /// handle keeps adjusting the binding it was returned for, not the last
    /// alias recorded.
    ///
    /// Not `#[must_use]`: the alias is recorded by the call itself, and the
    /// returned handle exists only so that a second call can chain onto it.
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
    /// registry
    ///     .provide(Provider::from_value(Arc::new(Plain)))
    ///     .also(|plain: Arc<Plain>| plain as Arc<dyn Surface>);
    /// # }
    /// ```
    pub fn also<A>(self, cast: impl Fn(Arc<C>) -> Arc<A> + Send + Sync + 'static) -> Self
    where
        A: ?Sized + Send + Sync + 'static,
    {
        let aliased = &self.entries[self.index];
        let source = aliased.contract;
        let bundle = aliased.bundle;
        let lifetime = aliased.lifetime;
        let order = aliased.order;
        let alias = ContractRef::of::<A>();

        let cast: Arc<dyn Fn(Arc<C>) -> Arc<A> + Send + Sync> = Arc::new(cast);
        let name = source.name();
        let build: BuildFn<A> = Box::new(move |container| {
            let cast = Arc::clone(&cast);
            Box::pin(async move {
                let value = match name {
                    Some(name) => container.get_named::<C>(name).await,
                    None => container.get::<C>().await,
                }
                .map_err(|error| BuildError::new(alias.type_name(), Box::new(error)))?;
                Ok(cast(value))
            })
        });

        self.entries.push(BindingEntry {
            id: ContractId::of::<A>(),
            contract: alias,
            bundle,
            lifetime,
            // The one resolution the alias performs, declared where every
            // other resolution is declared.
            requires: vec![source],
            // What the unit resolves inside a scope it opens is declared on
            // the binding that builds it; the alias builds nothing.
            requires_scoped: Vec::new(),
            build: erase_build(build),
            is_default: false,
            order,
        });
        self
    }
}

/// Returned by [`Registry::listen`] so the listener's needs can be declared.
///
/// A listener is the one registered thing whose dependencies nothing can
/// observe: a provider hands its build the container and declares what that
/// build resolves, while a listener resolves during dispatch, long after phase
/// three could have checked anything. This handle is where that declaration is
/// made, and the borrow ties it to the registry that produced it, so it cannot
/// outlive the entry it adjusts.
///
/// `E` is the event the listener was registered for; the handle carries it so a
/// declaration cannot be attached to the wrong registration.
pub struct Listening<'r, E: Event> {
    requires: &'r mut Vec<ContractRef>,
    event: PhantomData<fn() -> E>,
}

impl<'r, E: Event> Listening<'r, E> {
    /// Wraps the recorded entry's requirement list.
    ///
    /// [`Registry::listen`] calls this; nothing else can build a `Listening`.
    pub(crate) fn new(requires: &'r mut Vec<ContractRef>) -> Self {
        Self {
            requires,
            event: PhantomData,
        }
    }

    /// Declares the contracts this listener resolves while handling its event.
    ///
    /// Phase three checks each one exactly as it checks a provider's: a
    /// requirement nothing satisfies is a graph error reported with all the
    /// others, before anything boots. Without the declaration the resolution is
    /// invisible until the first event, at which point it fails on every event
    /// and reports to telemetry alone.
    ///
    /// The entries are *appended*, never replaced, so a listener declared in
    /// several steps cannot silently lose a declaration it already made.
    ///
    /// Not `#[must_use]`: the declaration is recorded by the call itself, and
    /// the returned handle exists only so that a second call can chain onto it.
    pub fn requires(self, requires: impl IntoIterator<Item = ContractRef>) -> Self {
        self.requires.extend(requires);
        self
    }
}

impl<E: Event> fmt::Debug for Listening<'_, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Listening")
            .field("event", &E::NAME)
            .field("requires", &self.requires)
            .finish()
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

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use kernel_core::error::ConfigErrorKind;
    use kernel_core::{
        ComponentDescriptor, ComponentError, ConfigNode, Flow, ListenerError, NoopTelemetry,
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
    // One instance, several contracts
    // ------------------------------------------------------------------

    // The gap this closes: without it, one object under two contracts costs a
    // second provider that resolves the first, and nothing makes the two the
    // same object. Here it is mechanical -- one build, one `Arc`.
    #[tokio::test]
    async fn alias_shares_instance() {
        let built = counter();
        let handle = Arc::clone(&built);
        let mut registry = registry();
        registry
            .provide(Provider::from_fn(move |_container| {
                let handle = Arc::clone(&handle);
                Box::pin(async move {
                    handle.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(Plain))
                })
            }))
            .also(|plain: Arc<Plain>| plain as Arc<dyn Surface>);

        let container = container(registry.into_parts());
        let concrete = container.get::<Plain>().await.expect("concrete");
        let surface = container.get::<dyn Surface>().await.expect("alias");

        assert_eq!(built.load(Ordering::SeqCst), 1);
        assert!(core::ptr::addr_eq(
            Arc::as_ptr(&concrete),
            Arc::as_ptr(&surface)
        ));
    }

    // An alias resolves the exact binding it aliases. A named one is reachable
    // only by its name, so an alias that fell back to `get` would hand back
    // the unnamed sibling instead.
    #[tokio::test]
    async fn alias_follows_the_name() {
        let mut registry = registry();
        registry.provide(Provider::from_value(Arc::new(Plain)));
        registry
            .provide_named("secondary", Provider::from_value(Arc::new(Plain)))
            .also(|plain: Arc<Plain>| plain as Arc<dyn Surface>);

        let container = container(registry.into_parts());
        let named = container
            .get_named::<Plain>("secondary")
            .await
            .expect("named");
        let surface = container.get::<dyn Surface>().await.expect("alias");

        assert!(core::ptr::addr_eq(
            Arc::as_ptr(&named),
            Arc::as_ptr(&surface)
        ));
    }

    // The alias is the same object, so it is kept for as long: a scoped unit
    // stays one value per scope under both contracts.
    #[tokio::test]
    async fn alias_keeps_lifetime() {
        let mut registry = registry();
        registry
            .provide(
                Provider::from_fn(|_container| Box::pin(async { Ok(Arc::new(Plain)) }))
                    .lifetime(Lifetime::Scoped),
            )
            .also(|plain: Arc<Plain>| plain as Arc<dyn Surface>);

        let parts = registry.into_parts();
        assert_eq!(parts.bindings[1].lifetime, Lifetime::Scoped);
        let scope = container(parts).scope();

        let concrete = scope.get::<Plain>().await.expect("concrete");
        let surface = scope.get::<dyn Surface>().await.expect("alias");

        assert!(core::ptr::addr_eq(
            Arc::as_ptr(&concrete),
            Arc::as_ptr(&surface)
        ));
    }

    // The alias states the one resolution it performs, so phase three orders
    // it after what it aliases instead of taking it for a free-standing
    // binding.
    #[test]
    fn alias_declares_its_source() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry
            .provide_named("secondary", Provider::from_value(Arc::new(Plain)))
            .also(|plain: Arc<Plain>| plain as Arc<dyn Surface>);
        registry.contribute(Item);

        let parts = registry.into_parts();
        let alias = &parts.bindings[1];
        assert_eq!(alias.contract, ContractRef::of::<dyn Surface>());
        assert_eq!(alias.requires, [ContractRef::named::<Plain>("secondary")]);
        assert!(alias.requires_scoped.is_empty());
        assert_eq!(alias.bundle, "one");
        // Not a second registration: it stands at the rank of what it aliases,
        // and the next verb takes the rank that follows.
        assert_eq!(alias.order, parts.bindings[0].order);
        assert_eq!(parts.contributions[0].order, 1);
    }

    // The handle keeps adjusting the binding it was returned for, so an alias
    // does not steal the default position from it.
    #[test]
    fn alias_leaves_handle_alone() {
        let mut registry = registry();
        let _ = registry
            .provide_named("secondary", Provider::from_value(Arc::new(Plain)))
            .also(|plain: Arc<Plain>| plain as Arc<dyn Surface>)
            .as_default();

        let parts = registry.into_parts();
        assert!(parts.bindings[0].is_default);
        assert!(!parts.bindings[1].is_default);
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
        // Declaring nothing is what a listener that resolves nothing does.
        assert!(parts.listeners[0].requires.is_empty());
    }

    // What a listener resolves is invisible to everything but this
    // declaration: phase three checks the list, and nothing else can produce
    // it.
    #[test]
    fn listener_declares_needs() {
        let mut registry = registry();

        let listening: Listening<'_, Signal> = registry.listen(Watcher, Priority(0));
        listening
            .requires([ContractRef::of::<dyn Surface>()])
            .requires([ContractRef::named::<dyn Surface>("secondary")]);

        let parts = registry.into_parts();
        let requires = &parts.listeners[0].requires;
        assert_eq!(requires.len(), 2);
        assert_eq!(requires[0], ContractRef::of::<dyn Surface>());
        assert_eq!(requires[1].name(), Some("secondary"));
    }

    #[test]
    fn listeners_keep_own_needs() {
        let mut registry = registry();

        registry
            .listen(Watcher, Priority(0))
            .requires([ContractRef::of::<dyn Surface>()]);
        registry.listen(Watcher, Priority(0));

        let parts = registry.into_parts();
        assert_eq!(parts.listeners[0].requires.len(), 1);
        assert!(parts.listeners[1].requires.is_empty());
    }

    #[test]
    fn listening_names_event() {
        let mut registry = registry();

        let listening = registry
            .listen(Watcher, Priority(0))
            .requires([ContractRef::of::<dyn Surface>()]);

        let rendered = format!("{listening:?}");
        assert!(rendered.contains("signal"), "{rendered}");
        assert!(rendered.contains("Surface"), "{rendered}");
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
