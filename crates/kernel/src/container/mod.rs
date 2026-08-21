//! The container: what holds the object graph once it has been validated.
//!
//! Resolution is dynamic, by type erasure, and every graph error is paid for
//! once in phase three rather than on the first unit of work. The container
//! itself therefore does very little: it looks a contract up in a table fixed
//! at construction, builds the value if the lifetime says to, and hands back an
//! `Arc`. It never hands out `&mut` — interior mutability is the provider's
//! problem, not the container's.
//!
//! # Lifetimes
//!
//! * [`Lifetime::Shared`] — built at most once, kept in the container's own
//!   table, shared for the life of the process.
//! * [`Lifetime::Scoped`] — built at most once per [`Scope`], kept in that
//!   scope's table. Resolved outside a scope there is no unit of work to attach
//!   the value to, so the resolution fails with
//!   [`ContainerError::NoScope`].
//! * [`Lifetime::Factory`] — built on every resolution; the caller owns the
//!   result.
//!
//! # Sealing
//!
//! [`seal`](Container::seal) closes the shared table against *first*
//! instantiations. Reading a value that was already built keeps working, and
//! scoped and factory bindings keep building — what is forbidden is discovering
//! at run time that something was never buildable. A resolution that would fail
//! on the first unit of work in production is a design defect, not an
//! operational incident, so the container refuses to be the place where it
//! surfaces.
//!
//! Because the table's slots are allocated at construction and each is written
//! at most once, a read after sealing contends with nothing at all.
//!
//! # The debug guard
//!
//! `Provider::requires` is declarative: nothing can look inside a build closure
//! and see what it will resolve. In `debug_assertions` builds the container
//! therefore hands each build a container that remembers which provider is
//! running, and panics if that provider resolves a contract it did not declare.
//! Release builds carry none of that. The guard is what turns a convention into
//! a checked fact.

pub(crate) mod erased;

use core::any::TypeId;
use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use kernel_core::{ConfigTree, ContainerError, ContractId, ContractRef, Lifetime, Telemetry};
use tokio::sync::OnceCell;

use crate::shutdown::KernelHandle;
use erased::{AnyArc, ErasedBuild, restore};

/// One binding, as the container keeps it.
///
/// Produced by the registry and validated by phase three; the container only
/// ever reads it. `bundle` and `order` carry the attribution and the
/// registration rank that make a phase-three diagnostic readable, and that fix
/// the order [`Container::get_all`] reports.
pub(crate) struct BindingEntry {
    /// The contract this binding answers, name included.
    pub id: ContractId,
    /// The same contract, in the form that renders in a diagnostic.
    pub contract: ContractRef,
    /// The bundle that registered it.
    pub bundle: &'static str,
    /// How often the value is built.
    pub lifetime: Lifetime,
    /// The contracts the build declares it will resolve.
    pub requires: Vec<ContractRef>,
    /// The build, with its result type erased.
    pub build: ErasedBuild,
    /// Whether this binding claims the default position for its contract type.
    pub is_default: bool,
    /// Zero-based registration rank across the whole registry.
    pub order: u32,
}

impl fmt::Debug for BindingEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindingEntry")
            .field("contract", &self.contract)
            .field("bundle", &self.bundle)
            .field("lifetime", &self.lifetime)
            .field("is_default", &self.is_default)
            .field("order", &self.order)
            .finish_non_exhaustive()
    }
}

/// The binding table plus the indices that make lookups constant-time.
struct Bindings {
    entries: Vec<BindingEntry>,
    by_id: HashMap<ContractId, usize>,
    by_type: HashMap<TypeId, Vec<usize>>,
    defaults: HashMap<TypeId, usize>,
}

impl Bindings {
    fn new(entries: Vec<BindingEntry>) -> Self {
        let mut by_id = HashMap::with_capacity(entries.len());
        let mut by_type: HashMap<TypeId, Vec<usize>> = HashMap::new();
        let mut defaults = HashMap::new();

        for (index, entry) in entries.iter().enumerate() {
            by_id.entry(entry.id).or_insert(index);
            by_type.entry(entry.id.type_id).or_default().push(index);
            if entry.id.name.is_none() || entry.is_default {
                defaults.entry(entry.id.type_id).or_insert(index);
            }
        }

        Self {
            entries,
            by_id,
            by_type,
            defaults,
        }
    }
}

/// Everything shared by every clone of a container, scopes included.
struct SharedState {
    bindings: Bindings,
    values: Box<[OnceCell<AnyArc>]>,
    sealed: AtomicBool,
    config: Arc<ConfigTree>,
    telemetry: Arc<dyn Telemetry>,
    handle: KernelHandle,
}

/// The table of one unit of work.
struct ScopeState {
    values: Box<[OnceCell<AnyArc>]>,
}

/// Which provider is currently building, and what it declared.
#[cfg(debug_assertions)]
struct Frame {
    contract: ContractRef,
    declared: Vec<ContractId>,
}

fn cells(count: usize) -> Box<[OnceCell<AnyArc>]> {
    (0..count).map(|_| OnceCell::new()).collect()
}

/// Resolves contracts to values.
///
/// Cheap to clone: every clone shares one table, one seal flag and one set of
/// values. Cloning a container does not open a unit of work — [`scope`] does
/// that.
///
/// [`scope`]: Container::scope
#[derive(Clone)]
pub struct Container {
    shared: Arc<SharedState>,
    scope: Option<Arc<ScopeState>>,
    #[cfg(debug_assertions)]
    frame: Option<Arc<Frame>>,
}

impl Container {
    /// Builds a container over a validated binding table.
    ///
    /// Phase three calls this once the graph has been checked; the table is
    /// fixed from here on. `bindings` must be in registration order — that
    /// order is what [`get_all`](Self::get_all) reports.
    pub(crate) fn new(
        bindings: Vec<BindingEntry>,
        config: Arc<ConfigTree>,
        telemetry: Arc<dyn Telemetry>,
        handle: KernelHandle,
    ) -> Self {
        let values = cells(bindings.len());
        Self {
            shared: Arc::new(SharedState {
                bindings: Bindings::new(bindings),
                values,
                sealed: AtomicBool::new(false),
                config,
                telemetry,
                handle,
            }),
            scope: None,
            #[cfg(debug_assertions)]
            frame: None,
        }
    }

    /// The implementation of `C` bound under no name.
    ///
    /// That is the unnamed binding, or the named binding that claimed the
    /// default position with `Binding::as_default`. A contract with neither is
    /// [`ContainerError::NotProvided`], which after phase three can only mean
    /// the caller asked for something it never declared.
    pub async fn get<C: ?Sized + Send + Sync + 'static>(&self) -> Result<Arc<C>, ContainerError> {
        let contract = ContractRef::of::<C>();
        self.guard(contract);
        let index = self
            .shared
            .bindings
            .defaults
            .get(&TypeId::of::<C>())
            .copied()
            .ok_or(ContainerError::NotProvided { contract })?;
        self.resolve(index).await
    }

    /// The implementation of `C` bound under `name`.
    ///
    /// The name is part of the contract's identity, so this never falls back to
    /// the default binding.
    pub async fn get_named<C: ?Sized + Send + Sync + 'static>(
        &self,
        name: &'static str,
    ) -> Result<Arc<C>, ContainerError> {
        let contract = ContractRef::named::<C>(name);
        self.guard(contract);
        let index = self
            .shared
            .bindings
            .by_id
            .get(&contract.id())
            .copied()
            .ok_or(ContainerError::NotProvided { contract })?;
        self.resolve(index).await
    }

    /// Every implementation of the contract, in registration order.
    ///
    /// Named and unnamed bindings alike, ordered by the rank they were
    /// registered under, so two runs of the same program produce the same
    /// sequence. A contract nobody bound yields an empty vector rather than an
    /// error: "no implementations" is an answer, not a failure.
    ///
    /// A provider that calls this declares the contract itself —
    /// `ContractRef::of::<C>()` — and that one declaration covers every
    /// implementation returned.
    pub async fn get_all<C: ?Sized + Send + Sync + 'static>(
        &self,
    ) -> Result<Vec<Arc<C>>, ContainerError> {
        self.guard(ContractRef::of::<C>());
        let Some(indices) = self.shared.bindings.by_type.get(&TypeId::of::<C>()) else {
            return Ok(Vec::new());
        };

        let mut values = Vec::with_capacity(indices.len());
        for index in indices.iter().copied() {
            values.push(self.resolve(index).await?);
        }
        Ok(values)
    }

    /// Opens a unit of work.
    ///
    /// Called on a container that is already a scope, this returns *that same*
    /// scope rather than a silent sibling: a unit of work opened inside a unit
    /// of work is the same unit of work. Scopes do not nest.
    #[must_use]
    pub fn scope(&self) -> Scope {
        if self.scope.is_some() {
            return Scope {
                container: self.clone(),
            };
        }

        Scope {
            container: Self {
                shared: Arc::clone(&self.shared),
                scope: Some(Arc::new(ScopeState {
                    values: cells(self.shared.bindings.entries.len()),
                })),
                #[cfg(debug_assertions)]
                frame: self.frame.clone(),
            },
        }
    }

    /// The configuration tree, frozen at the end of phase one.
    #[must_use]
    pub fn config(&self) -> &ConfigTree {
        &self.shared.config
    }

    /// The telemetry sink every unit reports through.
    #[must_use]
    pub fn telemetry(&self) -> &Arc<dyn Telemetry> {
        &self.shared.telemetry
    }

    /// A handle on the kernel that owns this container.
    #[must_use]
    pub fn handle(&self) -> KernelHandle {
        self.shared.handle.clone()
    }

    /// Forbid any further first instantiation of a `Shared` value.
    ///
    /// Idempotent. Values already built stay readable, and `Scoped` and
    /// `Factory` bindings keep building — sealing bans lazy resolution, not
    /// resolution.
    pub fn seal(&self) {
        self.shared.sealed.store(true, Ordering::Release);
    }

    /// Whether [`seal`](Self::seal) has been called.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.shared.sealed.load(Ordering::Acquire)
    }

    async fn resolve<C: ?Sized + Send + Sync + 'static>(
        &self,
        index: usize,
    ) -> Result<Arc<C>, ContainerError> {
        let entry = &self.shared.bindings.entries[index];
        match entry.lifetime {
            Lifetime::Shared => {
                let cell = &self.shared.values[index];
                if let Some(value) = cell.get() {
                    return restore(value, entry.contract);
                }
                if self.is_sealed() {
                    return Err(ContainerError::Sealed {
                        contract: entry.contract,
                    });
                }
                let value = cell
                    .get_or_try_init(|| async {
                        if self.is_sealed() {
                            return Err(ContainerError::Sealed {
                                contract: entry.contract,
                            });
                        }
                        self.build(entry).await
                    })
                    .await?;
                restore(value, entry.contract)
            }
            Lifetime::Scoped => match &self.scope {
                Some(scope) => {
                    let value = scope.values[index]
                        .get_or_try_init(|| self.build(entry))
                        .await?;
                    restore(value, entry.contract)
                }
                None => Err(ContainerError::NoScope {
                    contract: entry.contract,
                }),
            },
            Lifetime::Factory => {
                let value = self.build(entry).await?;
                restore(&value, entry.contract)
            }
        }
    }

    async fn build(&self, entry: &BindingEntry) -> Result<AnyArc, ContainerError> {
        let inner = self.child(entry);
        (entry.build)(&inner).await.map_err(ContainerError::Build)
    }

    /// The container a provider builds against: the same tables, plus the note
    /// of which provider is running.
    #[cfg(debug_assertions)]
    fn child(&self, entry: &BindingEntry) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            scope: self.scope.clone(),
            frame: Some(Arc::new(Frame {
                contract: entry.contract,
                declared: entry.requires.iter().map(ContractRef::id).collect(),
            })),
        }
    }

    #[cfg(not(debug_assertions))]
    fn child(&self, _entry: &BindingEntry) -> Self {
        self.clone()
    }

    /// Panics if a provider resolves a contract it never declared.
    #[cfg(debug_assertions)]
    fn guard(&self, wanted: ContractRef) {
        if let Some(frame) = &self.frame
            && !frame.declared.contains(&wanted.id())
        {
            panic!(
                "provider for `{}` resolved `{}`, which it did not declare in `requires`",
                frame.contract, wanted
            );
        }
    }

    #[cfg(not(debug_assertions))]
    fn guard(&self, _wanted: ContractRef) {}
}

impl fmt::Debug for Container {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Container")
            .field("bindings", &self.shared.bindings.entries.len())
            .field("scoped", &self.scope.is_some())
            .field("sealed", &self.is_sealed())
            .finish_non_exhaustive()
    }
}

/// One unit of work: a request, a message, a job.
///
/// A scope is a container with a table of its own for [`Lifetime::Scoped`]
/// bindings; everything else it delegates to the container it was opened from.
/// It derefs to [`Container`], so it resolves exactly like one.
///
/// Cloning a scope keeps the same table — clones are two views of one unit of
/// work, not two units.
#[derive(Clone)]
pub struct Scope {
    container: Container,
}

impl Scope {
    /// The container this scope resolves through.
    #[must_use]
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// The same unit of work, not a nested one.
    ///
    /// Scopes do not nest: a scope opened on a scope is the same unit of work,
    /// so this returns a clone of `self` rather than a sibling nobody asked
    /// for.
    #[must_use]
    pub fn scope(&self) -> Self {
        self.clone()
    }
}

impl Deref for Scope {
    type Target = Container;

    fn deref(&self) -> &Self::Target {
        &self.container
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scope")
            .field("container", &self.container)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::BuildFn;
    use core::sync::atomic::AtomicUsize;
    use kernel_core::{BuildError, NoopTelemetry};

    trait Surface: Send + Sync + 'static {
        fn mark(&self) -> u8;
    }

    trait Sink: Send + Sync + 'static {}

    struct Plain(u8);

    impl Surface for Plain {
        fn mark(&self) -> u8 {
            self.0
        }
    }

    impl Sink for Plain {}

    struct Fixture {
        entries: Vec<BindingEntry>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }

        fn bind<C: ?Sized + Send + Sync + 'static>(
            mut self,
            contract: ContractRef,
            lifetime: Lifetime,
            requires: Vec<ContractRef>,
            build: BuildFn<C>,
        ) -> Self {
            let order = u32::try_from(self.entries.len()).expect("order fits");
            self.entries.push(BindingEntry {
                id: contract.id(),
                contract,
                bundle: "probe",
                lifetime,
                requires,
                build: erased::erase_build(build),
                is_default: false,
                order,
            });
            self
        }

        fn default_last(mut self) -> Self {
            self.entries.last_mut().expect("a binding").is_default = true;
            self
        }

        fn build(self) -> Container {
            Container::new(
                self.entries,
                Arc::new(ConfigTree::empty()),
                Arc::new(NoopTelemetry),
                KernelHandle::detached(),
            )
        }
    }

    fn surface(mark: u8) -> BuildFn<dyn Surface> {
        Box::new(move |_container| {
            Box::pin(async move { Ok(Arc::new(Plain(mark)) as Arc<dyn Surface>) })
        })
    }

    fn counted(counter: Arc<AtomicUsize>, mark: u8) -> BuildFn<dyn Surface> {
        Box::new(move |_container| {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                Ok(Arc::new(Plain(mark)) as Arc<dyn Surface>)
            })
        })
    }

    #[tokio::test]
    async fn resolves_the_default() {
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Shared,
                vec![],
                surface(4),
            )
            .build();

        let value = container.get::<dyn Surface>().await.expect("resolved");

        assert_eq!(value.mark(), 4);
    }

    #[tokio::test]
    async fn named_claims_default() {
        let container = Fixture::new()
            .bind(
                ContractRef::named::<dyn Surface>("primary"),
                Lifetime::Shared,
                vec![],
                surface(9),
            )
            .default_last()
            .build();

        assert_eq!(
            container
                .get::<dyn Surface>()
                .await
                .expect("default")
                .mark(),
            9
        );
        assert_eq!(
            container
                .get_named::<dyn Surface>("primary")
                .await
                .expect("named")
                .mark(),
            9
        );
    }

    #[tokio::test]
    async fn unbound_contract_reports() {
        let container = Fixture::new().build();

        assert!(matches!(
            container.get::<dyn Surface>().await,
            Err(ContainerError::NotProvided { .. })
        ));
        assert!(matches!(
            container.get_named::<dyn Surface>("primary").await,
            Err(ContainerError::NotProvided { .. })
        ));
        assert!(
            container
                .get_all::<dyn Surface>()
                .await
                .expect("empty")
                .is_empty()
        );
    }

    // Load-bearing rule: sealing bans a first `Shared` build, and nothing else.
    #[tokio::test]
    async fn seal_blocks_first_build() {
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Shared,
                vec![],
                surface(1),
            )
            .bind(
                ContractRef::named::<dyn Surface>("late"),
                Lifetime::Shared,
                vec![],
                surface(2),
            )
            .bind(
                ContractRef::named::<dyn Surface>("scoped"),
                Lifetime::Scoped,
                vec![],
                surface(3),
            )
            .bind(
                ContractRef::named::<dyn Surface>("factory"),
                Lifetime::Factory,
                vec![],
                surface(4),
            )
            .build();

        let early = container
            .get::<dyn Surface>()
            .await
            .expect("built before sealing");
        container.seal();

        assert!(container.is_sealed());
        let again = container.get::<dyn Surface>().await.expect("already built");
        assert!(Arc::ptr_eq(&early, &again));

        assert!(matches!(
            container.get_named::<dyn Surface>("late").await,
            Err(ContainerError::Sealed { .. })
        ));

        let scope = container.scope();
        assert_eq!(
            scope
                .get_named::<dyn Surface>("scoped")
                .await
                .expect("scoped")
                .mark(),
            3
        );
        assert_eq!(
            container
                .get_named::<dyn Surface>("factory")
                .await
                .expect("factory")
                .mark(),
            4
        );
    }

    // Load-bearing rule: racing callers of one uninitialised `Shared` binding
    // build it exactly once and observe the same value.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn builds_once_under_race() {
        let counter = Arc::new(AtomicUsize::new(0));
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Shared,
                vec![],
                counted(Arc::clone(&counter), 5),
            )
            .build();

        let racers: Vec<_> = (0..16)
            .map(|_| {
                let container = container.clone();
                tokio::spawn(async move { container.get::<dyn Surface>().await })
            })
            .collect();

        let mut values = Vec::new();
        for racer in racers {
            values.push(racer.await.expect("task").expect("resolved"));
        }

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        for value in &values {
            assert!(Arc::ptr_eq(value, &values[0]));
        }
    }

    // Load-bearing rule: `get_all` is deterministic and follows registration.
    #[tokio::test]
    async fn get_all_keeps_order() {
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Shared,
                vec![],
                surface(1),
            )
            .bind(
                ContractRef::named::<dyn Surface>("second"),
                Lifetime::Shared,
                vec![],
                surface(2),
            )
            .bind(
                ContractRef::named::<dyn Surface>("third"),
                Lifetime::Factory,
                vec![],
                surface(3),
            )
            .bind(ContractRef::of::<dyn Sink>(), Lifetime::Shared, vec![], {
                let build: BuildFn<dyn Sink> = Box::new(|_container| {
                    Box::pin(async { Ok(Arc::new(Plain(0)) as Arc<dyn Sink>) })
                });
                build
            })
            .build();

        let marks: Vec<u8> = container
            .get_all::<dyn Surface>()
            .await
            .expect("all")
            .iter()
            .map(|value| value.mark())
            .collect();
        assert_eq!(marks, vec![1, 2, 3]);

        let again: Vec<u8> = container
            .get_all::<dyn Surface>()
            .await
            .expect("all")
            .iter()
            .map(|value| value.mark())
            .collect();
        assert_eq!(again, marks);
    }

    // Load-bearing rule: `requires` is checked, not trusted.
    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "did not declare in `requires`")]
    async fn guard_panics_on_undeclared() {
        let container = Fixture::new()
            .bind(ContractRef::of::<dyn Sink>(), Lifetime::Shared, vec![], {
                let build: BuildFn<dyn Sink> = Box::new(|container| {
                    Box::pin(async move {
                        let _ = container.get::<dyn Surface>().await;
                        Ok(Arc::new(Plain(0)) as Arc<dyn Sink>)
                    })
                });
                build
            })
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Shared,
                vec![],
                surface(1),
            )
            .build();

        let _ = container.get::<dyn Sink>().await;
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn declared_resolution_passes() {
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Sink>(),
                Lifetime::Shared,
                vec![ContractRef::of::<dyn Surface>()],
                {
                    let build: BuildFn<dyn Sink> = Box::new(|container| {
                        Box::pin(async move {
                            let surface = container
                                .get::<dyn Surface>()
                                .await
                                .map_err(|error| BuildError::new("sink", Box::new(error)))?;
                            Ok(Arc::new(Plain(surface.mark())) as Arc<dyn Sink>)
                        })
                    });
                    build
                },
            )
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Shared,
                vec![],
                surface(6),
            )
            .build();

        assert!(container.get::<dyn Sink>().await.is_ok());
    }

    #[tokio::test]
    async fn scope_caches_per_unit() {
        let counter = Arc::new(AtomicUsize::new(0));
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Scoped,
                vec![],
                counted(Arc::clone(&counter), 8),
            )
            .build();

        let first = container.scope();
        let a = first.get::<dyn Surface>().await.expect("scoped");
        let b = first.get::<dyn Surface>().await.expect("scoped");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let second = container.scope();
        let c = second.get::<dyn Surface>().await.expect("scoped");
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn nested_scope_same_unit() {
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Scoped,
                vec![],
                surface(2),
            )
            .build();

        let outer = container.scope();
        let inner = outer.scope();
        let deeper = outer.container().scope();

        let a = outer.get::<dyn Surface>().await.expect("scoped");
        let b = inner.get::<dyn Surface>().await.expect("scoped");
        let c = deeper.get::<dyn Surface>().await.expect("scoped");

        assert!(Arc::ptr_eq(&a, &b));
        assert!(Arc::ptr_eq(&a, &c));
    }

    // Load-bearing rule: a scoped binding has nowhere to live outside a scope,
    // so it is refused rather than quietly rebuilt per call.
    #[tokio::test]
    async fn scoped_needs_a_scope() {
        let counter = Arc::new(AtomicUsize::new(0));
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Scoped,
                vec![],
                counted(Arc::clone(&counter), 1),
            )
            .build();

        assert!(matches!(
            container.get::<dyn Surface>().await,
            Err(ContainerError::NoScope { .. })
        ));
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn factory_builds_each_time() {
        let counter = Arc::new(AtomicUsize::new(0));
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Factory,
                vec![],
                counted(Arc::clone(&counter), 1),
            )
            .build();

        let a = container.get::<dyn Surface>().await.expect("built");
        let b = container.get::<dyn Surface>().await.expect("built");

        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_build_retries() {
        let counter = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::clone(&counter);
        let build: BuildFn<dyn Surface> = Box::new(move |_container| {
            let attempts = Arc::clone(&attempts);
            Box::pin(async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(BuildError::new(
                        "surface",
                        "first attempt".to_owned().into(),
                    ));
                }
                Ok(Arc::new(Plain(1)) as Arc<dyn Surface>)
            })
        });
        let container = Fixture::new()
            .bind(
                ContractRef::of::<dyn Surface>(),
                Lifetime::Shared,
                vec![],
                build,
            )
            .build();

        assert!(matches!(
            container.get::<dyn Surface>().await,
            Err(ContainerError::Build(_))
        ));
        assert!(container.get::<dyn Surface>().await.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn container_is_shareable() {
        fn assert_bounds<T: Send + Sync + 'static>() {}
        assert_bounds::<Container>();
        assert_bounds::<Scope>();
    }
}
