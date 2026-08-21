//! Phase three: the graph is checked once, completely, before anything exists.
//!
//! # The rule this module exists to enforce
//!
//! > Every graph error — a missing contract, a cycle, an ambiguity, an invalid
//! > declaration — appears here at the latest. No lazy resolution is permitted
//! > once the kernel is running.
//!
//! That is the price of dynamic resolution and its counterpart. A resolution
//! that would fail on the first unit of work in production is a design defect,
//! not an operational incident, so the one moment at which the graph can still
//! be rejected for free is this one — after every bundle has spoken and before
//! any value has been built.
//!
//! # Nothing is instantiated
//!
//! [`resolve`] never calls a provider. It reads declarations: the contracts
//! each binding says it will resolve, the names each binding claims, the points
//! each contribution targets, the manifests the bundles published. A provider
//! whose build would panic resolves exactly as cleanly as one that would not,
//! which is what makes "we validate the graph" a different statement from "we
//! validate the graph by running it".
//!
//! # Aggregation, not first failure
//!
//! Every check collects. A start-up that reveals one error at a time, six times
//! in a row, is a tooling defect, so all nine checks run over the whole
//! registry and the violations come back together in one
//! [`Vec<ResolveError>`](ResolveError). The order is deterministic: checks run
//! in a fixed sequence and each one walks its input in registration order.
//!
//! # What an edge is
//!
//! The dependency graph is induced by the declared requirements of the
//! bindings. A requirement that names a binding edges to that binding; a
//! requirement that names none edges to whatever holds the default position for
//! the contract type — the same resolution [`Container::get`] performs. A
//! provider that collects *every* implementation of a contract is therefore
//! ordered against the default one and not against its named siblings: the two
//! resolutions share a single declaration, and over-approximating the edge set
//! would reject graphs that cannot deadlock while giving the author no way to
//! express the distinction.

use core::any::TypeId;
use core::fmt;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kernel_core::{BundleManifest, ComponentId, ContractId, ContractRef, ResolveError, RunnableId};

use crate::component::Component;
use crate::container::{BindingEntry, Container};
use crate::dispatcher::EventDispatcher;
use crate::extension::ExtensionPoints;
use crate::registry::{Registry, RegistryParts, UnitEntry};
use crate::runnable::Runnable;
use crate::shutdown::KernelHandle;

// --------------------------------------------------------------------------
// What phase three produces
// --------------------------------------------------------------------------

/// The order the kernel drives the registered units in.
///
/// Fixed here and never recomputed. Two runs of the same assembly produce the
/// same plan, because every ordering decision is either topological or broken
/// on registration order — never on a hash map's iteration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootPlan {
    /// Components in the order they must boot.
    ///
    /// Topological over the binding graph: a component boots after every
    /// component its declared requirements reach, transitively. Components
    /// that do not reach each other keep registration order.
    pub components: Vec<ComponentId>,
    /// Runnables, in registration order.
    ///
    /// Runnables are never ordered against each other — they all start once
    /// every component has booted, so there is nothing to sort.
    pub runnables: Vec<RunnableId>,
}

/// A graph that has been checked, with everything the later phases drive it
/// with.
///
/// The container is built but sealed by nobody yet, and no provider has run:
/// this is the graph proved buildable, not the graph built.
#[derive(Debug)]
pub struct Resolved {
    /// The container, over the validated binding table.
    pub container: Container,
    /// The dispatcher, already attached to the container.
    pub dispatcher: Arc<EventDispatcher>,
    /// The collected extension points.
    pub extensions: Arc<ExtensionPoints>,
    /// The order the units are driven in.
    pub plan: BootPlan,
    /// Components in *registration* order, so that a [`ComponentId`] taken
    /// from the plan indexes straight into this list.
    pub(crate) components: Vec<UnitEntry<Arc<dyn Component>>>,
    /// Runnables in registration order, indexed by [`RunnableId::index`] the
    /// same way.
    pub(crate) runnables: Vec<UnitEntry<Arc<dyn Runnable>>>,
}

// --------------------------------------------------------------------------
// The function
// --------------------------------------------------------------------------

/// Checks the whole graph, then assembles what the later phases need.
///
/// `manifests` is what the bundles published, in the order they registered.
/// The registry is consumed: after this call the declarations are either a
/// resolved graph or a list of everything wrong with it.
///
/// # Errors
///
/// Returns **every** violation found, never the first alone:
///
/// 1. [`MissingContract`] — a requirement, declared by a provider or by a
///    manifest, that no binding satisfies.
/// 2. [`DuplicateDefault`] — two bindings claiming the default position of one
///    contract, whether by being unnamed or by asking for it.
/// 3. [`DuplicateNamed`] — two bindings claiming one name for one contract.
/// 4. [`Cycle`] — a cycle in the binding graph, reported as the full closed
///    walk.
/// 5. [`UndeclaredExtensionPoint`] — a contribution to a point nobody declared.
/// 6. [`ManifestMismatch`] — a manifest requirement that none of the bundle's
///    own providers declares, which is what makes a lying manifest detectable
///    rather than decorative.
/// 7. [`UnknownBundleOrder`] — an ordering constraint naming a bundle that is
///    not registered.
/// 8. [`BundleCycle`] — a cycle among the bundle ordering constraints, which
///    no registration order can satisfy.
/// 9. [`DuplicateBundle`] — one name registered by two bundles, which makes
///    every ordering constraint and every attribution that names it ambiguous.
///
/// [`BundleCycle`]: ResolveError::BundleCycle
/// [`DuplicateBundle`]: ResolveError::DuplicateBundle
/// [`MissingContract`]: ResolveError::MissingContract
/// [`DuplicateDefault`]: ResolveError::DuplicateDefault
/// [`DuplicateNamed`]: ResolveError::DuplicateNamed
/// [`Cycle`]: ResolveError::Cycle
/// [`UndeclaredExtensionPoint`]: ResolveError::UndeclaredExtensionPoint
/// [`ManifestMismatch`]: ResolveError::ManifestMismatch
/// [`UnknownBundleOrder`]: ResolveError::UnknownBundleOrder
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use kernel::core::ContractRef;
/// use kernel::{Provider, Registry};
///
/// trait Surface: Send + Sync + 'static {}
/// trait Sink: Send + Sync + 'static {}
///
/// struct Plain;
/// impl Surface for Plain {}
///
/// // A binding that declares one requirement. Resolution refuses the graph
/// // unless something binds `dyn Sink` as well.
/// # fn example(registry: &mut Registry) {
/// registry.provide(
///     Provider::from_value(Arc::new(Plain) as Arc<dyn Surface>)
///         .requires([ContractRef::of::<dyn Sink>()]),
/// );
/// # }
/// ```
pub fn resolve(
    registry: Registry,
    manifests: &[BundleManifest],
) -> Result<Resolved, Vec<ResolveError>> {
    let parts = registry.into_parts();
    let index = BindingIndex::new(&parts.bindings);
    let edges = binding_edges(&parts.bindings, &index);

    let mut errors = Vec::new();
    missing_contracts(&parts.bindings, manifests, &index, &mut errors);
    duplicate_defaults(&parts.bindings, &mut errors);
    duplicate_names(&parts.bindings, &mut errors);
    binding_cycles(&parts.bindings, &edges, &mut errors);
    undeclared_points(&parts, &mut errors);
    manifest_mismatches(&parts.bindings, manifests, &mut errors);
    unknown_bundle_order(manifests, &mut errors);
    bundle_cycles(manifests, &mut errors);
    duplicate_bundles(manifests, &mut errors);

    if !errors.is_empty() {
        return Err(errors);
    }

    let plan = boot_plan(&parts, &index, &edges);

    let RegistryParts {
        bindings,
        components,
        runnables,
        listeners,
        declared_points,
        contributions,
        config,
        telemetry,
    } = parts;

    let dispatcher = Arc::new(EventDispatcher::new(listeners, Arc::clone(&telemetry)));
    let container = Container::new(bindings, config, telemetry, KernelHandle::new());
    dispatcher.attach(container.clone());

    Ok(Resolved {
        container,
        dispatcher,
        extensions: Arc::new(ExtensionPoints::from_parts(declared_points, contributions)),
        plan,
        components,
        runnables,
    })
}

// --------------------------------------------------------------------------
// Looking a requirement up
// --------------------------------------------------------------------------

/// Where a requirement lands, resolved exactly as the container resolves it.
///
/// First registration wins in both tables, which is what makes the index agree
/// with the container's own for a graph that also passes the duplicate checks.
struct BindingIndex {
    by_id: HashMap<ContractId, usize>,
    defaults: HashMap<TypeId, usize>,
}

impl BindingIndex {
    fn new(bindings: &[BindingEntry]) -> Self {
        let mut by_id = HashMap::with_capacity(bindings.len());
        let mut defaults = HashMap::new();

        for (position, entry) in bindings.iter().enumerate() {
            by_id.entry(entry.id).or_insert(position);
            if holds_default(entry) {
                defaults.entry(entry.id.type_id).or_insert(position);
            }
        }

        Self { by_id, defaults }
    }

    /// The binding a requirement resolves to, if any.
    ///
    /// A named requirement never falls back to the default binding, which is
    /// the rule [`Container::get_named`] follows.
    fn target(&self, contract: &ContractRef) -> Option<usize> {
        let id = contract.id();
        if id.name.is_some() {
            self.by_id.get(&id).copied()
        } else {
            self.defaults.get(&id.type_id).copied()
        }
    }
}

/// Whether a binding claims the default position of its contract type.
fn holds_default(entry: &BindingEntry) -> bool {
    entry.id.name.is_none() || entry.is_default
}

// --------------------------------------------------------------------------
// 1. Missing contracts
// --------------------------------------------------------------------------

/// Every requirement nothing satisfies, attributed to whoever stated it.
///
/// A provider is named by the contract it binds and a manifest by its bundle,
/// so the same missing contract asked for by three units reports three times —
/// three units have to change, not one.
fn missing_contracts(
    bindings: &[BindingEntry],
    manifests: &[BundleManifest],
    index: &BindingIndex,
    errors: &mut Vec<ResolveError>,
) {
    let mut seen: HashSet<(&'static str, ContractId)> = HashSet::new();

    for entry in bindings {
        for contract in &entry.requires {
            missing_one(
                entry.contract.type_name(),
                *contract,
                index,
                &mut seen,
                errors,
            );
        }
    }
    for manifest in manifests {
        for contract in manifest.requires {
            missing_one(manifest.name, *contract, index, &mut seen, errors);
        }
    }
}

/// Reports one requirement, once per requiring unit.
fn missing_one(
    required_by: &'static str,
    contract: ContractRef,
    index: &BindingIndex,
    seen: &mut HashSet<(&'static str, ContractId)>,
    errors: &mut Vec<ResolveError>,
) {
    if index.target(&contract).is_some() {
        return;
    }
    if seen.insert((required_by, contract.id())) {
        errors.push(ResolveError::MissingContract {
            required_by,
            contract,
        });
    }
}

// --------------------------------------------------------------------------
// 2. Duplicate defaults
// --------------------------------------------------------------------------

/// Every extra claim on the default position of a contract type.
///
/// Being unnamed is a claim, and so is asking for the position explicitly, so
/// this catches an unnamed binding shadowed by a named one as readily as two
/// unnamed ones. The first claimant in registration order is the one every
/// later claim is reported against.
fn duplicate_defaults(bindings: &[BindingEntry], errors: &mut Vec<ResolveError>) {
    let mut first: HashMap<TypeId, usize> = HashMap::new();

    for (position, entry) in bindings.iter().enumerate() {
        if !holds_default(entry) {
            continue;
        }
        match first.entry(entry.id.type_id) {
            Entry::Vacant(slot) => {
                slot.insert(position);
            }
            Entry::Occupied(slot) => {
                let earlier = &bindings[*slot.get()];
                errors.push(ResolveError::DuplicateDefault {
                    contract: contested(earlier, entry),
                    first: earlier.bundle,
                    second: entry.bundle,
                });
            }
        }
    }
}

/// The reference that names the contested position rather than one claimant's
/// own binding.
///
/// A named binding that asked for the default position renders as `Type#name`,
/// which would describe a binding the other claimant does not have. Whenever
/// one of the two is unnamed, that one names the position.
fn contested(first: &BindingEntry, second: &BindingEntry) -> ContractRef {
    if first.id.name.is_none() {
        first.contract
    } else if second.id.name.is_none() {
        second.contract
    } else {
        first.contract
    }
}

// --------------------------------------------------------------------------
// 3. Duplicate names
// --------------------------------------------------------------------------

/// Every extra claim on one name of one contract.
fn duplicate_names(bindings: &[BindingEntry], errors: &mut Vec<ResolveError>) {
    let mut first: HashMap<ContractId, usize> = HashMap::new();

    for (position, entry) in bindings.iter().enumerate() {
        let Some(name) = entry.id.name else {
            continue;
        };
        match first.entry(entry.id) {
            Entry::Vacant(slot) => {
                slot.insert(position);
            }
            Entry::Occupied(slot) => {
                let earlier = &bindings[*slot.get()];
                errors.push(ResolveError::DuplicateNamed {
                    contract: entry.contract,
                    name,
                    first: earlier.bundle,
                    second: entry.bundle,
                });
            }
        }
    }
}

// --------------------------------------------------------------------------
// 4. Cycles in the binding graph
// --------------------------------------------------------------------------

/// The adjacency list of the binding graph: `edges[b]` is what `b` resolves.
///
/// A requirement nothing satisfies contributes no edge — it is already
/// reported as a missing contract, and inventing a node for it would turn one
/// defect into two.
fn binding_edges(bindings: &[BindingEntry], index: &BindingIndex) -> Vec<Vec<usize>> {
    bindings
        .iter()
        .map(|entry| {
            let mut targets = Vec::with_capacity(entry.requires.len());
            for contract in &entry.requires {
                if let Some(target) = index.target(contract)
                    && !targets.contains(&target)
                {
                    targets.push(target);
                }
            }
            targets
        })
        .collect()
}

/// Every cycle in the binding graph, each as a closed walk.
fn binding_cycles(bindings: &[BindingEntry], edges: &[Vec<usize>], errors: &mut Vec<ResolveError>) {
    for cycle in find_cycles(edges) {
        let mut path: Vec<ContractRef> = cycle
            .iter()
            .map(|position| bindings[*position].contract)
            .collect();
        if let Some(head) = path.first().copied() {
            path.push(head);
        }
        errors.push(ResolveError::Cycle { path });
    }
}

// --------------------------------------------------------------------------
// 5. Undeclared extension points
// --------------------------------------------------------------------------

/// Every contribution aimed at a point nobody opened.
///
/// One report per contributing bundle and point: ten items posted to the same
/// typo are one mistake, and ten identical lines would only bury the other
/// eight checks.
fn undeclared_points(parts: &RegistryParts, errors: &mut Vec<ResolveError>) {
    let declared: HashSet<TypeId> = parts
        .declared_points
        .iter()
        .map(|extension| extension.type_id)
        .collect();
    let mut seen: HashSet<(TypeId, &'static str)> = HashSet::new();

    for entry in &parts.contributions {
        if declared.contains(&entry.extension.type_id) {
            continue;
        }
        if seen.insert((entry.extension.type_id, entry.bundle)) {
            errors.push(ResolveError::UndeclaredExtensionPoint {
                extension: entry.extension,
                contributed_by: entry.bundle,
            });
        }
    }
}

// --------------------------------------------------------------------------
// 6. Manifest mismatches
// --------------------------------------------------------------------------

/// Every manifest requirement none of the bundle's own providers declares.
///
/// A manifest is a claim about what the bundle needs; the providers are what
/// the bundle actually asks the container for. When the first is not a subset
/// of the union of the second, the manifest describes a bundle that was not
/// registered — the check is what keeps a manifest a fact rather than a
/// comment.
fn manifest_mismatches(
    bindings: &[BindingEntry],
    manifests: &[BundleManifest],
    errors: &mut Vec<ResolveError>,
) {
    for manifest in manifests {
        if manifest.requires.is_empty() {
            continue;
        }

        let mut asked: HashSet<ContractId> = HashSet::new();
        for entry in bindings
            .iter()
            .filter(|entry| entry.bundle == manifest.name)
        {
            asked.extend(entry.requires.iter().map(ContractRef::id));
        }

        let mut seen: HashSet<ContractId> = HashSet::new();
        for declared in manifest.requires {
            let id = declared.id();
            if asked.contains(&id) {
                continue;
            }
            if seen.insert(id) {
                errors.push(ResolveError::ManifestMismatch {
                    bundle: manifest.name,
                    declared: *declared,
                });
            }
        }
    }
}

// --------------------------------------------------------------------------
// 7 and 8. Bundle ordering
// --------------------------------------------------------------------------

/// Every ordering constraint naming a bundle that is not registered.
fn unknown_bundle_order(manifests: &[BundleManifest], errors: &mut Vec<ResolveError>) {
    let known = bundle_positions(manifests);

    for manifest in manifests {
        for after in manifest.after {
            if !known.contains_key(after) {
                errors.push(ResolveError::UnknownBundleOrder {
                    bundle: manifest.name,
                    after,
                });
            }
        }
    }
}

/// Every cycle among the bundle ordering constraints.
///
/// A cycle of `after` constraints cannot be satisfied by any registration
/// order, so the ordering is not merely unmet — it is unmeetable, and the
/// bundles that state it have to change.
fn bundle_cycles(manifests: &[BundleManifest], errors: &mut Vec<ResolveError>) {
    let known = bundle_positions(manifests);
    let edges: Vec<Vec<usize>> = manifests
        .iter()
        .map(|manifest| {
            let mut targets = Vec::new();
            for after in manifest.after {
                if let Some(target) = known.get(after).copied()
                    && !targets.contains(&target)
                {
                    targets.push(target);
                }
            }
            targets
        })
        .collect();

    for cycle in find_cycles(&edges) {
        let mut path: Vec<&'static str> = cycle
            .iter()
            .map(|position| manifests[*position].name)
            .collect();
        if let Some(head) = path.first().copied() {
            path.push(head);
        }
        errors.push(ResolveError::BundleCycle { path });
    }
}

/// The first registration of each bundle name.
fn bundle_positions(manifests: &[BundleManifest]) -> HashMap<&'static str, usize> {
    let mut positions = HashMap::with_capacity(manifests.len());
    for (position, manifest) in manifests.iter().enumerate() {
        positions.entry(manifest.name).or_insert(position);
    }
    positions
}

// --------------------------------------------------------------------------
// 9. Duplicate bundle names
// --------------------------------------------------------------------------

/// Every name registered by more than one bundle.
///
/// A bundle name is what an `after` constraint targets and what every other
/// phase-three report blames. Two bundles under one name therefore make the
/// ordering ambiguous — the first registration silently wins every lookup —
/// and send every diagnostic to whichever of them the reader guesses.
///
/// One report per contested name: the error carries the name alone, so a second
/// identical line would say nothing the first did not and would only bury the
/// other eight checks.
fn duplicate_bundles(manifests: &[BundleManifest], errors: &mut Vec<ResolveError>) {
    let mut seen: HashSet<&'static str> = HashSet::with_capacity(manifests.len());
    let mut reported: HashSet<&'static str> = HashSet::new();

    for manifest in manifests {
        if !seen.insert(manifest.name) && reported.insert(manifest.name) {
            errors.push(ResolveError::DuplicateBundle {
                name: manifest.name,
            });
        }
    }
}

// --------------------------------------------------------------------------
// Cycle search
// --------------------------------------------------------------------------

/// Every distinct cycle in `edges`, as node indices, first node not repeated.
///
/// Each cycle is reported once: the nodes of a cycle already found are removed
/// from the search, so a graph with three tangles reports three lines and not
/// one line per path through them. Each returned walk starts at its own lowest
/// node index, so the report does not depend on where the search happened to
/// enter the tangle.
fn find_cycles(edges: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let mut settled = vec![false; edges.len()];
    let mut cycles = Vec::new();

    for start in 0..edges.len() {
        if settled[start] {
            continue;
        }
        match reach_cycle(start, edges, &settled) {
            Some(cycle) => {
                for node in &cycle {
                    settled[*node] = true;
                }
                cycles.push(rotate(cycle));
            }
            // Nothing cyclic is reachable from here, so this node is on no
            // cycle that has not already been reported.
            None => settled[start] = true,
        }
    }

    cycles
}

/// The first cycle reachable from `start`, ignoring nodes already settled.
///
/// Iterative rather than recursive: the depth of the walk is the size of the
/// graph, and a graph large enough to matter is a graph large enough to
/// overflow a call stack.
fn reach_cycle(start: usize, edges: &[Vec<usize>], settled: &[bool]) -> Option<Vec<usize>> {
    let mut on_path = vec![false; edges.len()];
    let mut done = vec![false; edges.len()];
    let mut path = vec![start];
    let mut stack = vec![(start, 0usize)];
    on_path[start] = true;

    while let Some(&(node, cursor)) = stack.last() {
        if cursor < edges[node].len() {
            let top = stack.len() - 1;
            stack[top].1 += 1;

            let next = edges[node][cursor];
            if settled[next] || done[next] {
                continue;
            }
            if on_path[next] {
                let at = path.iter().position(|item| *item == next)?;
                return Some(path[at..].to_vec());
            }
            on_path[next] = true;
            path.push(next);
            stack.push((next, 0));
        } else {
            done[node] = true;
            on_path[node] = false;
            path.pop();
            stack.pop();
        }
    }

    None
}

/// Turns a cycle so that it starts at its lowest node index.
fn rotate(cycle: Vec<usize>) -> Vec<usize> {
    let Some(at) = cycle
        .iter()
        .enumerate()
        .min_by_key(|(_, node)| **node)
        .map(|(position, _)| position)
    else {
        return cycle;
    };
    let mut rotated = cycle;
    rotated.rotate_left(at);
    rotated
}

// --------------------------------------------------------------------------
// The plan
// --------------------------------------------------------------------------

/// The order the units are driven in, once the graph is known to be sound.
fn boot_plan(parts: &RegistryParts, index: &BindingIndex, edges: &[Vec<usize>]) -> BootPlan {
    BootPlan {
        components: component_order(parts, index, edges),
        runnables: parts
            .runnables
            .iter()
            .enumerate()
            .map(|(position, entry)| RunnableId::new(entry.name, position_of(position)))
            .collect(),
    }
}

/// Components, topologically ordered, ties broken on registration order.
fn component_order(
    parts: &RegistryParts,
    index: &BindingIndex,
    edges: &[Vec<usize>],
) -> Vec<ComponentId> {
    let count = parts.components.len();

    // The binding each component resolves through, and the reverse map, so a
    // reached binding can be recognised as another component's.
    let mut nodes = Vec::with_capacity(count);
    let mut owner: HashMap<usize, usize> = HashMap::with_capacity(count);
    for (position, entry) in parts.components.iter().enumerate() {
        let node = index.by_id.get(&entry.contract).copied();
        if let Some(node) = node {
            owner.entry(node).or_insert(position);
        }
        nodes.push(node);
    }

    // A component depends on every component its own binding reaches.
    let mut depends: Vec<Vec<usize>> = vec![Vec::new(); count];
    for (position, node) in nodes.iter().enumerate() {
        let Some(node) = *node else {
            continue;
        };
        for reached in reachable(node, edges) {
            if let Some(other) = owner.get(&reached).copied()
                && other != position
            {
                depends[position].push(other);
            }
        }
    }

    let mut placed = vec![false; count];
    let mut order = Vec::with_capacity(count);
    while order.len() < count {
        // Lowest registration rank among the ready components, which is what
        // makes the tie-break deterministic.
        let ready = (0..count).find(|position| {
            !placed[*position] && depends[*position].iter().all(|other| placed[*other])
        });
        match ready {
            Some(position) => {
                placed[position] = true;
                order.push(position);
            }
            // Unreachable once the cycle check has passed: a cycle among
            // components is a cycle among their bindings. Falling back to
            // registration order keeps the function total rather than
            // spinning.
            None => {
                for (position, done) in placed.iter_mut().enumerate() {
                    if !*done {
                        *done = true;
                        order.push(position);
                    }
                }
            }
        }
    }

    order
        .into_iter()
        .map(|position| ComponentId::new(parts.components[position].name, position_of(position)))
        .collect()
}

/// Every binding reachable from `node`, `node` itself excluded unless the
/// graph leads back to it.
fn reachable(node: usize, edges: &[Vec<usize>]) -> Vec<usize> {
    let mut seen = vec![false; edges.len()];
    let mut found = Vec::new();
    let mut stack = edges[node].clone();

    while let Some(current) = stack.pop() {
        if seen[current] {
            continue;
        }
        seen[current] = true;
        found.push(current);
        stack.extend_from_slice(&edges[current]);
    }

    found
}

/// A registration position as the unit identities carry it.
///
/// Saturating rather than truncating: a registry with more than `u32::MAX`
/// entries has already saturated the registry's own rank counter, and two
/// identities colliding at the ceiling is less wrong than one wrapping to zero.
fn position_of(position: usize) -> u32 {
    u32::try_from(position).unwrap_or(u32::MAX)
}

impl fmt::Display for BootPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("boot ")?;
        for (position, component) in self.components.iter().enumerate() {
            if position > 0 {
                f.write_str(" -> ")?;
            }
            write!(f, "{component}")?;
        }
        if self.components.is_empty() {
            f.write_str("(nothing)")?;
        }
        write!(f, ", then run {} runnable(s)", self.runnables.len())
    }
}

#[cfg(test)]
mod tests {
    use kernel_core::{
        BoxFuture, ComponentDescriptor, ComponentError, ConfigTree, Event, Extension, Flow,
        Lifetime, ListenerError, NoopTelemetry, Priority, RunError, RunnableDescriptor, Telemetry,
    };

    use super::*;
    use crate::component::{BootContext, ShutdownContext};
    use crate::dispatcher::{Listener, ListenerContext};
    use crate::provider::Provider;

    // ------------------------------------------------------------------
    // Neutral placeholders
    // ------------------------------------------------------------------

    trait Alpha: Send + Sync + 'static {}
    trait Beta: Send + Sync + 'static {}
    trait Gamma: Send + Sync + 'static {}

    struct Plain;

    impl Alpha for Plain {}
    impl Beta for Plain {}
    impl Gamma for Plain {}

    struct Item;

    impl Extension for Item {}

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

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    fn registry() -> Registry {
        Registry::new(
            Arc::new(ConfigTree::empty()),
            Arc::new(NoopTelemetry) as Arc<dyn Telemetry>,
        )
    }

    fn alpha() -> Provider<dyn Alpha> {
        Provider::from_value(Arc::new(Plain) as Arc<dyn Alpha>)
    }

    fn beta() -> Provider<dyn Beta> {
        Provider::from_value(Arc::new(Plain) as Arc<dyn Beta>)
    }

    fn gamma() -> Provider<dyn Gamma> {
        Provider::from_value(Arc::new(Plain) as Arc<dyn Gamma>)
    }

    fn rendered(errors: &[ResolveError]) -> Vec<String> {
        errors.iter().map(ToString::to_string).collect()
    }

    fn kinds(errors: &[ResolveError]) -> Vec<&'static str> {
        errors
            .iter()
            .map(|error| match error {
                ResolveError::MissingContract { .. } => "missing",
                ResolveError::DuplicateDefault { .. } => "default",
                ResolveError::DuplicateNamed { .. } => "named",
                ResolveError::Cycle { .. } => "cycle",
                ResolveError::UndeclaredExtensionPoint { .. } => "extension",
                ResolveError::ManifestMismatch { .. } => "manifest",
                ResolveError::UnknownBundleOrder { .. } => "order",
                ResolveError::BundleCycle { .. } => "bundle-cycle",
                ResolveError::DuplicateBundle { .. } => "duplicate-bundle",
            })
            .collect()
    }

    // ------------------------------------------------------------------
    // Components, declared through their requirements
    // ------------------------------------------------------------------

    macro_rules! component {
        ($name:ident, $label:literal) => {
            struct $name;

            impl Component for $name {
                fn name() -> &'static str {
                    $label
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
        };
    }

    component!(First, "first");
    component!(Second, "second");
    component!(Third, "third");

    struct Task;

    impl Runnable for Task {
        fn name() -> &'static str {
            "task"
        }

        fn descriptor(&self) -> RunnableDescriptor {
            RunnableDescriptor::new()
        }

        fn run(
            self: Arc<Self>,
            _cx: crate::runnable::RunContext,
        ) -> BoxFuture<'static, Result<(), RunError>> {
            Box::pin(async { Ok(()) })
        }
    }

    // ------------------------------------------------------------------
    // Nothing is instantiated
    // ------------------------------------------------------------------

    #[test]
    fn builds_nothing() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(Provider::<dyn Alpha>::from_fn(|_container| {
            panic!("a provider ran during resolution")
        }));

        let resolved = resolve(registry, &[]).expect("a graph with one binding is sound");

        assert!(!resolved.container.is_sealed());
    }

    // ------------------------------------------------------------------
    // Aggregation
    // ------------------------------------------------------------------

    #[test]
    fn aggregates_every_violation() {
        static MANIFEST: BundleManifest = BundleManifest::new("one", "0.1.0");

        let mut registry = registry();
        registry.enter_bundle("one");
        // Violation 1: a requirement nothing satisfies.
        registry.provide(alpha().requires([ContractRef::of::<dyn Gamma>()]));
        // Violation 2: a second claim on the default position of `Beta`.
        registry.provide(beta());
        registry.provide(beta());
        // Violation 3: a contribution to a point nobody declared.
        registry.contribute(Item);

        let errors = resolve(registry, &[MANIFEST]).expect_err("three violations were planted");

        assert_eq!(kinds(&errors), ["missing", "default", "extension"]);
    }

    // One instance of every check, planted at once. The whole point of phase
    // three is that a start-up reveals all of them in one run.
    #[test]
    fn aggregates_all_nine() {
        static ONE_REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Beta>()];
        static AFTER_TWO: [&str; 1] = ["two"];
        static AFTER_ONE: [&str; 2] = ["one", "absent"];
        static ONE: BundleManifest = BundleManifest::new("one", "0.1.0")
            .requires(&ONE_REQUIRES)
            .after(&AFTER_TWO);
        static TWO: BundleManifest = BundleManifest::new("two", "0.1.0").after(&AFTER_ONE);
        static ONE_AGAIN: BundleManifest = BundleManifest::new("one", "0.1.0");

        let mut registry = registry();
        registry.enter_bundle("one");
        // 1. a requirement nothing satisfies, and 4. a binding that requires
        // itself.
        registry.provide(alpha().requires([
            ContractRef::of::<dyn Alpha>(),
            ContractRef::named::<dyn Alpha>("absent"),
        ]));
        // 2. two claims on the default position of `Beta`.
        registry.provide(beta());
        registry.provide(beta());
        // 3. two claims on one name of `Gamma`.
        registry.provide_named("primary", gamma());
        registry.provide_named("primary", gamma());
        // 5. a contribution to a point nobody declared.
        registry.contribute(Item);
        // 6. `one` declares a requirement none of its providers states.
        // 7. `two` orders itself after a bundle that is not registered.
        // 8. `one` and `two` order each other.
        // 9. a third manifest registers the name `one` a second time.

        let errors =
            resolve(registry, &[ONE, TWO, ONE_AGAIN]).expect_err("nine violations were planted");

        assert_eq!(
            kinds(&errors),
            [
                "missing",
                "default",
                "named",
                "cycle",
                "extension",
                "manifest",
                "order",
                "bundle-cycle",
                "duplicate-bundle",
            ],
            "{:?}",
            rendered(&errors)
        );
    }

    #[test]
    fn reports_every_missing_asker() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha().requires([ContractRef::of::<dyn Gamma>()]));
        registry.provide(beta().requires([ContractRef::of::<dyn Gamma>()]));

        let errors = resolve(registry, &[]).expect_err("nothing provides Gamma");

        assert_eq!(errors.len(), 2);
        assert!(kinds(&errors).iter().all(|kind| *kind == "missing"));
    }

    // ------------------------------------------------------------------
    // 1. Missing contracts
    // ------------------------------------------------------------------

    #[test]
    fn named_never_falls_back() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha());
        registry.provide(beta().requires([ContractRef::named::<dyn Alpha>("other")]));

        let errors = resolve(registry, &[]).expect_err("no binding is named `other`");

        assert_eq!(kinds(&errors), ["missing"]);
    }

    #[test]
    fn default_claim_satisfies() {
        let mut registry = registry();
        registry.enter_bundle("one");
        let _ = registry.provide_named("primary", alpha()).as_default();
        registry.provide(beta().requires([ContractRef::of::<dyn Alpha>()]));

        assert!(resolve(registry, &[]).is_ok());
    }

    #[test]
    fn manifest_requirement_checked() {
        static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Gamma>()];
        static MANIFEST: BundleManifest = BundleManifest::new("one", "0.1.0").requires(&REQUIRES);

        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha().requires([ContractRef::of::<dyn Gamma>()]));

        let errors = resolve(registry, &[MANIFEST]).expect_err("nothing provides Gamma");

        assert_eq!(errors.len(), 2, "{:?}", rendered(&errors));
        assert!(rendered(&errors).iter().any(|line| line.contains("`one`")));
    }

    // ------------------------------------------------------------------
    // 2 and 3. Ambiguity
    // ------------------------------------------------------------------

    #[test]
    fn duplicate_default_names_both() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha());
        registry.enter_bundle("two");
        registry.provide(alpha());

        let errors = resolve(registry, &[]).expect_err("two bundles claim the default");

        match &errors[..] {
            [ResolveError::DuplicateDefault { first, second, .. }] => {
                assert_eq!(*first, "one");
                assert_eq!(*second, "two");
            }
            other => panic!("expected one duplicate default, got {other:?}"),
        }
    }

    #[test]
    fn as_default_collides() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha());
        registry.enter_bundle("two");
        let _ = registry.provide_named("other", alpha()).as_default();

        let errors = resolve(registry, &[]).expect_err("both claim the default position");

        assert_eq!(kinds(&errors), ["default"]);
        // The contested position is the unnamed one, not the claimant's name.
        assert!(
            !rendered(&errors)[0].contains('#'),
            "{:?}",
            rendered(&errors)
        );
    }

    // Neither claimant is unnamed, so neither names the position better than
    // the other: the earlier claim in registration order is reported.
    #[test]
    fn named_claimants_report_first() {
        let mut registry = registry();
        registry.enter_bundle("one");
        let _ = registry.provide_named("primary", alpha()).as_default();
        registry.enter_bundle("two");
        let _ = registry.provide_named("secondary", alpha()).as_default();

        let errors = resolve(registry, &[]).expect_err("both claim the default position");

        assert_eq!(kinds(&errors), ["default"]);
        let rendered = rendered(&errors);
        assert!(rendered[0].contains("#primary"), "{rendered:?}");
        assert!(!rendered[0].contains("#secondary"), "{rendered:?}");
    }

    #[test]
    fn three_defaults_report_twice() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha());
        registry.provide(alpha());
        registry.provide(alpha());

        let errors = resolve(registry, &[]).expect_err("three claims on one position");

        assert_eq!(errors.len(), 2);
    }

    #[test]
    fn duplicate_named_reported() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide_named("primary", alpha());
        registry.enter_bundle("two");
        registry.provide_named("primary", alpha());

        let errors = resolve(registry, &[]).expect_err("one name claimed twice");

        match &errors[..] {
            [
                ResolveError::DuplicateNamed {
                    name,
                    first,
                    second,
                    ..
                },
            ] => {
                assert_eq!(*name, "primary");
                assert_eq!(*first, "one");
                assert_eq!(*second, "two");
            }
            other => panic!("expected one duplicate name, got {other:?}"),
        }
    }

    #[test]
    fn distinct_names_coexist() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha());
        registry.provide_named("primary", alpha());
        registry.provide_named("secondary", alpha());

        assert!(resolve(registry, &[]).is_ok());
    }

    // ------------------------------------------------------------------
    // 4. Cycles
    // ------------------------------------------------------------------

    #[test]
    fn cycle_reports_full_path() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha().requires([ContractRef::of::<dyn Beta>()]));
        registry.provide(beta().requires([ContractRef::of::<dyn Gamma>()]));
        registry.provide(gamma().requires([ContractRef::of::<dyn Alpha>()]));

        let errors = resolve(registry, &[]).expect_err("the three bindings close a loop");

        match &errors[..] {
            [ResolveError::Cycle { path }] => {
                assert_eq!(path.len(), 4, "the first node repeats at the end");
                assert_eq!(path.first(), path.last());
                let line = errors[0].to_string();
                let arrows = line.matches(" -> ").count();
                assert_eq!(arrows, 3, "{line}");
                assert!(line.starts_with("dependency cycle: "), "{line}");
            }
            other => panic!("expected one cycle, got {other:?}"),
        }
    }

    #[test]
    fn self_cycle_reported() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha().requires([ContractRef::of::<dyn Alpha>()]));

        let errors = resolve(registry, &[]).expect_err("a binding requires itself");

        match &errors[..] {
            [ResolveError::Cycle { path }] => assert_eq!(path.len(), 2),
            other => panic!("expected one cycle, got {other:?}"),
        }
    }

    #[test]
    fn two_cycles_reported() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha().requires([ContractRef::of::<dyn Alpha>()]));
        registry.provide(beta().requires([ContractRef::of::<dyn Beta>()]));

        let errors = resolve(registry, &[]).expect_err("two independent loops");

        assert_eq!(kinds(&errors), ["cycle", "cycle"]);
    }

    #[test]
    fn named_edge_breaks_loop() {
        let mut registry = registry();
        registry.enter_bundle("one");
        // A decorator over its own contract: the default wraps the named one,
        // and the named one requires nothing. No cycle.
        registry.provide_named("inner", alpha());
        registry.provide(alpha().requires([ContractRef::named::<dyn Alpha>("inner")]));

        assert!(resolve(registry, &[]).is_ok());
    }

    // ------------------------------------------------------------------
    // 5. Extension points
    // ------------------------------------------------------------------

    #[test]
    fn undeclared_point_reported() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.contribute(Item);
        registry.contribute(Item);

        let errors = resolve(registry, &[]).expect_err("nobody declared the point");

        assert_eq!(errors.len(), 1, "one bundle, one point, one report");
        assert_eq!(kinds(&errors), ["extension"]);
    }

    #[test]
    fn declared_point_accepts() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.declare_extension_point::<Item>();
        registry.enter_bundle("two");
        registry.contribute(Item);

        let resolved = resolve(registry, &[]).expect("the point was declared");

        assert_eq!(resolved.extensions.count::<Item>(), 1);
    }

    // ------------------------------------------------------------------
    // 6. Manifest mismatch
    // ------------------------------------------------------------------

    #[test]
    fn lying_manifest_detected() {
        static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Gamma>()];
        static MANIFEST: BundleManifest = BundleManifest::new("one", "0.1.0").requires(&REQUIRES);

        let mut registry = registry();
        registry.enter_bundle("one");
        // Gamma is provided, so nothing is missing — but no provider of this
        // bundle ever asks for it.
        registry.provide(gamma());
        registry.provide(alpha());

        let errors = resolve(registry, &[MANIFEST])
            .expect_err("the manifest declares more than the bundle asks");

        match &errors[..] {
            [ResolveError::ManifestMismatch { bundle, .. }] => assert_eq!(*bundle, "one"),
            other => panic!("expected one manifest mismatch, got {other:?}"),
        }
    }

    #[test]
    fn honest_manifest_passes() {
        static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Gamma>()];
        static MANIFEST: BundleManifest = BundleManifest::new("one", "0.1.0").requires(&REQUIRES);

        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(gamma());
        registry.provide(alpha().requires([ContractRef::of::<dyn Gamma>()]));

        assert!(resolve(registry, &[MANIFEST]).is_ok());
    }

    #[test]
    fn mismatch_is_per_bundle() {
        static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Gamma>()];
        static ONE: BundleManifest = BundleManifest::new("one", "0.1.0").requires(&REQUIRES);
        static TWO: BundleManifest = BundleManifest::new("two", "0.1.0");

        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(gamma());
        registry.enter_bundle("two");
        // The requirement is declared by the *other* bundle's provider.
        registry.provide(alpha().requires([ContractRef::of::<dyn Gamma>()]));

        let errors = resolve(registry, &[ONE, TWO]).expect_err("bundle one asks for nothing");

        assert_eq!(kinds(&errors), ["manifest"]);
    }

    // ------------------------------------------------------------------
    // 7 and 8. Bundle ordering
    // ------------------------------------------------------------------

    #[test]
    fn unknown_after_reported() {
        static AFTER: [&str; 1] = ["absent"];
        static MANIFEST: BundleManifest = BundleManifest::new("one", "0.1.0").after(&AFTER);

        let errors = resolve(registry(), &[MANIFEST]).expect_err("`absent` is not registered");

        match &errors[..] {
            [ResolveError::UnknownBundleOrder { bundle, after }] => {
                assert_eq!(*bundle, "one");
                assert_eq!(*after, "absent");
            }
            other => panic!("expected one unknown order, got {other:?}"),
        }
    }

    #[test]
    fn known_after_passes() {
        static AFTER: [&str; 1] = ["two"];
        static ONE: BundleManifest = BundleManifest::new("one", "0.1.0").after(&AFTER);
        static TWO: BundleManifest = BundleManifest::new("two", "0.1.0");

        assert!(resolve(registry(), &[ONE, TWO]).is_ok());
    }

    #[test]
    fn bundle_loop_reported() {
        static AFTER_TWO: [&str; 1] = ["two"];
        static AFTER_ONE: [&str; 1] = ["one"];
        static ONE: BundleManifest = BundleManifest::new("one", "0.1.0").after(&AFTER_TWO);
        static TWO: BundleManifest = BundleManifest::new("two", "0.1.0").after(&AFTER_ONE);

        let errors =
            resolve(registry(), &[ONE, TWO]).expect_err("the two bundles order each other");

        match &errors[..] {
            [ResolveError::BundleCycle { path }] => {
                assert_eq!(path, &["one", "two", "one"]);
                assert_eq!(
                    errors[0].to_string(),
                    "bundle ordering cycle: one -> two -> one"
                );
            }
            other => panic!("expected one bundle cycle, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // 9. Duplicate bundle names
    // ------------------------------------------------------------------

    #[test]
    fn duplicate_bundle_reported() {
        static ONE: BundleManifest = BundleManifest::new("one", "0.1.0");
        static AGAIN: BundleManifest = BundleManifest::new("one", "0.2.0");
        static TWO: BundleManifest = BundleManifest::new("two", "0.1.0");

        let errors =
            resolve(registry(), &[ONE, TWO, AGAIN]).expect_err("`one` is registered twice");

        match &errors[..] {
            [ResolveError::DuplicateBundle { name }] => assert_eq!(*name, "one"),
            other => panic!("expected one duplicate bundle, got {other:?}"),
        }
    }

    #[test]
    fn three_bundles_report_once() {
        static ONE: BundleManifest = BundleManifest::new("one", "0.1.0");

        let errors = resolve(registry(), &[ONE, ONE, ONE]).expect_err("one name, three bundles");

        assert_eq!(kinds(&errors), ["duplicate-bundle"]);
    }

    // The name an `after` constraint targets is the one the check contests, so
    // a graph that orders itself against a duplicated name reports both.
    #[test]
    fn duplicate_keeps_ordering() {
        static AFTER_ONE: [&str; 1] = ["one"];
        static ONE: BundleManifest = BundleManifest::new("one", "0.1.0");
        static TWO: BundleManifest = BundleManifest::new("two", "0.1.0").after(&AFTER_ONE);

        let errors = resolve(registry(), &[ONE, TWO, ONE]).expect_err("`one` is registered twice");

        assert_eq!(kinds(&errors), ["duplicate-bundle"]);
    }

    // ------------------------------------------------------------------
    // The plan
    // ------------------------------------------------------------------

    fn ordered() -> Registry {
        let mut registry = registry();
        registry.enter_bundle("one");
        // Third depends on Second, Second on First; registration is the
        // reverse, so registration order alone would boot them backwards.
        registry.component(
            Provider::<Third>::from_fn(|_container| panic!("no build during resolution"))
                .requires([ContractRef::of::<Second>()]),
        );
        registry.component(
            Provider::<Second>::from_fn(|_container| panic!("no build during resolution"))
                .requires([ContractRef::of::<First>()]),
        );
        registry.component(Provider::<First>::from_fn(|_container| {
            panic!("no build during resolution")
        }));
        registry
    }

    #[test]
    fn boots_in_order() {
        let resolved = resolve(ordered(), &[]).expect("the component graph is sound");

        let names: Vec<&'static str> = resolved
            .plan
            .components
            .iter()
            .map(ComponentId::name)
            .collect();
        let first = names
            .iter()
            .position(|name| *name == "first")
            .expect("First is in the plan");
        let second = names
            .iter()
            .position(|name| *name == "second")
            .expect("Second is in the plan");
        let third = names
            .iter()
            .position(|name| *name == "third")
            .expect("Third is in the plan");

        assert!(first < second, "{names:?}");
        assert!(second < third, "{names:?}");
    }

    #[test]
    fn plan_is_reproducible() {
        let one = resolve(ordered(), &[]).expect("sound").plan;
        let two = resolve(ordered(), &[]).expect("sound").plan;

        assert_eq!(one, two);
    }

    #[test]
    fn independent_keep_order() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.component(Provider::<First>::from_fn(|_container| panic!("no build")));
        registry.component(Provider::<Second>::from_fn(|_container| panic!("no build")));

        let plan = resolve(registry, &[]).expect("sound").plan;

        assert_eq!(
            plan.components
                .iter()
                .map(ComponentId::index)
                .collect::<Vec<u32>>(),
            [0, 1]
        );
    }

    #[test]
    fn identity_indexes_entries() {
        let resolved = resolve(ordered(), &[]).expect("sound");

        for id in &resolved.plan.components {
            let entry = &resolved.components[id.index() as usize];
            assert_eq!(entry.name, id.name());
        }
    }

    #[test]
    fn runnables_keep_order() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.runnable(Provider::<Task>::from_fn(|_container| panic!("no build")));

        let resolved = resolve(registry, &[]).expect("sound");
        let plan = &resolved.plan;

        assert_eq!(
            plan.runnables,
            [RunnableId::new(plan.runnables[0].name(), 0)]
        );
        assert!(plan.components.is_empty());

        // The identity in the plan indexes straight into the entry that builds
        // it, exactly as it does for components.
        for id in &plan.runnables {
            let entry = &resolved.runnables[id.index() as usize];
            assert_eq!(entry.name, id.name());
        }
    }

    #[test]
    fn order_is_transitive() {
        // Third reaches First only through a plain contract it never names.
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.component(
            Provider::<Third>::from_fn(|_container| panic!("no build"))
                .requires([ContractRef::of::<dyn Alpha>()]),
        );
        registry.provide(alpha().requires([ContractRef::of::<First>()]));
        registry.component(Provider::<First>::from_fn(|_container| panic!("no build")));

        let plan = resolve(registry, &[]).expect("sound").plan;

        assert_eq!(
            plan.components
                .iter()
                .map(ComponentId::index)
                .collect::<Vec<u32>>(),
            [1, 0],
            "First is registered second and still boots first"
        );
    }

    #[tokio::test]
    async fn container_is_wired() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha());

        let resolved = resolve(registry, &[]).expect("sound");

        assert!(resolved.container.get::<dyn Alpha>().await.is_ok());
    }

    #[tokio::test]
    async fn dispatcher_is_attached() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.listen::<Signal, _>(Watcher, Priority::NORMAL);

        let resolved = resolve(registry, &[]).expect("sound");

        assert_eq!(resolved.dispatcher.listener_count::<Signal>(), 1);
        let dispatched = resolved
            .dispatcher
            .dispatch(&mut Signal)
            .await
            .expect("a listener context needs the attached container");
        assert_eq!(dispatched.listeners_run, 1);
    }

    #[test]
    fn lifetimes_survive() {
        let mut registry = registry();
        registry.enter_bundle("one");
        registry.provide(alpha().lifetime(Lifetime::Factory));

        assert!(resolve(registry, &[]).is_ok());
    }

    // ------------------------------------------------------------------
    // The cycle search itself
    // ------------------------------------------------------------------

    #[test]
    fn finds_each_tangle_once() {
        // 0 -> 1 -> 0, 2 -> 3 -> 4 -> 2, 5 -> 0 (feeds a tangle, is on none).
        let edges = vec![vec![1], vec![0], vec![3], vec![4], vec![2], vec![0]];

        let cycles = find_cycles(&edges);

        assert_eq!(cycles, vec![vec![0, 1], vec![2, 3, 4]]);
    }

    #[test]
    fn acyclic_finds_nothing() {
        let edges = vec![vec![1, 2], vec![2], vec![]];

        assert!(find_cycles(&edges).is_empty());
    }

    #[test]
    fn plan_renders() {
        let plan = BootPlan {
            components: vec![ComponentId::new("first", 0), ComponentId::new("second", 1)],
            runnables: vec![RunnableId::new("task", 0)],
        };

        assert_eq!(
            plan.to_string(),
            "boot first#0 -> second#1, then run 1 runnable(s)"
        );
    }
}
