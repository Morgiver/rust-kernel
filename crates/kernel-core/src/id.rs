//! Identities: what the kernel uses as a key.
//!
//! Two families live here.
//!
//! * **Type identities** — [`ContractId`], [`ContractRef`] and [`ExtensionId`]
//!   name a trait or a type. They are derived from [`TypeId`], so they are
//!   stable within one build and need no registry of strings.
//! * **Unit identities** — [`BundleId`], [`ComponentId`] and [`RunnableId`]
//!   name a registered unit. They pair the declared name with the registration
//!   index, so that two units declared with the same name stay distinguishable.
//!
//! [`ContractId`] and [`ContractRef`] carry the same information but are not
//! interchangeable: `TypeId::of` is not callable from a `const` context on the
//! crate's minimum supported compiler, so a `static` cannot hold a
//! [`ContractId`]. [`ContractRef`] stores the function that produces the
//! identity instead of the identity itself and calls it on demand, which is
//! what lets a manifest declare its requirements as a
//! `&'static [ContractRef]` literal.

use core::any::{TypeId, type_name};
use core::fmt;
use core::hash::{Hash, Hasher};

/// Identity of a contract: the trait's type identity plus an optional binding
/// name.
///
/// The name is part of the identity. A default binding (`name: None`) and a
/// named binding of the same trait are two different contracts, which is what
/// allows several implementations of one trait to coexist in a container.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ContractId {
    /// Type identity of the contract trait, e.g. `TypeId::of::<dyn Surface>()`.
    pub type_id: TypeId,
    /// Binding name, or `None` for the default binding.
    pub name: Option<&'static str>,
}

impl ContractId {
    /// Identity of the default binding of `C`.
    ///
    /// # Examples
    ///
    /// ```
    /// use kernel_core::id::ContractId;
    ///
    /// trait Surface: Send + Sync + 'static {}
    ///
    /// assert_eq!(ContractId::of::<dyn Surface>(), ContractId::of::<dyn Surface>());
    /// ```
    #[must_use]
    pub fn of<C: ?Sized + 'static>() -> Self {
        Self {
            type_id: TypeId::of::<C>(),
            name: None,
        }
    }

    /// Identity of the binding of `C` published under `name`.
    ///
    /// # Examples
    ///
    /// ```
    /// use kernel_core::id::ContractId;
    ///
    /// trait Surface: Send + Sync + 'static {}
    ///
    /// assert_ne!(
    ///     ContractId::named::<dyn Surface>("secondary"),
    ///     ContractId::of::<dyn Surface>()
    /// );
    /// ```
    #[must_use]
    pub fn named<C: ?Sized + 'static>(name: &'static str) -> Self {
        Self {
            type_id: TypeId::of::<C>(),
            name: Some(name),
        }
    }
}

impl fmt::Display for ContractId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.type_id)?;
        if let Some(name) = self.name {
            write!(f, "#{name}")?;
        }
        Ok(())
    }
}

/// A `const`-constructible reference to a contract.
///
/// [`ContractId`] is built by calling `TypeId::of`, which is not `const` on the
/// crate's minimum supported compiler. `ContractRef` stores the *function* that
/// produces the identity instead of the identity itself, so it can be written
/// in a `static` item and resolved later:
///
/// ```
/// use kernel_core::id::ContractRef;
///
/// trait Surface: Send + Sync + 'static {}
/// trait Sink: Send + Sync + 'static {}
///
/// static REQUIRES: &[ContractRef] = &[
///     ContractRef::of::<dyn Surface>(),
///     ContractRef::named::<dyn Sink>("secondary"),
/// ];
///
/// assert_eq!(REQUIRES.len(), 2);
/// assert_eq!(REQUIRES[1].name(), Some("secondary"));
/// ```
///
/// Equality, ordering of hash buckets and `Debug` all go through [`id`] and
/// [`type_name`], never through the raw pointers, so two refs to the same
/// contract compare equal even when built at different call sites.
///
/// [`id`]: ContractRef::id
/// [`type_name`]: ContractRef::type_name
#[derive(Clone, Copy)]
pub struct ContractRef {
    type_id: fn() -> TypeId,
    type_name: fn() -> &'static str,
    name: Option<&'static str>,
}

impl ContractRef {
    /// Reference to the default binding of `C`.
    #[must_use]
    pub const fn of<C: ?Sized + 'static>() -> Self {
        Self {
            type_id: TypeId::of::<C>,
            type_name: type_name::<C>,
            name: None,
        }
    }

    /// Reference to the binding of `C` published under `name`.
    #[must_use]
    pub const fn named<C: ?Sized + 'static>(name: &'static str) -> Self {
        Self {
            type_id: TypeId::of::<C>,
            type_name: type_name::<C>,
            name: Some(name),
        }
    }

    /// Resolves the reference into the identity the container keys on.
    #[must_use]
    pub fn id(&self) -> ContractId {
        ContractId {
            type_id: (self.type_id)(),
            name: self.name,
        }
    }

    /// The compiler's name for the contract type, for diagnostics only.
    ///
    /// The exact string is not guaranteed across compiler versions and must
    /// never be used as a key.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        (self.type_name)()
    }

    /// The binding name, or `None` for the default binding.
    #[must_use]
    pub fn name(&self) -> Option<&'static str> {
        self.name
    }
}

impl fmt::Debug for ContractRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContractRef")
            .field("type_name", &self.type_name())
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ContractRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.type_name())?;
        if let Some(name) = self.name {
            write!(f, "#{name}")?;
        }
        Ok(())
    }
}

impl PartialEq for ContractRef {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for ContractRef {}

impl Hash for ContractRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl From<ContractRef> for ContractId {
    fn from(value: ContractRef) -> Self {
        value.id()
    }
}

/// Identity of an extension type contributed to an extension point.
///
/// Extensions are keyed by their concrete type, so no binding name is needed;
/// the type name is kept alongside for diagnostics.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ExtensionId {
    /// Type identity of the extension type.
    pub type_id: TypeId,
    /// The compiler's name for the extension type, for diagnostics only.
    pub type_name: &'static str,
}

impl ExtensionId {
    /// Identity of the extension type `X`.
    ///
    /// # Examples
    ///
    /// ```
    /// use kernel_core::id::ExtensionId;
    ///
    /// struct Marker;
    ///
    /// assert_eq!(ExtensionId::of::<Marker>(), ExtensionId::of::<Marker>());
    /// ```
    #[must_use]
    pub fn of<X: 'static>() -> Self {
        Self {
            type_id: TypeId::of::<X>(),
            type_name: type_name::<X>(),
        }
    }
}

impl fmt::Display for ExtensionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.type_name)
    }
}

/// Declares one of the unit identities, which differ only by the kind of unit
/// they name.
macro_rules! unit_id {
    ($name:ident, $unit:literal) => {
        #[doc = concat!("Identity of a registered ", $unit, ".")]
        ///
        /// The declared name is not assumed unique: the zero-based registration
        /// index is part of the identity, so two units declared under the same
        /// name remain distinguishable. `Display` renders `name#index`.
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
        pub struct $name {
            name: &'static str,
            index: u32,
        }

        impl $name {
            #[doc = concat!("Builds the identity of the ", $unit, " declared as `name` and registered at `index`.")]
            ///
            /// # Examples
            ///
            /// ```
            #[doc = concat!("use kernel_core::id::", stringify!($name), ";")]
            ///
            #[doc = concat!("let id = ", stringify!($name), "::new(\"alpha\", 2);")]
            /// assert_eq!(id.name(), "alpha");
            /// assert_eq!(id.to_string(), "alpha#2");
            /// ```
            #[must_use]
            pub const fn new(name: &'static str, index: u32) -> Self {
                Self { name, index }
            }

            /// The declared name.
            #[must_use]
            pub fn name(&self) -> &'static str {
                self.name
            }

            /// The zero-based registration index.
            #[must_use]
            pub fn index(&self) -> u32 {
                self.index
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}#{}", self.name, self.index)
            }
        }
    };
}

unit_id!(BundleId, "bundle");
unit_id!(ComponentId, "component");
unit_id!(RunnableId, "runnable");

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::collections::{HashMap, HashSet};

    trait Surface: Send + Sync + 'static {}
    trait Sink: Send + Sync + 'static {}

    struct Marker;

    // The point of `ContractRef`: this item must compile.
    static REQUIRES: &[ContractRef] = &[
        ContractRef::of::<dyn Surface>(),
        ContractRef::named::<dyn Sink>("secondary"),
    ];

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn static_requires_compiles() {
        assert_eq!(REQUIRES.len(), 2);
        assert_eq!(REQUIRES[0].id(), ContractId::of::<dyn Surface>());
        assert_eq!(REQUIRES[1].id(), ContractId::named::<dyn Sink>("secondary"));
    }

    #[test]
    fn name_distinguishes_contracts() {
        assert_ne!(
            ContractId::of::<dyn Surface>(),
            ContractId::named::<dyn Surface>("secondary")
        );
        assert_ne!(
            ContractId::named::<dyn Surface>("primary"),
            ContractId::named::<dyn Surface>("secondary")
        );
        assert_eq!(
            ContractId::named::<dyn Surface>("primary"),
            ContractId::named::<dyn Surface>("primary")
        );
    }

    #[test]
    fn distinct_traits_differ() {
        assert_ne!(
            ContractId::of::<dyn Surface>(),
            ContractId::of::<dyn Sink>()
        );
        assert_ne!(
            ContractRef::of::<dyn Surface>(),
            ContractRef::of::<dyn Sink>()
        );
    }

    #[test]
    fn refs_compare_on_id() {
        let a = ContractRef::of::<dyn Surface>();
        let b = ContractRef::of::<dyn Surface>();
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
        assert_ne!(a, ContractRef::named::<dyn Surface>("secondary"));
    }

    #[test]
    fn ref_keys_a_map() {
        let mut map = HashMap::new();
        map.insert(ContractRef::of::<dyn Surface>(), 1);
        map.insert(ContractRef::named::<dyn Surface>("secondary"), 2);
        map.insert(ContractRef::of::<dyn Surface>(), 3);

        assert_eq!(map.len(), 2);
        assert_eq!(map[&ContractRef::of::<dyn Surface>()], 3);
    }

    #[test]
    fn ref_reports_type_name() {
        let reference = ContractRef::named::<dyn Surface>("secondary");
        assert!(reference.type_name().contains("Surface"));
        assert_eq!(reference.name(), Some("secondary"));
        assert_eq!(ContractRef::of::<dyn Surface>().name(), None);
        assert!(format!("{reference:?}").contains("secondary"));
        assert!(reference.to_string().ends_with("#secondary"));
    }

    #[test]
    fn ref_converts_to_id() {
        let id: ContractId = ContractRef::of::<dyn Sink>().into();
        assert_eq!(id, ContractId::of::<dyn Sink>());
    }

    #[test]
    fn ref_is_thread_safe() {
        fn assert_bounds<T: Send + Sync + 'static>() {}
        assert_bounds::<ContractRef>();
        assert_bounds::<ContractId>();
        assert_bounds::<ExtensionId>();
    }

    #[test]
    fn extension_id_of() {
        assert_eq!(ExtensionId::of::<Marker>(), ExtensionId::of::<Marker>());
        assert_ne!(ExtensionId::of::<Marker>(), ExtensionId::of::<u8>());
        assert!(ExtensionId::of::<Marker>().to_string().contains("Marker"));
    }

    #[test]
    fn unit_ids_display() {
        assert_eq!(BundleId::new("alpha", 0).to_string(), "alpha#0");
        assert_eq!(ComponentId::new("beta", 7).to_string(), "beta#7");
        assert_eq!(RunnableId::new("gamma", 12).to_string(), "gamma#12");
    }

    #[test]
    fn index_separates_same_name() {
        let first = ComponentId::new("alpha", 0);
        let second = ComponentId::new("alpha", 1);

        assert_ne!(first, second);
        assert_eq!(first.name(), second.name());
        assert_eq!(second.index(), 1);

        let set: HashSet<_> = [first, second, ComponentId::new("alpha", 0)].into();
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn unit_id_is_const() {
        const ID: BundleId = BundleId::new("alpha", 3);
        static IDS: &[ComponentId] = &[ComponentId::new("beta", 0)];

        assert_eq!(ID.index(), 3);
        assert_eq!(IDS[0].name(), "beta");
    }
}
