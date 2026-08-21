//! The collected extension points, as they exist once assembly is over.
//!
//! An extension point is a typed collection: one component declares it for a
//! type, any number of bundles contribute values of that type, and the
//! declaring component reads the whole list once, at boot. The mechanism is
//! generic on purpose — the kernel never names what travels through a point.
//!
//! [`ExtensionPoints`] is the frozen result of that assembly. It is built once,
//! after every bundle has registered, and never changes afterwards: there is no
//! way to contribute to it, which is what makes the list a component reads at
//! boot the same list it would read at any later moment.
//!
//! # Order
//!
//! Contributions come out in bundle registration order. The registry stamps
//! every contribution with the position it was recorded at, and collection
//! replays those positions, so two runs of the same assembly hand the
//! collecting component the same list in the same order. An order that depended
//! on a hash map's iteration would be reproducible only by accident.
//!
//! # What is not checked here
//!
//! A declared point with no contribution is valid, and collects as an empty
//! list — a component must handle nothing being plugged in.
//!
//! Contributing to a point nobody declared is an error, but it is not detected
//! here: it is reported during resolution, alongside every other assembly
//! error, before anything is built. By the time this type exists that check has
//! already run.
//!
//! # Erasure
//!
//! Contributions of every type share one table, so they are stored erased.
//! Un-erasing them is the one thing here the type system cannot check, and this
//! module does not do it itself: it goes through the audited erasure module, so
//! that the places able to get a type wrong stay countable. What this module
//! owns is the invariant that makes the restore safe — the table is keyed by
//! the very type identity a restore asks for, and only
//! `from_parts` can write to it, so a key can
//! never name a type its values do not have.

use core::any::TypeId;
use core::fmt;
use std::collections::HashMap;

use kernel_core::{Extension, ExtensionId};

use crate::container::erased;
use crate::registry::ContributionEntry;

/// Every extension point declared during assembly, with the contributions made
/// to it.
///
/// # Examples
///
/// ```
/// use kernel::core::Extension;
///
/// /// The kind of value some component collects at boot.
/// pub struct Alpha {
///     /// Label the collecting component reads.
///     pub label: &'static str,
/// }
///
/// impl Extension for Alpha {}
///
/// # fn collect(points: &kernel::ExtensionPoints) {
/// let items: Vec<&Alpha> = points.collect::<Alpha>();
/// let labels: Vec<&str> = items.iter().map(|item| item.label).collect();
/// # let _ = labels;
/// # }
/// ```
pub struct ExtensionPoints {
    declared: HashMap<TypeId, ExtensionId>,
    items: HashMap<TypeId, Vec<ContributionEntry>>,
}

impl ExtensionPoints {
    /// Every contribution to the point `X`, in bundle registration order.
    ///
    /// The items are **borrowed**, not owned. Owning them would force `Clone`
    /// on every extension type, and would let a point be collected only once —
    /// a component that needs to keep an item past boot clones that item
    /// itself, which is a decision belonging to the component and not to the
    /// mechanism.
    ///
    /// A declared point with no contribution collects as an empty vector, and
    /// so does a point nobody declared: whether the point exists is
    /// [`is_declared`](Self::is_declared)'s question, and contributing to one
    /// that does not is an error reported long before this call.
    #[must_use]
    pub fn collect<X: Extension>(&self) -> Vec<&X> {
        let Some(items) = self.items.get(&TypeId::of::<X>()) else {
            return Vec::new();
        };

        items
            .iter()
            // Infallible by construction: an entry sits under the key its own
            // `ExtensionId` names, and nothing outside `from_parts` can add
            // one. The `filter_map` is how that invariant is spelled, not a
            // case that occurs.
            .filter_map(|entry| erased::borrow::<X>(entry.item.as_ref()))
            .collect()
    }

    /// Whether some bundle declared the point `X`.
    #[must_use]
    pub fn is_declared<X: Extension>(&self) -> bool {
        self.declared.contains_key(&TypeId::of::<X>())
    }

    /// How many contributions the point `X` received.
    ///
    /// Counting does not restore anything, so it stays constant-time however
    /// long the list is.
    #[must_use]
    pub fn count<X: Extension>(&self) -> usize {
        self.items
            .get(&TypeId::of::<X>())
            .map_or(0, |items| items.len())
    }

    /// Builds the frozen collection from what the registry recorded.
    ///
    /// Contributions are ordered by the position the registry stamped them
    /// with, so the result does not depend on the order the entries happen to
    /// arrive in. The sort is stable, which keeps two entries stamped alike in
    /// the order they were recorded.
    pub(crate) fn from_parts(
        declared: Vec<ExtensionId>,
        mut contributions: Vec<ContributionEntry>,
    ) -> Self {
        contributions.sort_by_key(|entry| entry.order);

        let mut items: HashMap<TypeId, Vec<ContributionEntry>> = HashMap::new();
        for entry in contributions {
            items
                .entry(entry.extension.type_id)
                .or_default()
                .push(entry);
        }

        Self {
            declared: declared
                .into_iter()
                .map(|extension| (extension.type_id, extension))
                .collect(),
            items,
        }
    }
}

impl fmt::Debug for ExtensionPoints {
    /// Renders one entry per declared point, `name: count`, sorted by name so
    /// that the rendering does not inherit a hash map's iteration order.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut points: Vec<(&'static str, usize)> = self
            .declared
            .iter()
            .map(|(type_id, extension)| {
                (
                    extension.type_name,
                    self.items.get(type_id).map_or(0, Vec::len),
                )
            })
            .collect();
        points.sort_unstable();

        let mut rendered = f.debug_struct("ExtensionPoints");
        for (name, count) in points {
            rendered.field(name, &count);
        }
        rendered.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Alpha(&'static str);

    impl Extension for Alpha {}

    struct Beta(u8);

    impl Extension for Beta {}

    struct Gamma;

    impl Extension for Gamma {}

    fn contribution<X: Extension>(bundle: &'static str, order: u32, item: X) -> ContributionEntry {
        ContributionEntry {
            extension: ExtensionId::of::<X>(),
            bundle,
            order,
            item: Box::new(item),
        }
    }

    fn labels(points: &ExtensionPoints) -> Vec<&'static str> {
        points
            .collect::<Alpha>()
            .iter()
            .map(|item| item.0)
            .collect()
    }

    #[test]
    fn collects_in_order() {
        let points = ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>()],
            vec![
                contribution("first", 0, Alpha("one")),
                contribution("second", 1, Alpha("two")),
                contribution("second", 2, Alpha("three")),
            ],
        );

        assert_eq!(labels(&points), ["one", "two", "three"]);
    }

    // The stamped position rules, not the order the entries arrive in: the
    // result has to be the same whichever way the registry hands them over.
    #[test]
    fn order_follows_stamp() {
        let points = ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>()],
            vec![
                contribution("third", 7, Alpha("three")),
                contribution("first", 2, Alpha("one")),
                contribution("second", 5, Alpha("two")),
            ],
        );

        assert_eq!(labels(&points), ["one", "two", "three"]);
    }

    #[test]
    fn declared_point_is_empty() {
        let points = ExtensionPoints::from_parts(vec![ExtensionId::of::<Alpha>()], Vec::new());

        assert!(points.is_declared::<Alpha>());
        assert!(points.collect::<Alpha>().is_empty());
        assert_eq!(points.count::<Alpha>(), 0);
    }

    #[test]
    fn undeclared_point_is_empty() {
        let points = ExtensionPoints::from_parts(vec![ExtensionId::of::<Alpha>()], Vec::new());

        assert!(!points.is_declared::<Beta>());
        assert!(points.collect::<Beta>().is_empty());
        assert_eq!(points.count::<Beta>(), 0);
    }

    #[test]
    fn separates_types() {
        let points = ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>(), ExtensionId::of::<Beta>()],
            vec![
                contribution("first", 0, Alpha("one")),
                contribution("first", 1, Beta(9)),
                contribution("second", 2, Alpha("two")),
            ],
        );

        assert_eq!(labels(&points), ["one", "two"]);
        assert_eq!(
            points
                .collect::<Beta>()
                .iter()
                .map(|item| item.0)
                .collect::<Vec<_>>(),
            [9]
        );
    }

    #[test]
    fn counts_contributions() {
        let points = ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>(), ExtensionId::of::<Beta>()],
            vec![
                contribution("first", 0, Alpha("one")),
                contribution("first", 1, Alpha("two")),
                contribution("first", 2, Beta(1)),
            ],
        );

        assert_eq!(points.count::<Alpha>(), 2);
        assert_eq!(points.count::<Beta>(), 1);
        assert_eq!(points.count::<Gamma>(), 0);
    }

    // Collecting twice is a plain read: the second caller sees the same list,
    // which is what borrowing rather than owning buys.
    #[test]
    fn collects_repeatedly() {
        let points = ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>()],
            vec![contribution("first", 0, Alpha("one"))],
        );

        assert_eq!(labels(&points), ["one"]);
        assert_eq!(labels(&points), ["one"]);
    }

    // A contribution to a point nobody declared is kept, not dropped: catching
    // it belongs to resolution, and a silent drop here would leave that check
    // with nothing to report.
    #[test]
    fn keeps_undeclared_contribution() {
        let points =
            ExtensionPoints::from_parts(Vec::new(), vec![contribution("first", 0, Alpha("one"))]);

        assert!(!points.is_declared::<Alpha>());
        assert_eq!(points.count::<Alpha>(), 1);
    }

    #[test]
    fn debug_lists_points() {
        let points = ExtensionPoints::from_parts(
            vec![ExtensionId::of::<Alpha>()],
            vec![contribution("first", 0, Alpha("one"))],
        );

        let rendered = format!("{points:?}");

        assert!(rendered.contains("Alpha"));
        assert!(rendered.contains('1'));
    }
}
