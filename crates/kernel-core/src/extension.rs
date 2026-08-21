//! Extension points: typed collections declared by one component and filled by
//! any number of bundles.
//!
//! An extension point is the generic mechanism behind every "plug your things
//! in here" list a system needs. One component declares the point for a type
//! `X`, any bundle contributes values of `X` to it, and the declaring component
//! collects the whole list once, at boot, in a deterministic order — the
//! registration order of the contributing bundles.
//!
//! Three rules make it predictable:
//!
//! - contributing to a point nobody declared is a resolution error, reported
//!   before anything boots, not a silently dropped value;
//! - a declared point with zero contributions is valid, and collects as an
//!   empty list;
//! - collection order is the bundle registration order, so two runs of the same
//!   assembly produce the same order.
//!
//! # The kernel never defines an extension type
//!
//! This is the altitude rule, stated at the exact place where it is most
//! tempting to break it. This crate provides the [`Extension`] marker and
//! nothing else. It does not define, and must never define, the types that
//! travel through extension points: a route, a command-line verb, a scheduled
//! task, a migration, an interceptor. Each of those names a transport, a
//! delivery channel or a domain concern, and each is defined by the component
//! that consumes it, in that component's own contracts crate. A type named
//! after any of them appearing in this crate is a defect, not a convenience.
//!
//! The single mechanism-level exception is the health probe in
//! [`crate::health`]: it carries no domain or transport meaning, only the
//! kernel's own aggregation of liveness, and the kernel is the component that
//! consumes it.

/// Marker for a value that can be contributed to an extension point.
///
/// It has no methods. Implementing it is a deliberate statement that this type
/// is meant to be collected: the bound is what lets the kernel store
/// contributions and hand them to the declaring component, and the absence of
/// a blanket implementation is what keeps arbitrary values from being swept
/// into a collection by accident.
///
/// The supertraits are load-bearing. Contributions are registered during
/// assembly, stored for the whole life of the process and read from any task,
/// so they must be `Send + Sync + 'static`.
///
/// # Examples
///
/// A component defines the type it will consume, in its own crate, and marks it
/// as collectable:
///
/// ```
/// use kernel_core::extension::Extension;
///
/// /// One entry in the list some component collects at boot.
/// pub struct Alpha {
///     /// Label the consuming component reads.
///     pub label: &'static str,
/// }
///
/// impl Extension for Alpha {}
///
/// fn collected<X: Extension>(items: Vec<X>) -> usize {
///     items.len()
/// }
///
/// assert_eq!(collected(vec![Alpha { label: "one" }, Alpha { label: "two" }]), 2);
/// ```
///
/// Declaration and contribution happen against the runtime registry, which
/// lives outside this crate:
///
/// ```ignore
/// registry.declare_extension_point::<Alpha>();
/// registry.contribute::<Alpha>(Alpha { label: "one" });
/// ```
pub trait Extension: Send + Sync + 'static {}

#[cfg(test)]
mod tests {
    use super::*;

    struct Alpha(&'static str);

    impl Extension for Alpha {}

    fn assert_bounds<X: Extension>() {}

    #[test]
    fn marker_carries_bounds() {
        assert_bounds::<Alpha>();
    }

    #[test]
    fn marker_is_object_safe() {
        let items: Vec<Box<dyn Extension>> = vec![Box::new(Alpha("one")), Box::new(Alpha("two"))];
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn keeps_contribution_order() {
        let items = [Alpha("one"), Alpha("two"), Alpha("three")];
        let labels: Vec<&str> = items.iter().map(|a| a.0).collect();
        assert_eq!(labels, ["one", "two", "three"]);
    }
}
