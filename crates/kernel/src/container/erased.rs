//! Type erasure — the one place `Any` and `downcast` are allowed to appear.
//!
//! # Why the value is boxed twice
//!
//! A container that resolves dynamically has to keep values of different types
//! in one table, which means erasing them. The obvious move —
//! `let erased: Arc<dyn Any + Send + Sync> = value;` where `value` is an
//! `Arc<dyn Contract>` — does not do what it looks like: the coercion replaces
//! the contract's vtable with `Any`'s, and the contract's methods are gone for
//! good. Nothing can bring them back, because nothing recorded which trait was
//! erased.
//!
//! So the stored value is the trait object *itself*, boxed once more:
//! `Arc::new(value)` where `value: Arc<C>`. The concrete type behind the `Any`
//! is `Arc<C>`, which is `Sized` even when `C` is not, so `TypeId` can name it
//! and [`restore`] can downcast to `Arc<Arc<C>>` and clone the inner `Arc` out.
//! The contract's vtable rides along inside that inner `Arc`, untouched.
//!
//! The cost is one extra indirection, paid once per resolution and never on a
//! hot path: a caller that has resolved holds an `Arc<C>` and calls through it
//! directly, without the container in the way.
//!
//! # Why this module is `pub(crate)` and audited
//!
//! Erasure is the only part of the container that can be wrong in a way the
//! type system does not catch. Confining it here means the audit surface is
//! this file, not the crate. Nothing outside it names `Any` or calls
//! `downcast`, with one sanctioned exception: `dispatcher.rs` erases listener
//! CALLS rather than values, and the higher-ranked `&mut E` in a listener
//! signature is not expressible through the value erasure here. Two sites, and
//! the count is the point — a third appearing later is a defect to report.

use core::any::Any;
use std::sync::Arc;

use kernel_core::{BoxFuture, BuildError, ContainerError, ContractRef};

use crate::container::Container;
use crate::provider::BuildFn;

/// A value whose type has been erased, ready to sit in a container table.
pub(crate) type AnyArc = Arc<dyn Any + Send + Sync>;

/// Borrows a value out of a singly-boxed erased item.
///
/// Distinct from [`restore`] on purpose. [`restore`] un-erases a CONTRACT, whose
/// stored concrete type is `Arc<C>` because the contract may be unsized, and
/// hands back an owned `Arc`. An extension contribution is a plain sized value
/// stored as `Box::new(item)`, and its collection is defined to borrow rather
/// than own — owning would force `Clone` on every extension type and make a
/// point collectable exactly once.
///
/// Returns `None` when the item is not an `X`. The caller filters, because a
/// table keyed by [`kernel_core::ExtensionId`] holds several types at once and a
/// non-match is ordinary, not a defect.
pub(crate) fn borrow<X: 'static>(item: &(dyn Any + Send + Sync)) -> Option<&X> {
    item.downcast_ref::<X>()
}

/// Erases `Arc<C>`, where `C` may be unsized.
///
/// The stored concrete type is `Arc<C>`, not `C`: see the module documentation
/// for why that extra box is load-bearing rather than incidental.
pub(crate) fn erase<C: ?Sized + Send + Sync + 'static>(value: Arc<C>) -> AnyArc {
    Arc::new(value)
}

/// Restores an `Arc<C>` erased by [`erase`].
///
/// `contract` is carried only so that a mismatch names something a human can
/// act on. Reaching [`ContainerError::TypeMismatch`] means the table holds a
/// value under a key that does not describe it — a defect in the container, not
/// in the caller.
pub(crate) fn restore<C: ?Sized + Send + Sync + 'static>(
    erased: &AnyArc,
    contract: ContractRef,
) -> Result<Arc<C>, ContainerError> {
    erased
        .downcast_ref::<Arc<C>>()
        .map(Arc::clone)
        .ok_or(ContainerError::TypeMismatch { contract })
}

/// A build whose result type has been erased, as the container stores it.
pub(crate) type ErasedBuild =
    Arc<dyn for<'a> Fn(&'a Container) -> BoxFuture<'a, Result<AnyArc, BuildError>> + Send + Sync>;

/// Erases the result of a [`BuildFn`], leaving the build itself untouched.
pub(crate) fn erase_build<C: ?Sized + Send + Sync + 'static>(build: BuildFn<C>) -> ErasedBuild {
    Arc::new(move |container: &Container| {
        let building = (build)(container);
        Box::pin(async move { building.await.map(erase::<C>) })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn container() -> Container {
        Container::new(
            Vec::new(),
            Arc::new(kernel_core::ConfigTree::empty()),
            Arc::new(kernel_core::NoopTelemetry),
            crate::shutdown::KernelHandle::detached(),
        )
    }

    // The test that matters: `C` is a trait object, so a naive coercion to
    // `Arc<dyn Any>` would have thrown the vtable away.
    #[test]
    fn round_trips_unsized() {
        let value: Arc<dyn Surface> = Arc::new(Plain(7));
        let erased = erase(Arc::clone(&value));

        let restored: Arc<dyn Surface> =
            restore(&erased, ContractRef::of::<dyn Surface>()).expect("restore");

        assert_eq!(restored.mark(), 7);
        assert!(Arc::ptr_eq(&restored, &value));
    }

    #[test]
    fn round_trips_sized() {
        let erased = erase(Arc::new(41_u32));

        let restored: Arc<u32> = restore(&erased, ContractRef::of::<u32>()).expect("restore");

        assert_eq!(*restored, 41);
    }

    #[test]
    fn round_trips_slice() {
        let value: Arc<[u8]> = Arc::from(vec![1, 2, 3]);
        let erased = erase(value);

        let restored: Arc<[u8]> = restore(&erased, ContractRef::of::<[u8]>()).expect("restore");

        assert_eq!(&*restored, &[1, 2, 3]);
    }

    #[test]
    fn wrong_contract_is_rejected() {
        let value: Arc<dyn Surface> = Arc::new(Plain(7));
        let erased = erase(value);

        let restored = restore::<dyn Sink>(&erased, ContractRef::of::<dyn Sink>());

        assert!(matches!(restored, Err(ContainerError::TypeMismatch { .. })));
    }

    // Erasing the *stored* type rather than the contract is exactly the bug
    // this module exists to prevent: it must not be mistakable for a success.
    #[test]
    fn inner_type_unreachable() {
        let value: Arc<dyn Surface> = Arc::new(Plain(7));
        let erased = erase(value);

        assert!(restore::<Plain>(&erased, ContractRef::of::<Plain>()).is_err());
    }

    #[tokio::test]
    async fn erased_build_keeps_vtable() {
        let build: BuildFn<dyn Surface> =
            Box::new(|_container| Box::pin(async { Ok(Arc::new(Plain(3)) as Arc<dyn Surface>) }));
        let erased_build = erase_build(build);

        let produced = erased_build(&container()).await.expect("build");
        let restored: Arc<dyn Surface> =
            restore(&produced, ContractRef::of::<dyn Surface>()).expect("restore");

        assert_eq!(restored.mark(), 3);
    }

    #[tokio::test]
    async fn erased_build_propagates_failure() {
        let build: BuildFn<dyn Surface> = Box::new(|_container| {
            Box::pin(async { Err(BuildError::new("surface", "deliberate".to_owned().into())) })
        });
        let erased_build = erase_build(build);

        assert!(erased_build(&container()).await.is_err());
    }
}
