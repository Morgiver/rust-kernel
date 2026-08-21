//! The boxed future alias used by every asynchronous kernel surface.
//!
//! Rust has no dyn-compatible `async fn` in traits: an `async fn` declared in a
//! trait returns an opaque, per-implementation type, which makes the trait
//! unusable behind `dyn`. Every kernel surface is held as a trait object, so
//! asynchronous methods return an explicitly boxed future instead.
//!
//! The cost is one heap allocation per call. It is paid only at lifecycle
//! boundaries — boot, shutdown, build, dispatch — which happen tens of times
//! over the life of a process, not millions. Once a caller holds a resolved
//! `Arc<dyn Trait>` it calls it directly, so no hot path crosses this alias.

use core::future::Future;
use core::pin::Pin;

/// A heap-allocated, pinned future that can be sent across threads.
///
/// `'a` is the lifetime the future may borrow from — typically the lifetime of
/// the `&self` receiver that produced it — and `T` is what it resolves to.
///
/// # Examples
///
/// ```
/// use kernel_core::future::BoxFuture;
///
/// trait Handle: Send + Sync + 'static {
///     fn call<'a>(&'a self, input: &'a str) -> BoxFuture<'a, usize>;
/// }
///
/// struct Unit;
///
/// impl Handle for Unit {
///     fn call<'a>(&'a self, input: &'a str) -> BoxFuture<'a, usize> {
///         Box::pin(async move { input.len() })
///     }
/// }
///
/// // The point of the alias: the trait stays usable behind `dyn`.
/// let handle: &dyn Handle = &Unit;
/// let _future = handle.call("abc");
/// ```
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll, Waker};

    /// Drives a future to completion without a runtime; the futures under test
    /// never yield, so a single poll is enough.
    fn drive<T>(mut future: BoxFuture<'_, T>) -> T {
        let mut cx = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("future parked without a runtime"),
        }
    }

    fn borrowing(input: &str) -> BoxFuture<'_, usize> {
        Box::pin(async move { input.len() })
    }

    #[test]
    fn resolves_value() {
        let future: BoxFuture<'static, u8> = Box::pin(async { 7 });
        assert_eq!(drive(future), 7);
    }

    #[test]
    fn borrows_receiver() {
        let owned = String::from("abcd");
        assert_eq!(drive(borrowing(&owned)), 4);
    }

    #[test]
    fn is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let future: BoxFuture<'static, ()> = Box::pin(async {});
        assert_send(&future);
    }

    #[test]
    fn works_behind_dyn() {
        trait Surface: Send + Sync + 'static {
            fn go(&self) -> BoxFuture<'_, i32>;
        }

        struct Unit(i32);

        impl Surface for Unit {
            fn go(&self) -> BoxFuture<'_, i32> {
                Box::pin(async move { self.0 * 2 })
            }
        }

        let surface: Box<dyn Surface> = Box::new(Unit(21));
        assert_eq!(drive(surface.go()), 42);
    }
}
