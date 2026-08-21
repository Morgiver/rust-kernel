//! The boxed future alias used by every asynchronous kernel surface, and the
//! one await point that needs no runtime.
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
//!
//! [`yield_now`] is here for the other half of the same problem: a unit that
//! loops must offer an await point, or nothing outside it — a deadline, a
//! shutdown signal, another task — ever gets to run. Taking a runtime
//! dependency for that alone would put an executor in the dependency graph of
//! crates that never name one, so the yield is written here, in twelve lines
//! of `core`.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

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

/// Yields control once, then completes.
///
/// The first poll wakes the waker and returns [`Poll::Pending`]; the next one
/// returns [`Poll::Ready`]. That is the whole contract: a loop that awaits it
/// each turn gives the executor an opportunity to run everything else — a
/// deadline, a shutdown, a sibling task — without depending on a runtime to
/// provide the await point.
///
/// The future re-arms nothing: awaiting it again yields again, because each
/// call builds a new one.
///
/// ```
/// use kernel_core::future::yield_now;
///
/// async fn drain(items: &mut Vec<u8>) -> usize {
///     let mut seen = 0;
///     while items.pop().is_some() {
///         seen += 1;
///         // Without this, a long drain never lets a deadline fire.
///         yield_now().await;
///     }
///     seen
/// }
/// ```
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// The future returned by [`yield_now`].
#[derive(Debug)]
#[must_use = "a future does nothing unless it is awaited"]
pub struct YieldNow {
    /// Whether the single pending poll has already happened.
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Wake, Waker};

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

    /// A waker that records whether it was woken, built from safe code alone.
    #[derive(Default)]
    struct Recording(AtomicBool);

    impl Recording {
        fn woken(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for Recording {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Polls to completion on nothing, counting the polls it took.
    fn spin<T>(future: impl Future<Output = T>) -> (T, usize) {
        let mut future = Box::pin(future);
        let mut cx = Context::from_waker(Waker::noop());
        for polls in 1.. {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return (value, polls);
            }
        }
        unreachable!("the loop returns or runs forever")
    }

    #[test]
    fn yields_once() {
        let recorder = Arc::new(Recording::default());
        let waker = Waker::from(Arc::clone(&recorder));
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(yield_now());

        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert!(recorder.woken(), "a pending yield that never wakes hangs");
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn yield_composes() {
        let (value, polls) = spin(async {
            let mut total = 0;
            for step in 1..=3 {
                total += step;
                yield_now().await;
            }
            total
        });
        assert_eq!(value, 6);
        assert_eq!(polls, 4);
    }

    #[test]
    fn yield_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&yield_now());
        let boxed: BoxFuture<'static, ()> = Box::pin(yield_now());
        assert_send(&boxed);
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
