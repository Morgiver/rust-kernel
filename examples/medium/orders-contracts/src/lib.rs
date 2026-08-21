//! What the order book promises, and what it announces.
//!
//! Depends on `kernel-core` and on nothing else — not on the runtime crate,
//! not on the other features' contracts, and above all not on the bundle that
//! implements any of this.
//!
//! # Why the event is declared here and not in the bundle
//!
//! [`OrderPlaced`] is emitted by the orders feature, and listened to by other
//! features. A listener has to *name the type* to register for it: dispatch is
//! indexed by type identity, never by a string, so there is no way to subscribe
//! to "order.placed" without the type in hand.
//!
//! If the type lived in the bundle that emits it, every listener would have to
//! depend on that bundle — and a `*-bundle` crate is exactly what another
//! feature must never depend on. Publishing the event beside the contract is
//! what keeps the emitter replaceable: swap the implementation, keep the
//! listeners, recompile nothing but the bundle.
//!
//! This is the whole reason a contracts crate exists. Anything that appears in
//! two features' code — a trait, a payload, an error, an extension point, an
//! event — belongs in one of these crates, and nothing else does.

use kernel_core::{BoxFuture, BoxSource, Event};

/// An order somebody wants placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    /// What the order is called, in the caller's own vocabulary.
    pub reference: String,
    /// The amount it is worth, in the smallest unit the application counts in.
    pub amount: i64,
}

impl Order {
    /// An order named `reference`, worth `amount`.
    ///
    /// # Examples
    ///
    /// ```
    /// use orders_contracts::Order;
    ///
    /// let order = Order::new("order-1", 250);
    /// assert_eq!(order.reference, "order-1");
    /// ```
    #[must_use]
    pub fn new(reference: impl Into<String>, amount: i64) -> Self {
        Self {
            reference: reference.into(),
            amount,
        }
    }
}

/// Why an order was not placed.
///
/// [`Downstream`](OrderError::Downstream) carries a boxed source rather than a
/// typed one on purpose: whatever the order book fails against — a ledger, a
/// queue, a remote service — is another feature's error type, and naming it
/// here would make this crate depend on that feature. The box keeps the cause
/// reachable through [`std::error::Error::source`] without the dependency.
#[derive(Debug)]
pub enum OrderError {
    /// The book is open and refused this order, for the reason given.
    Rejected(String),
    /// Something the book depends on failed.
    Downstream(BoxSource),
}

impl OrderError {
    /// Wraps whatever the book failed against.
    #[must_use]
    pub fn downstream(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Downstream(Box::new(source))
    }
}

impl core::fmt::Display for OrderError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Rejected(reason) => write!(f, "order rejected: {reason}"),
            Self::Downstream(source) => write!(f, "order failed downstream: {source}"),
        }
    }
}

impl std::error::Error for OrderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(_) => None,
            Self::Downstream(source) => Some(source.as_ref()),
        }
    }
}

/// Somewhere orders can be placed and counted.
///
/// Resolved as `Arc<dyn OrderBook>`, which is why the asynchronous methods
/// return a [`BoxFuture`] instead of being `async fn`: an `async fn` in a trait
/// makes that trait unusable behind `dyn`.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use orders_contracts::{Order, OrderBook, OrderError};
///
/// async fn place_one(book: Arc<dyn OrderBook>) -> Result<u64, OrderError> {
///     book.place(Order::new("order-1", 250)).await
/// }
/// ```
pub trait OrderBook: Send + Sync + 'static {
    /// Places one order and answers with its sequence number in this book,
    /// counting from one.
    fn place(&self, order: Order) -> BoxFuture<'_, Result<u64, OrderError>>;

    /// How many orders have been placed so far.
    fn placed(&self) -> BoxFuture<'_, Result<u64, OrderError>>;
}

/// An order made it into the book.
///
/// Dispatched sequentially rather than emitted, so that listeners run before
/// the emitter continues and what they leave in [`notes`](OrderPlaced::notes)
/// travels back with the event. A listener that has nothing to add returns
/// `Flow::Continue`; one that decides nobody else should see this event returns
/// `Flow::Stop`, and stopping is not an error.
///
/// # Examples
///
/// A listener names this type, and no crate of the orders feature.
///
/// ```
/// use kernel_core::Event;
/// use orders_contracts::{Order, OrderPlaced};
///
/// let mut event = OrderPlaced {
///     order: Order::new("order-1", 250),
///     sequence: 1,
///     notes: Vec::new(),
/// };
/// event.notes.push("audited".to_owned());
///
/// assert_eq!(OrderPlaced::NAME, "orders.placed");
/// assert_eq!(event.notes.len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderPlaced {
    /// The order as it was placed.
    pub order: Order,
    /// Its sequence number in the book that placed it.
    pub sequence: u64,
    /// What listeners had to say. Empty until one of them adds a line.
    pub notes: Vec<String>,
}

impl Event for OrderPlaced {
    /// Diagnostics only: dispatch routes on the type, never on this string.
    const NAME: &'static str = "orders.placed";
}

#[cfg(test)]
mod tests {
    use core::task::{Context, Poll, Waker};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// A book that only counts, so the contract can be exercised without a
    /// runtime or an implementation crate.
    struct Counter(AtomicU64);

    impl OrderBook for Counter {
        fn place(&self, _order: Order) -> BoxFuture<'_, Result<u64, OrderError>> {
            Box::pin(async move { Ok(self.0.fetch_add(1, Ordering::Relaxed) + 1) })
        }

        fn placed(&self) -> BoxFuture<'_, Result<u64, OrderError>> {
            Box::pin(async move { Ok(self.0.load(Ordering::Relaxed)) })
        }
    }

    /// Polls a future that never parks; this crate has no runtime.
    fn drive<T>(mut future: BoxFuture<'_, T>) -> T {
        let mut cx = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("future parked without a runtime"),
        }
    }

    #[test]
    fn places_behind_arc() {
        let book: Arc<dyn OrderBook> = Arc::new(Counter(AtomicU64::new(0)));

        assert_eq!(drive(book.place(Order::new("one", 1))).unwrap(), 1);
        assert_eq!(drive(book.placed()).unwrap(), 1);
    }

    #[test]
    fn listeners_leave_notes() {
        let mut event = OrderPlaced {
            order: Order::new("order-1", 250),
            sequence: 7,
            notes: Vec::new(),
        };

        event.notes.push("audited".to_owned());

        assert_eq!(event.sequence, 7);
        assert_eq!(event.notes, ["audited"]);
    }

    #[test]
    fn downstream_keeps_cause() {
        #[derive(Debug)]
        struct Elsewhere;

        impl core::fmt::Display for Elsewhere {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "it broke")
            }
        }

        impl std::error::Error for Elsewhere {}

        let error = OrderError::downstream(Elsewhere);

        assert_eq!(error.to_string(), "order failed downstream: it broke");
        assert!(std::error::Error::source(&error).is_some());
        assert!(std::error::Error::source(&OrderError::Rejected("no".to_owned())).is_none());
    }
}
