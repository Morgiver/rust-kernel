//! What a ledger promises. Not who keeps it, not what it writes to.
//!
//! This crate is one feature's public surface. It depends on `kernel-core` and
//! on nothing else — no runtime, no other feature — which is what lets any
//! crate name [`Ledger`] without pulling in the bundle that provides it, and
//! what lets this crate compile in a build that has no async runtime at all.
//!
//! Three kinds of thing live here, and they are here for the same reason:
//! each one appears in more than one feature's code.
//!
//! * [`Ledger`] — the contract, resolved from the container as
//!   `Arc<dyn Ledger>`;
//! * [`Entry`] and [`LedgerError`] — the types that contract's signatures
//!   name, so a caller cannot use the trait without them;
//! * [`OpeningNote`] — an extension point the ledger reads at boot and any
//!   feature may contribute to.
//!
//! Nothing here belongs to the kernel: `ledger` is this application's
//! vocabulary and the kernel never learns the word.

use kernel_core::{BoxFuture, Extension};

/// One line to append to a ledger.
///
/// It crosses a feature boundary — whoever calls [`Ledger::append`] builds one
/// — so it is published with the contract rather than kept by the
/// implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// What the line refers to, in the caller's own vocabulary.
    pub reference: String,
    /// The signed amount, in the smallest unit the application counts in.
    pub amount: i64,
}

impl Entry {
    /// An entry for `reference`, worth `amount`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ledger_contracts::Entry;
    ///
    /// let entry = Entry::new("order-1", 250);
    /// assert_eq!(entry.amount, 250);
    /// ```
    #[must_use]
    pub fn new(reference: impl Into<String>, amount: i64) -> Self {
        Self {
            reference: reference.into(),
            amount,
        }
    }
}

/// Why a ledger call did not do what was asked.
///
/// The kernel has no opinion on domain failures: its error model covers
/// registration, resolution, boot, run and shutdown, and stops there. A
/// contract that can fail therefore publishes its own error type, and the
/// implementation converts it into a kernel error only at the lifecycle
/// boundaries where a kernel error is what is expected.
#[derive(Debug)]
pub enum LedgerError {
    /// The ledger is not open: the call came before boot or after shutdown.
    Closed,
    /// The ledger is open and refused this entry, for the reason given.
    Rejected(String),
}

impl core::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Closed => write!(f, "ledger is closed"),
            Self::Rejected(reason) => write!(f, "ledger rejected the entry: {reason}"),
        }
    }
}

impl std::error::Error for LedgerError {}

/// Somewhere entries can be appended and counted.
///
/// # Why the futures are boxed
///
/// An `async fn` in a trait returns a different opaque type per implementation,
/// which makes the trait unusable behind `dyn`. This contract is resolved as
/// `Arc<dyn Ledger>` — that is the entire point of publishing it — so its
/// asynchronous methods return [`BoxFuture`] instead. One allocation per call,
/// and the trait stays dyn-compatible.
///
/// # Examples
///
/// The consumer's half of the contract: it names the trait, never an
/// implementation.
///
/// ```
/// use std::sync::Arc;
///
/// use ledger_contracts::{Entry, Ledger, LedgerError};
///
/// async fn write_one(ledger: Arc<dyn Ledger>) -> Result<u64, LedgerError> {
///     ledger.append(Entry::new("order-1", 250)).await
/// }
/// ```
pub trait Ledger: Send + Sync + 'static {
    /// Appends one entry and answers with its number, counting from one.
    fn append(&self, entry: Entry) -> BoxFuture<'_, Result<u64, LedgerError>>;

    /// How many entries have been appended so far.
    fn count(&self) -> BoxFuture<'_, Result<u64, LedgerError>>;
}

/// A line a feature wants written at the top of the ledger.
///
/// The extension point. The ledger declares it and collects it once, while it
/// boots; any bundle may contribute to it while it registers; neither side
/// names the other. A contributor depends on this crate — never on the bundle
/// that declared the point — which is why the type is published here.
///
/// A point that is declared and never contributed to collects as an empty
/// list. Contributing to a point nobody declared is a graph error, reported
/// before anything boots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpeningNote(pub String);

impl Extension for OpeningNote {}

#[cfg(test)]
mod tests {
    use core::task::{Context, Poll, Waker};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// A ledger that only counts, so the contract can be exercised without a
    /// runtime, a container or an implementation crate.
    struct Counter(AtomicU64);

    impl Ledger for Counter {
        fn append(&self, _entry: Entry) -> BoxFuture<'_, Result<u64, LedgerError>> {
            Box::pin(async move { Ok(self.0.fetch_add(1, Ordering::Relaxed) + 1) })
        }

        fn count(&self) -> BoxFuture<'_, Result<u64, LedgerError>> {
            Box::pin(async move { Ok(self.0.load(Ordering::Relaxed)) })
        }
    }

    /// Polls a future that never parks. The contract crate has no runtime and
    /// must not grow one.
    fn drive<T>(mut future: BoxFuture<'_, T>) -> T {
        let mut cx = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("future parked without a runtime"),
        }
    }

    #[test]
    fn resolves_behind_arc() {
        let ledger: Arc<dyn Ledger> = Arc::new(Counter(AtomicU64::new(0)));

        assert_eq!(drive(ledger.append(Entry::new("one", 1))).unwrap(), 1);
        assert_eq!(drive(ledger.append(Entry::new("two", 2))).unwrap(), 2);
        assert_eq!(drive(ledger.count()).unwrap(), 2);
    }

    #[test]
    fn errors_read_plainly() {
        assert_eq!(LedgerError::Closed.to_string(), "ledger is closed");
        assert_eq!(
            LedgerError::Rejected("amount is zero".to_owned()).to_string(),
            "ledger rejected the entry: amount is zero"
        );
    }

    #[test]
    fn note_is_extension() {
        fn contributed<X: Extension>(items: Vec<X>) -> usize {
            items.len()
        }

        assert_eq!(contributed(vec![OpeningNote("opened".to_owned())]), 1);
    }
}
