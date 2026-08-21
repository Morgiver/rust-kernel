//! Where records are sent. One contract, and the name of its second binding.
//!
//! Like every `*-contracts` crate here, this one depends on `kernel-core` and
//! on nothing else. It is the crate a feature depends on when it wants to be
//! audited; the crate that *implements* auditing is not on anybody's dependency
//! list but the application's.
//!
//! # This contract has two implementations
//!
//! [`Sink`] is the example's demonstration that a contract is not a singleton.
//! The audit feature binds it twice: once as the default binding, once under
//! the name [`ARCHIVE`]. A consumer therefore has three ways to ask, and they
//! mean different things:
//!
//! * `container.get::<dyn Sink>()` — the default binding, for a caller that
//!   wants *a* sink and does not care which;
//! * `container.get_named::<dyn Sink>(ARCHIVE)` — that one specifically, for a
//!   caller that means the archive and nothing else;
//! * `container.get_all::<dyn Sink>()` — every binding, in registration order,
//!   for a caller that wants a record to reach all of them.
//!
//! Two *unnamed* bindings of the same contract are a graph error, not a silent
//! overwrite. That is why the second binding has a name, and why the name is
//! published here rather than spelled out at both ends.

use kernel_core::BoxFuture;

/// The name of the second binding of [`Sink`].
///
/// It is a constant, in the contracts crate, because it is used twice in two
/// different crates: once where the binding is registered and once where it is
/// resolved. A string literal typed at both ends would be a decoupling that
/// only holds until someone fixes a typo on one side.
pub const ARCHIVE: &str = "archive";

/// Something worth writing down.
///
/// Unrelated to `kernel_core::telemetry::Record`, which carries the kernel's
/// own diagnostics. This one is the application's: a domain fact, produced by
/// a feature and kept by whoever the audit sinks write to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The feature that produced the record, as it names itself.
    pub source: &'static str,
    /// What happened, in one line.
    pub message: String,
}

impl Record {
    /// A record from `source` saying `message`.
    ///
    /// # Examples
    ///
    /// ```
    /// use audit_contracts::Record;
    ///
    /// let record = Record::new("orders", "order-1 placed");
    /// assert_eq!(record.source, "orders");
    /// ```
    #[must_use]
    pub fn new(source: &'static str, message: impl Into<String>) -> Self {
        Self {
            source,
            message: message.into(),
        }
    }
}

/// Why a record was not written.
///
/// A sink that cannot write is a domain failure, not a kernel one: the kernel's
/// error model has nothing to say about it. Publishing the type here is what
/// lets a caller match on the reason without knowing which sink it is holding.
#[derive(Debug)]
pub enum SinkError {
    /// The sink is not accepting records: not open yet, or already closed.
    Unavailable,
    /// The sink is open and refused this record, for the reason given.
    Rejected(String),
}

impl core::fmt::Display for SinkError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unavailable => write!(f, "sink is unavailable"),
            Self::Rejected(reason) => write!(f, "sink rejected the record: {reason}"),
        }
    }
}

impl std::error::Error for SinkError {}

/// Somewhere a record can be written.
///
/// Resolved as `Arc<dyn Sink>`, so [`write`](Sink::write) returns a
/// [`BoxFuture`] rather than being an `async fn`: a trait with an `async fn`
/// cannot be used behind `dyn`, and this one has to be.
///
/// # Examples
///
/// A caller that wants every sink to see the record holds them all and does
/// not care how many there are.
///
/// ```
/// use std::sync::Arc;
///
/// use audit_contracts::{Record, Sink, SinkError};
///
/// async fn write_everywhere(sinks: &[Arc<dyn Sink>]) -> Result<(), SinkError> {
///     for sink in sinks {
///         sink.write(Record::new("orders", "order-1 placed")).await?;
///     }
///     Ok(())
/// }
/// ```
pub trait Sink: Send + Sync + 'static {
    /// Writes one record.
    fn write(&self, record: Record) -> BoxFuture<'_, Result<(), SinkError>>;
}

#[cfg(test)]
mod tests {
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A sink that keeps what it is given, so the contract can be exercised
    /// without a runtime or an implementation crate.
    #[derive(Default)]
    struct Held(Mutex<Vec<Record>>);

    impl Sink for Held {
        fn write(&self, record: Record) -> BoxFuture<'_, Result<(), SinkError>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(record);
                Ok(())
            })
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
    fn writes_behind_arc() {
        let sink: Arc<dyn Sink> = Arc::new(Held::default());
        drive(sink.write(Record::new("orders", "placed"))).unwrap();

        let all: Vec<Arc<dyn Sink>> = vec![Arc::clone(&sink), Arc::new(Held::default())];
        for one in &all {
            drive(one.write(Record::new("orders", "again"))).unwrap();
        }
    }

    #[test]
    fn archive_name_is_shared() {
        assert_eq!(ARCHIVE, "archive");
    }

    #[test]
    fn errors_read_plainly() {
        assert_eq!(SinkError::Unavailable.to_string(), "sink is unavailable");
        assert_eq!(
            SinkError::Rejected("record is empty".to_owned()).to_string(),
            "sink rejected the record: record is empty"
        );
    }
}
