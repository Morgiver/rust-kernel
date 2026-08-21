//! What an acceptor needs from whoever answers. Not the socket, not the loop.
//!
//! This crate is the only thing the gateway feature and the worker feature
//! share. It depends on `kernel-core` and on nothing else — no runtime, no
//! other feature, and above all no `*-bundle` — so [`Handler`] can be named by
//! a crate that will never accept a connection, and by a crate that will never
//! open one.
//!
//! Three things live here, each because it appears in both features' code:
//!
//! * [`Handler`] — the contract, resolved from the container as
//!   `Arc<dyn Handler>`; implemented by the worker feature, called by the
//!   gateway feature, named by both;
//! * [`Request`], [`Reply`] and [`RequestId`] — the types that contract's
//!   signature mentions, so a caller cannot use the trait without them;
//! * [`HandlerError`] — the refusal, which is the part a caller gets wrong
//!   when it is not modelled.
//!
//! # Why the acceptor is a runnable, and the socket a component
//!
//! This is the one thing to take away from the example this crate belongs to,
//! and the design document does not say it.
//!
//! The split is not "who can see the ladder" — both units can. It is *what
//! each one owns*.
//!
//! The **socket** is a resource: bound at boot, ordered against everything else
//! the boot graph orders, refused-into at `Draining`, released at the end. All
//! four of those are a component's moments, the third one included — a
//! component is told to drain before any runnable has been asked to wind down,
//! which is exactly when the refusal has to start. So the door is shut by
//! whoever owns it.
//!
//! The **accept loop** owns something a component never holds: the requests it
//! has already accepted. Giving those the drain window and cutting what
//! outlives it needs the token whose two rungs distinguish *stop taking new
//! work* from *stop now*, and needs the set of tasks those rungs apply to. Only
//! a runnable has both. So the loop is a runnable, necessarily.
//!
//! Bind and shut in the component, accept and wind down in the runnable. The
//! split is not ceremony — it follows from which unit owns which thing.
//!
//! # The wire format is not a contract
//!
//! A line in, a line out. There is no protocol here on purpose: a reader of
//! the example must be able to see the shutdown behaviour without first
//! learning a framing. Whatever the acceptor writes on the socket for a
//! refusal is the acceptor's own business, not this crate's — what this crate
//! guarantees is that the refusal reached it *as a distinguishable value*.

use std::sync::Arc;

use kernel_core::{BoxFuture, BoxSource};

/// Identity of one request, stamped when it is accepted.
///
/// Assigned by whoever accepts, read by whoever answers, so it is published
/// with the contract rather than kept by either side. It exists so a reply can
/// be tied back to a request in a log, and — in the example this crate serves
/// — so a per-request scope can be named in the answer it produces.
///
/// It is deliberately not a `String`: an identity that can be built from any
/// text is an identity two features will eventually disagree about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(u64);

impl RequestId {
    /// The request numbered `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gateway_contracts::RequestId;
    ///
    /// assert_eq!(RequestId::new(7).get(), 7);
    /// ```
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The number underneath.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for RequestId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One request: an identity and a line.
///
/// The payload is a single line of text and stays that way. Anything richer
/// would be a protocol, and a protocol is what a reader would have to learn
/// before seeing the thing this example exists to show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// What to call this request in a log, and in the answer.
    pub id: RequestId,
    /// The line, without its terminator.
    pub line: String,
}

impl Request {
    /// Request `id`, carrying `line`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gateway_contracts::{Request, RequestId};
    ///
    /// let request = Request::new(RequestId::new(1), "work");
    /// assert_eq!(request.line, "work");
    /// ```
    #[must_use]
    pub fn new(id: RequestId, line: impl Into<String>) -> Self {
        Self {
            id,
            line: line.into(),
        }
    }
}

/// One answer: a line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    /// The line to write back, without its terminator.
    pub line: String,
}

impl Reply {
    /// A reply carrying `line`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gateway_contracts::Reply;
    ///
    /// assert_eq!(Reply::new("ok").line, "ok");
    /// ```
    #[must_use]
    pub fn new(line: impl Into<String>) -> Self {
        Self { line: line.into() }
    }
}

/// Why no reply was produced.
///
/// The two refusals and the failure are three different instructions to the
/// caller, and collapsing them is the defect this type exists to prevent:
///
/// * [`Busy`](HandlerError::Busy) — nothing is wrong. The system is at its
///   declared capacity and chose to say so instead of queueing without bound.
///   The same request, sent later, may well succeed.
/// * [`Closing`](HandlerError::Closing) — nothing is wrong either, but this
///   process is going away. Retrying against *it* is pointless; retrying at
///   all is a decision for whatever knows about other processes.
/// * [`Failed`](HandlerError::Failed) — something broke. Retrying may repeat
///   the breakage, and a caller that treats it as backpressure will hammer a
///   sick system.
///
/// [`retry_later`](HandlerError::retry_later) is the one bit a dumb caller
/// needs; the variants are there for a caller that is not dumb.
///
/// The worker feature has its own refusal vocabulary — its queue publishes
/// one, in its own contracts crate — and translates into this one at the
/// boundary. That duplication is the price of the boundary, and it is the
/// right price: neither feature's error type becomes a dependency of the
/// other.
#[derive(Debug)]
pub enum HandlerError {
    /// At capacity, on purpose. Try again later.
    Busy,
    /// No longer serving: the process is draining, or has stopped.
    Closing,
    /// Something broke while producing the reply.
    Failed(BoxSource),
}

impl HandlerError {
    /// Wraps whatever broke.
    ///
    /// The cause is boxed rather than typed because it belongs to whichever
    /// feature implements the handler, and naming that feature's error type
    /// here would invert the dependency this crate exists to keep straight.
    #[must_use]
    pub fn failed(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Failed(Box::new(source))
    }

    /// Whether sending the same request again, later, is sensible.
    ///
    /// True for [`Busy`](HandlerError::Busy) and nothing else. A closing
    /// process will not reopen, and a failure is not backpressure.
    ///
    /// # Examples
    ///
    /// ```
    /// use gateway_contracts::HandlerError;
    ///
    /// assert!(HandlerError::Busy.retry_later());
    /// assert!(!HandlerError::Closing.retry_later());
    /// ```
    #[must_use]
    pub const fn retry_later(&self) -> bool {
        matches!(self, Self::Busy)
    }
}

impl core::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Busy => write!(f, "handler is at capacity"),
            Self::Closing => write!(f, "handler is no longer serving"),
            Self::Failed(source) => write!(f, "handler failed: {source}"),
        }
    }
}

impl std::error::Error for HandlerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Busy | Self::Closing => None,
            Self::Failed(source) => Some(source.as_ref()),
        }
    }
}

/// Whoever answers a request.
///
/// Resolved as `Arc<dyn Handler>`, which is why the method returns a
/// [`BoxFuture`] rather than being an `async fn`: a trait with an `async fn`
/// cannot be used behind `dyn`, and this one has to be — the acceptor holds it
/// as a trait object and never learns which feature implements it.
///
/// # Why the receiver is `Arc<Self>`
///
/// The same reason the kernel's own `Runnable` takes one. An acceptor serves
/// each connection on its own task, so the future it gets back must be
/// `'static`; a `&self` receiver would tie that future to a borrow the
/// acceptor cannot keep alive across a spawn. Taking `Arc<Self>` hands the
/// handler shared ownership of itself and makes the returned future free of
/// the call frame. The caller clones the `Arc` per request; that clone is the
/// whole cost.
///
/// # Examples
///
/// The acceptor's half of the contract: it names the trait, never an
/// implementation, and it distinguishes the two refusals from a failure.
///
/// ```
/// use std::sync::Arc;
///
/// use gateway_contracts::{Handler, Request, RequestId};
///
/// async fn serve(handler: Arc<dyn Handler>, id: u64, line: String) -> String {
///     let request = Request::new(RequestId::new(id), line);
///     match Arc::clone(&handler).handle(request).await {
///         Ok(reply) => reply.line,
///         Err(error) if error.retry_later() => "busy".to_owned(),
///         Err(error) => format!("error {error}"),
///     }
/// }
/// ```
pub trait Handler: Send + Sync + 'static {
    /// Answers one request, or says why it will not.
    ///
    /// The future may be dropped before it completes — that is what a stop
    /// deadline does to work still in flight — so an implementation must leave
    /// nothing inconsistent behind when it is cancelled at an await point.
    fn handle(self: Arc<Self>, request: Request)
    -> BoxFuture<'static, Result<Reply, HandlerError>>;
}

#[cfg(test)]
mod tests {
    use core::task::{Context, Poll, Waker};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// A handler that echoes, so the contract can be exercised without a
    /// runtime, a socket or an implementation crate.
    #[derive(Default)]
    struct Echo(AtomicU64);

    impl Handler for Echo {
        fn handle(
            self: Arc<Self>,
            request: Request,
        ) -> BoxFuture<'static, Result<Reply, HandlerError>> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(Reply::new(format!("{} {}", request.id, request.line)))
            })
        }
    }

    /// Polls a future that never parks. This crate has no runtime and must not
    /// grow one; a future that parked here would hang the test, and a test
    /// that can hang is worse than no test.
    fn drive<T>(mut future: BoxFuture<'_, T>) -> T {
        let mut cx = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("future parked without a runtime"),
        }
    }

    #[test]
    fn answers_behind_arc() {
        let handler: Arc<dyn Handler> = Arc::new(Echo::default());
        let request = Request::new(RequestId::new(1), "work");

        let reply = drive(Arc::clone(&handler).handle(request)).expect("the echo answers");

        assert_eq!(reply, Reply::new("1 work"));
    }

    #[test]
    fn future_outlives_call() {
        let handler: Arc<dyn Handler> = Arc::new(Echo::default());
        let future = Arc::clone(&handler).handle(Request::new(RequestId::new(2), "work"));

        // The point of the `Arc<Self>` receiver: nothing borrowed from the
        // call site is still needed, so the future can be sent to a task.
        drop(handler);

        assert_eq!(drive(future).expect("the echo answers").line, "2 work");
    }

    #[test]
    fn refusal_is_not_failure() {
        #[derive(Debug)]
        struct Broke;

        impl core::fmt::Display for Broke {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "it broke")
            }
        }

        impl std::error::Error for Broke {}

        assert!(HandlerError::Busy.retry_later());
        assert!(!HandlerError::Closing.retry_later());

        let failed = HandlerError::failed(Broke);
        assert!(!failed.retry_later());
        assert_eq!(failed.to_string(), "handler failed: it broke");
        assert!(std::error::Error::source(&failed).is_some());
        assert!(std::error::Error::source(&HandlerError::Busy).is_none());
    }

    #[test]
    fn request_id_reads_plainly() {
        assert_eq!(RequestId::new(42).to_string(), "42");
        assert_ne!(RequestId::new(1), RequestId::new(2));
    }
}
