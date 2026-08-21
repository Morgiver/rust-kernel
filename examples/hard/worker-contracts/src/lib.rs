//! A bounded queue, and what it says when it is full.
//!
//! Like every `*-contracts` crate here, this one depends on `kernel-core` and
//! on nothing else — no runtime, no other feature, no bundle. The queue is
//! resolved as `Arc<dyn WorkQueue>` and the types its signature mentions are
//! published beside it.
//!
//! # What this crate is actually about
//!
//! Not "submitting work": anybody can model that. The part worth publishing is
//! the *refusal*. A queue with no bound is a memory leak with a scheduler
//! attached, and a queue with a bound but no way to report hitting it teaches
//! the caller to treat backpressure as breakage. So admission is separated
//! from completion, and each half has its own error:
//!
//! * [`WorkQueue::submit`] answers immediately, without awaiting anything, and
//!   either admits the job — handing back a [`Ticket`] — or refuses it with a
//!   [`Refusal`]. Nothing is enqueued on the refusal path, which is the whole
//!   point: the bound is enforced at the door, not after the fact.
//! * the [`Ticket`] is a future. Awaiting it yields the [`Done`] the job
//!   produced, or a [`WorkError`] if the work itself failed or was cut.
//!
//! A caller that cannot tell "refused, try later" from "broken" retries the
//! wrong one. Here it cannot make that mistake: they are not even the same
//! return type.
//!
//! # The queue refuses twice, for different reasons
//!
//! [`Refusal::Full`] is capacity, and it is temporary. [`Refusal::Closed`] is
//! the ladder: once the process starts draining, the queue stops admitting
//! work while the jobs already admitted run to completion. That distinction is
//! the same one the two-stage shutdown makes — refuse new work, finish held
//! work — expressed at the door of a queue instead of at the door of a socket.
//!
//! It is also why the unit that watches the ladder is a runnable and not a
//! component: a component is only handed its shutdown context after every
//! runnable has already stopped, so it never observes the drain window it
//! would need to close this queue at the right moment.

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use kernel_core::{BoxFuture, BoxSource};

/// Identity of one admitted job.
///
/// Handed out by the queue at admission, so a job that was refused never has
/// one — an identity that exists for work nobody accepted is an identity that
/// will be logged, correlated and chased for no reason.
///
/// It crosses the feature boundary: whoever submits keeps it to name the job
/// in its own reply and its own logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(u64);

impl JobId {
    /// The job numbered `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// use worker_contracts::JobId;
    ///
    /// assert_eq!(JobId::new(3).get(), 3);
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

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One unit of work: a line.
///
/// Trivial on purpose. The example this crate serves is about when work is
/// admitted, finished or cut — not about what the work is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    /// The line to work on.
    pub line: String,
}

impl Job {
    /// A job for `line`.
    ///
    /// # Examples
    ///
    /// ```
    /// use worker_contracts::Job;
    ///
    /// assert_eq!(Job::new("work").line, "work");
    /// ```
    #[must_use]
    pub fn new(line: impl Into<String>) -> Self {
        Self { line: line.into() }
    }
}

/// What a finished job produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Done {
    /// The job that produced it.
    pub job: JobId,
    /// The line it produced.
    pub line: String,
}

impl Done {
    /// The result of `job`, being `line`.
    ///
    /// # Examples
    ///
    /// ```
    /// use worker_contracts::{Done, JobId};
    ///
    /// let done = Done::new(JobId::new(1), "ok");
    /// assert_eq!(done.job, JobId::new(1));
    /// ```
    #[must_use]
    pub fn new(job: JobId, line: impl Into<String>) -> Self {
        Self {
            job,
            line: line.into(),
        }
    }
}

/// Why a job was not admitted.
///
/// This is not a failure and must not be reported as one. Nothing broke,
/// nothing was lost, and nothing was enqueued: the queue was asked to exceed a
/// bound it was given and declined. A caller holding one of these knows the
/// job never started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The queue is at capacity. Temporary: try again later.
    Full {
        /// The bound that was hit, so the caller can say what it hit.
        capacity: usize,
    },
    /// The queue is no longer admitting work — the process is draining or has
    /// stopped. Permanent for this queue: work already admitted is still
    /// finishing, but nothing new will be.
    Closed,
}

impl Refusal {
    /// Whether submitting the same job again, later, is sensible.
    ///
    /// True for [`Full`](Refusal::Full) and nothing else. A closed queue does
    /// not reopen: the ladder only moves one way.
    ///
    /// # Examples
    ///
    /// ```
    /// use worker_contracts::Refusal;
    ///
    /// assert!(Refusal::Full { capacity: 4 }.retry_later());
    /// assert!(!Refusal::Closed.retry_later());
    /// ```
    #[must_use]
    pub const fn retry_later(&self) -> bool {
        matches!(self, Self::Full { .. })
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full { capacity } => write!(f, "queue is full at {capacity}"),
            Self::Closed => write!(f, "queue is closed"),
        }
    }
}

impl std::error::Error for Refusal {}

/// Why an admitted job did not produce a result.
///
/// Distinct from [`Refusal`] because it answers a different question. A
/// refusal says the work never started; this says it started and did not
/// finish, which is the case a caller may have to compensate for.
#[derive(Debug)]
pub enum WorkError {
    /// The job was cut before it finished: the stop deadline elapsed, or the
    /// worker holding it went away. Whether it had any effect before being cut
    /// is not knowable from here.
    Cancelled,
    /// The job ran and failed.
    Failed(BoxSource),
}

impl WorkError {
    /// Wraps whatever the job failed with.
    #[must_use]
    pub fn failed(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Failed(Box::new(source))
    }
}

impl fmt::Display for WorkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "job was cut before it finished"),
            Self::Failed(source) => write!(f, "job failed: {source}"),
        }
    }
}

impl std::error::Error for WorkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cancelled => None,
            Self::Failed(source) => Some(source.as_ref()),
        }
    }
}

/// Proof that a job was admitted, and the future that carries its result.
///
/// Holding one means the bound was respected and the work is the queue's
/// problem now. Awaiting it yields what the job produced.
///
/// Dropping it is allowed and means the caller stopped waiting — it does not
/// promise to stop the job. Whether the work is also cancelled depends on how
/// the queue was built, and a caller that needs to know must ask the queue,
/// not this type.
///
/// # Examples
///
/// ```
/// use worker_contracts::{Done, JobId, Ticket, WorkError};
///
/// let job = JobId::new(1);
/// let ticket = Ticket::new(job, Box::pin(async move { Ok(Done::new(job, "ok")) }));
///
/// assert_eq!(ticket.job(), job);
///
/// async fn wait(ticket: Ticket) -> Result<Done, WorkError> {
///     ticket.await
/// }
/// ```
pub struct Ticket {
    job: JobId,
    outcome: BoxFuture<'static, Result<Done, WorkError>>,
}

impl Ticket {
    /// A ticket for `job`, resolving to whatever `outcome` resolves to.
    ///
    /// The future is `'static` and boxed: it is handed to a caller that knows
    /// nothing about the queue that made it, and it usually outlives the call
    /// that produced it.
    #[must_use]
    pub fn new(job: JobId, outcome: BoxFuture<'static, Result<Done, WorkError>>) -> Self {
        Self { job, outcome }
    }

    /// Which job was admitted.
    ///
    /// Available before the work finishes, which is the point: a caller can
    /// name the job in a log line the moment it is accepted.
    #[must_use]
    pub const fn job(&self) -> JobId {
        self.job
    }
}

impl fmt::Debug for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ticket").field("job", &self.job).finish()
    }
}

impl Future for Ticket {
    type Output = Result<Done, WorkError>;

    /// Every field is `Unpin`, so the projection needs no pin ceremony: the
    /// boxed future is already pinned to the heap and moving the ticket never
    /// moves what it points at.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().outcome.as_mut().poll(cx)
    }
}

/// Somewhere work can be submitted, up to a bound.
///
/// Resolved as `Arc<dyn WorkQueue>`. Note what is *not* in this signature: no
/// [`BoxFuture`], because admission does not await. A queue whose `submit` is
/// asynchronous invites an implementation that waits for room, and waiting for
/// room is an unbounded queue wearing a bounded queue's name.
///
/// # Examples
///
/// The caller's half: it distinguishes the two refusals from a failure, and it
/// never learns which queue it is holding.
///
/// ```
/// use std::sync::Arc;
///
/// use worker_contracts::{Job, WorkQueue};
///
/// async fn run_one(queue: Arc<dyn WorkQueue>, line: &str) -> String {
///     match queue.submit(Job::new(line)) {
///         Ok(ticket) => match ticket.await {
///             Ok(done) => done.line,
///             Err(error) => format!("error {error}"),
///         },
///         Err(refusal) if refusal.retry_later() => "busy".to_owned(),
///         Err(refusal) => format!("refused {refusal}"),
///     }
/// }
/// ```
pub trait WorkQueue: Send + Sync + 'static {
    /// Admits one job, or refuses it. Never blocks and never awaits.
    ///
    /// # Errors
    ///
    /// [`Refusal::Full`] when the bound is reached, [`Refusal::Closed`] once
    /// the queue has stopped admitting. In both cases the job was not
    /// enqueued and no [`JobId`] was spent on it.
    fn submit(&self, job: Job) -> Result<Ticket, Refusal>;

    /// The bound this queue admits up to.
    ///
    /// Published so a caller can report what it hit, and so a test can fill
    /// the queue deliberately instead of guessing.
    fn capacity(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use core::task::{Context, Poll, Waker};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use super::*;

    /// A queue with a bound and no worker: enough to exercise the contract
    /// without a runtime or an implementation crate.
    struct Bounded {
        capacity: usize,
        admitted: AtomicUsize,
        next: AtomicU64,
        closed: bool,
    }

    impl Bounded {
        fn new(capacity: usize) -> Self {
            Self {
                capacity,
                admitted: AtomicUsize::new(0),
                next: AtomicU64::new(0),
                closed: false,
            }
        }

        fn closed() -> Self {
            Self {
                closed: true,
                ..Self::new(1)
            }
        }
    }

    impl WorkQueue for Bounded {
        fn submit(&self, job: Job) -> Result<Ticket, Refusal> {
            if self.closed {
                return Err(Refusal::Closed);
            }
            if self.admitted.fetch_add(1, Ordering::Relaxed) >= self.capacity {
                self.admitted.fetch_sub(1, Ordering::Relaxed);
                return Err(Refusal::Full {
                    capacity: self.capacity,
                });
            }

            let id = JobId::new(self.next.fetch_add(1, Ordering::Relaxed) + 1);
            Ok(Ticket::new(
                id,
                Box::pin(async move { Ok(Done::new(id, job.line)) }),
            ))
        }

        fn capacity(&self) -> usize {
            self.capacity
        }
    }

    /// Polls a future that never parks. This crate has no runtime and must not
    /// grow one; a test that can park is a test that can wedge a build.
    fn drive<T>(mut future: Pin<Box<dyn Future<Output = T> + Send>>) -> T {
        let mut cx = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("future parked without a runtime"),
        }
    }

    #[test]
    fn admits_behind_arc() {
        let queue: Arc<dyn WorkQueue> = Arc::new(Bounded::new(2));

        let ticket = queue.submit(Job::new("work")).expect("room for one");
        assert_eq!(ticket.job(), JobId::new(1));
        assert_eq!(queue.capacity(), 2);

        let done = drive(Box::pin(ticket)).expect("the job finished");
        assert_eq!(done, Done::new(JobId::new(1), "work"));
    }

    #[test]
    fn refuses_when_full() {
        let queue = Bounded::new(1);

        let held = queue.submit(Job::new("first")).expect("room for one");
        let refusal = queue.submit(Job::new("second")).expect_err("no room left");

        assert_eq!(refusal, Refusal::Full { capacity: 1 });
        assert!(refusal.retry_later());
        assert_eq!(refusal.to_string(), "queue is full at 1");
        // The refused job was never admitted: only one identity was spent.
        assert_eq!(held.job(), JobId::new(1));
    }

    #[test]
    fn closed_is_not_full() {
        let refusal = Bounded::closed()
            .submit(Job::new("work"))
            .expect_err("a closed queue admits nothing");

        assert_eq!(refusal, Refusal::Closed);
        assert!(!refusal.retry_later());
        assert_eq!(refusal.to_string(), "queue is closed");
    }

    #[test]
    fn cut_is_not_refusal() {
        #[derive(Debug)]
        struct Broke;

        impl fmt::Display for Broke {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "it broke")
            }
        }

        impl std::error::Error for Broke {}

        let cut = Ticket::new(JobId::new(9), Box::pin(async { Err(WorkError::Cancelled) }));
        assert_eq!(cut.job(), JobId::new(9));
        match drive(Box::pin(cut)) {
            Err(WorkError::Cancelled) => {}
            other => panic!("expected a cut job, got {other:?}"),
        }

        let failed = WorkError::failed(Broke);
        assert_eq!(failed.to_string(), "job failed: it broke");
        assert!(std::error::Error::source(&failed).is_some());
        assert!(std::error::Error::source(&WorkError::Cancelled).is_none());
    }
}
