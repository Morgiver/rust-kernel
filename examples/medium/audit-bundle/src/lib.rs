//! The audit feature: one contract, two implementations, and one runnable
//! that fails on purpose.
//!
//! This bundle depends on `audit-contracts` and on the kernel. It names no
//! other feature — not `ledger`, not `orders` — and no other bundle names it.
//! Whoever wants to be audited depends on `audit-contracts` and resolves
//! [`Sink`] from the container; the crate that *implements* the sinks appears
//! on nobody's dependency list but the application's.
//!
//! # Two bindings of one contract, and the three questions
//!
//! [`Sink`] is bound twice here, and the two implementations are not variants
//! of each other:
//!
//! * the **journal** keeps every record, whole and in order. Nothing is
//!   folded, nothing is dropped, and it refuses nothing. It answers *what
//!   happened*.
//! * the **archive**, bound under [`ARCHIVE`], keeps one folded line per
//!   source: how many records that source produced and the last of them.
//!   Everything in between is gone the moment the next record arrives, and it
//!   holds a bounded number of sources. It answers *how much, from whom, and
//!   most recently*.
//!
//! So the three ways of asking are three different questions, not three
//! spellings of one:
//!
//! | Call | Ask it when |
//! |---|---|
//! | `container.get::<dyn Sink>()` | you want *a* sink and the choice is the application's, not yours. This is the journal, because it is the binding registered under no name. |
//! | `container.get_named::<dyn Sink>(ARCHIVE)` | you mean the archive and nothing else — you want the folded view and would be wrong to get the full one. |
//! | `container.get_all::<dyn Sink>()` | the record must reach every sink. You do not know how many there are, and adding a third one must not change your code. |
//!
//! A consumer that resolves the default and a consumer that resolves all of
//! them are making different promises to their caller, and swapping one for
//! the other is a behaviour change: one record written through `get` reaches
//! the journal only.
//!
//! # Why the journal is not `provide_named`
//!
//! A binding registered with [`Registry::provide`] carries no name and
//! therefore already holds the default position — [`Binding::as_default`](kernel::provider::Binding::as_default)
//! would add nothing. That method exists for the other shape, where *every*
//! binding is named and one of them claims the default:
//!
//! ```text
//! registry.provide_named::<dyn Sink>(LIVE, live).as_default();
//! registry.provide_named::<dyn Sink>(ARCHIVE, archive);
//! ```
//!
//! Use it when both implementations must stay reachable by name — typically
//! when the default is chosen from configuration. Here `audit-contracts`
//! publishes one name, so one binding is named and the other is not. Two
//! *unnamed* bindings of one contract, or two claims on the default position,
//! are both phase-three errors rather than a silent overwrite.
//!
//! # The ancillary runnable
//!
//! `Bookend` is registered as [`Criticality::Ancillary`] and fails its first
//! `DELIBERATE_FAILURES` starts **on purpose**. The kernel restarts it per
//! its [`RestartPolicy`], the rest of the process never notices, and the third
//! start settles into the real work. That is the whole difference between
//! `Ancillary` and `Essential`: an essential runnable ending takes the process
//! with it, an ancillary one is recorded and retried.
//!
//! It also has no timer. A runnable does not need one to be a runnable: this
//! one writes a mark to every sink when it starts, waits for the shutdown
//! token, writes a closing mark and returns. That is the shortest shape that
//! still obeys the one rule — `run` returns when the token fires.

use core::fmt;
use core::time::Duration;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use audit_contracts::{ARCHIVE, Record, Sink, SinkError};
use kernel::{Bundle, ContractRef, Provider, Registry, RunContext, Runnable};
use kernel_core::{
    Backoff, BoxFuture, BuildError, BundleManifest, Criticality, RegisterError, RestartPolicy,
    RunError, RunnableDescriptor,
};

/// The name this bundle publishes, and the name every record it writes carries.
const NAME: &str = "audit";

/// How many distinct sources the archive folds before it starts refusing.
///
/// The bound is the point: an archive that grows without limit is a journal
/// with extra steps, and the two bindings would stop teaching anything.
const ARCHIVE_SOURCES: usize = 8;

/// How many starts of [`Bookend`] fail on purpose, and how many restarts its
/// policy therefore allows.
///
/// One constant for both because they are the same fact: the policy must grant
/// exactly enough restarts to get past the demonstration. Raise it and the
/// demonstration lengthens; nothing else moves.
const DELIBERATE_FAILURES: u32 = 2;

// ---------------------------------------------------------------------------
// The default binding
// ---------------------------------------------------------------------------

/// Keeps every record, whole and in order.
///
/// It refuses nothing, because its promise is that nothing is lost. A sink
/// that can be closed would answer [`SinkError::Unavailable`] outside its open
/// window; this one is open for as long as the process is, so that variant
/// never comes from here.
struct Journal {
    /// Every record ever written, rendered as one line each.
    lines: Mutex<Vec<String>>,
}

impl Journal {
    /// An empty journal.
    fn new() -> Self {
        Self {
            lines: Mutex::new(Vec::new()),
        }
    }

    /// The lines, whatever a previous panic did to the lock.
    fn lines(&self) -> MutexGuard<'_, Vec<String>> {
        self.lines.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Sink for Journal {
    fn write(&self, record: Record) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            let mut lines = self.lines();
            lines.push(format!("{}: {}", record.source, record.message));
            println!("[journal] #{} {}", lines.len(), lines[lines.len() - 1]);
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// The named binding
// ---------------------------------------------------------------------------

/// What the archive keeps about one source.
#[derive(Default)]
struct Tally {
    /// How many records that source has produced.
    count: u64,
    /// The last of them. Everything before it is gone.
    last: String,
}

/// Folds records into one line per source, and holds a bounded number of them.
///
/// Genuinely different work from [`Journal`], not a flag on it: the archive
/// discards the body of every record but the newest, counts what it discarded,
/// and refuses input the journal accepts. A caller that resolved this one and
/// expected a history would be wrong, which is why the choice is a question
/// the caller answers rather than a detail the container hides.
struct Archive {
    /// One folded line per source, ordered by source name so two runs read the
    /// same.
    sources: Mutex<BTreeMap<&'static str, Tally>>,
    /// How many distinct sources this archive will hold.
    capacity: usize,
}

impl Archive {
    /// An empty archive with room for `capacity` sources.
    fn new(capacity: usize) -> Self {
        Self {
            sources: Mutex::new(BTreeMap::new()),
            capacity,
        }
    }

    /// The folded lines, whatever a previous panic did to the lock.
    fn sources(&self) -> MutexGuard<'_, BTreeMap<&'static str, Tally>> {
        self.sources.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Sink for Archive {
    fn write(&self, record: Record) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            if record.message.trim().is_empty() {
                return Err(SinkError::Rejected("the record has no message".to_owned()));
            }

            let mut sources = self.sources();
            if !sources.contains_key(record.source) && sources.len() == self.capacity {
                return Err(SinkError::Rejected(format!(
                    "the archive holds {} sources and `{}` is not one of them",
                    self.capacity, record.source
                )));
            }

            let tally = sources.entry(record.source).or_default();
            tally.count += 1;
            tally.last = record.message;
            println!(
                "[archive] {} x{}, last: {}",
                record.source, tally.count, tally.last
            );
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// The ancillary runnable
// ---------------------------------------------------------------------------

/// The error [`Bookend`] raises on purpose, so a reader of a log can tell a
/// demonstration from a defect.
#[derive(Debug)]
struct DeliberateFailure(u32);

impl fmt::Display for DeliberateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "deliberate failure {} of {}: this ancillary runnable fails on \
             purpose so that the restart policy can be seen working",
            self.0, DELIBERATE_FAILURES
        )
    }
}

impl std::error::Error for DeliberateFailure {}

/// Marks the run in every sink: once when it starts, once when the kernel
/// drains.
///
/// It holds `Vec<Arc<dyn Sink>>` rather than resolving inside `run`, which is
/// what makes it testable without a kernel: the detached run context has an
/// empty container, so a runnable that resolved there could not be tested at
/// all.
struct Bookend {
    /// Every sink, in registration order. The count is not known here and does
    /// not need to be.
    sinks: Vec<Arc<dyn Sink>>,
    /// How many times the supervisor has entered `run`, restarts included.
    starts: AtomicU32,
}

impl Bookend {
    /// A bookend over `sinks`, before its first start.
    fn new(sinks: Vec<Arc<dyn Sink>>) -> Self {
        Self {
            sinks,
            starts: AtomicU32::new(0),
        }
    }

    /// Writes one mark to every sink.
    ///
    /// A sink refusing a mark is not a reason to skip the next sink and not a
    /// reason to fail the run: the archive refuses on purpose once it is full,
    /// and that is its answer, not a fault of the caller's.
    async fn mark(&self, moment: &str) {
        for sink in &self.sinks {
            let record = Record::new(NAME, format!("run {moment}"));
            if let Err(error) = sink.write(record).await {
                println!("[bookend] {moment} not written: {error}");
            }
        }
    }
}

impl Runnable for Bookend {
    fn name() -> &'static str {
        "bookend"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        // Ancillary: this ending — cleanly or not — is recorded and the kernel
        // keeps running. An `Essential` runnable failing here would take the
        // process down, which is precisely what this bundle exists to show is
        // not happening.
        RunnableDescriptor::new()
            .criticality(Criticality::Ancillary)
            .restart(RestartPolicy::on_failure(
                DELIBERATE_FAILURES,
                Backoff::Fixed(Duration::from_millis(50)),
            ))
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            let start = self.starts.fetch_add(1, Ordering::Relaxed);

            // ---------------------------------------------------------------
            // THE DEMONSTRATION, and the only reason this branch exists.
            //
            // The first `DELIBERATE_FAILURES` starts return an error on
            // purpose. The supervisor records each one, waits out the backoff
            // and starts this same object again — the counter above survives
            // because the runnable is resolved once and restarted, not rebuilt.
            // Nothing else in the process stops. Delete this branch and the
            // bundle still works; what is lost is the evidence.
            // ---------------------------------------------------------------
            if start < DELIBERATE_FAILURES {
                let failure = DeliberateFailure(start + 1);
                println!("[bookend] {failure}");
                return Err(RunError::failed(cx.id(), Box::new(failure)));
            }

            self.mark("opened").await;

            // The one rule a runnable must obey. There is nothing to race here
            // — no timer, no socket — so the await is the whole body.
            cx.shutdown().draining().await;

            self.mark("closing").await;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// The bundle
// ---------------------------------------------------------------------------

/// Registers both sinks and the bookend.
///
/// The manifest declares no requirement: this feature provides and does not
/// consume. The one requirement in the file is on the provider of `Bookend`,
/// which resolves the sinks it will write to.
#[derive(Debug, Default)]
pub struct Bundled;

impl Bundle for Bundled {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new(NAME, "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        // Registered under no name, which is what makes it the binding
        // `container.get::<dyn Sink>()` answers with.
        registry.provide::<dyn Sink>(Provider::from_value(Arc::new(Journal::new())));

        // Registered under the name the contracts crate publishes, so that the
        // two ends of the name are one constant rather than two literals.
        // Reachable through `get_named` and through `get_all`, never through
        // `get`.
        registry.provide_named::<dyn Sink>(
            ARCHIVE,
            Provider::from_value(Arc::new(Archive::new(ARCHIVE_SOURCES))),
        );

        registry.runnable(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    // Every implementation, in registration order. Both of them
                    // today; a third would arrive here without this line
                    // changing.
                    let sinks = container
                        .get_all::<dyn Sink>()
                        .await
                        .map_err(|error| BuildError::new("Bookend", Box::new(error)))?;
                    Ok(Arc::new(Bookend::new(sinks)))
                })
            })
            // One declaration covers every implementation `get_all` returns:
            // the requirement names the contract, not a binding of it.
            .requires([ContractRef::of::<dyn Sink>()]),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Resolves a future that this crate's sinks never park in.
    async fn write(sink: &dyn Sink, source: &'static str, message: &str) -> Result<(), SinkError> {
        sink.write(Record::new(source, message)).await
    }

    /// A bookend over both sinks, as the container would build it.
    fn bookend() -> (Arc<Bookend>, Arc<Journal>, Arc<Archive>) {
        let journal = Arc::new(Journal::new());
        let archive = Arc::new(Archive::new(ARCHIVE_SOURCES));
        let sinks: Vec<Arc<dyn Sink>> = vec![
            Arc::clone(&journal) as Arc<dyn Sink>,
            Arc::clone(&archive) as Arc<dyn Sink>,
        ];
        (Arc::new(Bookend::new(sinks)), journal, archive)
    }

    /// The default binding loses nothing: three records, three lines, in order.
    #[tokio::test]
    async fn journal_keeps_everything() {
        let journal = Journal::new();

        for round in 1..=3 {
            write(&journal, "orders", &format!("order-{round} placed"))
                .await
                .expect("the journal refuses nothing");
        }

        assert_eq!(
            *journal.lines(),
            [
                "orders: order-1 placed",
                "orders: order-2 placed",
                "orders: order-3 placed",
            ]
        );
    }

    /// The named binding folds: the same three records leave one line behind.
    #[tokio::test]
    async fn archive_folds_records() {
        let archive = Archive::new(ARCHIVE_SOURCES);

        for round in 1..=3 {
            write(&archive, "orders", &format!("order-{round} placed"))
                .await
                .expect("under capacity");
        }

        let sources = archive.sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources["orders"].count, 3);
        assert_eq!(sources["orders"].last, "order-3 placed");
    }

    /// What the journal accepts, the archive refuses: a summary of nothing.
    #[tokio::test]
    async fn archive_refuses_empty() {
        let journal = Journal::new();
        let archive = Archive::new(ARCHIVE_SOURCES);

        assert!(write(&journal, "orders", "  ").await.is_ok());
        let refused = write(&archive, "orders", "  ")
            .await
            .expect_err("an empty message folds to nothing");

        assert!(matches!(refused, SinkError::Rejected(_)));
    }

    /// The bound is real: the source past capacity is refused, the ones inside
    /// it keep working.
    #[tokio::test]
    async fn archive_bounds_sources() {
        const SOURCES: [&str; 3] = ["one", "two", "three"];
        let archive = Archive::new(2);

        assert!(write(&archive, SOURCES[0], "first").await.is_ok());
        assert!(write(&archive, SOURCES[1], "second").await.is_ok());
        assert!(write(&archive, SOURCES[2], "third").await.is_err());
        // Full does not mean closed: a source already held still folds.
        assert!(write(&archive, SOURCES[0], "again").await.is_ok());

        assert_eq!(archive.sources().len(), 2);
    }

    /// The test the design makes mandatory for every runnable: a token that has
    /// already fired makes `run` return instead of waiting.
    ///
    /// The starts counter is moved past the deliberate failures first, because
    /// what is under test here is the token, not the demonstration.
    #[tokio::test]
    async fn bookend_returns_on_token() {
        let (bookend, journal, _archive) = bookend();
        bookend.starts.store(DELIBERATE_FAILURES, Ordering::Relaxed);

        let (cx, controller) = RunContext::detached();
        controller.begin_draining();

        bookend.run(cx).await.expect("a clean end");

        // Opened and closing, in both sinks.
        assert_eq!(journal.lines().len(), 2);
    }

    /// The demonstration itself: the first starts fail, the next one settles,
    /// and the object carrying the count is the same one throughout — which is
    /// what a restart hands back to `run`.
    #[tokio::test]
    async fn bookend_fails_then_settles() {
        let (bookend, journal, archive) = bookend();

        for attempt in 1..=DELIBERATE_FAILURES {
            let (cx, _controller) = RunContext::detached();
            let error = Arc::clone(&bookend)
                .run(cx)
                .await
                .expect_err("this start fails on purpose");
            assert!(error.to_string().contains("deliberate"));
            assert_eq!(bookend.starts.load(Ordering::Relaxed), attempt);
        }

        let (cx, controller) = RunContext::detached();
        controller.begin_draining();
        Arc::clone(&bookend).run(cx).await.expect("a clean end");

        // Nothing was written while it was failing, and both sinks saw both
        // marks of the run that succeeded — the visible consequence of
        // `get_all` rather than `get`.
        assert_eq!(journal.lines().len(), 2);
        assert_eq!(archive.sources()[NAME].count, 2);
    }

    /// The policy grants exactly the restarts the demonstration needs.
    #[tokio::test]
    async fn policy_allows_the_failures() {
        let (bookend, _journal, _archive) = bookend();
        let descriptor = bookend.descriptor();

        assert_eq!(descriptor.criticality, Criticality::Ancillary);
        assert!(descriptor.restart.allows(DELIBERATE_FAILURES - 1));
        assert!(!descriptor.restart.allows(DELIBERATE_FAILURES));
    }

    /// The bundle registers what it says it registers, and asks for nothing.
    #[test]
    fn manifest_requires_nothing() {
        let manifest = Bundled.manifest();

        assert_eq!(manifest.name, NAME);
        assert!(manifest.requires.is_empty());
    }
}
