//! The block that owns the store.
//!
//! One crate, one resource. [`Book`] holds the journal, implements
//! [`Ledger`] so callers can reach it through the contract, implements
//! [`Component`] so the kernel decides when it opens and when it closes, and
//! is watched by [`BookProbe`] so the health report answers about the thing
//! that owns the resource rather than about the bundle that registered it.
//!
//! # Why this is not in the bundle crate
//!
//! Nothing here mentions a [`Registry`](kernel::Registry), a
//! [`Provider`](kernel::Provider) or a manifest. That separation buys two
//! things a single crate does not:
//!
//! * the store can be embedded by an application that assembles the graph some
//!   other way — the type is usable with no bundle in the build;
//! * the bundle crate stays short enough to be read as wiring, which is the
//!   only thing it should ever be.
//!
//! It costs one crate boundary, and the boundary is where the public surface
//! of the store gets stated on purpose instead of by accident.
//!
//! # Where the knobs live
//!
//! [`Settings`] is here, next to the thing it configures, not in the bundle:
//! the *values* belong to whatever owns the resource, and only the *path* they
//! are read under is the bundle's business. `ledger-bundle` decides that path
//! is `ledger`; this crate never learns it.
//!
//! # The two moments the kernel guarantees
//!
//! `boot` opens the journal and writes whatever any bundle contributed as an
//! [`OpeningNote`]. `shutdown` flushes what is still buffered, and it is
//! bounded: the descriptor declares
//! [`shutdown_timeout`](kernel::core::ComponentDescriptor::shutdown_timeout)
//! and the flush reads
//! [`ShutdownContext::deadline`](kernel::ShutdownContext::deadline) to know
//! when to give up. Those are two halves of one fact — the descriptor states
//! the bound, the context states the instant it lands on — and the component
//! never recomputes the second from the first.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use kernel::core::{
    ComponentError, ComponentId, ConfigError, ConfigErrorKind, ConfigNode, FromConfig, Health,
    HealthProbe, Secret,
};
use kernel::{BootContext, BoxFuture, Component, ComponentDescriptor, Extension, ShutdownContext};
use ledger_contracts::{Entry, Ledger, LedgerError, OpeningNote};

/// The name this component is registered under, and the name every diagnostic
/// blames.
pub const NAME: &str = "book";

/// Upper bound on opening the journal. Opening touches memory only, so this is
/// generous rather than tuned.
const BOOT_TIMEOUT: Duration = Duration::from_secs(2);

// --------------------------------------------------------------------------
// Configuration
// --------------------------------------------------------------------------

/// What the store needs before it can be built.
///
/// Read once, during registration, before anything exists — a tree that does
/// not hold what this needs fails the build in phase two, which is the moment
/// a misconfiguration costs nothing.
///
/// # The key is a [`Secret`]
///
/// `signing_key` is wrapped, so `Debug` and `Display` render `<redacted>` and
/// reading it takes a call named [`Secret::expose`] that is visible at the
/// call site. There is exactly one such call in this crate, where a committed
/// line is sealed. Everything else — telemetry, panics, the `Debug` of this
/// struct — carries the wrapper and therefore carries nothing.
#[derive(Debug)]
pub struct Settings {
    /// How many entries may sit unflushed before an append flushes the buffer.
    ///
    /// Zero is refused: it would mean a buffer that is full when empty.
    pub batch: usize,
    /// The key each committed line is sealed with.
    pub signing_key: Secret<String>,
    /// How long the flush at shutdown may take.
    ///
    /// It becomes
    /// [`ComponentDescriptor::shutdown_timeout`](kernel::core::ComponentDescriptor::shutdown_timeout),
    /// so raising it here is what actually buys the flush more time — up to
    /// the kernel's own stop budget, which a descriptor may shorten and never
    /// extend.
    pub flush_timeout: Duration,
}

impl FromConfig for Settings {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        let batch: usize = field(node, "batch")?;
        if batch == 0 {
            return Err(ConfigError::invalid("batch", "a batch of zero never fills"));
        }
        Ok(Self {
            batch,
            signing_key: field(node, "signing_key")?,
            flush_timeout: field(node, "flush_timeout")?,
        })
    }
}

/// Reads `key` out of `node`, reporting failures against the leaf rather than
/// the struct.
///
/// Every hand-written [`FromConfig`] for a struct needs this and the public
/// surface offers no way to reach the copies `kernel-core` and `kernel` each
/// keep privately. A [`ConfigError`] can be built with a path but not
/// re-rooted under one, which is why the re-rooting is spelled out below.
///
/// [`ConfigErrorKind::Source`] carries a foreign cause that cannot be rebuilt,
/// so such an error passes through with whatever path it was raised with. No
/// source in this example produces one; a source that parsed a file would.
fn field<T: FromConfig>(node: &ConfigNode, key: &str) -> Result<T, ConfigError> {
    let Some(value) = node.get(key) else {
        return Err(ConfigError::missing(key));
    };
    T::from_config(value).map_err(|error| {
        let path = if error.path().is_empty() {
            key.to_owned()
        } else {
            format!("{key}.{}", error.path())
        };
        match error.kind() {
            ConfigErrorKind::Missing => ConfigError::missing(path),
            ConfigErrorKind::TypeMismatch { expected, found } => {
                ConfigError::type_mismatch(path, expected, found)
            }
            ConfigErrorKind::Invalid(detail) => ConfigError::invalid(path, detail.clone()),
            ConfigErrorKind::Source(_) => error,
        }
    })
}

// --------------------------------------------------------------------------
// The store
// --------------------------------------------------------------------------

/// Everything the journal holds, behind one lock.
///
/// Separate from [`Book`] so that the lock covers the whole state and no field
/// can be read in isolation while another is being written.
#[derive(Debug, Default)]
struct State {
    /// Whether [`Book::boot`] has run and [`Book::shutdown`] has not.
    open: bool,
    /// The committed lines, sealed, in order.
    journal: Vec<String>,
    /// Entries accepted and not yet committed, with the number they were given.
    pending: VecDeque<(u64, Entry)>,
    /// How many numbers have been handed out.
    numbered: u64,
}

/// The journal, and the one implementation of [`Ledger`] in this example.
///
/// It owns a resource, so the kernel owns its lifecycle: the resource is
/// opened in [`boot`](Component::boot) and closed in
/// [`shutdown`](Component::shutdown), and nothing between those two moments
/// has to check whether it exists. In a real application the same shape holds
/// a connection pool or a file handle; here it holds a `Vec`, so that the
/// shape is the only thing on display.
///
/// # An append does not commit
///
/// [`append`](Ledger::append) buffers. The buffer is committed when it reaches
/// `batch` entries, and whatever is left is committed at shutdown. That is
/// what makes the stop do real work, and what makes
/// [`shutdown_timeout`](kernel::core::ComponentDescriptor::shutdown_timeout)
/// something other than decoration.
#[derive(Debug)]
pub struct Book {
    /// How many entries may sit unflushed.
    batch: usize,
    /// The key every committed line is sealed with.
    signing_key: Secret<String>,
    /// How long the flush at shutdown may take.
    flush_timeout: Duration,
    /// The resource.
    state: Mutex<State>,
}

impl Book {
    /// A closed book. Nothing is committed and nothing is accepted until it
    /// boots.
    ///
    /// # Examples
    ///
    /// ```
    /// use core::time::Duration;
    ///
    /// use kernel::core::Secret;
    /// use ledger_component::{Book, Settings};
    ///
    /// let book = Book::new(Settings {
    ///     batch: 8,
    ///     signing_key: Secret::new("key".to_owned()),
    ///     flush_timeout: Duration::from_secs(2),
    /// });
    ///
    /// assert_eq!(book.pending(), 0);
    /// ```
    #[must_use]
    pub fn new(settings: Settings) -> Self {
        Self {
            batch: settings.batch,
            signing_key: settings.signing_key,
            flush_timeout: settings.flush_timeout,
            state: Mutex::new(State::default()),
        }
    }

    /// How many entries are accepted and not yet committed.
    ///
    /// What [`BookProbe`] reports on, and the only reason this is public.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.locked().pending.len()
    }

    /// Whether the journal is open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.locked().open
    }

    /// The state, whatever a previous panic did to the lock.
    ///
    /// A poisoned lock means a caller unwound while holding it; the journal is
    /// a `Vec` of finished lines, so the worst it can be is short one entry.
    /// Refusing every later call would turn that into an outage.
    fn locked(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Opens the journal, heading it with what the bundles contributed.
    ///
    /// Split out of [`boot`](Component::boot) because a
    /// [`BootContext`] cannot be built outside the `kernel` crate — the
    /// container, the dispatcher and the extension table it borrows all have
    /// private constructors — so this is the largest part of booting that can
    /// be exercised by a test.
    fn open<'n>(&self, notes: impl IntoIterator<Item = &'n OpeningNote>) {
        let mut state = self.locked();
        for note in notes {
            state.journal.push(format!("; {}", note.0));
        }
        state.open = true;
    }

    /// Closes the journal. Later appends are refused; the buffer is untouched.
    fn close(&self) {
        self.locked().open = false;
    }

    /// Seals one line with the signing key.
    ///
    /// The one place the secret is exposed. The digest is a plain FNV-1a: this
    /// example demonstrates where a credential travels, not cryptography, and
    /// saying so here is cheaper than letting a reader assume otherwise.
    fn seal(&self, number: u64, entry: &Entry) -> String {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in self
            .signing_key
            .expose()
            .bytes()
            .chain(entry.reference.bytes())
            .chain(entry.amount.to_le_bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("#{number} {} {} {hash:016x}", entry.reference, entry.amount)
    }

    /// Commits buffered entries until the buffer is empty or `deadline` has
    /// passed, and answers with how many are still buffered.
    ///
    /// The yield between two entries is what makes the bound enforceable.
    /// A deadline is a promise the caller can only keep at an await point: a
    /// flush that never parked would run to completion whatever the kernel
    /// wrapped it in, and the timeout would fire after the work it was meant
    /// to cut short. Yielding also keeps a long flush from monopolising the
    /// executor while everything else is stopping.
    async fn flush(&self, deadline: Option<Instant>) -> usize {
        loop {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return self.pending();
            }

            // One guard for the whole commit: taking the entry and writing its
            // line under two separate locks would let a concurrent flush
            // interleave and put the journal out of order.
            {
                let mut state = self.locked();
                let Some((number, entry)) = state.pending.pop_front() else {
                    return 0;
                };
                let line = self.seal(number, &entry);
                state.journal.push(line);
            }

            Yielded::once().await;
        }
    }
}

impl Ledger for Book {
    fn append(&self, entry: Entry) -> BoxFuture<'_, Result<u64, LedgerError>> {
        Box::pin(async move {
            let number = {
                let mut state = self.locked();
                if !state.open {
                    return Err(LedgerError::Closed);
                }
                if entry.amount == 0 {
                    return Err(LedgerError::Rejected("amount is zero".to_owned()));
                }
                state.numbered += 1;
                let number = state.numbered;
                state.pending.push_back((number, entry));
                number
            };

            // Outside the lock: the flush awaits, and a guard must never be
            // held across an await point.
            if self.pending() >= self.batch {
                self.flush(None).await;
            }
            Ok(number)
        })
    }

    fn count(&self) -> BoxFuture<'_, Result<u64, LedgerError>> {
        Box::pin(async move {
            let state = self.locked();
            if state.open {
                Ok(state.numbered)
            } else {
                Err(LedgerError::Closed)
            }
        })
    }
}

impl Component for Book {
    fn name() -> &'static str {
        NAME
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
            .boot_timeout(BOOT_TIMEOUT)
            .shutdown_timeout(self.flush_timeout)
    }

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            // Whoever wrote these notes is not known here and does not need to
            // be: the point is declared by the bundle that registers this
            // component, and contributed to by anyone.
            self.open(cx.collect::<OpeningNote>());
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            // Closed first, so the buffer being drained cannot grow behind the
            // drain.
            self.close();

            // `cx.deadline()` is this component's own budget, counted from the
            // moment this call started. `cx.shutdown().deadline()` is the
            // stage's, shared by every unit, and reading it here would make
            // this component hurry for a bound nobody is enforcing on it.
            let left = self.flush(cx.deadline()).await;
            if left == 0 {
                Ok(())
            } else {
                Err(ComponentError::new(
                    ComponentId::new(NAME, 0),
                    format!("{left} entries were not committed before the deadline").into(),
                ))
            }
        })
    }
}

// --------------------------------------------------------------------------
// Health
// --------------------------------------------------------------------------

/// Reports on the journal.
///
/// It lives in this crate rather than in the bundle because health is a
/// property of whatever owns the resource: the bundle knows that a book was
/// registered, this crate knows whether the book is open and how far behind it
/// is. The bundle still does the contributing — it is the crate that holds a
/// [`Registry`](kernel::Registry) — but what it contributes is defined here.
///
/// # What it answers
///
/// * [`Health::Down`] when the journal is closed: the ledger refuses every
///   call in that state, so nothing depending on it can work.
/// * [`Health::Degraded`] when the buffer holds at least half a batch, which
///   means a stop would have real work to do before the process could exit.
/// * [`Health::Up`] otherwise.
///
/// It reads state it already observes and returns at once, which is what a
/// probe is required to do: the kernel caps every check at
/// [`PROBE_TIMEOUT`](kernel::health::PROBE_TIMEOUT) and reports a probe that
/// overruns as down.
#[derive(Debug)]
pub struct BookProbe {
    /// The book being watched — the same object the kernel booted, not a copy.
    book: Arc<Book>,
}

impl BookProbe {
    /// Watches `book`.
    #[must_use]
    pub fn new(book: Arc<Book>) -> Self {
        Self { book }
    }
}

impl Extension for BookProbe {}

impl HealthProbe for BookProbe {
    fn name(&self) -> &'static str {
        NAME
    }

    fn check(&self) -> BoxFuture<'_, Health> {
        let verdict = if !self.book.is_open() {
            Health::down("journal is closed")
        } else {
            let pending = self.book.pending();
            // Half a batch. Anything at or past a full batch has already been
            // flushed by the append that filled it, so a threshold there would
            // never fire.
            if pending * 2 >= self.book.batch {
                Health::degraded(format!("buffer holds {pending}"))
            } else {
                Health::Up
            }
        };
        Box::pin(async move { verdict })
    }
}

// --------------------------------------------------------------------------
// Cooperation
// --------------------------------------------------------------------------

/// Yields to the executor exactly once.
///
/// Hand-written because neither `kernel` nor `kernel-core` publishes a yield,
/// and reaching for `tokio::task::yield_now` would make this crate depend on a
/// runtime that its two lifecycle hooks otherwise never name.
struct Yielded(bool);

impl Yielded {
    /// A yield that has not happened yet.
    fn once() -> Self {
        Yielded(false)
    }
}

impl Future for Yielded {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use core::task::Waker;

    use kernel::core::ConfigTree;

    use super::*;

    /// The key no assertion below is allowed to find in rendered output.
    const KEY: &str = "key-that-must-not-leak";

    /// Runs a future to completion on nothing.
    ///
    /// `#[tokio::test]` is what this crate should use, and cannot: its
    /// manifest declares no dev-dependencies, so neither `tokio` nor
    /// `kernel-testkit` is nameable here. Everything awaited below either
    /// completes at once or parks on [`Yielded`], which wakes itself, so a
    /// spinning driver is faithful — a timer would not be.
    fn drive<T>(future: impl Future<Output = T>) -> T {
        let mut future = Box::pin(future);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
        }
    }

    /// A book with the given batch size and a key nothing should ever print.
    fn book(batch: usize) -> Book {
        Book::new(Settings {
            batch,
            signing_key: Secret::new(KEY.to_owned()),
            flush_timeout: Duration::from_secs(2),
        })
    }

    /// The shape `registry.config("ledger")` hands a reader.
    fn section(batch: ConfigNode, flush_timeout: ConfigNode) -> ConfigNode {
        let mut tree = ConfigTree::empty();
        for (path, node) in [
            ("batch", batch),
            ("signing_key", ConfigNode::from(KEY)),
            ("flush_timeout", flush_timeout),
        ] {
            tree.insert(path, node)
                .expect("literal paths cannot collide");
        }
        tree.into_root()
    }

    /// A section every reader below accepts, except where a test says
    /// otherwise.
    fn complete() -> ConfigNode {
        section(ConfigNode::from(8_i64), ConfigNode::from("2s"))
    }

    #[test]
    fn refuses_before_boot() {
        let book = book(4);

        assert!(matches!(
            drive(book.append(Entry::new("order-1", 250))),
            Err(LedgerError::Closed)
        ));
        assert!(matches!(drive(book.count()), Err(LedgerError::Closed)));
    }

    #[test]
    fn notes_head_the_journal() {
        let book = book(4);
        let notes = [OpeningNote("opened by orders".to_owned())];

        book.open(&notes);

        assert_eq!(book.locked().journal, ["; opened by orders"]);
    }

    #[test]
    fn append_buffers_then_commits() {
        let book = book(2);
        book.open([]);

        assert_eq!(drive(book.append(Entry::new("one", 1))).unwrap(), 1);
        assert_eq!(book.pending(), 1);
        assert_eq!(book.locked().journal.len(), 0);

        // The second append fills the batch, so it commits both.
        assert_eq!(drive(book.append(Entry::new("two", 2))).unwrap(), 2);
        assert_eq!(book.pending(), 0);
        assert_eq!(book.locked().journal.len(), 2);
        assert_eq!(drive(book.count()).unwrap(), 2);
    }

    #[test]
    fn rejects_empty_amount() {
        let book = book(4);
        book.open([]);

        assert!(matches!(
            drive(book.append(Entry::new("order-1", 0))),
            Err(LedgerError::Rejected(_))
        ));
    }

    #[test]
    fn flush_empties_the_buffer() {
        let book = book(8);
        book.open([]);
        for index in 1..=3 {
            drive(book.append(Entry::new(format!("order-{index}"), index))).unwrap();
        }

        assert_eq!(drive(book.flush(None)), 0);
        assert_eq!(book.locked().journal.len(), 3);
    }

    #[test]
    fn flush_stops_at_deadline() {
        let book = book(8);
        book.open([]);
        for index in 1..=3 {
            drive(book.append(Entry::new(format!("order-{index}"), index))).unwrap();
        }

        // A deadline already in the past: nothing is committed and the count
        // left behind is what `shutdown` turns into a `ComponentError`.
        let past = Instant::now() - Duration::from_secs(1);

        assert_eq!(drive(book.flush(Some(past))), 3);
        assert_eq!(book.locked().journal.len(), 0);
    }

    #[test]
    fn seal_hides_the_key() {
        let book = book(4);
        let line = book.seal(1, &Entry::new("order-1", 250));

        assert!(line.starts_with("#1 order-1 250 "));
        assert!(!line.contains("key-that-must-not-leak"));
    }

    #[test]
    fn descriptor_carries_timeouts() {
        let descriptor = book(4).descriptor();

        assert_eq!(descriptor.boot_timeout, Some(BOOT_TIMEOUT));
        assert_eq!(descriptor.shutdown_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn probe_follows_the_book() {
        let book = Arc::new(book(2));
        let probe = BookProbe::new(Arc::clone(&book));

        assert_eq!(probe.name(), NAME);
        assert_eq!(drive(probe.check()), Health::down("journal is closed"));

        book.open([]);
        assert_eq!(drive(probe.check()), Health::Up);

        // Half a batch buffered: a stop would now have work to do.
        drive(book.append(Entry::new("order-1", 250))).unwrap();
        assert_eq!(drive(probe.check()), Health::degraded("buffer holds 1"));
    }

    #[test]
    fn settings_read_the_tree() {
        let settings = Settings::from_config(&complete()).expect("a complete section");

        assert_eq!(settings.batch, 8);
        assert_eq!(settings.flush_timeout, Duration::from_secs(2));
        assert_eq!(settings.signing_key.expose(), KEY);
    }

    #[test]
    fn settings_redact_the_key() {
        let settings = Settings::from_config(&complete()).expect("a complete section");
        let rendered = format!("{settings:?}");

        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains(KEY));
    }

    #[test]
    fn settings_refuse_zero_batch() {
        let node = section(ConfigNode::from(0_i64), ConfigNode::from("2s"));
        let error = Settings::from_config(&node).expect_err("zero never fills");

        assert_eq!(error.path(), "batch");
    }

    #[test]
    fn settings_name_the_leaf() {
        let node = section(ConfigNode::from(8_i64), ConfigNode::from(true));
        let error = Settings::from_config(&node).expect_err("a bool is not a duration");

        assert_eq!(error.path(), "flush_timeout");
    }

    #[test]
    fn settings_report_absence() {
        let mut tree = ConfigTree::empty();
        tree.insert("signing_key", ConfigNode::from(KEY))
            .expect("literal paths cannot collide");

        let error = Settings::from_config(&tree.into_root()).expect_err("no batch");

        assert_eq!(error.path(), "batch");
    }
}
