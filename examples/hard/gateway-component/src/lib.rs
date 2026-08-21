//! The socket, and the loop that accepts on it.
//!
//! Two units live here and the split between them is the point of the whole
//! example:
//!
//! * [`Doorway`] is a [`Component`]. It owns the bound socket — it binds at
//!   boot, publishes the address it actually got, **shuts the door at
//!   `Draining`** and releases at shutdown — and it is watched by
//!   [`DoorwayProbe`].
//! * [`Acceptor`] is a [`Runnable`]. It accepts on that socket, serves each
//!   accepted request in its own [`Scope`], stops accepting when the ladder
//!   reaches `Draining`, lets what it already holds finish, and cuts the rest
//!   at `Stopping`.
//!
//! # Which half is whose
//!
//! This is the sentence the design document does not contain, and it is the
//! most useful thing in this crate.
//!
//! *Refusing new work* is a property of the RESOURCE. The socket belongs to
//! [`Doorway`], so [`Component::drain`] — called as the ladder reaches
//! `Draining`, before a single runnable has been asked to wind down — is where
//! it is shut. The component does not need a loop, a token or a task to do it:
//! it is told once, at the one instant the refusal has to start.
//!
//! *Finishing work already in flight, and cutting what outlives the window* is
//! a property of the LOOP, and only a [`Runnable`] can express it. Only a
//! `RunContext` carries the [`Shutdown`](kernel::Shutdown) token whose two
//! rungs — `draining()` and `stopping()` — are the difference between *stop
//! taking new work* and *stop now*, and only a runnable holds the set of
//! requests those two rungs apply to. Anything that must tell them apart over
//! work it owns is a runnable, necessarily.
//!
//! So: bind and shut in the component, accept and wind down in the runnable.
//! The two are wired together by the container, not by ownership —
//! [`Acceptor`] holds an `Arc<Doorway>` to accept on, and lets go of its own
//! clone when the loop breaks. Neither waits on the other, and the address is
//! unbound when the second of the two drops lands.
//!
//! # At `Draining` the listener is CLOSED, not merely ignored
//!
//! There are two ways to stop accepting, and a reader will copy whichever one
//! is written here, so the choice is stated rather than left to the code.
//!
//! *Stop selecting on the listener* keeps the socket bound. The operating
//! system goes on completing handshakes into the accept backlog, so a caller
//! that connects during the drain is told it connected, sends its line, and
//! waits — for an answer that will never come, until the process exits and the
//! kernel resets the connection under it. The caller learns nothing until the
//! very end, and what it learns then is indistinguishable from a crash.
//!
//! *Close the listener*, which is what [`Doorway`]'s [`Component::drain`] does,
//! unbinds the address. A caller that connects during the drain is refused
//! **immediately**, by the operating system, with the one error every client
//! already knows how to act on. It can fail over to another process in the same
//! millisecond instead of holding a socket open against one that is leaving.
//!
//! What closing costs is the courtesy line: there is no connection left to
//! write "I am closing" on. That is the trade, and it is worth taking — a
//! refusal a caller can act on at once beats a polite message it receives too
//! late to use. A deployment that needs the polite message takes the address
//! out of rotation *before* asking the process to stop, which is where that
//! decision belongs anyway.
//!
//! # A request is a unit of work
//!
//! Each accepted request opens a [`Scope`] and is served inside it. [`Visit`]
//! is bound [`Scoped`](kernel::Lifetime::Scoped), so every resolution inside
//! one request reaches the same object and two concurrent requests reach two
//! different ones. Nothing is threaded through the calls: the scope is the
//! thread.
//!
//! The requirement is declared, not implicit: the bundle names `Visit` in
//! [`Provider::requires_scoped`](kernel::Provider::requires_scoped), so phase
//! three refuses a graph in which nobody provides it and the container's debug
//! guard refuses a resolution nobody declared.
//!
//! The tasks the requests run on are [`Children`], the kernel's own per-request
//! set: it reaps as it goes, refuses a child once the ladder has left
//! `Running`, waits the set out across the drain window and cuts what outlives
//! the stopping budget. That is the whole of this loop's stages two and three,
//! so this crate hand-rolls none of it.
//!
//! # The wire format, in one line
//!
//! A line in, a line out, then the connection closes. Out is
//!
//! ```text
//! <request> <visit> <reached> <status> [<reply>]
//! ```
//!
//! where `<status>` is one of `ok`, `busy`, `closing`, `failed`, `cut` or
//! `timeout`, and `<visit> <reached>` is `- -` when the scoped binding could
//! not be resolved. There is no framing to learn because learning a framing is
//! not what a reader is here for.

use core::fmt;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use core::time::Duration;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gateway_contracts::{Handler, HandlerError, Request, RequestId};
use kernel::core::telemetry::{Level, Record as Diagnostic};
use kernel::core::{ComponentError, ComponentId, Criticality, Health, HealthProbe, RunError};
use kernel::{
    BootContext, BoxFuture, Children, Component, ComponentDescriptor, Extension, RestartPolicy,
    RunContext, Runnable, RunnableDescriptor, Scope, ShutdownContext,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::time::timeout;

/// The name the socket owner is registered and blamed under.
pub const DOORWAY: &str = "doorway";

/// The name the accept loop is registered and blamed under.
pub const ACCEPTOR: &str = "acceptor";

/// Upper bound on binding. Binding touches the operating system and nothing
/// else, so this is generous rather than tuned.
const BIND_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound on releasing. Dropping a listener is immediate.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(1);

/// How many accept failures in a row mean the socket is unusable.
///
/// One `accept` failure is ordinary — a descriptor limit, a connection reset
/// between the handshake and the accept — and retrying is right. A loop that
/// retries an error that will not clear is a busy loop, so the retry is
/// counted and the count is bounded.
const ACCEPT_GIVE_UP: u32 = 8;

/// The address to bind when the configuration names none.
///
/// Port zero, deliberately: every test in this example binds ephemerally and
/// reads back what it got, which is the only way a suite survives two runs
/// overlapping. An application states its own address in configuration.
pub const DEFAULT_ADDRESS: &str = "127.0.0.1:0";

/// How long a connection may stay silent before it is answered `timeout`.
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// What this feature needs before it can bind.
///
/// It lives next to the thing it configures, not in the bundle: the *values*
/// belong to whatever owns the resource, and only the *path* they are read
/// under is the bundle's business.
#[derive(Clone, Debug)]
pub struct Settings {
    /// The address to bind, as `TcpListener::bind` takes it.
    pub address: String,
    /// How long a connection may stay silent before it is answered and closed.
    ///
    /// This is not decoration. A caller that connects and says nothing holds a
    /// slot in the drain window, so a process that always waits forever for a
    /// first line can always be kept from draining by one idle connection.
    pub read_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            address: DEFAULT_ADDRESS.to_owned(),
            read_timeout: DEFAULT_READ_TIMEOUT,
        }
    }
}

// ---------------------------------------------------------------------------
// The resource
// ---------------------------------------------------------------------------

/// The bound socket, and the one [`Component`] in this crate.
///
/// It binds in [`boot`](Component::boot), publishes the address it actually
/// got, and releases in [`shutdown`](Component::shutdown). It watches no
/// stage and holds no loop — see the module documentation for why it could not
/// hold one usefully even if it wanted to.
///
/// # The address is published, not assumed
///
/// The configured address and the bound address are two different facts
/// whenever the configured port is zero, which is the case in every test here.
/// [`address`](Self::address) answers with the second, and
/// [`opened`](Self::opened) waits for it, so a caller never has to guess when
/// boot finished or what the kernel handed out.
#[derive(Debug)]
pub struct Doorway {
    /// The address that was asked for.
    wanted: String,
    /// The socket, while it is open.
    open: Mutex<Option<Arc<TcpListener>>>,
    /// The address that was granted, announced once and never withdrawn.
    ///
    /// It outlives the socket on purpose: a diagnostic written after the stop
    /// still has to be able to say which address this process was serving.
    announced: watch::Sender<Option<SocketAddr>>,
}

impl Doorway {
    /// A door that is not open yet.
    ///
    /// # Examples
    ///
    /// ```
    /// use gateway_component::{Doorway, Settings};
    ///
    /// let doorway = Doorway::new(Settings::default());
    /// assert!(!doorway.is_open());
    /// assert_eq!(doorway.address(), None);
    /// ```
    #[must_use]
    pub fn new(settings: Settings) -> Self {
        Self {
            wanted: settings.address,
            open: Mutex::new(None),
            announced: watch::Sender::new(None),
        }
    }

    /// The address that was actually bound, once boot has run.
    #[must_use]
    pub fn address(&self) -> Option<SocketAddr> {
        *self.announced.borrow()
    }

    /// Waits until the address is known, then answers with it.
    ///
    /// The wait an application and a test both need: the port is not knowable
    /// before boot, and polling for it is a race written out by hand. It is
    /// **unbounded on purpose** — a door that never opens never resolves it —
    /// so every caller wraps it in a timeout of its own. `None` is only ever
    /// returned if the announcement channel closed, which cannot happen while
    /// the caller holds this borrow.
    pub async fn opened(&self) -> Option<SocketAddr> {
        let mut changes = self.announced.subscribe();
        loop {
            if let Some(address) = *changes.borrow_and_update() {
                return Some(address);
            }
            if changes.changed().await.is_err() {
                return None;
            }
        }
    }

    /// The socket, while it is open.
    ///
    /// The [`Acceptor`] takes one clone of this and accepts on it. Closing
    /// therefore takes two drops — the one behind this lock, released by
    /// [`drain`](Component::drain), and the acceptor's own, released when its
    /// loop breaks on the same rung. Both land at `Draining`, and neither
    /// waits on the other.
    #[must_use]
    pub fn listener(&self) -> Option<Arc<TcpListener>> {
        self.held().clone()
    }

    /// Whether the socket is bound right now.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.held().is_some()
    }

    /// Releases the socket. Idempotent.
    ///
    /// Called twice on every clean run, and that is deliberate:
    /// [`drain`](Component::drain) calls it when the ladder reaches
    /// `Draining`, which is when the refusal has to start, and
    /// [`shutdown`](Component::shutdown) calls it again as the backstop for
    /// every path where the drain never ran — a graph that failed before phase
    /// five, a boot that is being rolled back.
    pub fn close(&self) {
        *self.held() = None;
    }

    /// The socket, whatever a previous panic did to the lock.
    ///
    /// The poison is recovered rather than propagated because this lock guards
    /// exactly one field and every mutation of it is a single statement: a
    /// caller that unwound while holding this guard left either the socket or
    /// no socket, never half of one. There is no torn value for the next
    /// caller to act on.
    ///
    /// A lock whose state *can* be half written earns no such permission; the
    /// honest call there is `.expect()`, which turns a torn value into a
    /// stopped process instead of a wrong answer.
    fn held(&self) -> MutexGuard<'_, Option<Arc<TcpListener>>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Binds, and publishes what was granted.
    ///
    /// Split out of [`boot`](Component::boot) because a [`BootContext`]
    /// carries nothing this needs, and a function that takes no context can be
    /// exercised by a test that builds no kernel.
    async fn bind(&self) -> Result<SocketAddr, std::io::Error> {
        let listener = TcpListener::bind(&self.wanted).await?;
        let address = listener.local_addr()?;

        // Announced only after both fallible steps have succeeded: a published
        // address for a socket that failed to bind is worse than none.
        *self.held() = Some(Arc::new(listener));
        self.announced.send_replace(Some(address));
        Ok(address)
    }
}

impl Component for Doorway {
    fn name() -> &'static str {
        DOORWAY
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
            .boot_timeout(BIND_TIMEOUT)
            .shutdown_timeout(RELEASE_TIMEOUT)
    }

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            let address = self.bind().await.map_err(|error| {
                ComponentError::new(
                    ComponentId::new(DOORWAY, 0),
                    format!("cannot bind {}: {error}", self.wanted).into(),
                )
            })?;

            cx.telemetry().record(
                Diagnostic::new(Level::Info, "gateway.bound")
                    .with("wanted", self.wanted.clone())
                    .with("bound", address.to_string()),
            );
            Ok(())
        })
    }

    /// Shuts the door, at the rung where refusing new work belongs.
    ///
    /// Refusing new work is a property of the RESOURCE, so it is the owner of
    /// the resource that does it. The accept loop is still running when this
    /// returns, and everything it already accepted is still its to finish.
    fn drain<'a>(
        &'a self,
        cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            // Idempotent, and cheap: dropping a listener touches the operating
            // system once. The acceptor's own clone goes when its loop breaks
            // on this same rung, and the address is unbound when the second of
            // the two drops lands.
            self.close();
            cx.telemetry()
                .record(Diagnostic::new(Level::Info, "gateway.door_shut").with(
                    "bound",
                    self.address().map_or_else(String::new, |a| a.to_string()),
                ));
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        _cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            // Almost always a no-op: on a clean run `drain` closed the socket a
            // whole drain window ago, because that is where the refusal had to
            // start. What is left here is the backstop for the paths that never
            // reached a drain at all.
            self.close();
            Ok(())
        })
    }
}

/// Reports on the socket.
///
/// It answers the one question a load balancer asks: is this process taking
/// connections? [`Health::Down`] the instant the drain shuts the door,
/// which is *before* the process stops and is exactly when traffic should stop
/// arriving.
#[derive(Debug)]
pub struct DoorwayProbe {
    /// The door being watched — the same object the kernel booted.
    doorway: Arc<Doorway>,
}

impl DoorwayProbe {
    /// Watches `doorway`.
    #[must_use]
    pub fn new(doorway: Arc<Doorway>) -> Self {
        Self { doorway }
    }
}

impl Extension for DoorwayProbe {}

impl HealthProbe for DoorwayProbe {
    fn name(&self) -> &'static str {
        DOORWAY
    }

    fn check(&self) -> BoxFuture<'_, Health> {
        let verdict = match (self.doorway.address(), self.doorway.is_open()) {
            (Some(_), true) => Health::Up,
            // Bound once, closed since: the acceptor shut the door when the
            // ladder reached `Draining`. Saying which address it was is the
            // whole value of keeping the announcement after the socket.
            (Some(address), false) => Health::down(format!("no longer accepting on {address}")),
            (None, _) => Health::down("not bound"),
        };
        Box::pin(async move { verdict })
    }
}

// ---------------------------------------------------------------------------
// The unit of work
// ---------------------------------------------------------------------------

/// One request's unit of work.
///
/// Bound [`Scoped`](kernel::Lifetime::Scoped) by the bundle, so it is built
/// once per [`Scope`] and shared by everything that resolves it inside that
/// scope. Two concurrent requests reach two different visits; two resolutions
/// inside one request reach the same one.
///
/// [`reach`](Self::reach) is what makes that observable from outside: it
/// counts resolutions, so a reply carrying `reached = 2` proves the second
/// resolution found the *same object* rather than a second one built to look
/// like it.
#[derive(Debug)]
pub struct Visit {
    /// Which unit of work this is, counting from one.
    id: u64,
    /// How many times it has been reached.
    reached: AtomicU32,
}

impl Visit {
    /// The visit numbered `id`, not yet reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use gateway_component::Visit;
    ///
    /// let visit = Visit::new(1);
    /// assert_eq!(visit.reach(), 1);
    /// assert_eq!(visit.reach(), 2);
    /// ```
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self {
            id,
            reached: AtomicU32::new(0),
        }
    }

    /// Which unit of work this is.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Counts one resolution and answers with the running total.
    pub fn reach(&self) -> u32 {
        self.reached.fetch_add(1, Ordering::Relaxed) + 1
    }
}

// ---------------------------------------------------------------------------
// The accept loop
// ---------------------------------------------------------------------------

/// What the acceptor has done, shared with whoever wants to look.
///
/// Plain counters rather than an event: a test asserting "nothing was accepted
/// after the door closed" wants a number it can read at any moment, and a
/// health probe is not the place to publish one.
#[derive(Debug, Default)]
pub struct Tally {
    /// Connections accepted.
    accepted: AtomicU64,
    /// Requests that got an answer from the handler, refusals included.
    answered: AtomicU64,
    /// Requests still running when `Stopping` arrived.
    cut: AtomicU64,
}

impl Tally {
    /// Connections accepted since the loop started.
    #[must_use]
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Requests the handler answered, refusals included.
    #[must_use]
    pub fn answered(&self) -> u64 {
        self.answered.load(Ordering::Relaxed)
    }

    /// Requests cut by the stop.
    #[must_use]
    pub fn cut(&self) -> u64 {
        self.cut.load(Ordering::Relaxed)
    }
}

/// The socket is unusable, so there is nothing to accept on.
#[derive(Debug)]
struct NotOpen;

impl fmt::Display for NotOpen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the door is not open")
    }
}

impl std::error::Error for NotOpen {}

/// Accepts on the door's socket and serves each request in its own scope.
///
/// This is the [`Runnable`], and everything the two-stage ladder exists for
/// happens in [`run`](Runnable::run):
///
/// 1. accept and serve while the ladder is at `Running`;
/// 2. at `Draining`, close the socket — new connections are refused by the
///    operating system from that instant — and let what is already accepted
///    run to completion;
/// 3. at `Stopping`, cut what is still running, give each cut request one last
///    line so its caller is told rather than reset, and return.
///
/// It never blocks indefinitely at any of the three, which is what makes it
/// safe for the kernel to await.
pub struct Acceptor {
    /// The socket owner. Held as the component the kernel booted, not as a
    /// second listener built alongside it.
    doorway: Arc<Doorway>,
    /// Whoever answers. The contract, resolved — no type of the feature that
    /// implements it is nameable here, which is the isolation rule working.
    handler: Arc<dyn Handler>,
    /// How long a connection may stay silent before it is answered `timeout`.
    read_timeout: Duration,
    /// What has happened, readable from outside.
    tally: Arc<Tally>,
}

impl fmt::Debug for Acceptor {
    /// Names the door and says nothing about the handler.
    ///
    /// `dyn Handler` carries no `Debug` bound and must not grow one: the
    /// contract is implemented by a feature this crate cannot name, and taxing
    /// every implementor for the sake of one diagnostic line is the wrong
    /// trade.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Acceptor")
            .field("address", &self.doorway.address())
            .field("read_timeout", &self.read_timeout)
            .finish_non_exhaustive()
    }
}

impl Acceptor {
    /// An acceptor over `doorway`, answering through `handler`.
    #[must_use]
    pub fn new(doorway: Arc<Doorway>, handler: Arc<dyn Handler>, read_timeout: Duration) -> Self {
        Self {
            doorway,
            handler,
            read_timeout,
            tally: Arc::new(Tally::default()),
        }
    }

    /// The counters, shared with the acceptor rather than copied from it.
    #[must_use]
    pub fn tally(&self) -> Arc<Tally> {
        Arc::clone(&self.tally)
    }

    /// The whole of the run, written as one function so the three stages are
    /// read in the order they happen.
    async fn accept_until_stopped(self: Arc<Self>, cx: RunContext) -> Result<(), RunError> {
        let Some(listener) = self.doorway.listener() else {
            // Essential, so this ends the process — which is right: a gateway
            // that cannot accept is a process pretending to serve.
            return Err(RunError::failed(cx.id(), Box::new(NotOpen)));
        };

        // Every accepted request runs as its own task. `Children` is the
        // kernel's own per-request task facility: it reaps as it goes, refuses
        // a child once the ladder has left `Running`, and knows how to wait the
        // set out under the two rungs — which is the whole of stages two and
        // three below.
        let mut serving = Children::new(cx.shutdown().clone());
        let mut numbered: u64 = 0;
        let mut failures: u32 = 0;

        // ---- Stage one: accept ------------------------------------------
        loop {
            tokio::select! {
                // Biased, and the order is the guarantee. Left to chance, a
                // connection ready in the same instant the ladder moves would
                // be accepted half the time, and "nothing is accepted after
                // draining" would be a statement about scheduling luck.
                biased;

                // Draining means stop taking new work, and a new connection is
                // new work. This is the branch a component could never have.
                () = cx.shutdown().draining() => break,

                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        failures = 0;
                        numbered += 1;
                        self.tally.accepted.fetch_add(1, Ordering::Relaxed);
                        // Refused only if the ladder moved between the biased
                        // branch above and here, which is the answer this loop
                        // wants: the connection is dropped unanswered because
                        // the door is already shut behind it.
                        let _ = serving.spawn(Arc::clone(&self).converse(
                            cx.clone(),
                            stream,
                            RequestId::new(numbered),
                        ));
                    }
                    Err(error) => {
                        failures += 1;
                        cx.telemetry().record(
                            Diagnostic::new(Level::Warn, "gateway.accept_failed")
                                .with("error", error.to_string())
                                .with("in_a_row", failures),
                        );
                        if failures >= ACCEPT_GIVE_UP {
                            return Err(RunError::failed(cx.id(), Box::new(error)));
                        }
                    }
                },
            }
        }

        // ---- Stage two: let go of the socket -----------------------------
        //
        // Only this loop's own clone. The one the component holds is released
        // by `Doorway::drain`, which the kernel calls on this same rung — the
        // door is the component's resource, and shutting it is the component's
        // business. The address is unbound when the second of the two drops
        // lands, and a caller connecting from then on is refused by the
        // operating system rather than left waiting on a process that is
        // leaving.
        drop(listener);
        cx.telemetry().record(
            Diagnostic::new(Level::Info, "gateway.draining").with("in_flight", serving.len()),
        );

        // ---- Stage three: the window, then the cut -----------------------
        //
        // Nothing is cancelled while the ladder is at `Draining` — that is the
        // entire point of there being two rungs instead of one. At `Stopping`
        // the same wait runs against the stage's own deadline, and whatever
        // ignores both is aborted. The budget is the ladder's throughout: a
        // bound this loop invented would be a second deadline nobody
        // configured, racing the one the supervisor is already enforcing.
        //
        // Each conversation races `stopping` itself and writes one last line,
        // so a cut caller is told rather than reset.
        let abandoned = serving.finish().await;
        if abandoned > 0 {
            cx.telemetry()
                .record(Diagnostic::new(Level::Warn, "gateway.cut").with("abandoned", abandoned));
        }

        Ok(())
    }

    /// One connection: a line in, one line out, then the socket closes.
    ///
    /// The scope is opened here, before anything is read, so that everything
    /// this request touches — including the failure paths — is inside one unit
    /// of work.
    async fn converse(self: Arc<Self>, cx: RunContext, stream: TcpStream, id: RequestId) {
        let scope = cx.container().scope();

        // First resolution. It exists to be the first: the second one, taken
        // when the answer is composed, must find this very object.
        if let Some(visit) = visit_of(&scope, &cx).await {
            visit.reach();
        }

        let mut wire = BufReader::new(stream);
        let mut line = String::new();

        let line = match timeout(self.read_timeout, wire.read_line(&mut line)).await {
            // The caller hung up without asking anything. Nothing to answer.
            Ok(Ok(0)) => return,
            Ok(Ok(_)) => line.trim_end().to_owned(),
            Ok(Err(error)) => {
                cx.telemetry().record(
                    Diagnostic::new(Level::Warn, "gateway.read_failed")
                        .with("request", id.to_string())
                        .with("error", error.to_string()),
                );
                return;
            }
            // Silent for too long. Answered rather than dropped, so the caller
            // learns why, and closed rather than kept, so it stops holding a
            // slot in a drain window that has not started yet.
            Err(_) => {
                say(&mut wire, &format!("{id} - - timeout"), &cx, id).await;
                return;
            }
        };

        let answer = self.answer(&cx, &scope, id, line).await;
        say(&mut wire, &answer, &cx, id).await;
    }

    /// Calls the handler, unless the stop arrives first, and renders the line.
    ///
    /// The race is against `stopping` and not against `draining`, and that is
    /// the whole behaviour under test: a request already in flight when the
    /// ladder reaches `Draining` runs to completion and its caller reads a real
    /// answer. Only the second rung cuts it.
    async fn answer(&self, cx: &RunContext, scope: &Scope, id: RequestId, line: String) -> String {
        let handler = Arc::clone(&self.handler);
        let outcome = tokio::select! {
            // Biased, handler first: when the work finishes in the same instant
            // the ladder moves, the caller gets its answer. A real result beats
            // a refusal whenever both are available.
            biased;
            result = handler.handle(Request::new(id, line)) => Some(result),
            () = cx.shutdown().stopping() => None,
        };

        // Second resolution, of the same contract, in the same scope. The
        // count it answers with is the proof that a scope is one object and
        // not one per lookup.
        let label = match visit_of(scope, cx).await {
            Some(visit) => format!("{} {}", visit.id(), visit.reach()),
            None => "- -".to_owned(),
        };

        match outcome {
            Some(Ok(reply)) => {
                self.tally.answered.fetch_add(1, Ordering::Relaxed);
                format!("{id} {label} ok {}", reply.line)
            }
            Some(Err(error)) => {
                self.tally.answered.fetch_add(1, Ordering::Relaxed);
                // The three refusals stay three words on the wire. Collapsing
                // them into one is how a caller learns to retry a failure and
                // give up on backpressure.
                let word = match error {
                    HandlerError::Busy => "busy",
                    HandlerError::Closing => "closing",
                    HandlerError::Failed(ref source) => {
                        cx.telemetry().record(
                            Diagnostic::new(Level::Error, "gateway.handler_failed")
                                .with("request", id.to_string())
                                .with("error", source.to_string()),
                        );
                        "failed"
                    }
                };
                format!("{id} {label} {word}")
            }
            // Cut. The caller is told, in the same shape as every other
            // answer, instead of reading a reset and guessing.
            None => {
                self.tally.cut.fetch_add(1, Ordering::Relaxed);
                format!("{id} {label} cut")
            }
        }
    }
}

impl Runnable for Acceptor {
    fn name() -> &'static str {
        ACCEPTOR
    }

    fn descriptor(&self) -> RunnableDescriptor {
        // Essential: an acceptor that has returned is a process with nothing
        // left to serve, so its return must end the run rather than leave a
        // bound socket nobody is reading.
        //
        // Never restarted, and that follows from the socket being someone
        // else's: a second run would need the door reopened, and reopening it
        // is the component's business at the next boot, not a retry's.
        //
        // No `drain_timeout` and no `stop_timeout`. A descriptor may only
        // shorten what the `ShutdownPolicy` grants, and the whole point of
        // this runnable is to use the window the application configured.
        RunnableDescriptor::new()
            .criticality(Criticality::Essential)
            .restart(RestartPolicy::Never)
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(self.accept_until_stopped(cx))
    }
}

/// The visit of the unit of work `scope` stands for.
///
/// `None` is a real finding and it is recorded — a scoped binding that cannot
/// be resolved is a graph defect, and phase three would have caught it in any
/// kernel that started.
///
/// It is *not* fatal to the request, and the difference from a working note
/// that a batch is written on is worth stating: a visit labels an answer, it
/// is not an input to it. Refusing a request the handler could have served
/// because a correlation number was unavailable turns a diagnostic's failure
/// into an outage. So the answer goes out with `- -` where the label would
/// have been, and the defect is reported to telemetry, where a defect belongs.
async fn visit_of(scope: &Scope, cx: &RunContext) -> Option<Arc<Visit>> {
    match scope.get::<Visit>().await {
        Ok(visit) => Some(visit),
        Err(error) => {
            cx.telemetry().record(
                Diagnostic::new(Level::Error, "gateway.visit_unreachable")
                    .with("error", error.to_string()),
            );
            None
        }
    }
}

/// Writes one line and flushes it.
///
/// A failure here is the caller's connection, not this process's problem: it
/// is recorded and the conversation ends. There is nobody left to tell.
async fn say(wire: &mut BufReader<TcpStream>, line: &str, cx: &RunContext, id: RequestId) {
    let written = async {
        wire.write_all(line.as_bytes()).await?;
        wire.write_all(b"\n").await?;
        wire.flush().await
    }
    .await;

    if let Err(error) = written {
        cx.telemetry().record(
            Diagnostic::new(Level::Warn, "gateway.write_failed")
                .with("request", id.to_string())
                .with("error", error.to_string()),
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use gateway_contracts::{HandlerError, Reply};
    use kernel::ShutdownController;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Notify;
    use tokio::time::Instant as Deadline;

    use super::*;

    /// Every wait in this module is bounded by this. A regression must fail
    /// the test, not wedge the suite.
    const BOUND: Duration = Duration::from_secs(5);

    /// How long to keep probing for a socket that should already be closed.
    const REFUSAL_BOUND: Duration = Duration::from_secs(2);

    /// A handler that echoes, and can be made to wait first.
    ///
    /// `release` is what lets a test hold a request open across a stage
    /// change, which is the only way the drain window can be observed at all.
    struct Echo {
        /// Held until notified, when a test asked for a slow handler.
        ///
        /// Released with `notify_one`, which stores a permit when nobody is
        /// waiting yet. `notify_waiters` would lose the wake-up if the test
        /// released before the handler parked, and a test that depends on that
        /// order is a test that fails on a loaded machine.
        release: Option<Arc<Notify>>,
        /// Never answers, whatever else is set.
        stuck: bool,
    }

    impl Echo {
        fn prompt() -> Arc<dyn Handler> {
            Arc::new(Self {
                release: None,
                stuck: false,
            })
        }

        fn held(release: Arc<Notify>) -> Arc<dyn Handler> {
            Arc::new(Self {
                release: Some(release),
                stuck: false,
            })
        }

        fn stuck() -> Arc<dyn Handler> {
            Arc::new(Self {
                release: None,
                stuck: true,
            })
        }
    }

    impl Handler for Echo {
        fn handle(
            self: Arc<Self>,
            request: Request,
        ) -> BoxFuture<'static, Result<Reply, HandlerError>> {
            Box::pin(async move {
                if self.stuck {
                    core::future::pending::<()>().await;
                }
                if let Some(release) = &self.release {
                    release.notified().await;
                }
                Ok(Reply::new(request.line))
            })
        }
    }

    /// A booted door on an ephemeral address, and the address it got.
    async fn booted() -> (Arc<Doorway>, SocketAddr) {
        let doorway = Arc::new(Doorway::new(Settings::default()));
        let detached = BootContext::detached();
        doorway
            .boot(&detached.context())
            .await
            .expect("an ephemeral address binds");
        let address = doorway.address().expect("boot published the address");
        (doorway, address)
    }

    /// Sends one line and reads one back, bounded.
    async fn ask(address: SocketAddr, line: &str) -> String {
        let stream = timeout(BOUND, TcpStream::connect(address))
            .await
            .expect("the connect is bounded")
            .expect("the door is open");
        let mut wire = BufReader::new(stream);
        wire.write_all(format!("{line}\n").as_bytes())
            .await
            .expect("the request is written");
        wire.flush().await.expect("the request is flushed");

        let mut answer = String::new();
        timeout(BOUND, wire.read_line(&mut answer))
            .await
            .expect("the answer is bounded")
            .expect("the answer is read");
        answer.trim_end().to_owned()
    }

    /// Whether the address stops taking connections within the bound.
    ///
    /// Polled rather than asserted once: closing the socket happens on the
    /// acceptor's task, so "already refused" is only true after that task has
    /// been scheduled. The poll is bounded, so a socket that never closes
    /// fails the test instead of hanging it.
    async fn refuses(address: SocketAddr) -> bool {
        let until = Deadline::now() + REFUSAL_BOUND;
        while Deadline::now() < until {
            match timeout(Duration::from_millis(100), TcpStream::connect(address)).await {
                Ok(Err(_)) => return true,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        false
    }

    /// The drain rung, exactly as the kernel walks it.
    ///
    /// Two things happen on it and neither is the other's: the ladder moves,
    /// and every component is told to refuse new work. The acceptor watches
    /// the first; the door is shut by the second. A test that moved only the
    /// ladder would be testing half a rung.
    async fn drain_rung(doorway: &Doorway, controller: &ShutdownController) {
        controller.begin_draining();
        let (detached, _stopping) = ShutdownContext::detached();
        doorway
            .drain(&detached.context())
            .await
            .expect("shutting the door cannot fail");
    }

    /// An acceptor over `doorway`, and the context that drives its ladder.
    fn acceptor(
        doorway: &Arc<Doorway>,
        handler: Arc<dyn Handler>,
    ) -> (Arc<Acceptor>, RunContext, ShutdownController) {
        let (cx, controller) = RunContext::detached();
        let acceptor = Arc::new(Acceptor::new(
            Arc::clone(doorway),
            handler,
            Duration::from_secs(1),
        ));
        (acceptor, cx, controller)
    }

    #[tokio::test]
    async fn binds_and_publishes() {
        let (doorway, address) = booted().await;

        assert!(doorway.is_open());
        assert_ne!(address.port(), 0, "port zero must be read back, not kept");
        assert_eq!(
            timeout(BOUND, doorway.opened())
                .await
                .expect("already open"),
            Some(address),
            "a caller arriving after boot is not left waiting"
        );
    }

    #[tokio::test]
    async fn shutdown_releases_the_socket() {
        let (doorway, address) = booted().await;
        let (detached, _controller) = ShutdownContext::detached();

        doorway
            .shutdown(&detached.context())
            .await
            .expect("releasing cannot fail");

        assert!(!doorway.is_open());
        assert!(refuses(address).await, "the address is unbound");
        // The address outlives the socket: a diagnostic written now still
        // knows what this process was serving.
        assert_eq!(doorway.address(), Some(address));
    }

    #[tokio::test]
    async fn probe_follows_the_door() {
        let doorway = Arc::new(Doorway::new(Settings::default()));
        let probe = DoorwayProbe::new(Arc::clone(&doorway));
        assert_eq!(probe.name(), DOORWAY);
        assert_eq!(probe.check().await, Health::down("not bound"));

        let detached = BootContext::detached();
        doorway.boot(&detached.context()).await.expect("it binds");
        assert_eq!(probe.check().await, Health::Up);

        // Down the instant the drain rung shuts the door, which is a whole
        // drain window before the process stops. That is what makes it useful.
        let (stopping, _controller) = ShutdownContext::detached();
        doorway
            .drain(&stopping.context())
            .await
            .expect("shutting the door cannot fail");
        assert!(matches!(probe.check().await, Health::Down { .. }));
    }

    #[tokio::test]
    async fn visit_counts_reaches() {
        let visit = Visit::new(7);

        assert_eq!(visit.id(), 7);
        assert_eq!(visit.reach(), 1);
        assert_eq!(visit.reach(), 2);
    }

    #[tokio::test]
    async fn serves_then_refuses_at_drain() {
        let (doorway, address) = booted().await;
        let (acceptor, cx, controller) = acceptor(&doorway, Echo::prompt());
        let tally = acceptor.tally();
        let running = tokio::spawn(Arc::clone(&acceptor).run(cx));

        // A detached context carries no bindings, so `Visit` resolves to
        // nothing and the label reads `- -`. The status word is what this test
        // is about; the numbered label is proved where a real graph exists.
        assert_eq!(ask(address, "work").await, "1 - - ok work");

        drain_rung(&doorway, &controller).await;
        let outcome = timeout(BOUND, running)
            .await
            .expect("the loop returns at drain")
            .expect("the task joined");
        assert!(outcome.is_ok(), "a clean stop: {outcome:?}");

        assert!(refuses(address).await, "no new connection after draining");
        assert_eq!(
            tally.accepted(),
            1,
            "nothing was accepted after the door shut"
        );
        assert_eq!(tally.answered(), 1);
        assert_eq!(tally.cut(), 0);
    }

    #[tokio::test]
    async fn finishes_in_the_window() {
        let (doorway, address) = booted().await;
        let release = Arc::new(Notify::new());
        let (acceptor, cx, controller) = acceptor(&doorway, Echo::held(Arc::clone(&release)));
        let tally = acceptor.tally();
        let running = tokio::spawn(Arc::clone(&acceptor).run(cx));

        // One request, held open by the handler.
        let asking = tokio::spawn(ask(address, "held"));
        while tally.accepted() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The rung is walked under a request that is already in flight.
        drain_rung(&doorway, &controller).await;
        assert!(refuses(address).await, "refusing starts at once");

        // And the held request still finishes, with a real answer.
        release.notify_one();
        let answer = timeout(BOUND, asking)
            .await
            .expect("the caller is answered inside the window")
            .expect("the client task joined");
        assert_eq!(answer, "1 - - ok held");
        assert_eq!(tally.cut(), 0, "nothing was cut: the window did its job");

        let outcome = timeout(BOUND, running)
            .await
            .expect("the loop returns once the window empties")
            .expect("the task joined");
        assert!(outcome.is_ok(), "a clean stop: {outcome:?}");
    }

    #[tokio::test]
    async fn cuts_at_stopping() {
        let (doorway, address) = booted().await;
        let (acceptor, cx, controller) = acceptor(&doorway, Echo::stuck());
        let tally = acceptor.tally();
        let running = tokio::spawn(Arc::clone(&acceptor).run(cx));

        let asking = tokio::spawn(ask(address, "forever"));
        while tally.accepted() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        drain_rung(&doorway, &controller).await;
        controller.begin_stopping();

        // What the cut costs, and what it does not: the request never
        // completes, and its caller is still told so in one line rather than
        // reading a reset and guessing.
        let answer = timeout(BOUND, asking)
            .await
            .expect("the cut request is answered, not dropped")
            .expect("the client task joined");
        assert_eq!(answer, "1 - - cut");
        assert_eq!(tally.cut(), 1);
        assert_eq!(tally.answered(), 0, "the handler never produced a reply");

        let outcome = timeout(BOUND, running)
            .await
            .expect("the loop returns at stopping")
            .expect("the task joined");
        assert!(outcome.is_ok(), "a clean stop: {outcome:?}");
    }

    #[tokio::test]
    async fn silence_is_answered() {
        let (doorway, address) = booted().await;
        let (cx, controller) = RunContext::detached();
        let acceptor = Arc::new(Acceptor::new(
            Arc::clone(&doorway),
            Echo::prompt(),
            Duration::from_millis(50),
        ));
        let running = tokio::spawn(Arc::clone(&acceptor).run(cx));

        // Connects, says nothing, and is answered rather than held: an idle
        // connection must not be able to keep a process from draining.
        let stream = timeout(BOUND, TcpStream::connect(address))
            .await
            .expect("bounded")
            .expect("the door is open");
        let mut wire = BufReader::new(stream);
        let mut answer = String::new();
        timeout(BOUND, wire.read_line(&mut answer))
            .await
            .expect("the silent caller is answered")
            .expect("the answer is read");
        assert_eq!(answer.trim_end(), "1 - - timeout");

        drain_rung(&doorway, &controller).await;
        let outcome = timeout(BOUND, running)
            .await
            .expect("the loop returns")
            .expect("the task joined");
        assert!(outcome.is_ok(), "a clean stop: {outcome:?}");
    }

    #[tokio::test]
    async fn closed_door_ends_the_run() {
        let doorway = Arc::new(Doorway::new(Settings::default()));
        let (acceptor, cx, _controller) = acceptor(&doorway, Echo::prompt());

        let outcome = timeout(BOUND, acceptor.run(cx))
            .await
            .expect("it returns at once");

        assert!(
            outcome.is_err(),
            "an acceptor with no socket must not report a clean run"
        );
    }

    #[tokio::test]
    async fn descriptors_state_the_bounds() {
        let doorway = Doorway::new(Settings::default());
        let component = doorway.descriptor();
        assert_eq!(component.boot_timeout, Some(BIND_TIMEOUT));
        assert_eq!(component.shutdown_timeout, Some(RELEASE_TIMEOUT));

        let acceptor = Acceptor::new(Arc::new(doorway), Echo::prompt(), Duration::from_secs(1));
        let runnable = acceptor.descriptor();
        assert_eq!(runnable.criticality, Criticality::Essential);
        assert_eq!(runnable.restart, RestartPolicy::Never);
        // Deliberately unbounded here: the window this runnable uses is the
        // one the application configured, and a descriptor can only shorten it.
        assert_eq!(runnable.drain_timeout, None);
        assert_eq!(runnable.stop_timeout, None);
    }
}
