//! The drain window, put under load: six claims, each against a real socket
//! and a real kernel.
//!
//! `crates/kernel/tests/window.rs` proves the window EXISTS — a timer holding
//! work at `Draining` is not cut before `Stopping`. It cannot prove the window
//! is USEFUL, because a timer accepts nothing and so cannot tell the two rungs
//! apart. This file is the other half: something that takes work from outside
//! finishes what it holds and refuses what it does not, and a client on the
//! other end of a TCP connection reads the difference.
//!
//! # What the shape of the example forces, and what these tests hang on
//!
//! A [`Component`](kernel::Component) is handed its shutdown context only when
//! the kernel comes to stop it, which is AFTER every runnable has already
//! returned. By then the drain window is over, so a component cannot react to
//! `Draining` at all. Only a runnable holds a `RunContext`, and only a
//! `RunContext` carries the ladder. The socket may be owned by a component —
//! bound at boot, released at shutdown — but the accept loop that stops
//! accepting at `Draining` and finishes in flight before `Stopping` is a
//! runnable, necessarily.
//!
//! Every test below reads that split from outside the process:
//! [`drain_refuses_new_work`] watches the door shut while the process is still
//! up and still working, which is a thing no component could have arranged.
//!
//! # The wire, in one line
//!
//! A line in, a line out. Out is
//!
//! ```text
//! <request> <visit> <reaches> <status> [<request> <docket> <stamps> <job> <line>]
//! ```
//!
//! The two scoped counters are why the format has any fields at all. `reaches`
//! and `stamps` read 2 on a served request because each feature resolved its
//! own scoped binding twice inside one unit of work and reached the same
//! object both times; `visit` and `docket` differ between two concurrent
//! requests because they are two units of work. That is what a scope IS, and
//! it is checkable from a socket without a protocol.
//!
//! # Bounds
//!
//! Every wait here has one, and the port is always zero. A test that can wedge
//! CI on a regression is worse than no test, and a suite that names a port
//! fails whenever two runs overlap.

use core::future::Future;
use core::time::Duration;
use std::net::SocketAddr;
use std::sync::Arc;

use gateway_bundle::{Acceptor, Doorway, GatewayBundle, Tally};
use kernel::core::telemetry::{FieldValue, Record};
use kernel::core::{ConfigNode, ConfigTree, RecordingTelemetry};
use kernel::{MemorySource, Outcome, ShutdownPolicy};
use kernel_testkit::{TestBuilder, TestHarness};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::Instant;

/// Upper bound on every wait in this file.
///
/// Generous, because it is not a measurement: it is the difference between a
/// regression that fails and a regression that hangs a build.
const BOUND: Duration = Duration::from_secs(15);

/// How often a poll looks again.
const POLL: Duration = Duration::from_millis(5);

/// The address every test asks for.
///
/// Port zero, and the port that was granted is read back from the door. A
/// fixed port makes the suite fail whenever two runs overlap, which is a
/// failure about the harness rather than about the code.
const EPHEMERAL: &str = "127.0.0.1:0";

// ---------------------------------------------------------------------------
// The service under test
// ---------------------------------------------------------------------------

/// What a test asks the two features for.
///
/// All four are configuration keys the bundles read, so a test states its
/// setup the way a deployment would rather than by reaching inside anything.
#[derive(Clone, Copy, Debug)]
struct Setup {
    /// `worker.capacity`: how many jobs may wait for a hand.
    capacity: i64,
    /// `worker.hold`: how long one job takes. This is the knob that makes a
    /// request outlive a stage change, and without it nothing here would
    /// measure anything.
    hold: &'static str,
    /// `worker.trip`: whether the ancillary foreman fails once on purpose.
    trip: bool,
    /// The two budgets the ladder runs on.
    policy: ShutdownPolicy,
}

impl Setup {
    /// The configuration tree, built from the four values above.
    fn source(self) -> MemorySource {
        let mut tree = ConfigTree::empty();
        for (path, node) in [
            ("gateway.address", ConfigNode::from(EPHEMERAL)),
            ("worker.capacity", ConfigNode::from(self.capacity)),
            ("worker.hold", ConfigNode::from(self.hold)),
            ("worker.trip", ConfigNode::from(self.trip)),
        ] {
            tree.insert(path, node)
                .expect("literal paths cannot collide");
        }
        MemorySource::named("test", tree)
    }
}

/// A running kernel holding both features, and the handles a test asks it
/// questions through.
struct Service {
    /// The kernel, on its own task.
    harness: TestHarness,
    /// The component that owns the socket, so a test can watch the door shut.
    doorway: Arc<Doorway>,
    /// The port that was actually granted.
    address: SocketAddr,
    /// What the accept loop has done, shared rather than copied.
    tally: Arc<Tally>,
}

impl Service {
    /// Boots both features and waits until the door is open.
    ///
    /// The two bundles are named here and nowhere else: this is the
    /// application layer, and it is the only place they meet. Neither depends
    /// on the other — they are joined by the contracts crates and the
    /// container.
    async fn start(setup: Setup) -> Self {
        let harness = TestBuilder::new()
            .shutdown_policy(setup.policy)
            .config_source(setup.source())
            .bundle(GatewayBundle::new())
            .bundle(worker_bundle::Bundled)
            .start()
            .await
            .expect("each feature closes the other's graph");

        let doorway = harness
            .container()
            .get::<Doorway>()
            .await
            .expect("the door is bound");
        let address = bounded("the door to open", doorway.opened())
            .await
            .expect("the announcement outlives the socket");
        let tally = harness
            .container()
            .get::<Acceptor>()
            .await
            .expect("the accept loop is a binding like any other")
            .tally();

        Self {
            harness,
            doorway,
            address,
            tally,
        }
    }

    /// Waits until the worker feature has admitted `at_least` jobs.
    ///
    /// Accepting a connection and admitting the job it carries are two
    /// different facts, and only the second one puts work in the drain
    /// window's way. The foreman publishes the bench's counters on a turn of
    /// its own, so this reads what an operator would read; there is nothing
    /// else outside the process that says a job is being worked rather than
    /// merely accepted.
    async fn until_admitted(&self, at_least: i64) {
        let telemetry = self.harness.telemetry();
        until("a job to be admitted", || {
            telemetry.records().iter().any(|record| {
                record.event == "worker.bench"
                    && matches!(record.field("admitted"), Some(FieldValue::Int(seen)) if *seen >= at_least)
            })
        })
        .await;
    }

    /// Asks for the stop and waits for the run to end.
    async fn stop(self) -> Outcome {
        bounded("the kernel to stop", self.harness.stop()).await
    }

    /// Waits for a stop that was already asked for.
    async fn wait(self) -> Outcome {
        bounded("the kernel to stop", self.harness.wait()).await
    }
}

// ---------------------------------------------------------------------------
// The client. The tests are the client; there is nothing else.
// ---------------------------------------------------------------------------

/// One answer, split into the fields the two features put on it.
#[derive(Debug)]
struct Answer {
    /// What the acceptor numbered the request.
    request: u64,
    /// The gateway's scoped unit of work.
    visit: u64,
    /// How many times that visit was resolved inside this one request.
    reaches: u64,
    /// `ok`, `busy`, `closing`, `failed`, `cut` or `timeout`.
    status: String,
    /// The worker feature's half, present only when the request was served.
    work: Option<Work>,
}

/// The worker feature's half of a served answer.
#[derive(Debug)]
struct Work {
    /// The worker's scoped unit of work, which is not the gateway's.
    docket: u64,
    /// How many times that docket was resolved inside this one request.
    stamps: u64,
    /// The job the queue admitted.
    job: u64,
    /// The line that went in.
    line: String,
}

/// Reads one answer off the wire, or says what was unreadable.
fn parse(raw: &str) -> Answer {
    let line = raw.trim_end();
    let field: Vec<&str> = line.split(' ').collect();

    // Four fields is a refusal or a cut; nine is a served request. Anything
    // else is a change to the format, and a test that guessed past it would
    // report the wrong thing.
    let work = match field.len() {
        4 => None,
        9 => Some(Work {
            docket: number(line, field[5]),
            stamps: number(line, field[6]),
            job: number(line, field[7]),
            line: field[8].to_owned(),
        }),
        _ => panic!("unreadable answer {line:?}"),
    };

    Answer {
        request: number(line, field[0]),
        visit: number(line, field[1]),
        reaches: number(line, field[2]),
        status: field[3].to_owned(),
        work,
    }
}

/// One numeric field, blamed with the line it came from.
fn number(line: &str, field: &str) -> u64 {
    field
        .parse()
        .unwrap_or_else(|_| panic!("{field:?} is not a number, in {line:?}"))
}

/// Opens a connection and says nothing on it.
///
/// A conversation the acceptor has taken that has asked for nothing yet.
/// Separate from [`ask`] because the two halves have to be moved apart in
/// time: [`held_connection_survives_the_drain`] connects before the ladder
/// moves and speaks after it.
async fn hold(address: SocketAddr) -> BufReader<TcpStream> {
    BufReader::new(
        bounded("a connection", TcpStream::connect(address))
            .await
            .expect("the door is open"),
    )
}

/// Sends one line on a connection that is already open.
async fn speak(wire: &mut BufReader<TcpStream>, line: &str) {
    bounded(
        "the request",
        wire.get_mut().write_all(format!("{line}\n").as_bytes()),
    )
    .await
    .expect("the request reaches the socket");
}

/// Opens a connection and sends one line, without waiting for the answer.
///
/// Returning before the answer is the whole point: a test holds the request
/// open, moves the ladder underneath it, and only then reads.
async fn ask(address: SocketAddr, line: &str) -> BufReader<TcpStream> {
    let mut wire = hold(address).await;
    speak(&mut wire, line).await;
    wire
}

/// Reads the one line the service answers with.
async fn answer(wire: &mut BufReader<TcpStream>) -> Answer {
    let mut line = String::new();
    let read = bounded("an answer", wire.read_line(&mut line))
        .await
        .expect("the connection is readable");
    assert!(read > 0, "the connection was reset instead of answered");
    parse(&line)
}

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

/// Awaits `future` under [`BOUND`], and fails rather than hangs.
async fn bounded<F: Future>(what: &str, future: F) -> F::Output {
    tokio::time::timeout(BOUND, future)
        .await
        .unwrap_or_else(|_| panic!("waiting for {what} outran its bound"))
}

/// Polls `ready` until it holds, under [`BOUND`].
async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + BOUND;
    while !ready() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        tokio::time::sleep(POLL).await;
    }
}

/// The `runnable` field of a supervisor record.
fn blamed(record: &Record) -> String {
    match record.field("runnable") {
        Some(FieldValue::Str(name)) => name.clone(),
        other => panic!("a supervisor record names its runnable: {other:?}"),
    }
}

/// Every runnable the supervisor recorded `event` about, in order.
///
/// It reads the sink rather than the harness because the records that matter
/// most are written on the way out: a test asking about them holds the sink
/// across the stop, which it could not do with a harness the stop consumed.
fn supervised(telemetry: &RecordingTelemetry, event: &str) -> Vec<String> {
    telemetry
        .records()
        .iter()
        .filter(|record| record.event == event)
        .map(blamed)
        .collect()
}

// ---------------------------------------------------------------------------
// 1. Completion in the window
// ---------------------------------------------------------------------------

/// A request already in flight when the stop is asked for gets a real answer.
///
/// This is the claim the whole example exists for, and it is written so that
/// it cannot pass by being fast. The job takes 800ms; the stop is asked for
/// once the worker has reported the job admitted, which is a few hundred
/// milliseconds in at most. So more than half the work is still ahead of the
/// request when the ladder starts moving, and the drain budget — two seconds,
/// several times what is left to do — is what lets it finish.
///
/// The discriminator is the assertion that the door had ALREADY shut when the
/// answer arrived: the process was refusing new connections, `Draining` was
/// long past, and this request still came back `ok`. Collapse the two rungs
/// into one and the same line reads `cut`, which is what
/// [`stop_cuts_the_overrun`] shows on purpose.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_request_completes() {
    let service = Service::start(Setup {
        capacity: 4,
        hold: "800ms",
        trip: false,
        policy: ShutdownPolicy::new(Duration::from_secs(2), Duration::from_millis(500)),
    })
    .await;

    let mut wire = ask(service.address, "held").await;
    service.until_admitted(1).await;

    // From here the ladder is moving under a request that has not finished.
    service.harness.handle().shutdown();

    let answer = answer(&mut wire).await;
    assert_eq!(answer.status, "ok", "held work must finish: {answer:?}");
    assert!(
        !service.doorway.is_open(),
        "the door had already shut when the answer arrived, and it arrived anyway"
    );

    let work = answer
        .work
        .expect("a served request carries the worker's half");
    assert_eq!(work.line, "held");
    assert_eq!(answer.reaches, 2, "one unit of work, resolved twice");
    assert_eq!(work.stamps, 2, "and the same again in the other feature");

    assert_eq!(service.tally.answered(), 1);
    assert_eq!(service.tally.cut(), 0, "nothing was cut");

    let outcome = service.wait().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

// ---------------------------------------------------------------------------
// 2. Refusal at drain
// ---------------------------------------------------------------------------

/// Once draining starts, a new connection is refused while the old one works.
///
/// Both halves matter and neither is worth anything alone. A process that has
/// exited refuses connections too, so the refusal is only evidence of a drain
/// window if the process is still up and still serving when it happens — which
/// is why a slow request is held open across it and read afterwards.
///
/// This is also where the acceptor's choice is visible from outside. It CLOSES
/// the listener rather than merely ceasing to select on it, so the operating
/// system refuses the probe in the same millisecond. Had it left the socket
/// bound, the probe would have connected into the backlog and waited for an
/// answer nobody would ever write, and this assertion would fail — as it
/// should, because a caller left hanging until the process exits learns
/// nothing it can act on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_refuses_new_work() {
    let service = Service::start(Setup {
        capacity: 4,
        hold: "800ms",
        trip: false,
        policy: ShutdownPolicy::new(Duration::from_secs(2), Duration::from_millis(500)),
    })
    .await;

    let mut wire = ask(service.address, "held").await;
    service.until_admitted(1).await;
    service.harness.handle().shutdown();

    // The instant `Draining` reached the accept loop. No component could have
    // arranged this: a component is not told anything until every runnable has
    // already returned.
    until("the door to shut", || !service.doorway.is_open()).await;
    assert!(
        service.harness.is_running(),
        "the process is still up, and still holding a request"
    );

    let probe = bounded("the probe", TcpStream::connect(service.address)).await;
    assert!(
        probe.is_err(),
        "a connection during the drain must be refused, not queued: {probe:?}"
    );

    // And the request that was already held is unaffected by any of it.
    let answer = answer(&mut wire).await;
    assert_eq!(answer.status, "ok", "{answer:?}");
    assert_eq!(
        service.tally.accepted(),
        1,
        "the probe was never accepted, so it never became work"
    );

    let outcome = service.wait().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

/// A connection taken before the door shut is answered after it, not reset.
///
/// The other half of the refusal, and the half nothing else here checks. Every
/// document in this example claims the window holds the whole CONVERSATION and
/// not only the request already admitted; that claim is what makes closing the
/// listener safe. It is also the thing an acceptor is likeliest to get wrong,
/// because "close the door" is easy to write as "close everything the door
/// produced" — and a caller that had connected and not yet spoken then reads a
/// reset, which is the outcome the whole ladder exists to avoid.
///
/// The status word is deliberately not pinned, because which door shut first
/// is a race and both answers are correct: the socket closes the instant
/// `Draining` fires, the bench closes when the hand next comes up for air, so
/// a late line is either admitted and served or refused `closing`. What is not
/// a race is that a line comes back at all — [`answer`] fails on an empty
/// read, which is exactly what a reset looks like from out here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_connection_survives_the_drain() {
    let service = Service::start(Setup {
        capacity: 4,
        hold: "400ms",
        trip: false,
        policy: ShutdownPolicy::new(Duration::from_secs(2), Duration::from_millis(500)),
    })
    .await;

    let mut working = ask(service.address, "in-flight").await;
    let mut silent = hold(service.address).await;

    // Both conversations belong to the acceptor before the ladder moves. A
    // connection still sitting in the backlog would go away with the listener,
    // and this test would then be about something else entirely.
    until("both connections to be accepted", || {
        service.tally.accepted() == 2
    })
    .await;
    service.until_admitted(1).await;

    service.harness.handle().shutdown();
    until("the door to shut", || !service.doorway.is_open()).await;

    // Spoken after the door shut, on a socket taken before it.
    speak(&mut silent, "late").await;

    let late = answer(&mut silent).await;
    assert!(
        matches!(late.status.as_str(), "ok" | "closing"),
        "a conversation the acceptor holds is answered, never reset: {late:?}"
    );

    let held = answer(&mut working).await;
    assert_eq!(
        held.status, "ok",
        "and the admitted request finishes: {held:?}"
    );
    assert_eq!(service.tally.cut(), 0, "nothing was cut");

    let outcome = service.wait().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

// ---------------------------------------------------------------------------
// 3. The cut at stop
// ---------------------------------------------------------------------------

/// Work that outlives the budget is cut, and both audiences are told.
///
/// The mirror of [`held_request_completes`], differing only in arithmetic: the
/// job takes five seconds and the whole ladder is 250 milliseconds, so the
/// request cannot finish and the kernel does not wait for it. This is where a
/// reader sees what the guarantee costs.
///
/// Two things are told, and a system that tells only one is broken in a way
/// this example must not teach. The CALLER reads `cut` — a line in the same
/// shape as every other answer, rather than a reset it would have to guess
/// about. The OPERATOR reads `runnable.abandoned` naming the unit that ignored
/// the deadline. Exactly one runnable is named: the accept loop and the
/// foreman both returned within their budgets, and only the hand holding the
/// five-second job did not.
///
/// The outcome is a success, and that is not an oversight. The stop was asked
/// for and the stop happened; a deadline the kernel enforced is the kernel
/// working, not failing. What went wrong went to telemetry, which is where a
/// thing an operator must act on belongs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_cuts_the_overrun() {
    let service = Service::start(Setup {
        capacity: 4,
        hold: "5s",
        trip: false,
        policy: ShutdownPolicy::new(Duration::from_millis(100), Duration::from_millis(150)),
    })
    .await;

    let mut wire = ask(service.address, "overrun").await;
    service.until_admitted(1).await;
    service.harness.handle().shutdown();

    let answer = answer(&mut wire).await;
    assert_eq!(answer.status, "cut", "{answer:?}");
    assert!(answer.work.is_none(), "there is no result to report");
    assert_eq!(
        answer.reaches, 2,
        "a cut request is still one unit of work, and is still labelled"
    );

    // Both read across the stop: the abandonment is filed as the ladder ends,
    // so a test that looked before the run returned would look too early.
    let tally = Arc::clone(&service.tally);
    let telemetry = service.harness.telemetry();
    let outcome = service.wait().await;

    assert!(outcome.is_success(), "the stop was asked for: {outcome:?}");
    assert_eq!(tally.cut(), 1);
    assert_eq!(tally.answered(), 0, "nothing was answered by the handler");
    assert_eq!(
        supervised(&telemetry, "runnable.abandoned"),
        ["hand"],
        "the one unit that outlived the deadline is named, and only it"
    );
}

// ---------------------------------------------------------------------------
// 4. A real unit of work
// ---------------------------------------------------------------------------

/// Two requests in flight together are two scopes; one request is one.
///
/// The medium example showed the syntax of a scope. This shows what a scope
/// IS, through a socket, with both features answering at once: each resolves
/// its own scoped binding twice inside one request and reaches the same object
/// both times — `reaches` and `stamps` read 2 — while the two requests reach
/// different objects, so `visit` and `docket` differ.
///
/// The concurrency is asserted rather than assumed. Both connections are
/// accepted before either is answered, which is what makes "two units of work
/// alive at once" a fact of this run instead of a property that would hold
/// just as well one request after another.
///
/// A `Shared` binding would give both requests one object and count 1, 2, 3, 4
/// across them; a transient one would give each resolution its own and count 1
/// every time. Only `Scoped` reads 2 and 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scopes_are_per_request() {
    let service = Service::start(Setup {
        capacity: 4,
        hold: "400ms",
        trip: false,
        policy: ShutdownPolicy::new(Duration::from_millis(300), Duration::from_millis(300)),
    })
    .await;

    let mut first = ask(service.address, "first").await;
    let mut second = ask(service.address, "second").await;

    until("both requests to be accepted", || {
        service.tally.accepted() == 2
    })
    .await;
    assert_eq!(
        service.tally.answered(),
        0,
        "both requests are in flight at the same moment"
    );

    let one = answer(&mut first).await;
    let two = answer(&mut second).await;
    assert_eq!(one.status, "ok", "{one:?}");
    assert_eq!(two.status, "ok", "{two:?}");

    let one_work = one
        .work
        .expect("a served request carries the worker's half");
    let two_work = two
        .work
        .expect("a served request carries the worker's half");

    // One request resolving twice reaches the same object, in both features.
    assert_eq!(one.reaches, 2);
    assert_eq!(two.reaches, 2);
    assert_eq!(one_work.stamps, 2);
    assert_eq!(two_work.stamps, 2);

    // Two requests reach two objects, in both features.
    assert_ne!(one.request, two.request);
    assert_ne!(one.visit, two.visit, "two units of work, two visits");
    assert_ne!(one_work.docket, two_work.docket, "and two dockets");
    assert_ne!(one_work.job, two_work.job);

    let outcome = service.stop().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

// ---------------------------------------------------------------------------
// 5. Backpressure
// ---------------------------------------------------------------------------

/// A full queue refuses, and the caller can tell a refusal from a failure.
///
/// The bound is one waiting job and the work takes 400ms, so at most two of
/// the five requests can be in the system at once: one on the bench and one
/// waiting for it. The rest are turned away at the door — nothing is enqueued
/// for them, and no identity is spent on them.
///
/// What is under test is the WORD, not the count. `busy` and `failed` are two
/// different instructions: the first says the system is at its declared
/// capacity and the same request may succeed later, the second says something
/// broke and retrying may repeat it. A caller that cannot tell them apart
/// retries the wrong one, and a queue that grows without bound instead of
/// saying either is the commonest production defect this shape invites.
///
/// The refusals are also counted on the telemetry stream, one per refusal the
/// caller saw. Backpressure an operator cannot see is backpressure nobody
/// acts on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_queue_refuses() {
    /// How many requests arrive at once.
    const ASKED: usize = 5;

    let service = Service::start(Setup {
        capacity: 1,
        hold: "400ms",
        trip: false,
        policy: ShutdownPolicy::new(Duration::from_millis(300), Duration::from_millis(300)),
    })
    .await;

    let mut wires = Vec::with_capacity(ASKED);
    for number in 0..ASKED {
        wires.push(ask(service.address, &format!("job{number}")).await);
    }

    let mut answers = Vec::with_capacity(ASKED);
    for wire in &mut wires {
        answers.push(answer(wire).await);
    }

    let served = answers.iter().filter(|a| a.status == "ok").count();
    let refused = answers.iter().filter(|a| a.status == "busy").count();

    assert_eq!(
        served + refused,
        ASKED,
        "every request was either served or refused, and nothing else: {answers:?}"
    );
    assert!(
        (1..=2).contains(&served),
        "a bound of one admits the job on the bench and one behind it: {answers:?}"
    );
    assert!(
        !answers.iter().any(|a| a.status == "failed"),
        "a refusal is not a failure: {answers:?}"
    );

    let recorded = service
        .harness
        .telemetry()
        .records()
        .iter()
        .filter(|record| record.event == "worker.refused")
        .count();
    assert_eq!(
        recorded, refused,
        "every refusal the caller saw is on the telemetry stream too"
    );

    let outcome = service.stop().await;
    assert!(outcome.is_success(), "{outcome:?}");
}

// ---------------------------------------------------------------------------
// 6. An ancillary worker that fails
// ---------------------------------------------------------------------------

/// The foreman panics on purpose, is restarted, and the request never learns.
///
/// The failure is a demonstration and the worker feature says so where it
/// happens. What is under test here is that it stays contained: an ancillary
/// runnable coming apart is not a reason to take a process down, and it is not
/// a reason for a request already accepted to end badly.
///
/// The spanning is asserted rather than hoped for. The request is accepted
/// first; the restart is then waited for; and the acceptor's counters are read
/// at that moment to show the request had not yet been answered. So the
/// supervisor filed a failure and started a runnable again UNDER an open
/// connection, and the answer that arrives afterwards is intact.
///
/// Exactly one runnable is restarted. If the accept loop or the hand were ever
/// restarted this list would say so, and the containment claim would be false.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ancillary_panic_is_contained() {
    let service = Service::start(Setup {
        capacity: 4,
        hold: "600ms",
        trip: true,
        policy: ShutdownPolicy::new(Duration::from_millis(300), Duration::from_millis(300)),
    })
    .await;

    let mut wire = ask(service.address, "steady").await;
    until("the request to be accepted", || {
        service.tally.accepted() == 1
    })
    .await;

    assert_eq!(
        service
            .harness
            .wait_for_record("runnable.restarted", 1)
            .await,
        1,
        "the supervisor started the failed runnable again"
    );
    assert_eq!(
        service.tally.answered(),
        0,
        "the restart landed while the request was still in flight"
    );

    let telemetry = service.harness.telemetry();
    assert_eq!(supervised(&telemetry, "runnable.failed"), ["foreman"]);
    assert_eq!(
        supervised(&telemetry, "runnable.restarted"),
        ["foreman"],
        "nothing that serves a request was restarted"
    );

    let answer = answer(&mut wire).await;
    assert_eq!(answer.status, "ok", "the request never learned: {answer:?}");
    assert_eq!(
        answer
            .work
            .expect("a served request carries the worker's half")
            .line,
        "steady"
    );
    assert_eq!(service.tally.cut(), 0);
    assert!(service.harness.is_running(), "the process is still up");

    let outcome = service.stop().await;
    assert!(outcome.is_success(), "{outcome:?}");
}
