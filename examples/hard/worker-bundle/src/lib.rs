//! Whoever actually answers, and the thing that says no.
//!
//! This bundle provides [`Handler`] — the contract `gateway-contracts`
//! publishes — and backs it with a bounded queue, [`WorkQueue`] from
//! `worker-contracts`. It depends on those two contracts crates and on no
//! bundle, which is the rule `ci/check-bundle-graph.sh` enforces: nothing below
//! names a type of the gateway feature, and the gateway feature names nothing
//! of this one.
//!
//! # Why the door is closed by a runnable and never by a component
//!
//! This is the single most useful thing in the hard example, and the design
//! document does not say it.
//!
//! A [`Component`](kernel::Component) is handed its `ShutdownContext` when the
//! kernel comes to stop it, and that happens *after* every runnable has already
//! returned. By then the drain window is over. A component therefore never
//! observes `Draining` at all — it observes the end, which is a different fact.
//!
//! Only a [`Runnable`] holds a `RunContext`, and only a `RunContext` carries
//! the token that tells *stop taking new work* from *stop now*. So the unit
//! that closes a door at `Draining` is a runnable, necessarily. In the gateway
//! feature that door is the listening socket; here it is the queue, and `Hand` —
//! the runnable that works the bench — is what closes it. The queue itself is a
//! plain contract-bound value with no lifecycle of its own; it could not close
//! itself at the right moment if it wanted to, because nothing hands it the
//! ladder.
//!
//! The consequence is worth stating plainly: both doors shut on the same rung.
//! A connection accepted just before `Draining` whose line arrives just after it
//! is refused with [`HandlerError::Closing`], and that is not a bug — it is the
//! same refusal the acceptor makes, one layer in. What the window protects is
//! work already *admitted*, and that work runs to completion.
//!
//! # What it registers
//!
//! * `Bench` — the bounded queue, bound both as itself and as
//!   [`WorkQueue`]. It refuses at the door: [`Refusal::Full`] when the bound is
//!   reached, [`Refusal::Closed`] once the ladder has moved. It never waits for
//!   room, because a queue that waits for room is an unbounded queue wearing a
//!   bounded queue's name.
//! * `Clerk` — the [`Handler`]. It submits first and opens a [`Scope`] second,
//!   so a refused request spends no docket; then one request is one unit of
//!   work, and the queue's two refusals become the handler's two refusals.
//! * `Docket` — a [`Lifetime::Scoped`] binding. Two concurrent requests get two
//!   dockets; one request resolving twice gets one docket. That is what a scope
//!   *is*.
//! * `Hand` — the runnable that works the bench, closes it at `Draining`, and
//!   returns at `Stopping`.
//! * `Foreman` — an ancillary runnable that trips on purpose the first time it
//!   runs, is restarted by its own policy, and leaves every request in flight
//!   untouched. The failure is a demonstration, not a bug, and the code says so
//!   where it happens.
//!
//! # The wire format is one line, and it is not a contract
//!
//! A reply is five space-separated fields:
//!
//! ```text
//! <request-id> <docket-id> <docket-stamps> <job-id> <line>
//! ```
//!
//! `docket-stamps` is there for one reason: the clerk resolves its docket twice
//! and stamps it each time, so a reader outside the process can see that both
//! resolutions reached the same object. Anything richer would be a protocol,
//! and a reader must not have to learn a protocol to read the shutdown
//! behaviour.

use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use core::time::Duration;
use std::sync::Arc;

use gateway_contracts::{Handler, HandlerError, Reply, Request};
use kernel::core::{
    Backoff, BuildError, ConfigError, ContainerError, Level, Record, RegisterError, RunError,
};
use kernel::{
    BoxFuture, Bundle, BundleManifest, Container, ContractRef, Criticality, Lifetime, Provider,
    Registry, RestartPolicy, RunContext, Runnable, RunnableDescriptor, Scope, Telemetry,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use worker_contracts::{Done, Job, JobId, Refusal, Ticket, WorkError, WorkQueue};

/// The name this bundle publishes, and the prefix of every record it writes.
const NAME: &str = "worker";

/// How many jobs may wait for a hand when nothing says otherwise.
const CAPACITY: usize = 4;

/// How long one job takes when nothing says otherwise.
///
/// Non-zero on purpose. The work *is* the waiting here, and a job that finished
/// instantly could never be in flight when the ladder moves — which is the one
/// thing this example exists to show.
const HOLD: Duration = Duration::from_millis(50);

/// How long between two foreman reports.
const WATCH: Duration = Duration::from_millis(250);

/// How long the supervisor waits before starting the foreman again.
const REGROUP: Duration = Duration::from_millis(10);

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// What this feature reads out of the configuration tree.
///
/// Every field has a default, so the bundle registers in an application that
/// configured nothing.
#[derive(Clone, Copy, Debug)]
struct Settings {
    /// How many jobs may wait for a hand.
    capacity: usize,
    /// How long one job takes.
    hold: Duration,
    /// Whether the foreman trips on its first turn.
    trip: bool,
}

impl Settings {
    /// Reads the three keys, each optional.
    fn read(registry: &Registry) -> Result<Self, ConfigError> {
        Ok(Self {
            capacity: registry
                .config::<Option<usize>>("worker.capacity")?
                .unwrap_or(CAPACITY)
                .max(1),
            hold: registry
                .config::<Option<Duration>>("worker.hold")?
                .unwrap_or(HOLD),
            trip: registry
                .config::<Option<bool>>("worker.trip")?
                .unwrap_or(true),
        })
    }
}

// ---------------------------------------------------------------------------
// The queue
// ---------------------------------------------------------------------------

/// One admitted job, on its way to a hand.
///
/// The answer travels back on a one-shot channel rather than on a shared table:
/// dropping the sender *is* the cut. A hand abandoned at the stop deadline drops
/// whatever it was holding, the caller's [`Ticket`] sees the closed channel, and
/// [`WorkError::Cancelled`] is what it reports. No bookkeeping of ours has to be
/// correct for that to happen.
struct Assignment {
    /// Which job.
    id: JobId,
    /// What it says.
    line: String,
    /// Where its result goes.
    answer: oneshot::Sender<Result<Done, WorkError>>,
}

/// A queue with a bound, and two ways of saying no.
///
/// It holds both ends of the channel. That is not a shortcut: one provider has
/// to build the whole queue, because the sender and the receiver are made
/// together and no registration verb can hand one object's half to another
/// binding. The receiver sits behind a [`Mutex`] so it is reachable through
/// `&self` — not to arbitrate between consumers, of which there is exactly one.
struct Bench {
    /// Where an admitted job is put.
    jobs: mpsc::Sender<Assignment>,
    /// Where a hand takes it from.
    intake: Mutex<mpsc::Receiver<Assignment>>,
    /// How many jobs may wait for a hand.
    capacity: usize,
    /// Whether the door is still open. Closed by the hand at `Draining`.
    open: AtomicBool,
    /// The next job identity.
    next: AtomicU64,
    /// How many jobs were admitted, for the foreman's report.
    admitted: AtomicU32,
    /// How many were refused, for the same report. This is what makes
    /// backpressure something an operator can see rather than infer.
    refused: AtomicU32,
}

impl Bench {
    /// A bench admitting `capacity` waiting jobs.
    fn new(capacity: usize) -> Self {
        let (jobs, intake) = mpsc::channel(capacity);
        Self {
            jobs,
            intake: Mutex::new(intake),
            capacity,
            open: AtomicBool::new(true),
            next: AtomicU64::new(0),
            admitted: AtomicU32::new(0),
            refused: AtomicU32::new(0),
        }
    }

    /// Stops admitting. Work already admitted is untouched.
    ///
    /// Called from `Hand::run` when the ladder reaches `Draining`, which is
    /// the only place in this crate that can observe that stage.
    fn close(&self) {
        self.open.store(false, Ordering::Release);
    }

    /// The next job to work on, or `None` once nothing can arrive again.
    ///
    /// Cancel-safe: the guard and the receive are both dropped intact when the
    /// caller's `select!` picks another branch, and no admitted job is lost.
    async fn next_job(&self) -> Option<Assignment> {
        let mut intake = self.intake.lock().await;
        intake.recv().await
    }

    /// Admitted and refused, in that order.
    fn tally(&self) -> (u32, u32) {
        (
            self.admitted.load(Ordering::Relaxed),
            self.refused.load(Ordering::Relaxed),
        )
    }
}

impl WorkQueue for Bench {
    fn submit(&self, job: Job) -> Result<Ticket, Refusal> {
        if !self.open.load(Ordering::Acquire) {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Err(Refusal::Closed);
        }

        // `try_reserve` and not `try_send`: the seat is taken *before* an
        // identity is spent, so a refused job never leaves a `JobId` behind for
        // work nobody accepted. It also never awaits — this whole method is
        // synchronous, which is what stops a bounded queue from quietly
        // becoming an unbounded one by waiting for room.
        //
        // The receiver lives in this very struct, so the only way this fails is
        // the bound being reached.
        let Ok(seat) = self.jobs.try_reserve() else {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Err(Refusal::Full {
                capacity: self.capacity,
            });
        };

        let id = JobId::new(self.next.fetch_add(1, Ordering::Relaxed) + 1);
        let (answer, wait) = oneshot::channel();
        seat.send(Assignment {
            id,
            line: job.line,
            answer,
        });
        self.admitted.fetch_add(1, Ordering::Relaxed);

        Ok(Ticket::new(
            id,
            Box::pin(async move {
                // A closed channel means the hand holding this job went away
                // before it answered — the stop deadline, in practice. That is
                // a cut, not a failure of the work.
                wait.await.unwrap_or(Err(WorkError::Cancelled))
            }),
        ))
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

// ---------------------------------------------------------------------------
// The unit of work
// ---------------------------------------------------------------------------

/// The working note of one request.
///
/// Bound [`Lifetime::Scoped`], so it is built once per [`Scope`] and reached by
/// resolving it rather than by being passed. Resolved outside a scope it fails:
/// there is no unit of work to attach it to.
struct Docket {
    /// Which unit of work this is, counting from one.
    id: u64,
    /// How many times somebody reached it.
    stamps: AtomicU32,
}

impl Docket {
    /// Marks one visit and answers how many there have been.
    fn stamp(&self) -> u32 {
        self.stamps.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// The docket of the unit of work `scope` stands for.
///
/// It is *resolved*, not received, and that is the point: two callers inside one
/// request reach the same object with nothing threaded through the calls.
///
/// Phase three validated the binding, so a kernel that started cannot fail here.
/// A failure is nonetheless an answer to the caller and never a panic: a request
/// whose docket cannot be reached is a request this process cannot serve, and
/// [`HandlerError::Failed`] says exactly that.
async fn docket_of(scope: &Scope) -> Result<Arc<Docket>, ContainerError> {
    scope.get::<Docket>().await
}

// ---------------------------------------------------------------------------
// The handler
// ---------------------------------------------------------------------------

/// Whoever answers, on the other side of the acceptor.
///
/// It holds `Arc<dyn WorkQueue>` and a [`Container`]. The container is what lets
/// one request be one scope: a handler is resolved as a contract and is handed
/// no context of its own, so the only way it can open a unit of work is to have
/// kept the container it was built from.
struct Clerk {
    /// Where work goes. The contract, not `Bench` — the clerk has no business
    /// knowing which queue it got, even though the same crate defines one.
    queue: Arc<dyn WorkQueue>,
    /// What a request opens a [`Scope`] on.
    ///
    /// In debug builds this clone still carries the frame of the provider that
    /// built the clerk, so a *direct* resolution here would be checked against
    /// what that provider declared. [`Container::scope`] clears the frame,
    /// which is why every resolution below goes through the scope — and why a
    /// `Shared` unit may reach a `Scoped` binding without declaring it, a pair
    /// phase three would otherwise refuse as a lifetime conflict.
    container: Container,
    /// Where a refusal is recorded, so backpressure is visible from outside.
    telemetry: Arc<dyn Telemetry>,
}

impl Clerk {
    /// Records a refusal the caller is about to be told about.
    fn note(&self, refusal: Refusal) {
        self.telemetry.record(
            Record::new(Level::Warn, "worker.refused").with("refusal", refusal.to_string()),
        );
    }
}

impl Handler for Clerk {
    fn handle(
        self: Arc<Self>,
        request: Request,
    ) -> BoxFuture<'static, Result<Reply, HandlerError>> {
        Box::pin(async move {
            // Admission first, and nothing before it. `submit` is synchronous,
            // so the refusal arrives without a scope having been opened or a
            // docket spent — a request nobody served leaves no working note
            // behind, for the same reason a refused job is given no `JobId`.
            //
            // The two refusals are two different instructions to the caller and
            // are translated as such: `Full` is capacity and may be retried,
            // `Closed` is the ladder and may not.
            let ticket = self
                .queue
                .submit(Job::new(request.line))
                .map_err(|refusal| {
                    self.note(refusal);
                    match refusal {
                        Refusal::Full { .. } => HandlerError::Busy,
                        Refusal::Closed => HandlerError::Closing,
                    }
                })?;
            let job = ticket.job();

            // One request, one unit of work. Everything `Scoped` resolved from
            // here on is built once and shared until this future ends; the next
            // request opens a scope of its own and gets its own.
            let scope = self.container.scope();
            let docket = docket_of(&scope).await.map_err(HandlerError::failed)?;
            docket.stamp();

            // The await the drain window is about. If the ladder moves now, the
            // work is already admitted and runs to completion; if the stop
            // deadline elapses first, the hand is dropped and this resolves to
            // `Cancelled`.
            let done = ticket.await.map_err(|error| match error {
                // Nothing broke. This process is going away and took the job
                // with it, which is what `Closing` tells a caller.
                WorkError::Cancelled => HandlerError::Closing,
                WorkError::Failed(source) => HandlerError::Failed(source),
            })?;

            // Resolved a second time, across an await, from the same scope —
            // and it is the same object, which is what the stamp count in the
            // reply lets a reader outside the process check.
            let docket = docket_of(&scope).await.map_err(HandlerError::failed)?;

            Ok(Reply::new(format!(
                "{} {} {} {} {}",
                request.id,
                docket.id,
                docket.stamp(),
                job,
                done.line
            )))
        })
    }
}

// ---------------------------------------------------------------------------
// The runnables
// ---------------------------------------------------------------------------

/// The runnable that works the bench.
///
/// It is the only unit in this crate that sees the ladder, so it is the only one
/// that can close the queue's door at the right moment. See the crate
/// documentation for why that follows from the design rather than from taste.
struct Hand {
    /// The queue, concretely: closing it is not part of the contract, and
    /// should not be — nothing that merely submits work may close the door.
    bench: Arc<Bench>,
    /// How long one job takes.
    hold: Duration,
}

impl Hand {
    /// Works one job and answers whoever is holding the ticket.
    async fn work(&self, assignment: Assignment) {
        if !self.hold.is_zero() {
            tokio::time::sleep(self.hold).await;
        }

        // The work is the waiting. What a job produces is not what this example
        // is about, so the line comes back as it went in.
        let done = Done::new(assignment.id, assignment.line);

        // The caller may have stopped waiting, which is allowed and is not an
        // error: a dropped `Ticket` means the caller gave up, not that the work
        // failed.
        let _ = assignment.answer.send(Ok(done));
    }
}

impl Runnable for Hand {
    fn name() -> &'static str {
        "hand"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        // Ancillary: this runnable returning is not on its own a reason to take
        // the process down, and the acceptor is what defines the service.
        RunnableDescriptor::new().criticality(Criticality::Ancillary)
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            // Guards the `draining` branch. Without it the branch is ready
            // forever once the stage has been entered, and the loop spins.
            let mut open = true;

            loop {
                tokio::select! {
                    // The clock is running. Whatever is still admitted is
                    // dropped rather than finished, and every dropped
                    // `Assignment` becomes a `Cancelled` ticket for its caller.
                    () = cx.shutdown().stopping() => break,

                    // Stop taking new work, finish what is held. This is the
                    // whole reason a runnable and not a component owns this
                    // decision, and it is the window the example exists to
                    // prove useful: the loop keeps going, so every job already
                    // on the bench is still worked.
                    () = cx.shutdown().draining(), if open => {
                        self.bench.close();
                        open = false;
                        let (admitted, refused) = self.bench.tally();
                        cx.telemetry().record(
                            Record::new(Level::Info, "worker.bench_closed")
                                .with("admitted", admitted)
                                .with("refused", refused),
                        );
                    }

                    assignment = self.bench.next_job() => match assignment {
                        Some(assignment) => self.work(assignment).await,
                        // Nothing can arrive again.
                        None => break,
                    },
                }
            }

            Ok(())
        })
    }
}

/// Reports on the bench, and trips on purpose the first time it runs.
///
/// The trip is the demonstration this crate owes: an [`Ancillary`] runnable that
/// fails while requests are in flight, is restarted by its own policy, and takes
/// nothing else down with it.
///
/// [`Ancillary`]: Criticality::Ancillary
struct Foreman {
    /// What it reports on.
    bench: Arc<Bench>,
    /// Whether it trips at all. Configurable so a test can ask for a foreman
    /// that only ever reports.
    trip: bool,
    /// Whether it has tripped already. It trips once and never again, which is
    /// what makes the restart observable instead of a loop.
    tripped: AtomicBool,
}

impl Runnable for Foreman {
    fn name() -> &'static str {
        "foreman"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
            .criticality(Criticality::Ancillary)
            .restart(RestartPolicy::on_failure(1, Backoff::Fixed(REGROUP)))
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            // DELIBERATE. This is not a bug and it is not a placeholder: it is
            // the failure the restart policy above exists to answer. The panic
            // is caught at the join and reported as a `RunError`, the
            // supervisor starts this runnable again after `REGROUP`, and the
            // jobs the bench is working on never learn that it happened. It
            // fires once — the flag is on `self`, and the supervisor keeps the
            // same object across a restart — so every later turn does the real
            // work below.
            if self.trip && !self.tripped.swap(true, Ordering::SeqCst) {
                panic!("the foreman trips on purpose, to be restarted");
            }

            loop {
                let (admitted, refused) = self.bench.tally();
                cx.telemetry().record(
                    Record::new(Level::Info, "worker.bench")
                        .with("admitted", admitted)
                        .with("refused", refused)
                        .with(
                            "capacity",
                            u32::try_from(self.bench.capacity).unwrap_or(u32::MAX),
                        ),
                );

                // A new turn is new work, so `draining` is the stage that ends
                // this loop. The kernel publishes the wait, so this crate names
                // no timer of its own.
                if !cx.shutdown().sleep_until_draining(WATCH).await.is_elapsed() {
                    break;
                }
            }

            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// The bundle
// ---------------------------------------------------------------------------

/// Registers the bench, the clerk, the docket, the hand and the foreman.
///
/// Registration is deaf: no container, no view of another bundle, nothing built.
/// Everything below is a declaration phase three checks and phase four acts on.
#[derive(Debug, Default)]
pub struct Bundled;

impl Bundle for Bundled {
    fn manifest(&self) -> BundleManifest {
        // Requires nothing. This feature answers; it asks nobody for anything,
        // and claiming otherwise would be a manifest phase three rejects.
        BundleManifest::new(NAME, "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        let settings =
            Settings::read(registry).map_err(|error| RegisterError::new(NAME, Box::new(error)))?;

        // The bench, bound as itself. `Hand` needs the concrete type — closing
        // the door is not part of the queue contract and must not be — so the
        // object is bound once here and reached twice below.
        registry.provide(Provider::from_fn(move |_container| {
            Box::pin(async move { Ok(Arc::new(Bench::new(settings.capacity))) })
        }));

        // The same object, under the contract everyone else uses. Resolving the
        // binding above rather than building a second bench is what makes "one
        // queue" a mechanical fact instead of a convention.
        registry.provide(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    let bench = container
                        .get::<Bench>()
                        .await
                        .map_err(|error| BuildError::new("Bench", Box::new(error)))?;
                    Ok(bench as Arc<dyn WorkQueue>)
                })
            })
            .requires([ContractRef::of::<Bench>()]),
        );

        // One docket per unit of work. The counter lives in the closure, so it
        // survives every build and numbers them in order.
        let dockets = Arc::new(AtomicU64::new(0));
        registry.provide(
            Provider::from_fn(move |_container| {
                let dockets = Arc::clone(&dockets);
                Box::pin(async move {
                    Ok(Arc::new(Docket {
                        id: dockets.fetch_add(1, Ordering::Relaxed) + 1,
                        stamps: AtomicU32::new(0),
                    }))
                })
            })
            .lifetime(Lifetime::Scoped),
        );

        // The contract the gateway feature resolves. Nothing outside this crate
        // may name `Clerk`, and nothing outside it needs to.
        //
        // `Docket` is deliberately absent from `requires`: a `Shared` binding
        // that declared a `Scoped` one is a phase-three lifetime conflict, and
        // the clerk reaches its docket through a scope, where no such
        // declaration is possible or wanted.
        registry.provide(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    let queue = container
                        .get::<dyn WorkQueue>()
                        .await
                        .map_err(|error| BuildError::new("Clerk", Box::new(error)))?;
                    Ok(Arc::new(Clerk {
                        queue,
                        container: container.clone(),
                        telemetry: Arc::clone(container.telemetry()),
                    }) as Arc<dyn Handler>)
                })
            })
            .requires([ContractRef::of::<dyn WorkQueue>()]),
        );

        registry.runnable(
            Provider::from_fn(move |container| {
                Box::pin(async move {
                    let bench = container
                        .get::<Bench>()
                        .await
                        .map_err(|error| BuildError::new("Hand", Box::new(error)))?;
                    Ok(Arc::new(Hand {
                        bench,
                        hold: settings.hold,
                    }))
                })
            })
            .requires([ContractRef::of::<Bench>()]),
        );

        registry.runnable(
            Provider::from_fn(move |container| {
                Box::pin(async move {
                    let bench = container
                        .get::<Bench>()
                        .await
                        .map_err(|error| BuildError::new("Foreman", Box::new(error)))?;
                    Ok(Arc::new(Foreman {
                        bench,
                        trip: settings.trip,
                        tripped: AtomicBool::new(false),
                    }))
                })
            })
            .requires([ContractRef::of::<Bench>()]),
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use gateway_contracts::RequestId;
    use kernel::core::{ConfigNode, ConfigTree, RecordingTelemetry, ShutdownPolicy};
    use kernel::{Kernel, MemorySource, ShutdownController};

    use super::*;

    /// Every test in this module is bounded by this, and none of them may take
    /// anything like it. A test that can wedge a build on a regression is worse
    /// than no test.
    const LIMIT: Duration = Duration::from_secs(5);

    /// Awaits `future` under [`LIMIT`], and fails rather than hangs.
    async fn bounded<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(LIMIT, future)
            .await
            .expect("the test outran its bound")
    }

    /// Polls `ready` until it holds, under [`LIMIT`].
    async fn until(mut ready: impl FnMut() -> bool) {
        bounded(async {
            while !ready() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
    }

    /// A bench nobody works, for the tests that are about the door alone.
    fn bench(capacity: usize) -> Arc<Bench> {
        Arc::new(Bench::new(capacity))
    }

    /// A clerk over `bench`, resolving through `container`.
    fn clerk(
        bench: &Arc<Bench>,
        container: Container,
        telemetry: &Arc<RecordingTelemetry>,
    ) -> Arc<Clerk> {
        Arc::new(Clerk {
            queue: Arc::clone(bench) as Arc<dyn WorkQueue>,
            container,
            telemetry: Arc::clone(telemetry) as Arc<dyn Telemetry>,
        })
    }

    /// A container with no binding in it at all — the shape a broken graph has.
    fn empty() -> Container {
        RunContext::detached().0.container().clone()
    }

    /// The three keys, so the configuration path is exercised rather than
    /// assumed.
    fn settings(capacity: i64, hold: &str, trip: bool) -> MemorySource {
        let mut tree = ConfigTree::empty();
        for (path, node) in [
            ("worker.capacity", ConfigNode::from(capacity)),
            ("worker.hold", ConfigNode::from(hold)),
            ("worker.trip", ConfigNode::from(trip)),
        ] {
            tree.insert(path, node)
                .expect("literal paths cannot collide");
        }
        MemorySource::named("test", tree)
    }

    /// A kernel holding this feature alone.
    async fn build(
        capacity: i64,
        hold: &str,
        trip: bool,
        telemetry: &Arc<RecordingTelemetry>,
    ) -> Kernel {
        Kernel::builder()
            .capture_signals(false)
            .shutdown_policy(ShutdownPolicy::new(
                Duration::from_millis(200),
                Duration::from_millis(200),
            ))
            .telemetry(Arc::clone(telemetry) as Arc<dyn Telemetry>)
            .config_source(settings(capacity, hold, trip))
            .bundle(Bundled)
            .build()
            .await
            .expect("the graph closes")
    }

    /// The fields of one reply, in the order the crate documentation gives.
    fn fields(reply: &Reply) -> Vec<String> {
        reply.line.split(' ').map(str::to_owned).collect()
    }

    // ------------------------------------------------------------------
    // Backpressure
    // ------------------------------------------------------------------

    /// The bound is enforced at the door, the refusal is a refusal, and the job
    /// that was turned away cost nothing — not even an identity.
    #[tokio::test]
    async fn refuses_when_full() {
        let bench = bench(2);

        let first = bench.submit(Job::new("one")).expect("room");
        let second = bench.submit(Job::new("two")).expect("room");
        let refusal = bench.submit(Job::new("three")).expect_err("no room left");

        assert_eq!(refusal, Refusal::Full { capacity: 2 });
        assert!(refusal.retry_later());
        assert_eq!(bench.capacity(), 2);
        // Two seats, two identities. The third job spent none.
        assert_eq!(first.job(), JobId::new(1));
        assert_eq!(second.job(), JobId::new(2));
        assert_eq!(bench.tally(), (2, 1));
    }

    /// A closed queue is not a full one, and the difference is what a caller
    /// acts on: one may be retried, the other may not.
    #[tokio::test]
    async fn closed_is_not_full() {
        let bench = bench(2);
        bench.close();

        let refusal = bench.submit(Job::new("one")).expect_err("the door is shut");

        assert_eq!(refusal, Refusal::Closed);
        assert!(!refusal.retry_later());
    }

    /// The refusal reaches the caller of the *handler* as a refusal too, and is
    /// recorded on the way past so it is visible from outside the process.
    ///
    /// The container here is empty on purpose: a refusal must reach the caller
    /// without a scope having been opened, so a clerk that could not resolve a
    /// docket if it tried still answers `Busy`.
    #[tokio::test]
    async fn clerk_reports_busy() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let bench = bench(1);
        let clerk = clerk(&bench, empty(), &telemetry);

        let _held = bench.submit(Job::new("one")).expect("room");
        let error = bounded(clerk.handle(Request::new(RequestId::new(1), "two")))
            .await
            .expect_err("the bench is full");

        assert!(matches!(error, HandlerError::Busy));
        assert!(error.retry_later());
        assert!(telemetry.contains("worker.refused"));
    }

    // ------------------------------------------------------------------
    // The cut
    // ------------------------------------------------------------------

    /// A job whose hand goes away resolves to a cut, not to a failure and not
    /// to a hang. This is the path the stop deadline takes.
    #[tokio::test]
    async fn cut_job_is_cancelled() {
        let bench = bench(2);
        let ticket = bench.submit(Job::new("one")).expect("room");

        // Nothing ever worked it, and now nothing ever will.
        drop(bench);

        match bounded(ticket).await {
            Err(WorkError::Cancelled) => {}
            other => panic!("expected a cut job, got {other:?}"),
        }
    }

    /// And the clerk turns that cut into `Closing`: nothing broke, this process
    /// is going away.
    #[tokio::test]
    async fn cut_reaches_the_caller() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let kernel = build(4, "1ms", false, &telemetry).await;
        let bench = bench(2);
        let clerk = clerk(&bench, kernel.container().clone(), &telemetry);
        let answering = tokio::spawn(clerk.handle(Request::new(RequestId::new(1), "one")));

        // Exactly what the stop deadline does: a hand picks the job up and is
        // then abandoned, taking the unanswered channel with it.
        let held = bounded(bench.next_job()).await.expect("a job was admitted");
        drop(held);

        let error = bounded(answering)
            .await
            .expect("join")
            .expect_err("the job was cut");
        assert!(matches!(error, HandlerError::Closing));
        assert!(!error.retry_later());
    }

    /// A docket that cannot be resolved is answered, not panicked on.
    #[tokio::test]
    async fn docket_unreachable_is_failure() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let bench = bench(2);

        let error = bounded(
            clerk(&bench, empty(), &telemetry).handle(Request::new(RequestId::new(1), "one")),
        )
        .await
        .expect_err("an empty container has no docket");

        assert!(matches!(error, HandlerError::Failed(_)));
        assert!(std::error::Error::source(&error).is_some());
    }

    // ------------------------------------------------------------------
    // The runnables
    // ------------------------------------------------------------------

    /// The test every runnable owes: the token fires, `run` returns.
    #[tokio::test]
    async fn hand_yields_on_shutdown() {
        let (cx, controller) = RunContext::detached();
        controller.begin_stopping();

        let hand = Arc::new(Hand {
            bench: bench(2),
            hold: Duration::from_secs(3600),
        });

        assert!(bounded(hand.run(cx)).await.is_ok());
    }

    /// The claim the whole example rests on, at this crate's own door: the
    /// runnable is what sees `Draining`, and what it does with it is stop
    /// admitting while it keeps working.
    #[tokio::test]
    async fn hand_closes_at_drain() {
        let bench = bench(2);
        let (cx, controller): (RunContext, ShutdownController) = RunContext::detached();
        let hand = Arc::new(Hand {
            bench: Arc::clone(&bench),
            hold: Duration::ZERO,
        });
        let running = tokio::spawn(Arc::clone(&hand).run(cx));

        // Admitted before the ladder moves, and answered after it: the window.
        let ticket = bench.submit(Job::new("held")).expect("room");
        controller.begin_draining();
        let done = bounded(ticket).await.expect("work in flight finishes");
        assert_eq!(done.line, "held");

        // The door is shut, and it says so as `Closed` rather than as `Full`.
        until(|| bench.submit(Job::new("late")).is_err()).await;
        assert_eq!(
            bench.submit(Job::new("late")).expect_err("shut"),
            Refusal::Closed
        );

        controller.begin_stopping();
        assert!(bounded(running).await.expect("join").is_ok());
    }

    /// The foreman trips once, on purpose, and runs clean forever after.
    #[tokio::test]
    async fn foreman_trips_once() {
        let foreman = Arc::new(Foreman {
            bench: bench(2),
            trip: true,
            tripped: AtomicBool::new(false),
        });

        let (cx, _controller) = RunContext::detached();
        let first = tokio::spawn(Arc::clone(&foreman).run(cx));
        assert!(
            bounded(first).await.expect_err("it trips").is_panic(),
            "the trip must reach the supervisor as a panic it can catch"
        );

        // What the supervisor does next. The same object, started again.
        let (cx, controller) = RunContext::detached();
        controller.begin_draining();
        assert!(bounded(Arc::clone(&foreman).run(cx)).await.is_ok());
    }

    /// A foreman told not to trip only reports, and still yields to the token.
    #[tokio::test]
    async fn foreman_yields_on_shutdown() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let (cx, controller) = RunContext::builder()
            .with_telemetry(Arc::clone(&telemetry) as Arc<dyn Telemetry>)
            .build();
        controller.begin_draining();

        let foreman = Arc::new(Foreman {
            bench: bench(2),
            trip: false,
            tripped: AtomicBool::new(false),
        });

        assert!(bounded(foreman.run(cx)).await.is_ok());
        assert!(telemetry.contains("worker.bench"));
    }

    // ------------------------------------------------------------------
    // The scope
    // ------------------------------------------------------------------

    /// What a scope IS: one docket throughout one unit of work, a different one
    /// in the next, and none at all outside.
    #[tokio::test]
    async fn scope_holds_one_docket() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let kernel = build(4, "1ms", false, &telemetry).await;

        let request = kernel.container().scope();
        let first = docket_of(&request).await.expect("a docket");
        let again = docket_of(&request).await.expect("the same docket");
        assert!(Arc::ptr_eq(&first, &again));

        let other = kernel.container().scope();
        let second = docket_of(&other).await.expect("another docket");
        assert!(!Arc::ptr_eq(&first, &second));
        assert_ne!(first.id, second.id);

        // No unit of work, nothing to attach the value to.
        assert!(kernel.container().get::<Docket>().await.is_err());
    }

    // ------------------------------------------------------------------
    // The whole feature
    // ------------------------------------------------------------------

    /// Two requests in flight, a worker that fails and is restarted underneath
    /// them, and a kernel that neither stops nor drops an answer.
    ///
    /// It also reads the two scope properties off the wire: two concurrent
    /// requests carry two docket numbers, and each reply reports two stamps —
    /// the clerk resolved its docket twice and reached the same object.
    #[tokio::test]
    async fn answers_while_worker_restarts() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let kernel = build(4, "30ms", true, &telemetry).await;

        // Resolved before the run starts, so the container is not being sealed
        // underneath this call.
        let handler = kernel
            .container()
            .get::<dyn Handler>()
            .await
            .expect("the clerk");
        let handle = kernel.handle();
        let running = tokio::spawn(kernel.run());

        let first =
            tokio::spawn(Arc::clone(&handler).handle(Request::new(RequestId::new(1), "alpha")));
        let second =
            tokio::spawn(Arc::clone(&handler).handle(Request::new(RequestId::new(2), "beta")));

        let one = fields(&bounded(first).await.expect("join").expect("answered"));
        let two = fields(&bounded(second).await.expect("join").expect("answered"));

        assert_eq!(one[0], "1");
        assert_eq!(two[0], "2");
        assert_eq!(one[4], "alpha");
        assert_eq!(two[4], "beta");
        // Two units of work, two dockets.
        assert_ne!(one[1], two[1]);
        // One unit of work, one docket, reached twice.
        assert_eq!(one[2], "2");
        assert_eq!(two[2], "2");

        handle.shutdown();
        let outcome = bounded(running).await.expect("the driver joined");

        // The foreman failed and came back; the kernel neither stopped for it
        // nor exited on it.
        assert!(
            telemetry.contains("runnable.restarted"),
            "the foreman restarted"
        );
        assert!(
            outcome.is_success(),
            "an ancillary blip is not a failure: {outcome:?}"
        );
    }

    /// A request submitted after the ladder has moved is refused, and told why.
    #[tokio::test]
    async fn late_request_is_refused() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let kernel = build(4, "1ms", false, &telemetry).await;

        let handler = kernel
            .container()
            .get::<dyn Handler>()
            .await
            .expect("the clerk");
        let bench = kernel.container().get::<Bench>().await.expect("the bench");
        let handle = kernel.handle();
        let running = tokio::spawn(kernel.run());

        handle.shutdown();
        until(|| bench.submit(Job::new("probe")).is_err()).await;

        let error = bounded(handler.handle(Request::new(RequestId::new(9), "late")))
            .await
            .expect_err("the door is shut");
        assert!(matches!(error, HandlerError::Closing));

        assert!(
            bounded(running)
                .await
                .expect("the driver joined")
                .is_success()
        );
    }

    /// The bundle registers what it says it registers, and needs nothing.
    #[test]
    fn manifest_asks_for_nothing() {
        let manifest = Bundled.manifest();

        assert_eq!(manifest.name, NAME);
        assert!(manifest.requires.is_empty());
        assert!(manifest.after.is_empty());
    }
}
