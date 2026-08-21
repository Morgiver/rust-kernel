//! Two features, one process, and a stop that is not a kill.
//!
//! This is the only crate in the example that names more than one feature. The
//! gateway accepts connections and knows nothing about who answers them; the
//! worker answers them and knows nothing about sockets. They meet here, and
//! only here, through `gateway-contracts` and the container:
//! `gateway-bundle` does not depend on `worker-bundle`, `worker-bundle` does
//! not depend on `gateway-bundle`, and neither *could* —
//! `ci/check-bundle-graph.sh` walks the resolved dependency graph and fails the
//! build on any `*-bundle` → `*-bundle` edge.
//!
//! # What this example exists to prove
//!
//! That the two-stage shutdown ladder is **useful**, not merely present.
//!
//! `crates/kernel/tests/window.rs` already proves the window exists: a unit
//! that reaches `Draining` gets time before `Stopping` arrives. What it cannot
//! prove is that the window is good for anything, because everything else in
//! this repository is a timer, and a timer accepts nothing from outside. For a
//! timer `Draining` and `Stopping` are interchangeable.
//!
//! Here they are not. Something outside the process is holding a connection
//! open and waiting for an answer, and the run prints the moment new callers
//! start being refused while an old one is still being served. With one stop
//! signal instead of two, the only way to refuse the new caller would be to
//! drop the old one.
//!
//! # The one thing to take away, which the design document does not say
//!
//! **The door is shut by whoever owns it; the window is held open by whoever
//! owns the work in flight.**
//!
//! A [`Component`](kernel::Component) has three moments, not two:
//! [`boot`](kernel::Component::boot), [`drain`](kernel::Component::drain) — run
//! as the ladder reaches `Draining`, before a single runnable has been asked to
//! wind down — and [`shutdown`](kernel::Component::shutdown). Refusing new work
//! is a property of the RESOURCE, so it happens in the middle one: the gateway's
//! socket closes itself there, and the worker's queue closes itself there.
//! Neither needs a loop or a task to do it.
//!
//! What a component never holds is the work already accepted. Giving that work
//! the drain window and cutting what outlives it needs the token whose two
//! rungs separate *stop taking new work* from *stop now*, and needs the set of
//! tasks those rungs apply to. Only a [`Runnable`](kernel::Runnable) has both —
//! so the accept loop is a runnable, necessarily, and so is the hand that works
//! the bench.
//!
//! Bind and shut in the component, accept and wind down in the runnable. The
//! split is not taste and not convention: it follows from which unit owns which
//! thing.
//!
//! # Running it
//!
//! ```text
//! cargo run -p service
//! ```
//!
//! It binds an ephemeral port, drives itself through the script in
//! [`caller`] — a served request, a burst that gets refused, then the window —
//! and exits 0. Every line is stamped with how long after the stop request it
//! happened, so the ordering reads off the page:
//!
//! ```text
//! [service      .] burst    5 5 2 busy
//! [service      .] 3 of 6 refused: the bench is bounded and said so on the wire
//! [service      .] one request is in flight; one connection is open and has said nothing
//! [service   +0ms] stop requested: Programmatic
//! [service   +0ms] DRAINING: the door shuts, what is already accepted keeps running
//! [service   +3ms] the door is shut — the acceptor stopped accepting at Draining
//! [service   +3ms] a new caller is refused: Connection refused (os error 111)
//! [service +421ms] held     9 9 2 ok 9 6 2 6 late
//! [service +421ms] window   8 8 2 ok 8 5 2 5 in-flight
//! [service +1001ms] STOPPING: whatever is still in flight is now on the clock
//! [service +1001ms] stopped: 0 abandoned, 0 unhandled, 1 failure(s) over the run
//! ```
//!
//! Three lines carry the example. A new caller is refused three milliseconds
//! into the stop. `window` is a request that was already admitted when the door
//! shut: it ran to completion and its caller read a real answer, four hundred
//! milliseconds into a shutdown. `held` is a connection that was accepted
//! before the stop and only spoke after it — the acceptor keeps that too,
//! because the window covers the whole conversation and not just the request
//! already in flight.
//!
//! The single failure in the last line is the foreman, which trips on purpose
//! on its first turn and is restarted while requests are being served. Nothing
//! else notices, which is what `Ancillary` means.
//!
//! To watch the *cut* instead — the other half of the ladder, and the half that
//! costs something — make a job outlast both budgets:
//!
//! ```text
//! HARD_WORKER__HOLD=3s cargo run -p service
//! ```
//!
//! Both held requests are then still running when `Stopping` fires, and both
//! callers read `cut`: told what happened, in the same shape as every other
//! answer, instead of inferring it from a reset. The run ends `1 abandoned` —
//! the runnable working the three-second job was asleep past its own budget and
//! was left behind rather than waited for. The process still exits, on time and
//! with status 0. That is the promise: the kernel never blocks indefinitely,
//! and what that costs is printed rather than hidden.
//!
//! To serve normally instead of driving itself, turn the script off and use
//! Ctrl-C:
//!
//! ```text
//! HARD_APP__DEMO=false cargo run -p service
//! ```
//!
//! Every value either feature reads comes from the configuration chain below,
//! so any of them moves to the environment: `HARD_GATEWAY__ADDRESS`,
//! `HARD_WORKER__CAPACITY`, `HARD_WORKER__TRIP`, and the rest.
//!
//! # What the public surface does not offer
//!
//! Written down because it is worth more than the code that works around it:
//!
//! * **Nothing publishes "this request has been admitted".** The script waits a
//!   fixed moment before asking for the stop. A test can assert the outcome
//!   instead of the timing, but a demonstration cannot, and there is no public
//!   edge to wait on.
//! * **A bundle cannot shorten the drain budget for its own component.** A
//!   [`ComponentDescriptor`](kernel::ComponentDescriptor) carries
//!   `shutdown_timeout` but no `drain_timeout`, so every component's drain is
//!   bounded by the policy's `drain` flat. Nothing here needs less; a component
//!   whose refusal is expensive would have no way to say so.

mod caller;
mod console;
mod timeline;

use core::time::Duration;
use std::process::ExitCode;
use std::sync::Arc;

use gateway_bundle::Doorway;
use kernel::core::{ConfigError, ConfigNode, ConfigTree, FromConfig, StderrTelemetry};
use kernel::{EnvSource, Kernel, MemorySource, ShutdownPolicy};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::timeline::Clock;

/// The prefix every environment variable of this application carries.
const PREFIX: &str = "HARD_";

/// Whether this run drives itself through the script. On by default.
const DEMO: &str = "app.demo";

/// How long the ladder is given, rung by rung.
///
/// Both numbers are chosen against `worker.hold` below, and the relation is the
/// whole subject:
///
/// * `drain` is longer than one job takes, so a request already admitted when
///   the door shuts finishes inside the window and its caller gets an answer;
/// * `stop` is what a request that is *still* running after that costs before
///   it is cut.
///
/// Raise `worker.hold` past `drain + stop` and the window stops being enough:
/// the request is cut, the caller is told, and the process still exits on time.
/// That is not a failure mode, it is the other half of the design — a budget
/// that is always sufficient is a budget nobody is enforcing.
const LADDER: ShutdownPolicy =
    ShutdownPolicy::new(Duration::from_millis(1_000), Duration::from_millis(1_000));

/// Where an operator may override [`LADDER`], key by key.
///
/// `HARD_LADDER__DRAIN=3s` moves the drain window and leaves the stop budget
/// where the constant put it. Read out of the same tree every other key comes
/// from — the budgets are an operational setting like any other, and pinning
/// them in the binary would make them the one setting a deployment could not
/// touch.
const LADDER_AT: &str = "ladder";

/// The values this application ships with.
///
/// A configuration source like any other, listed first so every later source
/// overrides it leaf by leaf. Only three keys, and each one is a choice this
/// application makes rather than a default a feature failed to ship:
///
/// * `worker.hold` — long enough that a request is still in flight when the
///   ladder moves, which is the only way the window is observable at all;
/// * `worker.capacity` — small enough that six simultaneous requests overflow
///   it, so backpressure is visible in a run that takes two seconds;
/// * `worker.trip` — the foreman fails once on purpose and is restarted. Left
///   on, because a stop that is clean *except* for the ancillary runnable that
///   died and came back is the interesting case.
///
/// `gateway.address` is deliberately absent: the feature's own default is port
/// zero, and an ephemeral port is what lets two runs of this example overlap.
/// An application that serves for real states its address here.
fn defaults() -> MemorySource {
    let mut tree = ConfigTree::empty();
    for (path, node) in [
        ("worker.hold", ConfigNode::from("250ms")),
        ("worker.capacity", ConfigNode::from(2_i64)),
        ("worker.trip", ConfigNode::from(true)),
    ] {
        tree.insert(path, node)
            .expect("the default paths are literals and cannot collide");
    }
    MemorySource::named("defaults", tree)
}

/// Whether this run drives itself, defaulting to yes.
///
/// Read from the loaded tree rather than in a bundle, because it is not a
/// feature's business: it configures `main`, and `main` is the only thing
/// holding a [`kernel::KernelHandle`] before anything is running.
///
/// # Errors
///
/// Whatever [`bool`] reports when the key is present and is not one.
fn demonstrating(config: &ConfigTree) -> Result<bool, ConfigError> {
    match config.get(DEMO) {
        Some(node) => bool::from_config(node),
        None => Ok(true),
    }
}

/// How long the runtime is given to let go of its worker threads.
///
/// The last unbounded wait in a process built on this kernel is not the
/// kernel's. The kernel bounds every rung: a runnable that overruns its budget
/// is aborted, recorded as `runnable.abandoned`, and never waited for. But an
/// abort is only observed at an await point, so a task that never reaches one
/// goes on running — and `Runtime`'s own `Drop`, which is how `#[tokio::main]`
/// ends a program, joins every worker thread with no bound at all. The ladder
/// finishes, says so, and the process still does not exit.
///
/// [`tokio::runtime::Runtime::shutdown_timeout`] is that bound, and it is why
/// this `main` builds its runtime by hand rather than taking the macro's. It is
/// generous: it is the wait before a task that ignores its own cancellation is
/// left behind, not a budget anything spends on the way out.
const TEARDOWN: Duration = Duration::from_secs(5);

/// Runs the application on a runtime whose teardown is bounded.
///
/// What the kernel guarantees ends when [`Kernel::run`] returns; what happens
/// to the runtime afterwards is the application's, and this is the whole of it.
/// See [`TEARDOWN`].
fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("service: no runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    let code = runtime.block_on(run());
    runtime.shutdown_timeout(TEARDOWN);
    code
}

/// Builds the kernel, arms the script, runs, and reports what happened.
///
/// `?` is not usable here: [`ExitCode`] does not implement `FromResidual`, so
/// each refusal is rendered explicitly instead of propagated.
async fn run() -> ExitCode {
    let clock = Arc::new(Clock::new());

    // Phases one to three: the sources load, the three bundles register, the
    // graph is validated. Nothing is built and no I/O happens. If this returns
    // `Ok`, somebody provides `dyn Handler` — and which crate does is not
    // knowable from the gateway's side, which is the isolation rule working.
    let kernel = match Kernel::builder()
        .telemetry(Arc::new(StderrTelemetry))
        .shutdown_policy(LADDER)
        .shutdown_policy_at(LADDER_AT)
        .config_source(defaults())
        .config_source(EnvSource::with_prefix(PREFIX))
        .bundle(gateway_bundle::GatewayBundle::new())
        .bundle(worker_bundle::Bundled)
        // Last, and nothing depends on it: every bundle registers before the
        // first phase event is emitted, so a listener added here still hears
        // the whole run. It is listed last because it is about the other two.
        .bundle(console::bundle(&clock))
        .build()
        .await
    {
        Ok(kernel) => kernel,
        Err(error) => {
            eprintln!("service: refused to start: {error}");
            return ExitCode::FAILURE;
        }
    };

    let script = match arm_script(&kernel, &clock).await {
        Ok(script) => script,
        Err(error) => {
            eprintln!("service: refused to start: {error}");
            return ExitCode::FAILURE;
        }
    };

    // Phases four to seven: build, boot, run, drain, stop.
    let outcome = kernel.run().await;

    // The script asked for the stop and has read its last answer by now, so
    // this joins something already finished. It is bounded anyway: `main` waits
    // for nothing without a bound, which is the same rule the kernel holds
    // itself to.
    if let Some(script) = script
        && timeout(caller::BUDGET, script).await.is_err()
    {
        eprintln!("service: the script outlived the kernel and was left behind");
    }

    if let Some(error) = outcome.error() {
        eprintln!("service: {error}");
    }
    clock.say(&format!("outcome: {outcome:?}"));
    outcome.into_exit_code()
}

/// Spawns the scripted caller, when this run is a demonstration.
///
/// The door is resolved before the kernel runs, and it is the very object the
/// kernel is about to boot: `registry.component` binds a component as a
/// contract too, so this reaches the instance that will hold the socket rather
/// than a second one built alongside it. That is what lets the script wait on
/// [`Doorway::opened`] for a port nobody can know before boot.
///
/// # Errors
///
/// Whatever [`demonstrating`] reports when `app.demo` is present and is not a
/// boolean.
async fn arm_script(
    kernel: &Kernel,
    clock: &Arc<Clock>,
) -> Result<Option<JoinHandle<()>>, ConfigError> {
    if !demonstrating(kernel.container().config())? {
        clock.say("serving until a signal arrives — press Ctrl-C to stop");
        return Ok(None);
    }

    let doorway = match kernel.container().get::<Doorway>().await {
        Ok(doorway) => doorway,
        Err(error) => {
            // Not fatal: a process that cannot find its own door can still
            // serve, it just cannot narrate itself. The kernel's own phase
            // three would have refused the graph if the door were missing.
            eprintln!("service: no door to watch, serving without the script: {error}");
            return Ok(None);
        }
    };

    Ok(Some(tokio::spawn(caller::demonstrate(
        doorway,
        kernel.handle(),
        Arc::clone(clock),
    ))))
}

#[cfg(test)]
mod tests {
    use kernel::core::ConfigSource;

    use super::*;

    #[test]
    fn defaults_shape_the_window() {
        let tree = defaults().load().expect("a memory source always loads");

        let hold = Duration::from_config(tree.get("worker.hold").expect("hold is shipped"))
            .expect("a suffixed string is a duration");

        // The relation the whole demonstration rests on: a job outlives the
        // stop request but not the drain budget, so it finishes in the window.
        assert!(hold < LADDER.drain, "{hold:?} must fit inside the window");
        assert!(tree.get("worker.capacity").is_some());
        assert!(tree.get("worker.trip").is_some());
    }

    #[test]
    fn address_is_left_ephemeral() {
        let tree = defaults().load().expect("a memory source always loads");

        // Port zero comes from the feature's own default. Pinning a port here
        // is what makes two overlapping runs fail.
        assert!(tree.get("gateway.address").is_none());
    }

    #[test]
    fn demo_is_on_by_default() {
        assert!(demonstrating(&ConfigTree::empty()).expect("an absent key is not a failure"));
    }

    #[test]
    fn demo_reads_a_bool() {
        let mut tree = ConfigTree::empty();
        tree.insert(DEMO, ConfigNode::from(false))
            .expect("a literal path");

        assert!(!demonstrating(&tree).expect("a boolean reads as one"));
    }

    #[test]
    fn wrong_demo_is_refused() {
        let mut tree = ConfigTree::empty();
        tree.insert(DEMO, ConfigNode::from("later"))
            .expect("a literal path");

        assert!(demonstrating(&tree).is_err());
    }
}
