//! The application's own bundle: narrating the ladder as it moves.
//!
//! The kernel publishes every phase transition as an event. A feature has no
//! reason to care that phase six started; whoever is reading the terminal does,
//! and the application is that reader's side of the program.
//!
//! # Why this narration is worth its five listeners
//!
//! The medium example narrates two events, because two were enough to show
//! that a kernel event is a normal event. This one narrates five, and the two
//! it adds — [`Draining`] and [`Stopping`] — are the whole subject.
//!
//! A single `Stopped` line proves nothing: a process that cut every connection
//! the instant it was asked to stop prints exactly the same line as one that
//! finished what it held. What separates them is *when* each thing happened
//! relative to `Draining`, so the two rungs are printed as they are climbed and
//! every line carries the offset from the stop request. The scripted caller in
//! [`crate::caller`] prints into the same column, and the interleaving is the
//! demonstration.
//!
//! # It is still a bundle, and it still names no other bundle
//!
//! [`FnBundle`] because the whole registration is five calls: [`Registry`] is
//! handed to [`Bundle::register`] and to nowhere else, so an application that
//! wants to listen has to be a bundle, and a type with two trait methods is
//! more ceremony than five lines deserve.
//!
//! It names no `*-bundle` crate — `ci/check-bundle-graph.sh` would fail the
//! build — and it happens to name no `*-contracts` crate either: lifecycle is
//! the kernel's vocabulary, not this application's.
//!
//! [`Bundle::register`]: kernel::Bundle::register
//! [`Registry`]: kernel::Registry

use std::sync::Arc;

use kernel::core::ListenerError;
use kernel::{
    BoxFuture, BundleManifest, Draining, Flow, FnBundle, Listener, ListenerContext, Priority,
    Running, ShutdownRequested, Stopped, Stopping,
};

use crate::timeline::Clock;

/// The name this bundle registers under.
const NAME: &str = "console";

/// Narrates the kernel's own phases onto the shared timeline.
///
/// One type, five events. Dispatch is indexed by the payload type, so the four
/// registrations at the bottom of this file need a turbofish each to say which
/// impl they mean — the type index made visible rather than a wart.
struct Phases {
    /// The timeline every line in this process is stamped against.
    clock: Arc<Clock>,
}

impl Listener<Running> for Phases {
    fn on_event<'a>(
        &'a self,
        event: &'a mut Running,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            self.clock.say(&format!(
                "running: {} runnable(s) started — the acceptor is one of them",
                event.runnables
            ));
            Ok(Flow::Continue)
        })
    }
}

impl Listener<ShutdownRequested> for Phases {
    /// The one kernel event that is dispatched rather than emitted, so a
    /// listener may add context before the ladder starts moving.
    ///
    /// It is also the earliest moment the stop is a fact, which is why the
    /// timeline starts here: everything printed from now on says how far into
    /// the shutdown it happened.
    fn on_event<'a>(
        &'a self,
        event: &'a mut ShutdownRequested,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            self.clock.start();
            self.clock
                .say(&format!("stop requested: {:?}", event.reason));
            event.notes.push("narrated by the application".to_owned());
            Ok(Flow::Continue)
        })
    }
}

impl Listener<Draining> for Phases {
    /// Rung one. New work is refused from here; held work is not touched.
    fn on_event<'a>(
        &'a self,
        _event: &'a mut Draining,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            self.clock
                .say("DRAINING: the door shuts, what is already accepted keeps running");
            Ok(Flow::Continue)
        })
    }
}

impl Listener<Stopping> for Phases {
    /// Rung two. What is still running is cut when its budget elapses.
    fn on_event<'a>(
        &'a self,
        _event: &'a mut Stopping,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            self.clock
                .say("STOPPING: whatever is still in flight is now on the clock");
            Ok(Flow::Continue)
        })
    }
}

impl Listener<Stopped> for Phases {
    /// The three counts are three different questions, so all three are
    /// printed. A run whose foreman tripped and was restarted exits
    /// successfully with `run_failures = 1`, and a reader who saw only
    /// `unhandled` would call that a clean run with nothing to look at.
    fn on_event<'a>(
        &'a self,
        event: &'a mut Stopped,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            self.clock.say(&format!(
                "stopped: {} abandoned, {} unhandled, {} failure(s) over the run",
                event.abandoned, event.unhandled, event.run_failures
            ));
            Ok(Flow::Continue)
        })
    }
}

/// The application's own bundle, listed last.
///
/// It requires nothing and provides nothing: it only listens. The manifest is
/// stated anyway, because a version is what a diagnostic naming this bundle
/// prints.
pub fn bundle(clock: &Arc<Clock>) -> FnBundle {
    let clock = Arc::clone(clock);
    FnBundle::new(NAME, move |registry| {
        // One listener object per registration: `listen` takes it by value, and
        // the five impls are five different traits on the same type.
        let watching = || Phases {
            clock: Arc::clone(&clock),
        };

        registry.listen::<Running, _>(watching(), Priority::NORMAL);
        registry.listen::<ShutdownRequested, _>(watching(), Priority::NORMAL);
        registry.listen::<Draining, _>(watching(), Priority::NORMAL);
        registry.listen::<Stopping, _>(watching(), Priority::NORMAL);
        registry.listen::<Stopped, _>(watching(), Priority::NORMAL);

        Ok(())
    })
    .manifest(BundleManifest::new(NAME, "0.1.0"))
}
