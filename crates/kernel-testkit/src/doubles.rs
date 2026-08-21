//! Doubles a test registers in place of the real thing.
//!
//! None of these stands in for a contract, because none of them can: a
//! contract is declared by whoever needs it, and only code that can name it can
//! implement it. What repeats from test to test is everything *around* that
//! implementation — a recording cell behind a mutex, a task that does nothing
//! but stay up, a component that says whether it was booted and stopped — and
//! that is what is here.
//!
//! [`Recorder`] is the part a hand-written double holds; [`Parking`] and
//! [`LifecycleLog`] are whole doubles, because a runnable and a component are
//! kernel traits and can be implemented here once and for all.

use core::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use kernel::{BootContext, Component, RunContext, Runnable, ShutdownContext};
use kernel_core::{BoxFuture, ComponentDescriptor, ComponentError, RunError, RunnableDescriptor};

/// The name [`Parking`] is registered and blamed under.
const PARKING: &str = "parking";

/// The name [`LifecycleLog`] is registered and blamed under.
const LIFECYCLE: &str = "lifecycle";

/// Keeps every value handed to it, in the order they arrived.
///
/// This is the inside of a recording double: the double implements the
/// contract, and hands what it was given to one of these. Sharing is by
/// [`Clone`], which shares the recording rather than forking it, so the copy
/// the double holds and the copy the test reads are one log.
///
/// # Examples
///
/// ```
/// use kernel_testkit::Recorder;
///
/// trait Notify: Send + Sync + 'static {
///     fn notify(&self, note: &str);
/// }
///
/// struct Noted(Recorder<String>);
///
/// impl Notify for Noted {
///     fn notify(&self, note: &str) {
///         self.0.record(note.to_owned());
///     }
/// }
///
/// let recorder = Recorder::new();
/// let double = Noted(recorder.clone());
/// double.notify("first");
///
/// assert_eq!(recorder.items(), ["first"]);
/// ```
pub struct Recorder<T> {
    /// The recording, shared with every clone of this handle.
    items: Arc<Mutex<Vec<T>>>,
}

impl<T> Recorder<T> {
    /// An empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Keeps one value.
    pub fn record(&self, item: T) {
        self.held().push(item);
    }

    /// How many values were recorded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.held().len()
    }

    /// Whether nothing was recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.held().is_empty()
    }

    /// Forgets everything recorded so far.
    pub fn clear(&self) {
        self.held().clear();
    }

    /// Hands back everything recorded and empties the recording.
    ///
    /// The counterpart of [`items`](Self::items) for a value that cannot be
    /// cloned: what was recorded leaves the log rather than being copied out
    /// of it.
    #[must_use]
    pub fn take(&self) -> Vec<T> {
        core::mem::take(&mut *self.held())
    }

    /// Borrows the recording.
    ///
    /// A poisoned lock is taken anyway: what was recorded before a panic is
    /// still what happened, and a test that has already failed somewhere else
    /// is not helped by a second panic here.
    fn held(&self) -> MutexGuard<'_, Vec<T>> {
        self.items.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: Clone> Recorder<T> {
    /// A copy of what has been recorded, in arrival order.
    #[must_use]
    pub fn items(&self) -> Vec<T> {
        self.held().clone()
    }
}

impl<T> Clone for Recorder<T> {
    /// Shares the recording rather than copying it.
    ///
    /// Derived `Clone` would demand `T: Clone` for the wrong reason — the
    /// handle is clonable whatever it records — and would suggest that the two
    /// halves record separately.
    fn clone(&self) -> Self {
        Self {
            items: Arc::clone(&self.items),
        }
    }
}

impl<T> Default for Recorder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for Recorder<T> {
    /// Reports how much was recorded, never what.
    ///
    /// `T: Debug` is not required of a recorder, and asking for it here would
    /// make the bound spread to every double that holds one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recorder")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

/// A runnable that starts, does nothing, and returns when the stop is asked
/// for.
///
/// It exists so that a graph with nothing to run can still be held open: a
/// kernel whose runnables have all returned goes straight to phase six, so a
/// bundle that registers a component and a contract and no runnable stops
/// itself the moment it has started. [`TestBuilder::keep_running`] registers
/// one of these, and that is the usual way to reach it; a test that wants it
/// under its own name registers it with
/// [`TestBuilder::substitute_runnable`].
///
/// It watches `stopping` rather than `draining`: parking has no work to
/// finish, and returning at the first rung would end the run while the ladder
/// is still descending.
///
/// [`TestBuilder::keep_running`]: crate::TestBuilder::keep_running
/// [`TestBuilder::substitute_runnable`]: crate::TestBuilder::substitute_runnable
#[derive(Debug, Default, Clone, Copy)]
pub struct Parking;

impl Runnable for Parking {
    fn name() -> &'static str {
        PARKING
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            cx.shutdown().stopping().await;
            Ok(())
        })
    }
}

/// One lifecycle call the kernel made on a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Call {
    /// [`Component::boot`] was called.
    Boot,
    /// [`Component::shutdown`] was called.
    Shutdown,
}

/// A component that records the lifecycle calls it received.
///
/// The double for the case where what is under test is not what the component
/// does but *whether the kernel drove it*: that it was booted at all, that it
/// was booted once rather than twice, and that it was stopped before the run
/// ended.
///
/// It is registered through [`TestBuilder::substitute_component`], with the
/// test keeping a clone of the [`Arc`] it hands over — the kernel drives that
/// same object, so what the test reads afterwards is what the kernel did.
///
/// [`TestBuilder::substitute_component`]: crate::TestBuilder::substitute_component
#[derive(Debug, Default)]
pub struct LifecycleLog {
    /// The calls, in the order the kernel made them.
    calls: Recorder<Call>,
}

impl LifecycleLog {
    /// A component nobody has driven yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The calls received, in order.
    #[must_use]
    pub fn calls(&self) -> Vec<Call> {
        self.calls.items()
    }

    /// How many times it was booted.
    ///
    /// One, on a run that reached phase four: the kernel boots a component
    /// once. Zero says the boot never got that far, and two says something
    /// registered the double twice.
    #[must_use]
    pub fn boots(&self) -> usize {
        self.count(Call::Boot)
    }

    /// How many times it was stopped.
    #[must_use]
    pub fn stops(&self) -> usize {
        self.count(Call::Shutdown)
    }

    /// How many of the recorded calls were `call`.
    fn count(&self, call: Call) -> usize {
        self.calls
            .items()
            .iter()
            .filter(|&&seen| seen == call)
            .count()
    }
}

impl Component for LifecycleLog {
    fn name() -> &'static str {
        LIFECYCLE
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, _cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.calls.record(Call::Boot);
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        _cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            self.calls.record(Call::Shutdown);
            Ok(())
        })
    }
}
