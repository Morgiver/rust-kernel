//! A listener that keeps every event it is given.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use kernel::dispatcher::{Listener, ListenerContext};
use kernel_core::{BoxFuture, Event, Flow, ListenerError};

/// Records each event of type `E` that reaches it.
///
/// Registered like any other listener, and cheap to clone-share so a test can
/// hold one end while the kernel holds the other.
pub struct EventLog<E: Event> {
    /// The recorded events, in dispatch order.
    ///
    /// Behind an [`Arc`] rather than owned, because the copy the registry takes
    /// and the copy the test keeps must be the same log. Cloning the handle is
    /// a pointer copy; it never forks the recording.
    events: Arc<Mutex<Vec<E>>>,
}

impl<E: Event> EventLog<E> {
    /// Borrows the recording.
    ///
    /// A poisoned lock is taken anyway: the events recorded before a panic are
    /// still the events that were dispatched, and a test that has already
    /// failed somewhere else is not helped by a second panic here.
    fn held(&self) -> MutexGuard<'_, Vec<E>> {
        self.events.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<E: Event> Clone for EventLog<E> {
    /// Shares the recording rather than copying it.
    ///
    /// Derived `Clone` would demand `E: Clone` for the wrong reason — the
    /// handle is clonable whatever the payload is — and would suggest that the
    /// two halves record separately.
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
        }
    }
}

impl<E: Event + Clone> EventLog<E> {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// A snapshot of what has been recorded, in dispatch order.
    #[must_use]
    pub fn events(&self) -> Vec<E> {
        self.held().clone()
    }

    /// How many events were recorded.
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
}

impl<E: Event + Clone> Default for EventLog<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Event + Clone> Listener<E> for EventLog<E> {
    /// Keeps a copy and lets the walk continue.
    ///
    /// It never stops propagation: an observer that changed what the other
    /// listeners see would make the test disagree with the run it is meant to
    /// describe.
    fn on_event<'a>(
        &'a self,
        event: &'a mut E,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            let seen = event.clone();
            self.held().push(seen);
            Ok(Flow::Continue)
        })
    }
}
