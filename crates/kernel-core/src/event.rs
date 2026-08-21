//! Typed events and listener flow control.
//!
//! An event **is a type**. The dispatch table is indexed by the type identity,
//! never by a string: the compiler checks the payload, and no two independent
//! authors can collide by picking the same label. [`Event::NAME`] exists purely
//! so that a log line, a metric or an error message can print something a human
//! reads; nothing in the dispatch path ever looks at it.
//!
//! # Why `Listener` is not in this module
//!
//! The obvious companion of this module — the `Listener` trait — deliberately
//! lives in the runtime crate instead. Its signature takes a listener context,
//! and that context hands out the resolved container, the dispatcher and the
//! shutdown handle: runtime machinery this crate does not have and must not
//! grow. Putting `Listener` here would drag the whole runtime into every
//! contract crate that only wanted to declare an event type. Declare event
//! types here; implement listeners against the runtime crate. Do not move the
//! trait back into this module.

/// A payload broadcast to listeners, identified by its type.
///
/// Implement this on a plain data type. The type itself is the contract: two
/// events with the same [`NAME`](Event::NAME) but different types are two
/// unrelated events, and listeners registered for one never see the other.
///
/// Events are handed to listeners by mutable reference during sequential
/// dispatch, so an event may carry fields that listeners are expected to fill
/// in (enrichment) or flags they may set (veto).
///
/// # Examples
///
/// ```
/// use kernel_core::event::Event;
///
/// struct Alpha {
///     counter: u32,
/// }
///
/// impl Event for Alpha {
///     const NAME: &'static str = "alpha";
/// }
///
/// let mut e = Alpha { counter: 0 };
/// e.counter += 1;
/// assert_eq!(Alpha::NAME, "alpha");
/// ```
pub trait Event: Send + Sync + 'static {
    /// Human-readable label for diagnostics **only**.
    ///
    /// It appears in telemetry records and error messages. It is never used to
    /// route, look up, compare or deduplicate events — the type is the sole
    /// routing key — so duplicate values across unrelated event types are
    /// harmless, if unhelpful when reading logs.
    const NAME: &'static str;
}

/// Whether sequential dispatch continues to the next listener.
///
/// Returned by a listener after it has handled an event. Only sequential,
/// awaited dispatch honours it; detached emission has no propagation to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Flow {
    /// Pass the event to the next listener, in priority order.
    #[default]
    Continue,
    /// Stop propagation: no further listener observes this event.
    ///
    /// Stopping is not an error. The emitter still gets a successful dispatch
    /// result, which reports that propagation ended early.
    Stop,
}

impl Flow {
    /// Returns `true` for [`Flow::Continue`].
    #[must_use]
    pub fn is_continue(self) -> bool {
        matches!(self, Flow::Continue)
    }

    /// Returns `true` for [`Flow::Stop`].
    #[must_use]
    pub fn is_stop(self) -> bool {
        matches!(self, Flow::Stop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Alpha;
    struct Beta;

    impl Event for Alpha {
        const NAME: &'static str = "alpha";
    }

    impl Event for Beta {
        // Deliberately the same label: names are diagnostics, not identity.
        const NAME: &'static str = "alpha";
    }

    fn type_of<E: Event>() -> core::any::TypeId {
        core::any::TypeId::of::<E>()
    }

    #[test]
    fn name_is_diagnostic() {
        assert_eq!(Alpha::NAME, Beta::NAME);
        assert_ne!(type_of::<Alpha>(), type_of::<Beta>());
    }

    #[test]
    fn flow_defaults_continue() {
        assert_eq!(Flow::default(), Flow::Continue);
    }

    #[test]
    fn flow_predicates() {
        assert!(Flow::Continue.is_continue());
        assert!(!Flow::Continue.is_stop());
        assert!(Flow::Stop.is_stop());
        assert!(!Flow::Stop.is_continue());
    }

    #[test]
    fn flow_is_copy() {
        let a = Flow::Stop;
        let b = a;
        assert_eq!(a, b);
    }
}
