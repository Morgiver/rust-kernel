//! In-process broadcast of typed events to listeners.
//!
//! # An event is a type, never a string
//!
//! The dispatch table is indexed by [`TypeId`]. Nothing in the dispatch path
//! ever compares a name: the compiler checks the payload, and two independent
//! authors cannot collide by picking the same label. [`Event::NAME`] exists for
//! **diagnostics only** — it appears in telemetry records and in error
//! messages, and never routes anything.
//!
//! # Two modes, two methods
//!
//! [`EventDispatcher::dispatch`] is sequential, awaited and priority-ordered:
//! listeners may mutate the event and stop propagation, and the first failure
//! reaches the emitter. [`EventDispatcher::emit`] is detached: it hands the
//! event to the runtime and returns, ordering across calls is not guaranteed,
//! and failures go to telemetry rather than to the emitter.
//!
//! They are two methods rather than one boolean argument because confusing
//! *sequential and awaited* with *detached and ignored* is the main source of
//! bugs in this kind of mechanism. A boolean at a call site reads the same in
//! both cases; two names do not. Use `dispatch` when the emitter's control flow
//! depends on the outcome, `emit` for notification.
//!
//! # The table is frozen
//!
//! Listeners are registered in phase two and the table is built once, in phase
//! three. It is immutable afterwards, so the whole of the run phase reads it
//! with no lock at all. There is no dynamic listener registration and there
//! will not be one: a table that can change under a reader would have to be
//! locked on every event, and an event that arrives while the table is being
//! rewritten has no defensible ordering.
//!
//! Within one table, order is [`Priority`] descending, then registration order.
//! Always. Both `dispatch` and one call to `emit` walk that same order.

use core::any::{Any, TypeId};
use core::fmt;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use kernel_core::telemetry::{Level, Record};
use kernel_core::{BoxFuture, DispatchError, Event, Flow, ListenerError, Priority, Telemetry};

use crate::container::Container;
use crate::registry::ListenerEntry;

/// Reacts to one event type.
///
/// A listener is registered against a single `E`, so the payload it receives is
/// checked at compile time — there is no cast in the listener's own code, and
/// no way to register it for the wrong event.
///
/// The event arrives by mutable reference: during sequential dispatch a
/// listener may enrich it, set a veto flag, or return [`Flow::Stop`] to end
/// propagation. During detached emission the mutation is still performed, but
/// nobody observes it afterwards — the event is dropped when the walk ends.
///
/// The method is not an `async fn`, because a trait with an `async fn` is not
/// dyn-compatible and every listener is held behind an erased pointer.
///
/// # Examples
///
/// ```
/// use kernel::core::{BoxFuture, Event, Flow, ListenerError};
/// use kernel::dispatcher::{Listener, ListenerContext};
///
/// struct Signal {
///     seen: u32,
/// }
///
/// impl Event for Signal {
///     const NAME: &'static str = "signal";
/// }
///
/// struct Counting;
///
/// impl Listener<Signal> for Counting {
///     fn on_event<'a>(
///         &'a self,
///         event: &'a mut Signal,
///         _cx: &'a ListenerContext<'a>,
///     ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
///         Box::pin(async move {
///             event.seen += 1;
///             Ok(Flow::Continue)
///         })
///     }
/// }
/// ```
pub trait Listener<E: Event>: Send + Sync + 'static {
    /// Handles one event.
    ///
    /// Returning [`Flow::Stop`] ends sequential propagation; it is not an
    /// error, and the emitter still gets a successful [`Dispatched`].
    fn on_event<'a>(
        &'a self,
        event: &'a mut E,
        cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>>;
}

/// One listener with its payload type erased, as the table stores it.
///
/// The typed payload is recovered inside the adapter built by
/// [`erase_listener`], which is the only place that performs the cast. The
/// higher-ranked lifetime ties the event, the context and the returned future
/// together, so a listener may borrow both of its arguments for exactly as long
/// as its future lives — and no longer.
pub(crate) type ErasedListener = Arc<
    dyn for<'a> Fn(
            &'a mut (dyn Any + Send),
            &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>>
        + Send
        + Sync,
>;

/// Coerces a closure of the right shape into an [`ErasedListener`].
///
/// The bound is what teaches closure inference the higher-ranked signature;
/// written inline at the call site, the two reference arguments and the
/// returned future would get three unrelated lifetimes and no coercion would
/// apply.
fn as_erased<F>(call: F) -> ErasedListener
where
    F: for<'a> Fn(
            &'a mut (dyn Any + Send),
            &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>>
        + Send
        + Sync
        + 'static,
{
    Arc::new(call)
}

/// Erases a typed listener so the table can hold listeners of every event type
/// in one map.
///
/// The recovered payload cannot be wrong in practice: the table is keyed by
/// `TypeId::of::<E>()` and only a walk over that key reaches this adapter. A
/// mismatch would therefore be a defect in the dispatcher itself, so it is
/// reported as a [`ListenerError`] rather than allowed to abort a detached
/// task.
pub(crate) fn erase_listener<E: Event, L: Listener<E>>(listener: L) -> ErasedListener {
    let listener = Arc::new(listener);
    as_erased(move |event, cx| {
        let listener = Arc::clone(&listener);
        Box::pin(async move {
            let Some(typed) = event.downcast_mut::<E>() else {
                return Err(ListenerError::new(
                    E::NAME,
                    "listener invoked for a payload of another type"
                        .to_owned()
                        .into(),
                ));
            };
            listener.on_event(typed, cx).await
        })
    })
}

/// What a listener is given besides the event itself.
///
/// It carries the resolved container, which is why it cannot exist before phase
/// three — and why listeners may only be registered in phase two, when there is
/// nothing to resolve against yet.
pub struct ListenerContext<'a> {
    container: &'a Container,
}

impl<'a> ListenerContext<'a> {
    /// Borrows a context over a resolved container.
    pub(crate) fn new(container: &'a Container) -> Self {
        Self { container }
    }

    /// The resolved container.
    ///
    /// A listener resolves what it needs here rather than holding it, because
    /// a listener is registered before anything is built.
    #[must_use]
    pub fn container(&self) -> &Container {
        self.container
    }

    /// The telemetry sink.
    #[must_use]
    pub fn telemetry(&self) -> &Arc<dyn Telemetry> {
        self.container.telemetry()
    }
}

impl fmt::Debug for ListenerContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenerContext").finish_non_exhaustive()
    }
}

/// What a sequential dispatch did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Dispatched {
    /// How many listeners observed the event.
    ///
    /// A listener that returned [`Flow::Stop`] is counted: it ran.
    pub listeners_run: usize,
    /// Whether a listener ended propagation before the end of the table.
    pub stopped: bool,
}

/// One entry of the frozen table.
struct Slot {
    call: ErasedListener,
    bundle: &'static str,
    priority: Priority,
}

impl fmt::Debug for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot")
            .field("bundle", &self.bundle)
            .field("priority", &self.priority)
            .finish_non_exhaustive()
    }
}

/// Telemetry event name for an emission that could not be delivered.
const DROPPED: &str = "dispatcher.dropped";
/// Telemetry event name for a listener that failed during a detached emission.
const FAILED: &str = "dispatcher.listener_failed";
/// Telemetry event name for a second attach.
const REATTACHED: &str = "dispatcher.reattached";

/// Broadcasts typed events to the listeners registered for them.
///
/// Built once in phase three from the registry's listener entries; the table
/// never changes afterwards. Cheap to hold behind an `Arc` and to read from any
/// number of tasks at once.
pub struct EventDispatcher {
    table: HashMap<TypeId, Arc<[Slot]>>,
    container: OnceLock<Container>,
    telemetry: Arc<dyn Telemetry>,
    runtime: Option<tokio::runtime::Handle>,
}

impl EventDispatcher {
    /// Freezes the listener entries into the dispatch table.
    ///
    /// Entries arrive in registration order and are sorted by [`Priority`]
    /// descending, ties broken by that registration order — the sort is stable
    /// on the rank, so the result is the same on every run of the same program.
    ///
    /// The runtime handle for [`emit`](Self::emit) is captured **here**, not at
    /// emission time: a dispatcher built inside the application's runtime keeps
    /// spawning correctly from a thread that is not itself a runtime thread.
    pub(crate) fn new(listeners: Vec<ListenerEntry>, telemetry: Arc<dyn Telemetry>) -> Self {
        let mut grouped: HashMap<TypeId, Vec<ListenerEntry>> = HashMap::new();
        for entry in listeners {
            grouped.entry(entry.event).or_default().push(entry);
        }

        let mut table = HashMap::with_capacity(grouped.len());
        for (event, mut entries) in grouped {
            entries.sort_by(|left, right| {
                right
                    .priority
                    .cmp(&left.priority)
                    .then(left.order.cmp(&right.order))
            });
            let slots: Arc<[Slot]> = entries
                .into_iter()
                .map(|entry| Slot {
                    call: entry.call,
                    bundle: entry.bundle,
                    priority: entry.priority,
                })
                .collect();
            table.insert(event, slots);
        }

        Self {
            table,
            container: OnceLock::new(),
            telemetry,
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }

    /// Sequential, awaited, priority-ordered dispatch.
    ///
    /// Listeners run one after another, highest [`Priority`] first, ties in
    /// registration order. Each one may mutate `event` and may end propagation
    /// with [`Flow::Stop`]. The first failure stops the walk and reaches the
    /// emitter as a [`DispatchError`]; the listeners after it do not run.
    ///
    /// An event nobody listens for is not an error: the walk is empty and the
    /// result reports zero listeners.
    pub async fn dispatch<E: Event>(&self, event: &mut E) -> Result<Dispatched, DispatchError> {
        let Some(slots) = self.table.get(&TypeId::of::<E>()) else {
            return Ok(Dispatched::default());
        };

        let Some(container) = self.container.get() else {
            return Err(DispatchError::new(
                E::NAME,
                ListenerError::new(
                    E::NAME,
                    "dispatcher has no container: dispatched before phase three"
                        .to_owned()
                        .into(),
                ),
            ));
        };

        let cx = ListenerContext::new(container);
        let erased: &mut (dyn Any + Send) = event;
        let mut listeners_run = 0;

        for slot in slots.iter() {
            let flow = (slot.call)(&mut *erased, &cx)
                .await
                .map_err(|cause| DispatchError::new(E::NAME, cause))?;
            listeners_run += 1;
            if flow.is_stop() {
                return Ok(Dispatched {
                    listeners_run,
                    stopped: true,
                });
            }
        }

        Ok(Dispatched {
            listeners_run,
            stopped: false,
        })
    }

    /// Detached notification.
    ///
    /// The event is moved onto a task of its own and this returns immediately.
    /// Within that task the listeners still run in priority order, but nothing
    /// orders one `emit` against another, against a concurrent `dispatch`, or
    /// against the caller that emitted. A listener failure is recorded at error
    /// level and the walk continues: there is no emitter left to tell, and a
    /// notification that stops halfway because one subscriber is broken is
    /// worse than one that does not.
    ///
    /// An event nobody listens for is silent: there is nothing to spawn, and
    /// that is checked before anything else so the same non-event never reports
    /// two different ways.
    ///
    /// With no runtime to spawn on — a synchronous caller, a dispatcher built
    /// outside the application's runtime — the event is recorded at error level
    /// and dropped. It does not panic: an emission is a notification, and
    /// taking a process down because a notification had nowhere to go is not a
    /// trade the kernel makes.
    pub fn emit<E: Event>(&self, event: E) {
        let Some(slots) = self.table.get(&TypeId::of::<E>()).cloned() else {
            return;
        };

        let Some(runtime) = self.runtime.clone() else {
            self.drop_event::<E>("no_runtime");
            return;
        };

        let Some(container) = self.container.get().cloned() else {
            self.drop_event::<E>("not_attached");
            return;
        };

        let telemetry = Arc::clone(&self.telemetry);
        runtime.spawn(async move {
            let mut event = event;
            let cx = ListenerContext::new(&container);
            let erased: &mut (dyn Any + Send) = &mut event;

            for slot in slots.iter() {
                match (slot.call)(&mut *erased, &cx).await {
                    Ok(flow) if flow.is_stop() => break,
                    Ok(_) => {}
                    Err(cause) => telemetry.record(
                        Record::new(Level::Error, FAILED)
                            .with("event", E::NAME)
                            .with("bundle", slot.bundle)
                            .with("priority", i64::from(slot.priority.get()))
                            .with("error", cause.to_string()),
                    ),
                }
            }
        });
    }

    /// How many listeners are registered for `E`.
    ///
    /// Constant for the life of the dispatcher, the table being frozen.
    #[must_use]
    pub fn listener_count<E: Event>(&self) -> usize {
        self.table
            .get(&TypeId::of::<E>())
            .map_or(0, |slots| slots.len())
    }

    /// Hands the dispatcher the resolved container.
    ///
    /// Called once in phase three, after the container exists and before
    /// anything can be dispatched. A second call cannot replace the first — the
    /// container a listener resolves through must not change under it — and is
    /// recorded at warning level instead.
    pub(crate) fn attach(&self, container: Container) {
        if self.container.set(container).is_err() {
            self.telemetry.record(Record::new(Level::Warn, REATTACHED));
        }
    }

    /// Records an emission that never reached a listener.
    fn drop_event<E: Event>(&self, reason: &'static str) {
        self.telemetry.record(
            Record::new(Level::Error, DROPPED)
                .with("event", E::NAME)
                .with("reason", reason),
        );
    }
}

impl fmt::Debug for EventDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventDispatcher")
            .field("event_types", &self.table.len())
            .field("attached", &self.container.get().is_some())
            .field("has_runtime", &self.runtime.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use kernel_core::{ConfigTree, NoopTelemetry, RecordingTelemetry};
    use tokio::sync::mpsc;

    use crate::shutdown::KernelHandle;

    struct Alpha {
        marks: Vec<&'static str>,
    }

    impl Event for Alpha {
        const NAME: &'static str = "alpha";
    }

    // Deliberately the same NAME as `Alpha`: the label is diagnostics, the type
    // is the routing key.
    struct Beta;

    impl Event for Beta {
        const NAME: &'static str = "alpha";
    }

    /// Appends its own mark, then returns `flow`.
    struct Marker {
        mark: &'static str,
        flow: Flow,
    }

    impl Marker {
        fn new(mark: &'static str) -> Self {
            Self {
                mark,
                flow: Flow::Continue,
            }
        }

        fn stopping(mark: &'static str) -> Self {
            Self {
                mark,
                flow: Flow::Stop,
            }
        }
    }

    impl Listener<Alpha> for Marker {
        fn on_event<'a>(
            &'a self,
            event: &'a mut Alpha,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async move {
                event.marks.push(self.mark);
                Ok(self.flow)
            })
        }
    }

    /// Fails, after recording that it ran.
    struct Failing {
        mark: &'static str,
    }

    impl Listener<Alpha> for Failing {
        fn on_event<'a>(
            &'a self,
            event: &'a mut Alpha,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async move {
                event.marks.push(self.mark);
                Err(ListenerError::new(
                    Alpha::NAME,
                    "deliberate".to_owned().into(),
                ))
            })
        }
    }

    /// Reports through a channel, so a detached walk is observable.
    struct Reporting {
        mark: &'static str,
        sender: mpsc::UnboundedSender<&'static str>,
    }

    impl Listener<Alpha> for Reporting {
        fn on_event<'a>(
            &'a self,
            _event: &'a mut Alpha,
            _cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async move {
                let _ = self.sender.send(self.mark);
                Ok(Flow::Continue)
            })
        }
    }

    /// Resolves nothing, only proves the context reaches the listener.
    struct Probing {
        seen: Mutex<bool>,
    }

    impl Listener<Alpha> for Probing {
        fn on_event<'a>(
            &'a self,
            _event: &'a mut Alpha,
            cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            Box::pin(async move {
                cx.telemetry()
                    .record(Record::new(Level::Info, "probe.seen"));
                let _ = cx.container();
                *self.seen.lock().expect("probe") = true;
                Ok(Flow::Continue)
            })
        }
    }

    fn alpha() -> Alpha {
        Alpha { marks: Vec::new() }
    }

    fn container() -> Container {
        container_with(Arc::new(NoopTelemetry))
    }

    fn container_with(telemetry: Arc<dyn Telemetry>) -> Container {
        Container::new(
            Vec::new(),
            Arc::new(ConfigTree::empty()),
            telemetry,
            KernelHandle::detached(),
        )
    }

    fn entry<E: Event, L: Listener<E>>(
        listener: L,
        bundle: &'static str,
        priority: i32,
        order: u32,
    ) -> ListenerEntry {
        ListenerEntry {
            event: TypeId::of::<E>(),
            event_name: E::NAME,
            bundle,
            priority: Priority(priority),
            order,
            call: erase_listener(listener),
        }
    }

    /// A dispatcher over `listeners`, already attached to an empty container.
    fn attached(listeners: Vec<ListenerEntry>) -> EventDispatcher {
        let dispatcher = EventDispatcher::new(listeners, Arc::new(NoopTelemetry));
        dispatcher.attach(container());
        dispatcher
    }

    #[tokio::test]
    async fn walks_priority_descending() {
        // Registration order is deliberately the reverse of the wanted order.
        let dispatcher = attached(vec![
            entry::<Alpha, _>(Marker::new("low"), "b1", -100, 0),
            entry::<Alpha, _>(Marker::new("normal"), "b2", 0, 1),
            entry::<Alpha, _>(Marker::new("high"), "b3", 100, 2),
        ]);

        let mut event = alpha();
        let done = dispatcher.dispatch(&mut event).await.expect("dispatch");

        assert_eq!(event.marks, ["high", "normal", "low"]);
        assert_eq!(done.listeners_run, 3);
        assert!(!done.stopped);
    }

    #[tokio::test]
    async fn ties_keep_registration_order() {
        let dispatcher = attached(vec![
            entry::<Alpha, _>(Marker::new("first"), "b1", 0, 0),
            entry::<Alpha, _>(Marker::new("second"), "b2", 0, 1),
            entry::<Alpha, _>(Marker::new("third"), "b3", 0, 2),
        ]);

        let mut event = alpha();
        dispatcher.dispatch(&mut event).await.expect("dispatch");

        assert_eq!(event.marks, ["first", "second", "third"]);
    }

    // The same table, dispatched twice, must produce the same sequence: nothing
    // in the walk may depend on hash iteration order.
    #[tokio::test]
    async fn order_is_reproducible() {
        let dispatcher = attached(vec![
            entry::<Alpha, _>(Marker::new("a"), "b1", 5, 0),
            entry::<Alpha, _>(Marker::new("b"), "b2", 5, 1),
            entry::<Alpha, _>(Marker::new("c"), "b3", 9, 2),
            entry::<Alpha, _>(Marker::new("d"), "b4", 5, 3),
        ]);

        let mut first = alpha();
        let mut second = alpha();
        dispatcher.dispatch(&mut first).await.expect("dispatch");
        dispatcher.dispatch(&mut second).await.expect("dispatch");

        assert_eq!(first.marks, ["c", "a", "b", "d"]);
        assert_eq!(first.marks, second.marks);
    }

    #[tokio::test]
    async fn stop_ends_the_walk() {
        let dispatcher = attached(vec![
            entry::<Alpha, _>(Marker::new("first"), "b1", 10, 0),
            entry::<Alpha, _>(Marker::stopping("second"), "b2", 5, 1),
            entry::<Alpha, _>(Marker::new("third"), "b3", 0, 2),
        ]);

        let mut event = alpha();
        let done = dispatcher.dispatch(&mut event).await.expect("dispatch");

        assert_eq!(event.marks, ["first", "second"]);
        assert_eq!(done.listeners_run, 2);
        assert!(done.stopped);
    }

    #[tokio::test]
    async fn error_reaches_emitter() {
        let dispatcher = attached(vec![
            entry::<Alpha, _>(Marker::new("first"), "b1", 10, 0),
            entry::<Alpha, _>(Failing { mark: "second" }, "b2", 5, 1),
            entry::<Alpha, _>(Marker::new("third"), "b3", 0, 2),
        ]);

        let mut event = alpha();
        let error = dispatcher.dispatch(&mut event).await.expect_err("failure");

        assert_eq!(error.event(), Alpha::NAME);
        assert_eq!(event.marks, ["first", "second"]);
    }

    #[tokio::test]
    async fn listeners_mutate_the_event() {
        let dispatcher = attached(vec![entry::<Alpha, _>(Marker::new("touched"), "b1", 0, 0)]);

        let mut event = alpha();
        dispatcher.dispatch(&mut event).await.expect("dispatch");

        assert_eq!(event.marks, ["touched"]);
    }

    #[tokio::test]
    async fn unheard_dispatch_succeeds() {
        let dispatcher = attached(Vec::new());

        let mut event = alpha();
        let done = dispatcher.dispatch(&mut event).await.expect("dispatch");

        assert_eq!(done, Dispatched::default());
    }

    // Two event types sharing one NAME must not reach each other's listeners.
    #[tokio::test]
    async fn routes_by_type_id() {
        struct Silent;

        impl Listener<Beta> for Silent {
            fn on_event<'a>(
                &'a self,
                _event: &'a mut Beta,
                _cx: &'a ListenerContext<'a>,
            ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
                Box::pin(async { Ok(Flow::Continue) })
            }
        }

        let dispatcher = attached(vec![
            entry::<Alpha, _>(Marker::new("alpha"), "b1", 0, 0),
            entry::<Beta, _>(Silent, "b2", 0, 1),
        ]);

        assert_eq!(dispatcher.listener_count::<Alpha>(), 1);
        assert_eq!(dispatcher.listener_count::<Beta>(), 1);

        let mut event = alpha();
        let done = dispatcher.dispatch(&mut event).await.expect("dispatch");

        assert_eq!(done.listeners_run, 1);
        assert_eq!(event.marks, ["alpha"]);
    }

    #[tokio::test]
    async fn counts_only_registered() {
        let dispatcher = attached(vec![
            entry::<Alpha, _>(Marker::new("a"), "b1", 0, 0),
            entry::<Alpha, _>(Marker::new("b"), "b2", 0, 1),
        ]);

        assert_eq!(dispatcher.listener_count::<Alpha>(), 2);
        assert_eq!(dispatcher.listener_count::<Beta>(), 0);
    }

    #[tokio::test]
    async fn context_reaches_listener() {
        let probe = Arc::new(Probing {
            seen: Mutex::new(false),
        });
        let telemetry = Arc::new(RecordingTelemetry::new());
        let dispatcher = EventDispatcher::new(
            vec![entry::<Alpha, _>(ProbeRef(Arc::clone(&probe)), "b1", 0, 0)],
            Arc::new(NoopTelemetry),
        );
        dispatcher.attach(container_with(telemetry.clone()));

        let mut event = alpha();
        dispatcher.dispatch(&mut event).await.expect("dispatch");

        assert!(*probe.seen.lock().expect("probe"));
        assert!(telemetry.contains("probe.seen"));
    }

    /// Shares one `Probing` between the table and the assertion.
    struct ProbeRef(Arc<Probing>);

    impl Listener<Alpha> for ProbeRef {
        fn on_event<'a>(
            &'a self,
            event: &'a mut Alpha,
            cx: &'a ListenerContext<'a>,
        ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
            self.0.on_event(event, cx)
        }
    }

    #[tokio::test]
    async fn dispatch_before_attach_fails() {
        let dispatcher = EventDispatcher::new(
            vec![entry::<Alpha, _>(Marker::new("a"), "b1", 0, 0)],
            Arc::new(NoopTelemetry),
        );

        let mut event = alpha();
        assert!(dispatcher.dispatch(&mut event).await.is_err());
    }

    #[tokio::test]
    async fn second_attach_is_refused() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let dispatcher = EventDispatcher::new(Vec::new(), telemetry.clone());
        dispatcher.attach(container());
        dispatcher.attach(container());

        assert!(telemetry.contains(REATTACHED));
    }

    // The path a synchronous caller hits: no runtime, so nothing can be
    // spawned. It must record and drop, never panic.
    #[test]
    fn emit_without_runtime_drops() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let dispatcher = EventDispatcher::new(
            vec![entry::<Alpha, _>(Marker::new("never"), "b1", 0, 0)],
            telemetry.clone(),
        );

        dispatcher.emit(alpha());

        let records = telemetry.records();
        let record = records.first().expect("one record");
        assert_eq!(record.level, Level::Error);
        assert_eq!(record.event, DROPPED);
        assert_eq!(
            record.field("event"),
            Some(&kernel_core::telemetry::FieldValue::Str(
                Alpha::NAME.to_owned()
            ))
        );
    }

    // Same path, reached the other way: a runtime exists but phase three never
    // ran, so there is no container to give a listener.
    #[tokio::test]
    async fn emit_before_attach_drops() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let dispatcher = EventDispatcher::new(
            vec![entry::<Alpha, _>(Marker::new("never"), "b1", 0, 0)],
            telemetry.clone(),
        );

        dispatcher.emit(alpha());

        assert!(telemetry.contains(DROPPED));
    }

    #[tokio::test]
    async fn emit_runs_detached() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let dispatcher = attached(vec![
            entry::<Alpha, _>(
                Reporting {
                    mark: "low",
                    sender: sender.clone(),
                },
                "b1",
                -1,
                0,
            ),
            entry::<Alpha, _>(
                Reporting {
                    mark: "high",
                    sender,
                },
                "b2",
                1,
                1,
            ),
        ]);

        dispatcher.emit(alpha());

        assert_eq!(receiver.recv().await, Some("high"));
        assert_eq!(receiver.recv().await, Some("low"));
    }

    #[tokio::test]
    async fn emit_failure_reaches_telemetry() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let telemetry = Arc::new(RecordingTelemetry::new());
        let dispatcher = EventDispatcher::new(
            vec![
                entry::<Alpha, _>(Failing { mark: "boom" }, "b1", 10, 0),
                entry::<Alpha, _>(
                    Reporting {
                        mark: "after",
                        sender,
                    },
                    "b2",
                    0,
                    1,
                ),
            ],
            telemetry.clone(),
        );
        dispatcher.attach(container());

        dispatcher.emit(alpha());

        // A failure does not end a detached walk: the next listener still runs.
        assert_eq!(receiver.recv().await, Some("after"));
        assert!(telemetry.contains(FAILED));
    }

    // Unreachable through the dispatcher, whose table is keyed by `TypeId`, so
    // the erased closure is invoked directly: the arm reports a defect in the
    // erasure layer and that report is what is pinned here.
    #[tokio::test]
    async fn erased_rejects_wrong_payload() {
        let call = erase_listener::<Alpha, _>(Marker::new("never"));
        let container = container();
        let cx = ListenerContext::new(&container);
        let mut other = Beta;
        let erased: &mut (dyn Any + Send) = &mut other;

        let error = call(erased, &cx)
            .await
            .expect_err("payload of another type");

        assert_eq!(error.event(), Alpha::NAME);
        assert!(error.cause().to_string().contains("another type"));
    }

    #[tokio::test]
    async fn unheard_emit_is_silent() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let dispatcher = EventDispatcher::new(Vec::new(), telemetry.clone());
        dispatcher.attach(container());

        dispatcher.emit(alpha());

        assert!(telemetry.is_empty());
    }

    // The other half of the same no-op: without a runtime an unheard event is
    // still silent, so one non-event does not report two different ways.
    #[test]
    fn unheard_emit_without_runtime() {
        let telemetry = Arc::new(RecordingTelemetry::new());
        let dispatcher = EventDispatcher::new(Vec::new(), telemetry.clone());
        dispatcher.attach(container());

        dispatcher.emit(alpha());

        assert!(telemetry.is_empty());
    }

    // The table is built in phase three and never written again, so any number
    // of tasks may walk it at once. Nothing on this path takes a lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatches_concurrently() {
        let dispatcher = Arc::new(attached(vec![
            entry::<Alpha, _>(Marker::new("high"), "b1", 10, 0),
            entry::<Alpha, _>(Marker::new("low"), "b2", -10, 1),
        ]));

        let walkers: Vec<_> = (0..32)
            .map(|_| {
                let dispatcher = Arc::clone(&dispatcher);
                tokio::spawn(async move {
                    let mut event = alpha();
                    let done = dispatcher.dispatch(&mut event).await.expect("dispatched");
                    (done.listeners_run, event.marks)
                })
            })
            .collect();

        for walker in walkers {
            let (run, marks) = walker.await.expect("task");
            assert_eq!(run, 2);
            assert_eq!(marks, vec!["high", "low"]);
        }
    }

    #[tokio::test]
    async fn dispatcher_is_shareable() {
        fn assert_shared<T: Send + Sync + 'static>(_: &T) {}
        assert_shared(&attached(Vec::new()));
    }
}
