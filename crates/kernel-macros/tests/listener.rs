//! `#[listener]` — the generated impls, exercised through a real dispatch.
//!
//! The events are dispatched by a component during boot, which is the ordinary
//! way a listener runs. Nothing here inspects the expansion; what is asserted
//! is that the listener was reached, that it saw the event by mutable
//! reference, and that its `Flow` was honoured.

use std::sync::{Arc, Mutex};

use kernel::core::error::ListenerError;
use kernel::core::{
    BoxFuture, BundleManifest, ComponentDescriptor, ComponentError, Event, Flow, Priority,
    RegisterError,
};
use kernel::{BootContext, Bundle, Component, Kernel, ListenerContext, Provider, Registry};
use kernel_macros::listener;

struct Counted {
    hits: u32,
}

impl Event for Counted {
    const NAME: &'static str = "test.counted";
}

struct Halted {
    seen: u32,
}

impl Event for Halted {
    const NAME: &'static str = "test.halted";
}

/// Shared state every assertion reads back out.
type Log = Arc<Mutex<Vec<&'static str>>>;

struct Auditor {
    log: Log,
}

#[listener]
impl Auditor {
    async fn on_counted(&self, event: &mut Counted) -> Result<Flow, ListenerError> {
        event.hits += 1;
        self.log.lock().expect("log").push("counted");
        Ok(Flow::Continue)
    }

    async fn on_halted(
        &self,
        event: &mut Halted,
        cx: &ListenerContext<'_>,
    ) -> Result<Flow, ListenerError> {
        // Resolving through the context is the reason it exists; a listener is
        // registered long before anything is built.
        let _ = cx.container().is_sealed();
        event.seen += 1;
        self.log.lock().expect("log").push("halted");
        Ok(Flow::Stop)
    }
}

struct Blunt {
    log: Log,
}

#[listener]
impl Blunt {
    fn on_counted(&self, event: &mut Counted) -> Result<Flow, ListenerError> {
        event.hits += 10;
        self.log.lock().expect("log").push("blunt");
        Ok(Flow::Continue)
    }
}

/// Never reached: the handler before it stops propagation.
struct Late {
    log: Log,
}

#[listener]
impl Late {
    async fn on_halted(&self, event: &mut Halted) -> Result<Flow, ListenerError> {
        event.seen += 100;
        self.log.lock().expect("log").push("late");
        Ok(Flow::Continue)
    }
}

/// Dispatches both events while booting, and records what came back.
struct Emitter {
    counted: Arc<Mutex<u32>>,
    halted: Arc<Mutex<u32>>,
    stopped: Arc<Mutex<bool>>,
}

impl Component for Emitter {
    fn name() -> &'static str {
        "emitter"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new()
    }

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            let mut counted = Counted { hits: 0 };
            cx.dispatcher()
                .dispatch(&mut counted)
                .await
                .expect("dispatch");
            *self.counted.lock().expect("counted") = counted.hits;

            let mut halted = Halted { seen: 0 };
            let outcome = cx
                .dispatcher()
                .dispatch(&mut halted)
                .await
                .expect("dispatch");
            *self.halted.lock().expect("halted") = halted.seen;
            *self.stopped.lock().expect("stopped") = outcome.stopped;

            Ok(())
        })
    }
}

struct Fixture {
    log: Log,
    counted: Arc<Mutex<u32>>,
    halted: Arc<Mutex<u32>>,
    stopped: Arc<Mutex<bool>>,
}

impl Bundle for Fixture {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new("fixture", "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        // One type, two events, so the event has to be named at the
        // registration site. That is `Registry::listen` being generic over the
        // pair, not the attribute: a hand-written pair of impls needs the same
        // turbofish.
        registry.listen::<Counted, _>(
            Auditor {
                log: Arc::clone(&self.log),
            },
            Priority::HIGH,
        );
        registry.listen::<Halted, _>(
            Auditor {
                log: Arc::clone(&self.log),
            },
            Priority::HIGH,
        );
        registry.listen(
            Blunt {
                log: Arc::clone(&self.log),
            },
            Priority::NORMAL,
        );
        registry.listen(
            Late {
                log: Arc::clone(&self.log),
            },
            Priority::LOW,
        );
        registry.component(Provider::from_value(Arc::new(Emitter {
            counted: Arc::clone(&self.counted),
            halted: Arc::clone(&self.halted),
            stopped: Arc::clone(&self.stopped),
        })));
        Ok(())
    }
}

struct Run {
    log: Vec<&'static str>,
    counted: u32,
    halted: u32,
    stopped: bool,
}

async fn run() -> Run {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let counted = Arc::new(Mutex::new(0));
    let halted = Arc::new(Mutex::new(0));
    let stopped = Arc::new(Mutex::new(false));

    let outcome = Kernel::builder()
        .capture_signals(false)
        .bundle(Fixture {
            log: Arc::clone(&log),
            counted: Arc::clone(&counted),
            halted: Arc::clone(&halted),
            stopped: Arc::clone(&stopped),
        })
        .build()
        .await
        .expect("the graph closes")
        .run()
        .await;

    assert!(outcome.is_success(), "{outcome:?}");

    let log = log.lock().expect("log").clone();
    Run {
        log,
        counted: *counted.lock().expect("counted"),
        halted: *halted.lock().expect("halted"),
        stopped: *stopped.lock().expect("stopped"),
    }
}

#[tokio::test(start_paused = true)]
async fn handler_sees_event() {
    let run = run().await;

    assert!(run.log.contains(&"counted"));
    assert_eq!(run.counted, 11, "both handlers mutated the event");
}

#[tokio::test(start_paused = true)]
async fn sync_handler_runs() {
    let run = run().await;

    assert!(run.log.contains(&"blunt"));
}

#[tokio::test(start_paused = true)]
async fn context_handler_stops() {
    let run = run().await;

    assert_eq!(run.halted, 1, "only the stopping handler ran");
    assert!(run.stopped);
    assert!(!run.log.contains(&"late"));
}

#[tokio::test(start_paused = true)]
async fn priority_order_holds() {
    let run = run().await;

    let counted = run.log.iter().position(|entry| *entry == "counted");
    let blunt = run.log.iter().position(|entry| *entry == "blunt");

    assert!(counted < blunt, "{:?}", run.log);
}
