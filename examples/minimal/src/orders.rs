//! The bundle that produces orders.
//!
//! It depends on [`Ledger`] and on nothing else about the ledger — not on the
//! bundle that provides it, not on the component behind it. That is the whole
//! reason contracts live in a module of their own.
//!
//! It shows three more things: a runnable that does periodic work and returns
//! when the shutdown token fires, a contribution to a point another bundle
//! declared, and a listener on a kernel lifecycle event.

use core::time::Duration;
use std::sync::Arc;

use kernel::core::{
    BuildError, BundleManifest, ConfigError, ConfigNode, Criticality, Flow, FromConfig,
    ListenerError, Priority, RegisterError, RunError, RunnableDescriptor,
};
use kernel::{
    BoxFuture, Bundle, ContractRef, Listener, ListenerContext, Provider, Registry, RunContext,
    Runnable, ShutdownRequested,
};

use crate::contracts::{Ledger, OpeningNote};

/// The name this bundle publishes.
const NAME: &str = "orders";

/// What this bundle needs someone else to provide.
///
/// The manifest states it so that a missing ledger is reported against
/// `orders` by name, in phase three, instead of surfacing as a resolution
/// failure deep inside a build.
static REQUIRES: [ContractRef; 1] = [ContractRef::of::<dyn Ledger>()];

/// What the feed reads out of the configuration tree.
struct Settings {
    /// How long between two rounds.
    every: Duration,
    /// How many orders one round produces.
    batch: u32,
}

impl FromConfig for Settings {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        Ok(Self {
            every: node.field("every")?,
            batch: node.field("batch")?,
        })
    }
}

/// The task: a round of orders every `every`, until the kernel says stop.
struct Feed {
    /// Where the orders go. A contract, not an implementation.
    ledger: Arc<dyn Ledger>,
    /// How long between two rounds.
    every: Duration,
    /// How many orders one round produces.
    batch: u32,
}

impl Runnable for Feed {
    fn name() -> &'static str {
        "feed"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        // Essential: if this returns on its own, the process has no reason to
        // stay up, and the kernel stops the rest.
        RunnableDescriptor::new().criticality(Criticality::Essential)
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            let mut round: u64 = 0;

            // The one rule a runnable must obey: the token wins the race. A
            // loop that only slept would be abandoned at its deadline instead
            // of returning — and this is the wait that holds the rule, so this
            // crate names no timer and writes no `select!` of its own.
            //
            // Draining means *stop taking new work*, and a new round is new
            // work: the wait ends, `is_elapsed` is false, and the loop leaves.
            while cx
                .shutdown()
                .sleep_until_draining(self.every)
                .await
                .is_elapsed()
            {
                round += 1;
                for item in 0..self.batch {
                    let number = self.ledger.record(&format!("order {round}.{item}"));
                    println!("[feed] wrote entry {number}");
                }
            }

            println!(
                "[feed] returning after {round} rounds, {} entries",
                self.ledger.written()
            );
            Ok(())
        })
    }
}

/// Says out loud why the process is stopping, and signs the request.
///
/// [`ShutdownRequested`] is dispatched rather than emitted, so this runs
/// before the shutdown ladder moves — and the note it pushes travels with the
/// event.
struct Announce;

impl Listener<ShutdownRequested> for Announce {
    fn on_event<'a>(
        &'a self,
        event: &'a mut ShutdownRequested,
        _cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>> {
        Box::pin(async move {
            println!("[orders] shutdown requested: {:?}", event.reason);
            event.notes.push("orders heard it".to_owned());
            Ok(Flow::Continue)
        })
    }
}

/// Registers the feed, its contribution and its listener.
pub struct Bundled;

impl Bundle for Bundled {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new(NAME, "0.1.0")
            .requires(&REQUIRES)
            .after(&["ledger"])
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        let settings: Settings = registry
            .config(NAME)
            .map_err(|error| RegisterError::new(NAME, Box::new(error)))?;

        // A point this bundle did not declare, and whose declarer it does not
        // name.
        registry.contribute(OpeningNote(format!(
            "orders: {} every {:?}",
            settings.batch, settings.every
        )));

        registry.listen(Announce, Priority::NORMAL);

        registry.runnable(
            Provider::from_fn(move |container| {
                Box::pin(async move {
                    let ledger = container
                        .get::<dyn Ledger>()
                        .await
                        .map_err(|error| BuildError::new("Feed", Box::new(error)))?;
                    Ok(Arc::new(Feed {
                        ledger,
                        every: settings.every,
                        batch: settings.batch,
                    }))
                })
            })
            .requires(REQUIRES),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// A ledger that counts, so the feed can be run without a kernel.
    #[derive(Default)]
    struct Counter(AtomicU64);

    impl Ledger for Counter {
        fn record(&self, _order: &str) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed) + 1
        }

        fn written(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// The test the design makes mandatory for every runnable: given a token
    /// that has already fired, `run` returns instead of waiting for its timer.
    ///
    /// Nothing waits, so nothing needs paused time: the ladder is moved before
    /// the runnable is entered.
    ///
    /// Draining means *stop taking new work*, so the round the wait was about
    /// to start is not started: the ledger is untouched. A loop that read the
    /// token only after doing its work would write once and still return.
    #[tokio::test]
    async fn returns_on_token() {
        let (cx, controller) = RunContext::detached();
        controller.begin_draining();

        let ledger = Arc::new(Counter::default());
        let feed = Arc::new(Feed {
            ledger: Arc::clone(&ledger) as Arc<dyn Ledger>,
            // Long enough that a runnable which slept first would hang the
            // test rather than pass it slowly.
            every: Duration::from_secs(3600),
            batch: 1,
        });

        assert!(feed.run(cx).await.is_ok());
        assert_eq!(ledger.written(), 0, "a round was started while draining");
    }
}
