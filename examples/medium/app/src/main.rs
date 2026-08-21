//! Three features, four bundles, one process.
//!
//! This is the only crate in the example that names more than one feature.
//! Everything above it is coupled through `*-contracts` crates alone:
//! `orders-bundle` places orders against `dyn Ledger` and writes to `dyn Sink`
//! without depending on `ledger-bundle` or `audit-bundle`, and it could not
//! depend on them — `ci/check-bundle-graph.sh` walks the resolved dependency
//! graph and fails on any `*-bundle` → `*-bundle` edge. The rule is a build
//! failure, not a naming convention.
//!
//! What that buys is visible here and nowhere else: replacing a feature means
//! changing one line below. The consumers, which name only the contracts, do
//! not recompile.
//!
//! # Reading order
//!
//! One feature at a time, and each one reads on its own:
//!
//! * `ledger` — a contract, a component that owns a resource, an extension
//!   point read at boot, a health probe.
//! * `orders` — a runnable, a scope per unit of work, three listeners on one
//!   event dispatched before the commit they can veto, a second probe. It
//!   consumes the other two through their traits.
//! * `audit` — one contract bound twice, and an ancillary runnable that fails
//!   on purpose to show a restart.
//!
//! Then [`console`], which is this application's own bundle, and this file.
//!
//! # Running it
//!
//! ```text
//! cargo run -p app
//! ```
//!
//! The desk offers a batch of orders every 500 ms. Each one is dispatched for
//! screening first, and only what the screen allows is appended to the ledger;
//! the audit sinks print what they keep. Press Ctrl-C and the kernel drains,
//! stops and exits 0.
//!
//! Every value the features read comes from the configuration chain below, so
//! any of them can be moved from the environment:
//!
//! ```text
//! MEDIUM_ORDERS__EVERY=100ms MEDIUM_ORDERS__BATCH=5 cargo run -p app
//! MEDIUM_APP__RUN_FOR=3s cargo run -p app     # stops itself, for a scripted run
//! ```
//!
//! The kernel's own records go to standard error, one line per phase
//! transition; everything on standard output is the application and its
//! features talking.

mod console;

use core::time::Duration;
use std::process::ExitCode;
use std::sync::Arc;

use kernel::core::{ConfigError, ConfigNode, ConfigTree, FromConfig, StderrTelemetry};
use kernel::{EnvSource, Kernel, MemorySource};

/// The prefix every environment variable of this application carries.
const PREFIX: &str = "MEDIUM_";

/// How long to run before asking for a stop. Absent by default.
const RUN_FOR: &str = "app.run_for";

/// The values this application ships with.
///
/// A configuration source like any other, listed first so every later source
/// overrides it leaf by leaf.
///
/// Only the ledger's keys are here, and that is a difference between the
/// features rather than an oversight. `ledger` reads its three keys with no
/// fallback of its own: a value missing from this table refuses the build in
/// phase two, naming the path it could not find. `orders` ships defaults in
/// its own code and registers into an application that configured nothing, so
/// listing its keys here would state them twice; this application leaves them
/// alone and overrides one from the environment when it wants to. `audit`
/// reads no configuration at all.
fn defaults() -> MemorySource {
    let mut tree = ConfigTree::empty();
    for (path, node) in [
        ("ledger.batch", ConfigNode::from(4_i64)),
        ("ledger.signing_key", ConfigNode::from("example-key")),
        ("ledger.flush_timeout", ConfigNode::from("2s")),
    ] {
        tree.insert(path, node)
            .expect("the default paths are literals and cannot collide");
    }
    MemorySource::named("defaults", tree)
}

/// How long to run before asking for a stop, if this application was told.
///
/// Read from the loaded tree rather than in a bundle, because it is not a
/// feature's business: it configures `main`, and `main` is the only thing that
/// holds a [`kernel::KernelHandle`] before anything is running.
///
/// # Errors
///
/// Whatever [`Duration`] reports when the value is present and is not one.
fn stop_after(config: &ConfigTree) -> Result<Option<Duration>, ConfigError> {
    match config.get(RUN_FOR) {
        Some(node) => Duration::from_config(node).map(Some),
        None => Ok(None),
    }
}

/// Arms the self-stop, when [`RUN_FOR`] says how long.
///
/// [`Kernel::handle`] hands out the very handle every component and runnable is
/// given, so asking for a stop from out here and asking for one from inside the
/// graph are the same call on the same object. Nothing is armed when the key is
/// absent, and the process then runs until a signal arrives.
///
/// # Errors
///
/// Whatever [`stop_after`] reports.
fn arm_stop(kernel: &Kernel) -> Result<(), ConfigError> {
    let Some(delay) = stop_after(kernel.container().config())? else {
        return Ok(());
    };

    let handle = kernel.handle();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        println!("app: {delay:?} elapsed, asking the kernel to stop");
        handle.shutdown();
    });

    Ok(())
}

/// Builds the kernel, runs it, and reports what happened.
///
/// `?` is not usable here: [`ExitCode`] does not implement `FromResidual`, so
/// each refusal is rendered explicitly instead of propagated.
#[tokio::main]
async fn main() -> ExitCode {
    // Phases one to three: the sources load, the four bundles register, the
    // graph is validated. Nothing is built and no I/O happens. If this returns
    // `Ok`, every contract someone asked for is provided by someone.
    let kernel = match Kernel::builder()
        .telemetry(Arc::new(StderrTelemetry))
        .config_source(defaults())
        .config_source(EnvSource::with_prefix(PREFIX))
        .bundle(ledger_bundle::LedgerBundle::new())
        .bundle(audit_bundle::Bundled)
        .bundle(orders_bundle::Bundled)
        // Last, so its component boots after every component whose probe it
        // reads: ties in the boot order break on registration order.
        .bundle(console::bundle())
        .build()
        .await
    {
        Ok(kernel) => kernel,
        Err(error) => {
            eprintln!("app: refused to start: {error}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(error) = arm_stop(&kernel) {
        eprintln!("app: refused to start: {error}");
        return ExitCode::FAILURE;
    }

    // Phases four to seven: build, boot, run, drain, stop.
    let outcome = kernel.run().await;
    if let Some(error) = outcome.error() {
        eprintln!("app: {error}");
    }
    println!("app: {outcome:?}");
    outcome.into_exit_code()
}

#[cfg(test)]
mod tests {
    use kernel::core::ConfigSource;

    use super::*;

    #[test]
    fn defaults_cover_the_ledger() {
        let tree = defaults().load().expect("a memory source always loads");

        assert!(tree.get("ledger.batch").is_some());
        assert!(tree.get("ledger.signing_key").is_some());
        assert!(tree.get("ledger.flush_timeout").is_some());
    }

    #[test]
    fn absent_delay_reads_none() {
        let delay = stop_after(&ConfigTree::empty()).expect("an absent key is not a failure");

        assert_eq!(delay, None);
    }

    #[test]
    fn delay_reads_a_duration() {
        let mut tree = ConfigTree::empty();
        tree.insert(RUN_FOR, ConfigNode::from("3s"))
            .expect("a literal path");

        let delay = stop_after(&tree).expect("a suffixed string is a duration");

        assert_eq!(delay, Some(Duration::from_secs(3)));
    }

    #[test]
    fn wrong_delay_is_refused() {
        let mut tree = ConfigTree::empty();
        tree.insert(RUN_FOR, ConfigNode::from(true))
            .expect("a literal path");

        assert!(stop_after(&tree).is_err());
    }
}
