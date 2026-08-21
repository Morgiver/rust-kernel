//! The smallest application that uses every part of the kernel's public
//! surface honestly.
//!
//! Nothing in this crate belongs to the kernel. `Ledger`, `Book`, `Feed`,
//! `OpeningNote` — every one of these is application vocabulary, and this
//! directory is the one place in the repository where such a name is correct
//! rather than a defect. `ci/check_vocabulary.py` skips `examples/` for that
//! reason, and only for that reason.
//!
//! # What it shows
//!
//! * [`contracts`] — a contract in a module of its own, and an extension point
//!   next to it.
//! * [`ledger`] — typed configuration read in `register`, a component that
//!   owns a resource and boots it, an extension point declared and read.
//! * [`orders`] — a runnable that does periodic work and returns when the
//!   shutdown token fires, a contribution to a point it did not declare, and a
//!   listener on a kernel lifecycle event.
//! * this file — a `main` that builds, runs, and turns the outcome into an
//!   exit code.
//!
//! # Running it
//!
//! ```text
//! cargo run -p minimal
//! ```
//!
//! It writes a batch of entries every 500 ms and keeps going until it is asked
//! to stop. Press Ctrl-C: the listener reports the reason, the feed returns on
//! the token, the component closes its log, and the process exits 0.
//!
//! Every default can be overridden from the environment, because the builder
//! reads an [`EnvSource`] after the built-in defaults:
//!
//! ```text
//! MINIMAL_ORDERS__EVERY=100ms MINIMAL_ORDERS__BATCH=5 cargo run -p minimal
//! ```
//!
//! # What it does not show
//!
//! Health probes, named bindings, scoped lifetimes, runnable restart policies
//! and the test harness are all public and all absent here: each would add a
//! type without adding a phase, and this file is meant to be read in one
//! sitting. They are exercised in the kernel's own test suites.

mod contracts;
mod ledger;
mod orders;

use std::process::ExitCode;
use std::sync::Arc;

use kernel::core::{ConfigNode, ConfigTree, StderrTelemetry};
use kernel::{EnvSource, Kernel, MemorySource};

/// The values the application ships with.
///
/// A configuration source like any other, listed first so that every later
/// source overrides it leaf by leaf.
///
/// It is the ONE place a default is written. The readers in [`crate::ledger`]
/// and [`crate::orders`] have no fallback of their own: a second copy there
/// would win whenever a whole prefix is absent — [`kernel::Registry::config`]
/// hands a null node over rather than refusing — and the two copies could
/// disagree without anything saying so. Remove an entry from this table and
/// the bundle that reads it refuses to register, so the build stops in phase
/// two with the path it could not find.
fn defaults() -> MemorySource {
    let mut tree = ConfigTree::empty();
    for (path, node) in [
        ("ledger.capacity", ConfigNode::from(64_i64)),
        ("orders.every", ConfigNode::from("500ms")),
        ("orders.batch", ConfigNode::from(2_i64)),
    ] {
        tree.insert(path, node)
            .expect("the default paths are literals and cannot collide");
    }
    MemorySource::named("defaults", tree)
}

/// Builds the kernel, runs it, and reports what happened.
///
/// `?` is not usable here: [`ExitCode`] does not implement `FromResidual`, so
/// a refused build is rendered explicitly instead of propagated.
#[tokio::main]
async fn main() -> ExitCode {
    println!("minimal: starting — press Ctrl-C to stop");

    // Phases one to three: configuration is loaded, the bundles register, the
    // graph is validated. No I/O, nothing instantiated. If this returns `Ok`,
    // every contract is satisfied.
    let kernel = match Kernel::builder()
        .telemetry(Arc::new(StderrTelemetry))
        .config_source(defaults())
        .config_source(EnvSource::with_prefix("MINIMAL_"))
        .bundle(ledger::Bundled)
        .bundle(orders::Bundled)
        .build()
        .await
    {
        Ok(kernel) => kernel,
        Err(error) => {
            eprintln!("minimal: refused to start: {error}");
            return ExitCode::FAILURE;
        }
    };

    // Phases four to seven: build, boot, run, drain, stop.
    let outcome = kernel.run().await;
    if let Some(error) = outcome.error() {
        eprintln!("minimal: {error}");
    }
    println!("minimal: {outcome:?}");
    outcome.into_exit_code()
}
