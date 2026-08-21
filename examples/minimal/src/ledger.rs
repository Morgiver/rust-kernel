//! The bundle that owns the ledger.
//!
//! It shows three things at once: typed configuration read during `register`,
//! a component that owns a resource and opens it in `boot`, and an extension
//! point declared here and read here — but contributed to somewhere else.
//!
//! The component is bound twice: once under its own type, which is what makes
//! the kernel drive its lifecycle, and once behind [`Ledger`], which is what
//! the rest of the application resolves. Both bindings answer with the same
//! `Arc`, so the object the kernel booted is the object the callers get.

use core::time::Duration;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use kernel::core::{
    BuildError, BundleManifest, ComponentDescriptor, ComponentError, ConfigError, ConfigNode,
    FromConfig, RegisterError,
};
use kernel::{
    BootContext, BoxFuture, Bundle, Component, ContractRef, Provider, Registry, ShutdownContext,
};

use crate::contracts::{Ledger, OpeningNote};

/// The name this bundle publishes, and the name every diagnostic blames.
const NAME: &str = "ledger";

/// What the ledger reads out of the configuration tree.
///
/// Read once, in `register`, before anything exists. A tree that does not hold
/// what this needs fails the build, which is the moment a misconfiguration
/// costs nothing.
struct Settings {
    /// How many entries the log is sized for up front.
    capacity: usize,
}

impl FromConfig for Settings {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        Ok(Self {
            capacity: node.field("capacity")?,
        })
    }
}

/// The one implementation of [`Ledger`], and a component of its own.
///
/// It owns a resource — the log — and the kernel guarantees when that resource
/// is opened and when it is closed. In a real application the same shape holds
/// a connection pool or a file handle.
struct Book {
    /// The size `boot` reserves.
    capacity: usize,
    /// The resource itself. Empty until `boot` opens it.
    entries: Mutex<Vec<String>>,
    /// Entry numbers handed out so far.
    written: AtomicU64,
}

impl Book {
    /// A closed book. Nothing is allocated until it boots.
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Mutex::new(Vec::new()),
            written: AtomicU64::new(0),
        }
    }

    /// The log, whatever a previous panic did to the lock.
    fn log(&self) -> std::sync::MutexGuard<'_, Vec<String>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Component for Book {
    fn name() -> &'static str {
        "book"
    }

    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new().boot_timeout(Duration::from_secs(2))
    }

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            let mut log = self.log();
            log.reserve(self.capacity);

            // The contributions to the point this bundle declared. Whoever
            // wrote them is not known here, and does not need to be.
            for note in cx.collect::<OpeningNote>() {
                println!("[book] opening note: {}", note.0);
                log.push(note.0.clone());
            }

            println!("[book] open, room for {} entries", self.capacity);
            Ok(())
        })
    }

    fn shutdown<'a>(
        &'a self,
        _cx: &'a ShutdownContext<'a>,
    ) -> BoxFuture<'a, Result<(), ComponentError>> {
        Box::pin(async move {
            println!("[book] closed, {} lines held", self.log().len());
            Ok(())
        })
    }
}

impl Ledger for Book {
    fn record(&self, order: &str) -> u64 {
        let number = self.written.fetch_add(1, Ordering::Relaxed) + 1;
        self.log().push(format!("#{number} {order}"));
        number
    }

    fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
}

/// Registers the ledger: one point, one component, one contract.
pub struct Bundled;

impl Bundle for Bundled {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new(NAME, "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        let settings: Settings = registry
            .config(NAME)
            .map_err(|error| RegisterError::new(NAME, Box::new(error)))?;

        // Declared here; anyone may contribute. Contributing to a point nobody
        // declared is a graph error, which is why the declaration comes first.
        registry.declare_extension_point::<OpeningNote>();

        // Registered as a component: the kernel boots it and stops it.
        registry.component(Provider::from_value(Arc::new(Book::new(settings.capacity))));

        // And offered behind the contract, resolving to that same component.
        // The requirement is declared, not inferred: the container checks it.
        registry.provide::<dyn Ledger>(
            Provider::from_fn(|container| {
                Box::pin(async move {
                    let book = container
                        .get::<Book>()
                        .await
                        .map_err(|error| BuildError::new("Ledger", Box::new(error)))?;
                    Ok(book as Arc<dyn Ledger>)
                })
            })
            .requires([ContractRef::of::<Book>()]),
        );

        Ok(())
    }
}
