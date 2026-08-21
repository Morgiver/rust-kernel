//! A bundle assembled from a closure, for tests that need one without a crate.

use kernel::{Bundle, Registry};
use kernel_core::{BundleManifest, RegisterError};

/// Version a closure-built bundle reports until a test states another one.
///
/// A test bundle is not distributed, so it has no release to name; saying so
/// with a zero version is more honest than borrowing the workspace's.
const UNVERSIONED: &str = "0.0.0";

/// The registration pass, as this bundle keeps it.
///
/// `Fn` rather than `FnOnce`: [`Bundle::register`] takes `&self`, and the same
/// bundle value may be registered by two kernels in one test.
type RegisterFn = Box<dyn Fn(&mut Registry) -> Result<(), RegisterError> + Send + Sync>;

/// A [`Bundle`] whose `register` is a closure.
///
/// Lets a test declare two bindings and a listener without standing up a
/// distribution crate for them.
pub struct FnBundle {
    /// What this bundle reports in phase two and phase three.
    manifest: BundleManifest,
    /// What it writes into the registry when the kernel asks.
    register: RegisterFn,
}

impl FnBundle {
    /// A bundle with the given name that runs `register` when the kernel asks.
    pub fn new<F>(name: &'static str, register: F) -> Self
    where
        F: Fn(&mut Registry) -> Result<(), RegisterError> + Send + Sync + 'static,
    {
        Self {
            manifest: BundleManifest::new(name, UNVERSIONED),
            register: Box::new(register),
        }
    }

    /// Overrides the manifest this bundle reports.
    ///
    /// The default manifest declares no requirement and no ordering, which is
    /// what most tests want; a test exercising phase three's manifest checks
    /// needs to state a false one on purpose.
    #[must_use]
    pub fn manifest(mut self, manifest: BundleManifest) -> Self {
        self.manifest = manifest;
        self
    }
}

impl Bundle for FnBundle {
    fn manifest(&self) -> BundleManifest {
        self.manifest
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        (self.register)(registry)
    }
}
