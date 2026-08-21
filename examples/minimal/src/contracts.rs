//! What one part of this application is allowed to know about another.
//!
//! In a real application this module is a crate of its own — the thing every
//! other crate depends on, and the thing that depends on nothing but
//! `kernel-core`. Two rules make it worth separating:
//!
//! * a consumer depends on the *contract*, never on the crate that implements
//!   it, so an implementation can be replaced without recompiling its callers;
//! * an extension point is part of the contract too, because a bundle that
//!   contributes to one must be able to name the type without knowing who
//!   declared it.
//!
//! Nothing here belongs to the kernel. `Ledger` and `OpeningNote` are this
//! example's vocabulary; the kernel never learns either name.

use kernel::Extension;

/// Somewhere an order can be written down.
///
/// The whole surface the rest of the application sees. Who implements it, what
/// it writes to and when it is opened are the implementer's business.
pub trait Ledger: Send + Sync + 'static {
    /// Writes one order down and answers with its entry number.
    fn record(&self, order: &str) -> u64;

    /// How many entries have been written so far.
    fn written(&self) -> u64;
}

/// A line a bundle wants written at the top of the ledger.
///
/// The extension point: [`crate::ledger`] declares it and reads it while it
/// boots, [`crate::orders`] contributes to it while it registers, and neither
/// knows of the other. A point that is declared and never contributed to is
/// empty, not missing.
pub struct OpeningNote(pub String);

impl Extension for OpeningNote {}
