//! Test harness for the kernel.
//!
//! # Why substitution lives here and not on `KernelBuilder`
//!
//! [`TestBuilder`] reaches `kernel` through one `#[doc(hidden)]` hook,
//! `KernelBuilder::__register_hook`, gated by `kernel`'s `testing` feature.
//!
//! **What holds.** In a production build — a dependency graph that reaches
//! `kernel` without passing through a dev-dependency on this crate — `testing`
//! is off, `__register_hook` does not exist, and no substitution is reachable
//! at all. `ci/check-testing-feature.sh` fails the build if any workspace
//! member enables `kernel/testing` through a normal dependency, declares it
//! behind a feature of its own, or reaches it by taking this crate as a normal
//! dependency instead of a dev one.
//!
//! **What does not hold.** Inside `cargo test` of a crate that dev-depends on
//! this one, cargo unifies features across the build: `kernel/testing` is on
//! for the whole graph, and any `#[test]` in that crate can call
//! `KernelBuilder::new().__register_hook(...)` with no type of this crate in
//! scope. Rust has no way to scope a feature to one crate's dev graph, so the
//! type system does not hold that boundary and nothing here claims it does.
//! It is acceptable — whoever reaches the hook is writing a test — but going
//! through [`TestBuilder`] is what keeps the substitution in the phase order
//! and in front of the phase-three validation.

pub mod bundle;
pub mod harness;
pub mod log;
pub mod missing;

pub use bundle::FnBundle;
pub use harness::{TestBuilder, TestHarness};
pub use log::EventLog;
pub use missing::missing_contracts;
