//! Optional derives and declaration sugar for the kernel.
//!
//! Nothing here is required. Every macro expands to a public API the user could
//! write by hand, and the whole suite compiles and passes with this crate
//! removed — `ci/check-without-macros.sh` proves it rather than asserting it.
//!
//! # No dependency, by construction
//!
//! This crate parses tokens by hand: no `syn`, no `quote`, no `proc-macro2`.
//! That costs a parser; it is what lets `kernel-core` and `kernel` claim an
//! empty dependency allowlist without the claim being decorative — a macro
//! crate that dragged in a parser would put that parser into every build of
//! every bundle that used one derive.
//!
//! The cost is paid in reach, not in correctness: each macro reads the shapes
//! it documents and refuses everything else with a `compile_error!` that names
//! the construct and points at the hand-written alternative. Generics are the
//! main thing left out.
//!
//! # Where the generated code looks for the kernel
//!
//! Expansions emit absolute paths — `::kernel_core` for the derive, `::kernel`
//! for the two attributes. A crate that renames the dependency, or reaches it
//! through a re-export, says so once:
//!
//! ```
//! use kernel_core::config::{ConfigNode, FromConfig};
//! use kernel_macros::FromConfig;
//!
//! // `kernel` re-exports `kernel_core` as `kernel::core`, so a crate that
//! // depends on the runtime alone still derives.
//! #[derive(FromConfig)]
//! #[config(crate = ::kernel::core)]
//! struct Settings {
//!     depth: u32,
//! }
//!
//! let node = ConfigNode::Map([("depth".to_string(), ConfigNode::from(2_i64))].into());
//! assert_eq!(Settings::from_config(&node).unwrap().depth, 2);
//! ```

mod from_config;
mod listener;
mod parse;
mod provider;

use proc_macro::TokenStream;

/// Derives `kernel_core::FromConfig` for a struct of named fields.
///
/// Each field is read from the node under its own name; `Option<T>` fields
/// accept absence. Expands to the same `impl` a user would write.
///
/// # Reading a field
///
/// A field's key is its identifier, and the node it is read from is the child
/// of the node handed to `from_config` — one level, never a dotted path. The
/// field name is pushed onto the error path, so a failure deep in a nested
/// struct reports the whole path rather than the innermost leaf.
///
/// Absence is decided per field:
///
/// * `Option<T>` yields `None`, whether the key is missing or explicitly null.
/// * `#[config(default)]` yields `Default::default()`.
/// * anything else is a `ConfigError::missing` naming the key.
///
/// # Attributes
///
/// * `#[config(crate = <path>)]`, on the struct: where `kernel_core` lives.
///   Defaults to `::kernel_core`.
/// * `#[config(rename = "...")]`, on a field: the key to read, for keys that
///   are not identifiers.
/// * `#[config(default)]`, on a field: absence falls back to `Default`.
///
/// # What it refuses
///
/// Generic structs, tuple structs, unit structs, enums and unions. Each is a
/// `compile_error!` naming what was found; the `impl` is three lines per field
/// by hand.
///
/// # Examples
///
/// ```
/// use core::time::Duration;
/// use kernel_core::config::{ConfigNode, FromConfig};
/// use kernel_macros::FromConfig;
///
/// #[derive(Debug, FromConfig)]
/// struct Settings {
///     depth: u32,
///     #[config(rename = "max-wait")]
///     max_wait: Option<Duration>,
///     #[config(default)]
///     verbose: bool,
/// }
///
/// let node = ConfigNode::Map(
///     [
///         ("depth".to_string(), ConfigNode::from(3_i64)),
///         ("max-wait".to_string(), ConfigNode::from("250ms")),
///     ]
///     .into(),
/// );
///
/// let settings = Settings::from_config(&node).unwrap();
/// assert_eq!(settings.depth, 3);
/// assert_eq!(settings.max_wait, Some(Duration::from_millis(250)));
/// assert!(!settings.verbose, "absent, and `bool::default()` is false");
///
/// // The field name is on the error path, so a failure names the leaf.
/// let empty = ConfigNode::Map(Default::default());
/// assert_eq!(Settings::from_config(&empty).unwrap_err().path(), "depth");
/// ```
#[proc_macro_derive(FromConfig, attributes(config))]
pub fn derive_from_config(input: TokenStream) -> TokenStream {
    match from_config::expand(input) {
        Ok(stream) => stream,
        Err(error) => error.into_stream(),
    }
}

/// Builds a `Provider` from a constructor, deriving `requires` from its
/// signature.
///
/// This is the one piece of sugar that removes a real hazard rather than
/// keystrokes: `requires` is declarative because Rust offers no introspection,
/// so a hand-written list can drift from what `build` actually resolves.
///
/// The container's debug guard already catches that drift — but at run time,
/// on whichever code path happens to build the provider. Here the parameters
/// *are* what the generated closure resolves, so the two cannot disagree.
///
/// # What it generates
///
/// The constructor is moved inside a function of the same name, visibility and
/// attributes, which now returns the `Provider` binding the contract:
///
/// ```
/// use std::sync::Arc;
///
/// use kernel::core::ContractRef;
/// use kernel::{Lifetime, Provider};
/// use kernel_macros::provider;
///
/// trait Sink: Send + Sync + 'static {}
/// trait Surface: Send + Sync + 'static {}
///
/// struct Layered(Arc<dyn Sink>);
/// impl Surface for Layered {}
///
/// #[provider]
/// async fn surface(sink: Arc<dyn Sink>) -> Arc<dyn Surface> {
///     Arc::new(Layered(sink)) as Arc<dyn Surface>
/// }
///
/// // `surface` is now the function that builds the binding.
/// let built: Provider<dyn Surface> = surface();
/// assert_eq!(built.requires, vec![ContractRef::of::<dyn Sink>()]);
/// assert_eq!(built.lifetime, Lifetime::Shared);
/// ```
///
/// The result is a plain `Provider`, so lifetime and extra requirements are set
/// the usual way: `surface().lifetime(Lifetime::Scoped)`.
///
/// # Parameters
///
/// * `Arc<C>` resolves `C`, and declares `ContractRef::of::<C>()`.
/// * `#[named("...")] Arc<C>` resolves that named binding, and declares
///   `ContractRef::named::<C>("...")`.
/// * `Vec<Arc<C>>` resolves every implementation, and declares
///   `ContractRef::of::<C>()` once — which is what covers the collection.
/// * `&ConfigTree` hands over the configuration, and declares nothing: the
///   tree is not a contract.
///
/// A parameter of any other type is refused. The container itself is refused
/// above all: a provider holding the container can resolve anything, which is
/// the drift this attribute exists to remove.
///
/// # Return type
///
/// `Arc<C>`, or `Result<Arc<C>, E>` when the constructor can fail. `C` is the
/// contract the provider binds. An `E` that is not `BuildError` is wrapped in
/// one, named by the contract being built.
///
/// # What it refuses
///
/// Generic constructors, and any parameter or return type outside the list
/// above. Each is a `compile_error!` naming the offending parameter.
#[proc_macro_attribute]
pub fn provider(args: TokenStream, input: TokenStream) -> TokenStream {
    match provider::expand(args, input) {
        Ok(stream) => stream,
        Err(error) => error.into_stream(),
    }
}

/// Implements `Listener<E>` from a method taking `&mut E`.
///
/// Applied to an inherent `impl` block. Every method in it becomes one
/// `Listener<E>` impl for the block's type; the block itself is emitted
/// unchanged, so the methods stay callable and testable on their own.
///
/// # Handler shape
///
/// ```
/// use kernel::core::error::ListenerError;
/// use kernel::core::{Event, Flow, Priority};
/// use kernel::{Listener, ListenerContext};
/// use kernel_macros::listener;
///
/// struct Started {
///     seen: u32,
/// }
///
/// impl Event for Started {
///     const NAME: &'static str = "example.started";
/// }
///
/// struct Stopping;
///
/// impl Event for Stopping {
///     const NAME: &'static str = "example.stopping";
/// }
///
/// struct Auditor;
///
/// #[listener]
/// impl Auditor {
///     async fn on_started(&self, event: &mut Started) -> Result<Flow, ListenerError> {
///         event.seen += 1;
///         Ok(Flow::Continue)
///     }
///
///     async fn on_stopping(
///         &self,
///         event: &mut Stopping,
///         cx: &ListenerContext<'_>,
///     ) -> Result<Flow, ListenerError> {
///         let _ = (event, cx);
///         Ok(Flow::Stop)
///     }
/// }
///
/// // Both impls exist, which is what registration asks for.
/// fn accepts<E: Event, L: Listener<E>>(_: L, _: Priority) {}
/// accepts::<Started, _>(Auditor, Priority::NORMAL);
/// accepts::<Stopping, _>(Auditor, Priority::NORMAL);
/// ```
///
/// The context parameter is optional; a handler that does not resolve anything
/// leaves it out. Handlers may be `async` or not.
///
/// # Registering one type for several events
///
/// A type carrying two handlers implements `Listener` twice, so
/// `Registry::listen` can no longer infer which event is meant and the event is
/// named at the registration site:
///
/// ```ignore
/// registry.listen::<Started, _>(Auditor, Priority::NORMAL);
/// registry.listen::<Stopping, _>(Auditor, Priority::NORMAL);
/// ```
///
/// That is `listen` being generic over the pair, not the attribute: two
/// hand-written impls need the same turbofish.
///
/// # What it refuses
///
/// A generic or trait `impl` block, and any item in the block that is not a
/// handler — an associated constant, a helper method, a method whose event is
/// taken by shared reference. Those are refused rather than skipped: a helper
/// that quietly failed to become a listener is a listener that never runs, and
/// nothing would report it. Keep helpers in a second, plain `impl` block.
#[proc_macro_attribute]
pub fn listener(args: TokenStream, input: TokenStream) -> TokenStream {
    match listener::expand(args, input) {
        Ok(stream) => stream,
        Err(error) => error.into_stream(),
    }
}
