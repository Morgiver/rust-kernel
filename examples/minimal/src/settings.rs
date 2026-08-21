//! Reading one field out of a configuration node.
//!
//! Every hand-written [`FromConfig`] for a struct needs this, and it is not
//! public: a [`ConfigError`] can be *built* with a path but not *re-rooted*
//! under one, so a failure raised while reading a nested field arrives
//! reporting the parent's path and nothing else — `config `orders`: expected
//! int, found string`, where the reader needed `config `orders.batch``.
//!
//! Both `kernel-core` and `kernel` already carry this function, privately and
//! twice. It sits here because the public surface offers no way to reach
//! either copy.
//!
//! # There is no default here, and that is the point
//!
//! [`field`] has no fallback: an absent key is a failure. This application
//! states each default exactly once, in `defaults()` in `main.rs`, as a
//! configuration source listed first. A second copy written into the reader
//! would be reachable and would win in one case only — an absent prefix, for
//! which [`kernel::Registry::config`] hands the reader a null node instead of
//! refusing — so the two copies could disagree and the disagreement would
//! never be reported. One copy cannot.
//!
//! What that costs: a missing default no longer degrades quietly to a second
//! set of values, it refuses the build in phase two — `config `orders.batch`:
//! missing required value` for a dropped entry, `config `orders`: missing
//! required value` for the whole source. That is before anything is
//! instantiated, which is the cheapest place to find out.
//!
//! # The one error this cannot re-root
//!
//! [`ConfigErrorKind::Source`] carries a foreign cause — whatever a source
//! handed over — and a `ConfigError` cannot be rebuilt around one. Such an
//! error therefore passes through [`under`] untouched, keeping whatever path
//! it was raised with: for a failure under `orders.batch` the message names
//! `orders`, or nothing, rather than the leaf. Every other kind is re-rooted.
//! No source in this example produces one; a source that parses a file would.

use kernel::core::{ConfigError, ConfigErrorKind, ConfigNode, FromConfig};

/// Reads `key` out of `node`.
///
/// A failure reports `key` as part of its path, so the message names the leaf
/// that was wrong rather than the struct that contained it.
///
/// # Errors
///
/// [`ConfigErrorKind::Missing`] when `key` is absent — the defaults source is
/// what makes it present — and whatever `T` reports when the value is there
/// but wrong.
pub fn field<T: FromConfig>(node: &ConfigNode, key: &str) -> Result<T, ConfigError> {
    match node.get(key) {
        Some(value) => T::from_config(value).map_err(|error| under(key, error)),
        None => Err(ConfigError::missing(key)),
    }
}

/// Re-roots `error` under `segment`.
///
/// A [`ConfigErrorKind::Source`] carries a foreign cause that cannot be
/// rebuilt, so it passes through untouched. See the module documentation for
/// what that means for the message the operator reads.
fn under(segment: &str, error: ConfigError) -> ConfigError {
    let path = if error.path().is_empty() {
        segment.to_owned()
    } else {
        format!("{segment}.{}", error.path())
    };
    let rebuilt = match error.kind() {
        ConfigErrorKind::Missing => Some(ConfigError::missing(path)),
        ConfigErrorKind::TypeMismatch { expected, found } => {
            Some(ConfigError::type_mismatch(path, expected, found))
        }
        ConfigErrorKind::Invalid(detail) => Some(ConfigError::invalid(path, detail.clone())),
        ConfigErrorKind::Source(_) => None,
    };
    rebuilt.unwrap_or(error)
}
