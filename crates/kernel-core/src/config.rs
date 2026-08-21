//! Configuration tree, sources and typed extraction.
//!
//! The kernel imposes no configuration format. A [`ConfigSource`] produces a
//! [`ConfigTree`] — a plain tree of maps, sequences and scalars — and trees are
//! merged **by leaf**: overriding one key never removes its siblings. Typed
//! values are obtained through [`FromConfig`], which is deliberately a local
//! trait so that no serialization library leaks into contract crates.
//!
//! [`Secret`] wraps a value whose `Debug` and `Display` are redacted.

use core::fmt;
use core::time::Duration;
use std::collections::BTreeMap;

use crate::error::{ConfigError, ConfigErrorKind};

/// A terminal configuration value.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    /// An explicitly absent value.
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed 64-bit integer. Narrower targets are range-checked on read.
    Int(i64),
    /// A double precision float.
    Float(f64),
    /// A string.
    Str(String),
}

impl Scalar {
    /// Name of the scalar kind, used in type-mismatch errors.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Scalar::Null => "null",
            Scalar::Bool(_) => "bool",
            Scalar::Int(_) => "int",
            Scalar::Float(_) => "float",
            Scalar::Str(_) => "string",
        }
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Null => f.write_str("null"),
            Scalar::Bool(v) => write!(f, "{v}"),
            Scalar::Int(v) => write!(f, "{v}"),
            Scalar::Float(v) => write!(f, "{v}"),
            Scalar::Str(v) => f.write_str(v),
        }
    }
}

/// A node of the configuration tree.
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigNode {
    /// A terminal value.
    Scalar(Scalar),
    /// An ordered sequence, addressed by numeric path segment.
    Seq(Vec<ConfigNode>),
    /// A keyed map, addressed by name and merged key by key.
    Map(BTreeMap<String, ConfigNode>),
}

impl ConfigNode {
    /// Builds a null node.
    pub fn null() -> Self {
        ConfigNode::Scalar(Scalar::Null)
    }

    /// Builds an empty map node.
    pub fn map() -> Self {
        ConfigNode::Map(BTreeMap::new())
    }

    /// Resolves a dotted path against this node.
    ///
    /// Map levels are addressed by key, sequence levels by decimal index, so
    /// `"outer.2.leaf"` reads the `leaf` key of the third element of `outer`.
    /// An empty path yields the node itself.
    ///
    /// ```
    /// use kernel_core::config::{ConfigNode, Scalar};
    ///
    /// let node = ConfigNode::Seq(vec![ConfigNode::Scalar(Scalar::Int(7))]);
    /// assert_eq!(node.get("0"), Some(&ConfigNode::Scalar(Scalar::Int(7))));
    /// assert_eq!(node.get("1"), None);
    /// ```
    pub fn get(&self, path: &str) -> Option<&ConfigNode> {
        let mut current = self;
        for segment in split_path(path) {
            current = current.child(segment)?;
        }
        Some(current)
    }

    /// Resolves a single path segment.
    fn child(&self, segment: &str) -> Option<&ConfigNode> {
        match self {
            ConfigNode::Map(entries) => entries.get(segment),
            ConfigNode::Seq(items) => segment.parse::<usize>().ok().and_then(|i| items.get(i)),
            ConfigNode::Scalar(_) => None,
        }
    }

    /// Reads this node as a boolean.
    ///
    /// The error carries an empty path: the caller knows where the node came
    /// from and prefixes it.
    pub fn as_bool(&self) -> Result<bool, ConfigError> {
        match self {
            ConfigNode::Scalar(Scalar::Bool(v)) => Ok(*v),
            other => Err(mismatch("bool", other)),
        }
    }

    /// Reads this node as a signed 64-bit integer.
    pub fn as_i64(&self) -> Result<i64, ConfigError> {
        match self {
            ConfigNode::Scalar(Scalar::Int(v)) => Ok(*v),
            other => Err(mismatch("int", other)),
        }
    }

    /// Reads this node as a float. An integer node widens to a float.
    pub fn as_f64(&self) -> Result<f64, ConfigError> {
        match self {
            ConfigNode::Scalar(Scalar::Float(v)) => Ok(*v),
            ConfigNode::Scalar(Scalar::Int(v)) => Ok(*v as f64),
            other => Err(mismatch("float", other)),
        }
    }

    /// Reads this node as a string slice.
    pub fn as_str(&self) -> Result<&str, ConfigError> {
        match self {
            ConfigNode::Scalar(Scalar::Str(v)) => Ok(v.as_str()),
            other => Err(mismatch("string", other)),
        }
    }

    /// Name of the node kind, used in type-mismatch errors.
    pub fn kind_name(&self) -> &'static str {
        match self {
            ConfigNode::Scalar(scalar) => scalar.kind_name(),
            ConfigNode::Seq(_) => "sequence",
            ConfigNode::Map(_) => "map",
        }
    }

    /// True when the node is an explicit null.
    pub fn is_null(&self) -> bool {
        matches!(self, ConfigNode::Scalar(Scalar::Null))
    }

    /// Merges `other` into `self`, leaf by leaf.
    fn merge_node(&mut self, other: ConfigNode) {
        match (self, other) {
            (ConfigNode::Map(target), ConfigNode::Map(incoming)) => {
                for (key, value) in incoming {
                    match target.get_mut(&key) {
                        Some(existing) => existing.merge_node(value),
                        None => {
                            target.insert(key, value);
                        }
                    }
                }
            }
            (target, incoming) => *target = incoming,
        }
    }
}

impl fmt::Display for ConfigNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigNode::Scalar(scalar) => write!(f, "{scalar}"),
            ConfigNode::Seq(_) | ConfigNode::Map(_) => f.write_str(self.kind_name()),
        }
    }
}

/// A whole configuration, rooted at a single node.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigTree {
    root: ConfigNode,
}

impl ConfigTree {
    /// Builds a tree whose root is an empty map.
    pub fn empty() -> Self {
        ConfigTree {
            root: ConfigNode::map(),
        }
    }

    /// Builds a tree from an existing node.
    pub fn from_node(node: ConfigNode) -> Self {
        ConfigTree { root: node }
    }

    /// Borrows the root node.
    pub fn root(&self) -> &ConfigNode {
        &self.root
    }

    /// Consumes the tree and yields its root node.
    pub fn into_root(self) -> ConfigNode {
        self.root
    }

    /// Resolves a dotted path from the root. See [`ConfigNode::get`].
    pub fn get(&self, path: &str) -> Option<&ConfigNode> {
        self.root.get(path)
    }

    /// Merges `other` on top of `self`, leaf by leaf.
    ///
    /// Maps recurse, so overriding one key leaves its siblings untouched.
    /// Scalars and sequences are replaced wholesale by the incoming value: a
    /// sequence is a value, not a container to be zipped.
    ///
    /// ```
    /// use kernel_core::config::{ConfigNode, ConfigTree};
    ///
    /// let mut base = ConfigTree::empty();
    /// base.insert("outer.first", ConfigNode::from(1_i64)).unwrap();
    /// base.insert("outer.second", ConfigNode::from(2_i64)).unwrap();
    ///
    /// let mut over = ConfigTree::empty();
    /// over.insert("outer.first", ConfigNode::from(9_i64)).unwrap();
    /// base.merge(over);
    ///
    /// assert_eq!(base.get("outer.first"), Some(&ConfigNode::from(9_i64)));
    /// assert_eq!(base.get("outer.second"), Some(&ConfigNode::from(2_i64)));
    /// ```
    pub fn merge(&mut self, other: ConfigTree) {
        self.root.merge_node(other.root);
    }

    /// Inserts a node at a dotted path, without destroying what it walks past.
    ///
    /// Map levels are addressed by key, sequence levels by decimal index. A
    /// key that no intermediate map holds yet is created as an empty map; an
    /// in-range index addresses the element already there. The last segment
    /// overwrites whatever it names, in a map or in an in-range sequence slot:
    /// that write is the point of the call. An empty path replaces the root.
    ///
    /// Nothing else is ever replaced. A path that walks into a scalar, or into
    /// a sequence through a segment that is not one of its indices, fails and
    /// leaves the tree unchanged.
    ///
    /// ```
    /// use kernel_core::config::{ConfigNode, ConfigTree};
    ///
    /// let mut tree = ConfigTree::empty();
    /// tree.insert("items", ConfigNode::Seq(vec![ConfigNode::from(1_i64)])).unwrap();
    /// assert!(tree.insert("items.5.leaf", ConfigNode::from(9_i64)).is_err());
    /// assert_eq!(tree.get("items.0"), Some(&ConfigNode::from(1_i64)));
    /// ```
    ///
    /// # Errors
    ///
    /// [`ConfigErrorKind::Invalid`] when a segment addresses a sequence
    /// through a non-numeric or out-of-range index, located at the path that
    /// addresses nothing; [`ConfigErrorKind::TypeMismatch`] when a segment
    /// walks into a scalar, located at the scalar itself.
    pub fn insert(&mut self, path: &str, node: ConfigNode) -> Result<(), ConfigError> {
        let segments: Vec<&str> = split_path(path).collect();
        let Some((last, parents)) = segments.split_last() else {
            self.root = node;
            return Ok(());
        };
        let mut walked = String::new();
        let mut current = &mut self.root;
        for segment in parents {
            current = descend(current, segment, &walked)?;
            push_segment(&mut walked, segment);
        }
        match current {
            ConfigNode::Map(entries) => {
                entries.insert((*last).to_string(), node);
                Ok(())
            }
            ConfigNode::Seq(items) => match last.parse::<usize>() {
                Ok(index) if index < items.len() => {
                    items[index] = node;
                    Ok(())
                }
                _ => Err(no_such_element(&walked, last, items.len())),
            },
            scalar => Err(ConfigError::type_mismatch(
                walked,
                "map",
                scalar.kind_name(),
            )),
        }
    }
}

impl Default for ConfigTree {
    fn default() -> Self {
        ConfigTree::empty()
    }
}

/// Walks into `segment`, creating a missing map key and refusing anything else.
///
/// `walked` is the dotted path of `node` itself, reported in errors.
fn descend<'a>(
    node: &'a mut ConfigNode,
    segment: &str,
    walked: &str,
) -> Result<&'a mut ConfigNode, ConfigError> {
    match node {
        ConfigNode::Map(entries) => Ok(entries
            .entry(segment.to_string())
            .or_insert_with(ConfigNode::map)),
        ConfigNode::Seq(items) => {
            let len = items.len();
            match segment.parse::<usize>() {
                Ok(index) if index < len => Ok(&mut items[index]),
                _ => Err(no_such_element(walked, segment, len)),
            }
        }
        scalar => Err(ConfigError::type_mismatch(
            walked,
            "map",
            scalar.kind_name(),
        )),
    }
}

/// Appends one segment to the dotted path walked so far.
fn push_segment(walked: &mut String, segment: &str) {
    if !walked.is_empty() {
        walked.push('.');
    }
    walked.push_str(segment);
}

/// Builds the error for a segment that addresses no element of a sequence.
///
/// `walked` is the path of the sequence; the error is located at the segment.
fn no_such_element(walked: &str, segment: &str, len: usize) -> ConfigError {
    let mut path = walked.to_string();
    push_segment(&mut path, segment);
    ConfigError::invalid(
        path,
        format!("`{segment}` is not an index into a sequence of {len}"),
    )
}

/// Splits a dotted path, yielding nothing for the empty path.
fn split_path(path: &str) -> impl Iterator<Item = &str> {
    path.split('.').filter(|segment| !segment.is_empty())
}

/// Builds a path-less type-mismatch error for `node`.
fn mismatch(expected: &'static str, node: &ConfigNode) -> ConfigError {
    ConfigError::type_mismatch("", expected, node.kind_name())
}

/// Re-roots `error` under `segment`, so nested failures report where they are.
///
/// A [`ConfigErrorKind::Source`] error carries a foreign cause that cannot be
/// rebuilt, so it is passed through unchanged.
fn under(segment: &str, error: ConfigError) -> ConfigError {
    let path = if error.path().is_empty() {
        segment.to_string()
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

/// A provider of configuration, in whatever format it likes.
///
/// Sources are loaded in declaration order and merged leaf by leaf, so a later
/// source overrides only the leaves it actually defines.
pub trait ConfigSource: Send + Sync + 'static {
    /// Stable name of the source, used in errors and diagnostics.
    fn name(&self) -> &'static str;

    /// Loads the whole tree this source provides.
    fn load(&self) -> Result<ConfigTree, ConfigError>;
}

/// A type that can be built from a configuration node.
///
/// Implementations report failures with a path-relative to the node they were
/// given; the caller prefixes the absolute path.
///
/// ```
/// use kernel_core::config::{ConfigNode, FromConfig};
/// use kernel_core::error::ConfigError;
///
/// struct Threshold(u16);
///
/// impl FromConfig for Threshold {
///     fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
///         Ok(Threshold(u16::from_config(node)?))
///     }
/// }
///
/// assert!(Threshold::from_config(&ConfigNode::from(70_000_i64)).is_err());
/// ```
pub trait FromConfig: Sized {
    /// Builds `Self` from `node`.
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError>;
}

impl FromConfig for bool {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        node.as_bool()
    }
}

impl FromConfig for f64 {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        node.as_f64()
    }
}

impl FromConfig for f32 {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        let value = node.as_f64()?;
        let narrowed = value as f32;
        if narrowed.is_finite() || !value.is_finite() {
            Ok(narrowed)
        } else {
            Err(ConfigError::invalid(
                "",
                format!("value {value} is out of range for f32"),
            ))
        }
    }
}

impl FromConfig for String {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        Ok(node.as_str()?.to_string())
    }
}

impl FromConfig for ConfigNode {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        Ok(node.clone())
    }
}

/// Implements [`FromConfig`] for an integer type, range-checking the value.
macro_rules! impl_from_config_int {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl FromConfig for $ty {
                fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
                    let raw = node.as_i64()?;
                    <$ty>::try_from(raw).map_err(|_| {
                        ConfigError::invalid(
                            "",
                            format!(
                                concat!("value {} is out of range for ", stringify!($ty)),
                                raw
                            ),
                        )
                    })
                }
            }
        )+
    };
}

impl_from_config_int!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

impl<T: FromConfig> FromConfig for Option<T> {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        if node.is_null() {
            Ok(None)
        } else {
            T::from_config(node).map(Some)
        }
    }
}

impl<T: FromConfig> FromConfig for Vec<T> {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        match node {
            ConfigNode::Seq(items) => {
                let mut out = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    out.push(T::from_config(item).map_err(|e| under(&index.to_string(), e))?);
                }
                Ok(out)
            }
            other => Err(mismatch("sequence", other)),
        }
    }
}

impl<T: FromConfig> FromConfig for BTreeMap<String, T> {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        match node {
            ConfigNode::Map(entries) => {
                let mut out = BTreeMap::new();
                for (key, value) in entries {
                    let parsed = T::from_config(value).map_err(|e| under(key, e))?;
                    out.insert(key.clone(), parsed);
                }
                Ok(out)
            }
            other => Err(mismatch("map", other)),
        }
    }
}

impl<T: FromConfig> FromConfig for Secret<T> {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        T::from_config(node).map(Secret::new)
    }
}

/// Nanoseconds per unit accepted in a duration string.
const DURATION_UNITS: &[(&str, i128)] = &[
    ("ns", 1),
    ("us", 1_000),
    ("\u{b5}s", 1_000),
    ("ms", 1_000_000),
    ("s", 1_000_000_000),
    ("m", 60_000_000_000),
    ("min", 60_000_000_000),
    ("h", 3_600_000_000_000),
    ("d", 86_400_000_000_000),
];

impl FromConfig for Duration {
    /// Accepts a plain number of seconds, or a suffixed string such as
    /// `"10s"`, `"500ms"` or `"2m"`.
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError> {
        match node {
            ConfigNode::Scalar(Scalar::Int(secs)) => u64::try_from(*secs)
                .map(Duration::from_secs)
                .map_err(|_| ConfigError::invalid("", format!("negative duration: {secs}"))),
            ConfigNode::Scalar(Scalar::Float(secs)) => Duration::try_from_secs_f64(*secs)
                .map_err(|_| ConfigError::invalid("", format!("invalid duration: {secs}"))),
            ConfigNode::Scalar(Scalar::Str(text)) => parse_duration(text),
            other => Err(mismatch("duration", other)),
        }
    }
}

/// Parses a duration string: a number, optionally followed by a unit suffix.
fn parse_duration(text: &str) -> Result<Duration, ConfigError> {
    let trimmed = text.trim();
    let split = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '+' || c == '-'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split);
    let suffix = suffix.trim();
    let invalid = || ConfigError::invalid("", format!("invalid duration: {text}"));

    let nanos_per_unit = if suffix.is_empty() {
        1_000_000_000
    } else {
        DURATION_UNITS
            .iter()
            .find(|(unit, _)| *unit == suffix)
            .map(|(_, nanos)| *nanos)
            .ok_or_else(invalid)?
    };

    if number.contains('.') {
        let value: f64 = number.parse().map_err(|_| invalid())?;
        if !value.is_finite() || value < 0.0 {
            return Err(invalid());
        }
        let seconds = value * (nanos_per_unit as f64) / 1e9;
        return Duration::try_from_secs_f64(seconds).map_err(|_| invalid());
    }

    let value: i128 = number.parse().map_err(|_| invalid())?;
    let total = value.checked_mul(nanos_per_unit).ok_or_else(invalid)?;
    if total < 0 {
        return Err(invalid());
    }
    let secs = u64::try_from(total / 1_000_000_000).map_err(|_| invalid())?;
    let sub = (total % 1_000_000_000) as u32;
    Ok(Duration::new(secs, sub))
}

/// A value that must never reach a log.
///
/// Both `Debug` and `Display` print `<redacted>` and nothing else; reading the
/// value takes an explicitly named call.
///
/// ```
/// use kernel_core::config::Secret;
///
/// let held = Secret::new(String::from("value-that-must-not-leak"));
/// assert_eq!(format!("{held:?}"), "<redacted>");
/// assert_eq!(format!("{held}"), "<redacted>");
/// assert_eq!(held.expose(), "value-that-must-not-leak");
/// ```
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Wraps a value.
    pub fn new(v: T) -> Self {
        Secret(v)
    }

    /// Borrows the wrapped value. Named to be visible at the call site.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Consumes the wrapper and yields the value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for Secret<T> {
    fn from(value: T) -> Self {
        Secret(value)
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// The only text a [`Secret`] ever renders.
const REDACTED: &str = "<redacted>";

impl From<bool> for ConfigNode {
    fn from(value: bool) -> Self {
        ConfigNode::Scalar(Scalar::Bool(value))
    }
}

impl From<i64> for ConfigNode {
    fn from(value: i64) -> Self {
        ConfigNode::Scalar(Scalar::Int(value))
    }
}

impl From<f64> for ConfigNode {
    fn from(value: f64) -> Self {
        ConfigNode::Scalar(Scalar::Float(value))
    }
}

impl From<String> for ConfigNode {
    fn from(value: String) -> Self {
        ConfigNode::Scalar(Scalar::Str(value))
    }
}

impl From<&str> for ConfigNode {
    fn from(value: &str) -> Self {
        ConfigNode::Scalar(Scalar::Str(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(pairs: &[(&str, ConfigNode)]) -> ConfigTree {
        let mut out = ConfigTree::empty();
        for (path, node) in pairs {
            out.insert(path, node.clone()).expect("fixture insert");
        }
        out
    }

    #[test]
    fn walks_dotted_path() {
        let t = tree(&[("outer.inner.leaf", ConfigNode::from("value"))]);
        assert_eq!(t.get("outer.inner.leaf"), Some(&ConfigNode::from("value")));
        assert_eq!(t.get("outer.inner"), t.root().get("outer.inner"));
        assert_eq!(t.get("outer.absent"), None);
        assert_eq!(t.get("outer.inner.leaf.deeper"), None);
        assert_eq!(t.get(""), Some(t.root()));
    }

    #[test]
    fn indexes_sequences() {
        let node = ConfigNode::Seq(vec![
            tree(&[("port", ConfigNode::from(1_i64))]).into_root(),
            tree(&[("port", ConfigNode::from(2_i64))]).into_root(),
        ]);
        let t = ConfigTree::from_node(ConfigNode::Map(BTreeMap::from([(
            "units".to_string(),
            node,
        )])));
        assert_eq!(t.get("units.1.port"), Some(&ConfigNode::from(2_i64)));
        assert_eq!(t.get("units.2.port"), None);
        assert_eq!(t.get("units.x"), None);
    }

    #[test]
    fn merge_keeps_siblings() {
        let mut base = tree(&[
            ("one.two.three", ConfigNode::from(3_i64)),
            ("one.two.sibling", ConfigNode::from(4_i64)),
            ("one.uncle", ConfigNode::from(5_i64)),
            ("root", ConfigNode::from(6_i64)),
        ]);
        base.merge(tree(&[("one.two.three", ConfigNode::from(99_i64))]));

        assert_eq!(base.get("one.two.three"), Some(&ConfigNode::from(99_i64)));
        assert_eq!(base.get("one.two.sibling"), Some(&ConfigNode::from(4_i64)));
        assert_eq!(base.get("one.uncle"), Some(&ConfigNode::from(5_i64)));
        assert_eq!(base.get("root"), Some(&ConfigNode::from(6_i64)));
    }

    #[test]
    fn merge_replaces_leaves() {
        let mut base = ConfigTree::empty();
        base.insert(
            "items",
            ConfigNode::Seq(vec![ConfigNode::from(1_i64), ConfigNode::from(2_i64)]),
        )
        .expect("insert");
        base.insert("flag", ConfigNode::from(true)).expect("insert");

        let mut over = ConfigTree::empty();
        over.insert("items", ConfigNode::Seq(vec![ConfigNode::from(9_i64)]))
            .expect("insert");
        base.merge(over);

        assert_eq!(
            base.get("items"),
            Some(&ConfigNode::Seq(vec![ConfigNode::from(9_i64)]))
        );
        assert_eq!(base.get("flag"), Some(&ConfigNode::from(true)));
    }

    #[test]
    fn merge_adds_new_keys() {
        let mut base = tree(&[("a.b", ConfigNode::from(1_i64))]);
        base.merge(tree(&[("a.c", ConfigNode::from(2_i64))]));
        assert_eq!(base.get("a.b"), Some(&ConfigNode::from(1_i64)));
        assert_eq!(base.get("a.c"), Some(&ConfigNode::from(2_i64)));
    }

    fn with_items(items: Vec<ConfigNode>) -> ConfigTree {
        ConfigTree::from_node(ConfigNode::Map(BTreeMap::from([(
            "items".to_string(),
            ConfigNode::Seq(items),
        )])))
    }

    #[test]
    fn insert_creates_maps() {
        let mut t = ConfigTree::empty();
        t.insert("a.b.c", ConfigNode::from(1_i64)).expect("insert");
        assert_eq!(t.get("a.b.c"), Some(&ConfigNode::from(1_i64)));

        t.insert("a.b.d", ConfigNode::from(2_i64)).expect("insert");
        assert_eq!(t.get("a.b.c"), Some(&ConfigNode::from(1_i64)));
        assert_eq!(t.get("a.b.d"), Some(&ConfigNode::from(2_i64)));

        t.insert("a.b", ConfigNode::from("scalar")).expect("insert");
        assert_eq!(t.get("a.b"), Some(&ConfigNode::from("scalar")));

        t.insert("", ConfigNode::from(0_i64)).expect("insert");
        assert_eq!(t.root(), &ConfigNode::from(0_i64));
    }

    #[test]
    fn insert_into_sequence() {
        let mut t = with_items(vec![ConfigNode::map(), ConfigNode::from(1_i64)]);
        t.insert("items.0.leaf", ConfigNode::from(7_i64))
            .expect("insert");
        assert_eq!(t.get("items.0.leaf"), Some(&ConfigNode::from(7_i64)));

        t.insert("items.1", ConfigNode::from(8_i64))
            .expect("insert");
        assert_eq!(t.get("items.1"), Some(&ConfigNode::from(8_i64)));
        assert_eq!(t.get("items.0.leaf"), Some(&ConfigNode::from(7_i64)));
    }

    #[test]
    fn insert_keeps_sequence() {
        let mut t = with_items(vec![ConfigNode::from(1_i64), ConfigNode::from(2_i64)]);
        let before = t.clone();

        let err = t
            .insert("items.5.leaf", ConfigNode::from(9_i64))
            .unwrap_err();
        assert_eq!(err.path(), "items.5");
        assert!(matches!(err.kind(), ConfigErrorKind::Invalid(_)));
        assert_eq!(t, before);

        let err = t.insert("items.name", ConfigNode::from(9_i64)).unwrap_err();
        assert_eq!(err.path(), "items.name");
        assert!(matches!(err.kind(), ConfigErrorKind::Invalid(_)));
        assert_eq!(t, before);
    }

    #[test]
    fn insert_keeps_scalar() {
        let mut t = tree(&[("a.b", ConfigNode::from("scalar"))]);
        let before = t.clone();

        let err = t.insert("a.b.c", ConfigNode::from(1_i64)).unwrap_err();
        assert_eq!(err.path(), "a.b");
        assert!(matches!(
            err.kind(),
            ConfigErrorKind::TypeMismatch {
                expected: "map",
                found: "string"
            }
        ));
        assert_eq!(t, before);

        let err = t.insert("a.b.c.d", ConfigNode::from(1_i64)).unwrap_err();
        assert_eq!(err.path(), "a.b");
        assert_eq!(t, before);
    }

    #[test]
    fn reads_scalars() {
        assert!(ConfigNode::from(true).as_bool().unwrap());
        assert_eq!(ConfigNode::from(2_i64).as_i64().unwrap(), 2);
        assert_eq!(ConfigNode::from(2_i64).as_f64().unwrap(), 2.0);
        assert_eq!(ConfigNode::from("x").as_str().unwrap(), "x");
        assert_eq!(ConfigNode::map().kind_name(), "map");
        assert_eq!(ConfigNode::null().kind_name(), "null");

        let err = ConfigNode::from("x").as_i64().unwrap_err();
        assert!(matches!(
            err.kind(),
            ConfigErrorKind::TypeMismatch {
                expected: "int",
                found: "string"
            }
        ));
    }

    #[test]
    fn int_range_checked() {
        assert_eq!(u16::from_config(&ConfigNode::from(8080_i64)).unwrap(), 8080);

        let err = u16::from_config(&ConfigNode::from(70_000_i64)).unwrap_err();
        assert!(matches!(err.kind(), ConfigErrorKind::Invalid(_)));
        assert!(u8::from_config(&ConfigNode::from(-1_i64)).is_err());
        assert!(usize::from_config(&ConfigNode::from(-1_i64)).is_err());
        assert_eq!(i64::from_config(&ConfigNode::from(-1_i64)).unwrap(), -1);
    }

    #[test]
    fn narrow_ints_checked() {
        assert_eq!(i8::from_config(&ConfigNode::from(127_i64)).unwrap(), 127);
        let err = i8::from_config(&ConfigNode::from(128_i64)).unwrap_err();
        assert!(matches!(err.kind(), ConfigErrorKind::Invalid(_)));

        assert_eq!(
            i16::from_config(&ConfigNode::from(-32_768_i64)).unwrap(),
            -32_768
        );
        assert!(i16::from_config(&ConfigNode::from(32_768_i64)).is_err());

        assert_eq!(i32::from_config(&ConfigNode::from(-7_i64)).unwrap(), -7);
        assert!(i32::from_config(&ConfigNode::from(3_000_000_000_i64)).is_err());

        assert_eq!(isize::from_config(&ConfigNode::from(-7_i64)).unwrap(), -7);

        assert_eq!(u64::from_config(&ConfigNode::from(9_i64)).unwrap(), 9);
        assert!(u64::from_config(&ConfigNode::from(-1_i64)).is_err());
    }

    #[test]
    fn narrows_to_f32() {
        assert_eq!(f32::from_config(&ConfigNode::from(1.5)).unwrap(), 1.5_f32);
        assert_eq!(f32::from_config(&ConfigNode::from(2_i64)).unwrap(), 2.0_f32);

        let err = f32::from_config(&ConfigNode::from(1e300)).unwrap_err();
        assert!(matches!(err.kind(), ConfigErrorKind::Invalid(_)));

        assert!(
            f32::from_config(&ConfigNode::from(f64::INFINITY))
                .unwrap()
                .is_infinite()
        );
        assert!(
            f32::from_config(&ConfigNode::from(f64::NAN))
                .unwrap()
                .is_nan()
        );
        assert!(f32::from_config(&ConfigNode::from("x")).is_err());
    }

    #[test]
    fn parses_duration() {
        let secs = |n| Duration::from_secs(n);
        assert_eq!(
            Duration::from_config(&ConfigNode::from(10_i64)).unwrap(),
            secs(10)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from("10s")).unwrap(),
            secs(10)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from("500ms")).unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from("2m")).unwrap(),
            secs(120)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from("1h")).unwrap(),
            secs(3600)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from("250us")).unwrap(),
            Duration::from_micros(250)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from(" 1.5s ")).unwrap(),
            Duration::from_millis(1500)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from(0.25)).unwrap(),
            Duration::from_millis(250)
        );
        assert_eq!(
            Duration::from_config(&ConfigNode::from("30")).unwrap(),
            secs(30)
        );
    }

    #[test]
    fn rejects_bad_duration() {
        assert!(Duration::from_config(&ConfigNode::from("10 parsecs")).is_err());
        assert!(Duration::from_config(&ConfigNode::from("-5s")).is_err());
        assert!(Duration::from_config(&ConfigNode::from(-5_i64)).is_err());
        assert!(Duration::from_config(&ConfigNode::from("")).is_err());
        assert!(Duration::from_config(&ConfigNode::from(true)).is_err());
    }

    #[test]
    fn optional_values() {
        assert_eq!(
            Option::<i64>::from_config(&ConfigNode::null()).unwrap(),
            None
        );
        assert_eq!(
            Option::<i64>::from_config(&ConfigNode::from(1_i64)).unwrap(),
            Some(1)
        );
        assert!(Option::<i64>::from_config(&ConfigNode::from("x")).is_err());
    }

    #[test]
    fn container_values() {
        let seq = ConfigNode::Seq(vec![ConfigNode::from(1_i64), ConfigNode::from(2_i64)]);
        assert_eq!(Vec::<u8>::from_config(&seq).unwrap(), vec![1, 2]);

        let map = ConfigNode::Map(BTreeMap::from([
            ("a".to_string(), ConfigNode::from(1_i64)),
            ("b".to_string(), ConfigNode::from(2_i64)),
        ]));
        let parsed = BTreeMap::<String, i64>::from_config(&map).unwrap();
        assert_eq!(parsed.get("b"), Some(&2));
        assert!(Vec::<i64>::from_config(&map).is_err());
    }

    #[test]
    fn error_path_prefixed() {
        let seq = ConfigNode::Seq(vec![ConfigNode::from(1_i64), ConfigNode::from("x")]);
        let err = Vec::<i64>::from_config(&seq).unwrap_err();
        assert_eq!(err.path(), "1");

        let nested = ConfigNode::Map(BTreeMap::from([(
            "outer".to_string(),
            ConfigNode::Seq(vec![ConfigNode::from("x")]),
        )]));
        let err = BTreeMap::<String, Vec<i64>>::from_config(&nested).unwrap_err();
        assert_eq!(err.path(), "outer.0");
    }

    #[test]
    fn redacts_on_debug() {
        #[derive(Debug)]
        struct Revealing {
            token: &'static str,
        }

        let held = Secret::new(Revealing {
            token: "super-sensitive-token",
        });
        assert_eq!(format!("{held:?}"), "<redacted>");
        assert_eq!(format!("{held}"), "<redacted>");
        assert_eq!(format!("{held:#?}"), "<redacted>");
        assert!(!format!("{held:?} {held}").contains("super-sensitive-token"));
        assert_eq!(held.expose().token, "super-sensitive-token");
        assert_eq!(held.into_inner().token, "super-sensitive-token");
    }

    #[test]
    fn secret_from_config() {
        let held = Secret::<String>::from_config(&ConfigNode::from("hidden")).unwrap();
        assert_eq!(format!("{held:?}"), "<redacted>");
        assert_eq!(held.expose(), "hidden");
    }

    #[test]
    fn source_is_object_safe() {
        struct Fixed;

        impl ConfigSource for Fixed {
            fn name(&self) -> &'static str {
                "fixed"
            }

            fn load(&self) -> Result<ConfigTree, ConfigError> {
                Ok(tree(&[("a.b", ConfigNode::from(1_i64))]))
            }
        }

        let sources: Vec<Box<dyn ConfigSource>> = vec![Box::new(Fixed)];
        let mut merged = ConfigTree::empty();
        for source in &sources {
            assert_eq!(source.name(), "fixed");
            merged.merge(source.load().unwrap());
        }
        assert_eq!(merged.get("a.b"), Some(&ConfigNode::from(1_i64)));
    }
}
