//! The configuration chain and the two sources the kernel ships.
//!
//! A [`ConfigChain`] is an ordered list of [`ConfigSource`]s. Loading it loads
//! every source and merges the trees leaf by leaf, in order, so the last source
//! that defines a leaf wins while its siblings survive. Overriding one value
//! never erases the ones next to it — that is the whole reason the merge is
//! defined on leaves rather than on blocks.
//!
//! Loading reports **every** failing source, not the first. A chain that
//! stopped at the first failure would turn one bad deployment into as many
//! restarts as it has broken sources.
//!
//! # Two sources, deliberately
//!
//! The kernel ships [`MemorySource`] — values assembled in code — and
//! [`EnvSource`] — the process environment. It ships no file format at all, and
//! depends on no serialization library: a file format is a source like any
//! other, written by the application or by a bundle, and keeping it out is what
//! keeps the dependency out of every crate that only wanted to read a value.
//!
//! # Testing against the environment
//!
//! The process environment is global mutable state shared by every test in a
//! binary. [`EnvSource::from_pairs`] takes the variables as an argument
//! instead, so a test states the input it means and cannot be broken by an
//! unrelated test that happens to set a variable.

use core::fmt;

use kernel_core::{ConfigError, ConfigNode, ConfigSource, ConfigTree, Scalar};

/// An ordered list of configuration sources, merged leaf by leaf.
///
/// # Examples
///
/// ```
/// use kernel::config::{ConfigChain, EnvSource, MemorySource};
/// use kernel::core::{ConfigNode, ConfigTree};
///
/// let mut base = ConfigTree::empty();
/// base.insert("outer.first", ConfigNode::from(1_i64)).unwrap();
/// base.insert("outer.second", ConfigNode::from(2_i64)).unwrap();
///
/// let chain = ConfigChain::new()
///     .with(MemorySource::new(base))
///     .with(EnvSource::from_pairs(
///         "APP_",
///         [("APP_OUTER__FIRST".to_owned(), "9".to_owned())],
///     ));
///
/// let tree = chain.load().expect("load");
/// assert_eq!(tree.get("outer.first"), Some(&ConfigNode::from(9_i64)));
/// assert_eq!(tree.get("outer.second"), Some(&ConfigNode::from(2_i64)));
/// ```
pub struct ConfigChain {
    sources: Vec<Box<dyn ConfigSource>>,
}

impl ConfigChain {
    /// An empty chain, which loads as an empty tree.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    /// Appends a source. Later sources override earlier ones, leaf by leaf.
    pub fn push(&mut self, source: impl ConfigSource) -> &mut Self {
        self.sources.push(Box::new(source));
        self
    }

    /// Appends a source and hands the chain back, for use in an expression.
    #[must_use]
    pub fn with(mut self, source: impl ConfigSource) -> Self {
        self.push(source);
        self
    }

    /// Loads every source in order and merges the results, last one winning.
    ///
    /// # Errors
    ///
    /// Every failing source contributes one [`ConfigError`], and all of them
    /// are returned together. A source that fails contributes nothing to the
    /// tree, and one failure does not stop the sources after it from being
    /// tried — the point of the call is to name all of them at once.
    pub fn load(&self) -> Result<ConfigTree, Vec<ConfigError>> {
        let mut tree = ConfigTree::empty();
        let mut errors = Vec::new();

        for source in &self.sources {
            match source.load() {
                Ok(loaded) => tree.merge(loaded),
                Err(error) => errors.push(error),
            }
        }

        if errors.is_empty() {
            Ok(tree)
        } else {
            Err(errors)
        }
    }

    /// How many sources the chain holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Whether the chain holds no source at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

impl Default for ConfigChain {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ConfigChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&'static str> = self.sources.iter().map(|source| source.name()).collect();
        f.debug_struct("ConfigChain")
            .field("sources", &names)
            .finish()
    }
}

/// A source backed by a tree assembled in code.
///
/// This is how an application states its defaults, and how a test states the
/// configuration it means without going through any format at all.
#[derive(Clone, Debug)]
pub struct MemorySource {
    name: &'static str,
    tree: ConfigTree,
}

impl MemorySource {
    /// A source named `"memory"`, serving `tree`.
    #[must_use]
    pub fn new(tree: ConfigTree) -> Self {
        Self::named("memory", tree)
    }

    /// A source serving `tree` under a name of its own.
    ///
    /// Several memory sources in one chain are otherwise indistinguishable in
    /// a diagnostic; the name is what tells them apart.
    #[must_use]
    pub fn named(name: &'static str, tree: ConfigTree) -> Self {
        Self { name, tree }
    }
}

impl ConfigSource for MemorySource {
    fn name(&self) -> &'static str {
        self.name
    }

    fn load(&self) -> Result<ConfigTree, ConfigError> {
        Ok(self.tree.clone())
    }
}

/// Where an [`EnvSource`] reads its variables from.
enum Vars {
    /// The process environment.
    Process,
    /// An explicit list, supplied by the caller.
    Fixed(Vec<(String, String)>),
}

/// A source backed by environment variables.
///
/// # Mapping
///
/// The mapping is a user-visible contract, so every rule of it is fixed:
///
/// - a variable whose name does not start with the prefix is ignored;
///   [`all`](Self::all) uses an empty prefix and therefore takes everything;
/// - the prefix is stripped, and the remainder is lowercased;
/// - `__`, two underscores, separates path segments; a single `_` is an
///   ordinary character inside a segment. With prefix `APP_`,
///   `APP_SERVER__MAX_RETRIES` becomes `server.max_retries`;
/// - the value is parsed in this order: `true` and `false` become a boolean,
///   otherwise an `i64` becomes an integer, otherwise an `f64` becomes a float,
///   otherwise the value stays a string. An empty value is the empty string.
///
/// Two details follow from the rules rather than adding to them: the value is
/// never lowercased, so `TRUE` is the string `"TRUE"` and not a boolean; and
/// segments are joined with `.`, so a `.` that is already part of a variable's
/// name separates segments as well.
///
/// Variables are applied in sorted order by name, so a tree built from the
/// process environment does not depend on the order the operating system
/// happens to hand them over in.
///
/// # Examples
///
/// ```
/// use kernel::config::EnvSource;
/// use kernel::core::{ConfigNode, ConfigSource};
///
/// let source = EnvSource::from_pairs(
///     "APP_",
///     [("APP_SERVER__MAX_RETRIES".to_owned(), "3".to_owned())],
/// );
///
/// let tree = source.load().expect("load");
/// assert_eq!(tree.get("server.max_retries"), Some(&ConfigNode::from(3_i64)));
/// ```
pub struct EnvSource {
    prefix: String,
    vars: Vars,
}

impl EnvSource {
    /// Reads the process environment, keeping only the variables that start
    /// with `prefix`.
    #[must_use]
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            vars: Vars::Process,
        }
    }

    /// Reads the whole process environment, with no prefix to strip.
    #[must_use]
    pub fn all() -> Self {
        Self::with_prefix("")
    }

    /// Reads an explicit list of variables instead of the process environment.
    ///
    /// This is the seam that keeps a test off global state: the environment is
    /// shared by every test in a binary, so a test that reads it can be broken
    /// by an unrelated test that writes it. The mapping applied to `pairs` is
    /// exactly the one applied to the process environment.
    #[must_use]
    pub fn from_pairs<I>(prefix: impl Into<String>, pairs: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        Self {
            prefix: prefix.into(),
            vars: Vars::Fixed(pairs.into_iter().collect()),
        }
    }

    /// The dotted path a variable name maps to, or `None` if it maps to
    /// nothing.
    ///
    /// A name that does not carry the prefix is skipped, and so is one that is
    /// nothing but the prefix: an empty path addresses the root of the tree,
    /// and a variable that replaced the whole tree with one scalar would be a
    /// trap rather than a feature.
    fn path_of(&self, key: &str) -> Option<String> {
        let lowered = key.strip_prefix(self.prefix.as_str())?.to_lowercase();
        let segments: Vec<&str> = lowered
            .split("__")
            .filter(|segment| !segment.is_empty())
            .collect();

        if segments.is_empty() {
            None
        } else {
            Some(segments.join("."))
        }
    }

    /// The variables to apply, sorted by name.
    fn sorted_vars(&self) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = match &self.vars {
            // `vars_os` rather than `vars`: a variable that is not valid UTF-8
            // is skipped, where `vars` would panic and take the process with it.
            Vars::Process => std::env::vars_os()
                .filter_map(|(key, value)| {
                    Some((key.into_string().ok()?, value.into_string().ok()?))
                })
                .collect(),
            Vars::Fixed(pairs) => pairs.clone(),
        };
        pairs.sort();
        pairs
    }
}

impl ConfigSource for EnvSource {
    fn name(&self) -> &'static str {
        "environment"
    }

    fn load(&self) -> Result<ConfigTree, ConfigError> {
        let mut tree = ConfigTree::empty();

        for (key, value) in self.sorted_vars() {
            let Some(path) = self.path_of(&key) else {
                continue;
            };
            tree.insert(&path, parse_value(&value))?;
        }

        Ok(tree)
    }
}

impl fmt::Debug for EnvSource {
    /// Never renders the values: a source over the environment holds whatever
    /// secrets the environment holds.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let origin = match self.vars {
            Vars::Process => "process",
            Vars::Fixed(_) => "fixed",
        };
        f.debug_struct("EnvSource")
            .field("prefix", &self.prefix)
            .field("vars", &origin)
            .finish()
    }
}

/// Parses a raw environment value into a scalar node.
///
/// The order is fixed and total: boolean, then integer, then float, then
/// string. Every input maps to exactly one node, so a value is never rejected
/// for being the wrong shape — the shape is whatever it parsed as.
fn parse_value(raw: &str) -> ConfigNode {
    if raw == "true" {
        return ConfigNode::Scalar(Scalar::Bool(true));
    }
    if raw == "false" {
        return ConfigNode::Scalar(Scalar::Bool(false));
    }
    if let Ok(int) = raw.parse::<i64>() {
        return ConfigNode::Scalar(Scalar::Int(int));
    }
    if let Ok(float) = raw.parse::<f64>() {
        return ConfigNode::Scalar(Scalar::Float(float));
    }
    ConfigNode::Scalar(Scalar::Str(raw.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use kernel_core::error::ConfigErrorKind;

    /// A source that always fails, so a chain's aggregation can be observed.
    struct Broken(&'static str);

    impl ConfigSource for Broken {
        fn name(&self) -> &'static str {
            self.0
        }

        fn load(&self) -> Result<ConfigTree, ConfigError> {
            Err(ConfigError::invalid(self.0, "deliberate"))
        }
    }

    fn pairs<const N: usize>(entries: [(&str, &str); N]) -> Vec<(String, String)> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }

    fn env(prefix: &str, entries: &[(&str, &str)]) -> ConfigTree {
        EnvSource::from_pairs(
            prefix,
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
        )
        .load()
        .expect("load")
    }

    fn tree_of(entries: &[(&str, ConfigNode)]) -> ConfigTree {
        let mut tree = ConfigTree::empty();
        for (path, node) in entries {
            tree.insert(path, node.clone()).expect("insert");
        }
        tree
    }

    #[test]
    fn empty_chain_loads_empty() {
        let chain = ConfigChain::new();

        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);
        assert_eq!(chain.load().expect("load"), ConfigTree::empty());
    }

    #[test]
    fn last_source_wins() {
        let chain = ConfigChain::new()
            .with(MemorySource::new(tree_of(&[(
                "a",
                ConfigNode::from(1_i64),
            )])))
            .with(MemorySource::new(tree_of(&[(
                "a",
                ConfigNode::from(2_i64),
            )])));

        let tree = chain.load().expect("load");

        assert_eq!(tree.get("a"), Some(&ConfigNode::from(2_i64)));
    }

    #[test]
    fn merges_by_leaf() {
        let chain = ConfigChain::new()
            .with(MemorySource::new(tree_of(&[
                ("outer.first", ConfigNode::from(1_i64)),
                ("outer.second", ConfigNode::from(2_i64)),
            ])))
            .with(MemorySource::new(tree_of(&[(
                "outer.first",
                ConfigNode::from(9_i64),
            )])));

        let tree = chain.load().expect("load");

        assert_eq!(tree.get("outer.first"), Some(&ConfigNode::from(9_i64)));
        assert_eq!(tree.get("outer.second"), Some(&ConfigNode::from(2_i64)));
    }

    #[test]
    fn reports_every_failure() {
        let chain = ConfigChain::new()
            .with(Broken("first"))
            .with(MemorySource::new(ConfigTree::empty()))
            .with(Broken("second"))
            .with(Broken("third"));

        let errors = chain.load().expect_err("failures");

        let paths: Vec<&str> = errors.iter().map(ConfigError::path).collect();
        assert_eq!(paths, ["first", "second", "third"]);
    }

    #[test]
    fn push_counts_sources() {
        let mut chain = ConfigChain::new();
        chain
            .push(MemorySource::new(ConfigTree::empty()))
            .push(EnvSource::from_pairs("APP_", pairs([])));

        assert_eq!(chain.len(), 2);
        assert!(!chain.is_empty());
    }

    #[test]
    fn debug_names_sources() {
        let chain = ConfigChain::new().with(MemorySource::named("defaults", ConfigTree::empty()));

        assert!(format!("{chain:?}").contains("defaults"));
    }

    #[test]
    fn memory_serves_its_tree() {
        let source = MemorySource::named("defaults", tree_of(&[("a", ConfigNode::from(1_i64))]));

        assert_eq!(source.name(), "defaults");
        assert_eq!(
            source.load().expect("load").get("a"),
            Some(&ConfigNode::from(1_i64))
        );
    }

    #[test]
    fn strips_the_prefix() {
        let tree = env("APP_", &[("APP_ALPHA", "1")]);

        assert_eq!(tree.get("alpha"), Some(&ConfigNode::from(1_i64)));
    }

    #[test]
    fn ignores_other_prefixes() {
        let tree = env("APP_", &[("OTHER_ALPHA", "1"), ("APP_ALPHA", "2")]);

        assert_eq!(tree.get("alpha"), Some(&ConfigNode::from(2_i64)));
        assert_eq!(tree.get("other_alpha"), None);
    }

    #[test]
    fn lowercases_the_remainder() {
        let tree = env("APP_", &[("APP_ALPHA__BETA", "1")]);

        assert_eq!(tree.get("alpha.beta"), Some(&ConfigNode::from(1_i64)));
        assert_eq!(tree.get("ALPHA.BETA"), None);
    }

    #[test]
    fn double_underscore_splits() {
        let tree = env("APP_", &[("APP_ALPHA__BETA__GAMMA", "1")]);

        assert_eq!(tree.get("alpha.beta.gamma"), Some(&ConfigNode::from(1_i64)));
    }

    // The rule most likely to be broken by a naive implementation that splits
    // on `_`: a single underscore is an ordinary character.
    #[test]
    fn single_underscore_stays() {
        let tree = env("APP_", &[("APP_ALPHA__MAX_RETRIES", "3")]);

        assert_eq!(
            tree.get("alpha.max_retries"),
            Some(&ConfigNode::from(3_i64))
        );
        assert_eq!(tree.get("alpha.max.retries"), None);
        assert_eq!(tree.get("alpha.max"), None);
    }

    #[test]
    fn all_takes_everything() {
        let source = EnvSource::from_pairs("", pairs([("ALPHA__BETA", "1")]));

        let tree = source.load().expect("load");

        assert_eq!(tree.get("alpha.beta"), Some(&ConfigNode::from(1_i64)));
    }

    #[test]
    fn bare_prefix_is_skipped() {
        let tree = env(
            "APP_",
            &[("APP_", "1"), ("APP___", "2"), ("APP_ALPHA", "3")],
        );

        // The root is still the map the other variable was written into, not
        // a scalar that replaced the whole tree.
        assert_eq!(tree.get("alpha"), Some(&ConfigNode::from(3_i64)));
        assert_eq!(tree.root().kind_name(), "map");
    }

    #[test]
    fn parses_booleans() {
        let tree = env("P_", &[("P_YES", "true"), ("P_NO", "false")]);

        assert_eq!(tree.get("yes"), Some(&ConfigNode::from(true)));
        assert_eq!(tree.get("no"), Some(&ConfigNode::from(false)));
    }

    #[test]
    fn parses_integers() {
        let tree = env("P_", &[("P_A", "3"), ("P_B", "-7"), ("P_C", "0")]);

        assert_eq!(tree.get("a"), Some(&ConfigNode::from(3_i64)));
        assert_eq!(tree.get("b"), Some(&ConfigNode::from(-7_i64)));
        assert_eq!(tree.get("c"), Some(&ConfigNode::from(0_i64)));
    }

    // Integer before float: `3` must not become `3.0`.
    #[test]
    fn integer_wins_over_float() {
        let tree = env("P_", &[("P_A", "3"), ("P_B", "3.5")]);

        assert_eq!(tree.get("a"), Some(&ConfigNode::from(3_i64)));
        assert_eq!(tree.get("b"), Some(&ConfigNode::from(3.5_f64)));
    }

    #[test]
    fn falls_back_to_string() {
        let tree = env("P_", &[("P_A", "3 apples"), ("P_B", "TRUE")]);

        assert_eq!(tree.get("a"), Some(&ConfigNode::from("3 apples")));
        assert_eq!(tree.get("b"), Some(&ConfigNode::from("TRUE")));
    }

    #[test]
    fn empty_value_is_string() {
        let tree = env("P_", &[("P_A", "")]);

        assert_eq!(tree.get("a"), Some(&ConfigNode::from("")));
    }

    // An integer too large for `i64` is not silently narrowed; it falls to the
    // next rule in the order, which is float.
    #[test]
    fn oversized_integer_is_float() {
        let tree = env("P_", &[("P_A", "99999999999999999999")]);

        assert!(matches!(
            tree.get("a"),
            Some(ConfigNode::Scalar(Scalar::Float(_)))
        ));
    }

    #[test]
    fn applies_in_sorted_order() {
        let source = EnvSource::from_pairs(
            "P_",
            pairs([("P_B__X", "2"), ("P_A__X", "1"), ("P_C__X", "3")]),
        );

        let tree = source.load().expect("load");

        assert_eq!(tree.get("a.x"), Some(&ConfigNode::from(1_i64)));
        assert_eq!(tree.get("b.x"), Some(&ConfigNode::from(2_i64)));
        assert_eq!(tree.get("c.x"), Some(&ConfigNode::from(3_i64)));
    }

    // `P_A` writes a scalar, `P_A__B` then tries to walk into it. The tree
    // refuses rather than dropping either value silently.
    #[test]
    fn conflicting_paths_fail() {
        let source = EnvSource::from_pairs("P_", pairs([("P_A", "1"), ("P_A__B", "2")]));

        let error = source.load().expect_err("conflict");

        assert!(matches!(error.kind(), ConfigErrorKind::TypeMismatch { .. }));
    }

    #[test]
    fn chain_reports_source_failure() {
        let chain = ConfigChain::new().with(EnvSource::from_pairs(
            "P_",
            pairs([("P_A", "1"), ("P_A__B", "2")]),
        ));

        assert_eq!(chain.load().expect_err("conflict").len(), 1);
    }

    #[test]
    fn debug_hides_values() {
        let source = EnvSource::from_pairs("P_", pairs([("P_TOKEN", "s3cret")]));

        let rendered = format!("{source:?}");

        assert!(rendered.contains("P_"));
        assert!(!rendered.contains("s3cret"));
    }
}
