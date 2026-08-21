//! `#[derive(FromConfig)]` — what the expansion does, not what it looks like.
//!
//! Every assertion here reads a value or an error out of a configuration node.
//! Nothing compares generated text: a derive that produced the right string and
//! the wrong behaviour would pass such a test, and a derive that produced
//! different text and the same behaviour would fail it.

use std::collections::BTreeMap;
use std::time::Duration;

use kernel_core::config::{ConfigNode, FromConfig, Scalar, Secret};
use kernel_core::error::ConfigErrorKind;
use kernel_macros::FromConfig;

#[derive(Debug, FromConfig)]
#[config(crate = ::kernel_core)]
struct Simple {
    depth: u32,
    label: String,
    enabled: bool,
}

#[derive(Debug, FromConfig)]
struct Optional {
    depth: u32,
    label: Option<String>,
    wait: core::option::Option<Duration>,
}

#[derive(Debug, FromConfig)]
struct Renamed {
    #[config(rename = "max-wait")]
    max_wait: Duration,
    #[config(default)]
    verbose: bool,
}

#[derive(Debug, FromConfig)]
struct Outer {
    inner: Simple,
    tokens: Vec<u16>,
}

#[derive(Debug, FromConfig)]
struct Guarded {
    key: Secret<String>,
}

#[derive(Debug, FromConfig)]
struct Nothing {}

fn node(entries: &[(&str, ConfigNode)]) -> ConfigNode {
    let mut map = BTreeMap::new();
    for (key, value) in entries {
        map.insert((*key).to_string(), value.clone());
    }
    ConfigNode::Map(map)
}

#[test]
fn reads_named_fields() {
    let source = node(&[
        ("depth", ConfigNode::from(3_i64)),
        ("label", ConfigNode::from("here")),
        ("enabled", ConfigNode::from(true)),
    ]);

    let value = Simple::from_config(&source).expect("reads");

    assert_eq!(value.depth, 3);
    assert_eq!(value.label, "here");
    assert!(value.enabled);
}

#[test]
fn option_accepts_absence() {
    let source = node(&[("depth", ConfigNode::from(1_i64))]);

    let value = Optional::from_config(&source).expect("reads");

    assert_eq!(value.depth, 1);
    assert!(value.label.is_none());
    assert!(value.wait.is_none());
}

#[test]
fn option_accepts_null() {
    let source = node(&[
        ("depth", ConfigNode::from(1_i64)),
        ("label", ConfigNode::Scalar(Scalar::Null)),
    ]);

    let value = Optional::from_config(&source).expect("reads");

    assert!(value.label.is_none());
}

#[test]
fn missing_field_reported() {
    let source = node(&[("depth", ConfigNode::from(3_i64))]);

    let error = Simple::from_config(&source).expect_err("label is required");

    assert_eq!(error.path(), "label");
    assert!(matches!(error.kind(), ConfigErrorKind::Missing));
}

#[test]
fn mismatch_names_field() {
    let source = node(&[
        ("depth", ConfigNode::from("three")),
        ("label", ConfigNode::from("here")),
        ("enabled", ConfigNode::from(true)),
    ]);

    let error = Simple::from_config(&source).expect_err("depth is not a string");

    assert_eq!(error.path(), "depth");
    assert!(matches!(
        error.kind(),
        ConfigErrorKind::TypeMismatch {
            expected: "int",
            ..
        }
    ));
}

#[test]
fn nested_struct_reads() {
    let source = node(&[
        (
            "inner",
            node(&[
                ("depth", ConfigNode::from(2_i64)),
                ("label", ConfigNode::from("deep")),
                ("enabled", ConfigNode::from(false)),
            ]),
        ),
        ("tokens", ConfigNode::Seq(vec![ConfigNode::from(5_i64)])),
    ]);

    let value = Outer::from_config(&source).expect("reads");

    assert_eq!(value.inner.label, "deep");
    assert_eq!(value.tokens, vec![5_u16]);
}

#[test]
fn error_path_nests() {
    let source = node(&[
        (
            "inner",
            node(&[
                ("depth", ConfigNode::from(1_i64)),
                ("enabled", ConfigNode::from(true)),
            ]),
        ),
        ("tokens", ConfigNode::Seq(Vec::new())),
    ]);

    let error = Outer::from_config(&source).expect_err("inner.label is required");

    assert_eq!(error.path(), "inner.label");
}

#[test]
fn error_path_keeps_index() {
    let source = node(&[
        (
            "inner",
            node(&[
                ("depth", ConfigNode::from(1_i64)),
                ("label", ConfigNode::from("here")),
                ("enabled", ConfigNode::from(true)),
            ]),
        ),
        (
            "tokens",
            ConfigNode::Seq(vec![ConfigNode::from(70_000_i64)]),
        ),
    ]);

    let error = Outer::from_config(&source).expect_err("70000 overflows u16");

    assert_eq!(error.path(), "tokens.0");
}

#[test]
fn rename_reads_key() {
    let source = node(&[("max-wait", ConfigNode::from("250ms"))]);

    let value = Renamed::from_config(&source).expect("reads");

    assert_eq!(value.max_wait, Duration::from_millis(250));
    assert!(!value.verbose);
}

#[test]
fn rename_hides_identifier() {
    let source = node(&[("max_wait", ConfigNode::from("250ms"))]);

    let error = Renamed::from_config(&source).expect_err("the key is `max-wait`");

    assert_eq!(error.path(), "max-wait");
}

#[test]
fn default_fills_absence() {
    let source = node(&[
        ("max-wait", ConfigNode::from("1s")),
        ("verbose", ConfigNode::from(true)),
    ]);

    let value = Renamed::from_config(&source).expect("reads");

    assert!(value.verbose);
}

#[test]
fn wrapper_types_read() {
    let source = node(&[("key", ConfigNode::from("opaque"))]);

    let value = Guarded::from_config(&source).expect("reads");

    assert_eq!(value.key.expose(), "opaque");
    assert!(!format!("{:?}", value.key).contains("opaque"));
}

#[test]
fn empty_struct_reads() {
    let source = node(&[]);

    assert!(Nothing::from_config(&source).is_ok());
}

#[test]
fn expansion_needs_no_import() {
    // The generated impl names every path absolutely, so a module that imports
    // nothing at all still compiles.
    mod isolated {
        #[derive(kernel_macros::FromConfig)]
        pub struct Alone {
            pub depth: u32,
        }
    }

    let source = node(&[("depth", ConfigNode::from(9_i64))]);
    let value = <isolated::Alone as FromConfig>::from_config(&source).expect("reads");

    assert_eq!(value.depth, 9);
}
