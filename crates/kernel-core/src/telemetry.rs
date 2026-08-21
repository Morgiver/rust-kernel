//! Telemetry: the emission contract and the three implementations shipped here.
//!
//! The kernel needs to emit, and must depend on no telemetry implementation.
//! [`Telemetry`] is therefore a plain object-safe contract with a single
//! method; any real backend is provided by a bundle. Three trivial
//! implementations live here so that the kernel is usable, testable and
//! diagnosable on its own:
//!
//! * [`NoopTelemetry`] — discards everything.
//! * [`StderrTelemetry`] — one line per record on the standard error stream.
//! * [`RecordingTelemetry`] — keeps records in memory, shareable by clone.
//!
//! A [`Record`] carries a [`Level`], a static event name and an ordered list of
//! [`Field`]s. Field order is insertion order and is never sorted.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// Severity attached to a [`Record`].
///
/// Ordering follows severity: `Debug < Info < Warn < Error`, so a threshold
/// filter is a plain comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    /// Fine-grained detail, useful while diagnosing.
    Debug,
    /// Normal progress worth reporting.
    Info,
    /// Something unexpected that did not stop progress.
    Warn,
    /// A failure.
    Error,
}

impl Level {
    /// Upper-case name used by the textual output, e.g. `"WARN"`.
    ///
    /// ```
    /// use kernel_core::telemetry::Level;
    ///
    /// assert_eq!(Level::Warn.as_str(), "WARN");
    /// ```
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Value carried by a [`Field`].
///
/// The set of shapes is deliberately closed: it is what the kernel itself
/// needs to report, and it keeps every backend's mapping total.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldValue {
    /// Textual value.
    Str(String),
    /// Signed integer value.
    Int(i64),
    /// Boolean value.
    Bool(bool),
    /// Elapsed or configured duration.
    Duration(Duration),
}

impl FieldValue {
    /// Name of the shape, for diagnostics: `"str"`, `"int"`, `"bool"` or
    /// `"duration"`.
    #[must_use]
    pub const fn kind_name(&self) -> &'static str {
        match self {
            FieldValue::Str(_) => "str",
            FieldValue::Int(_) => "int",
            FieldValue::Bool(_) => "bool",
            FieldValue::Duration(_) => "duration",
        }
    }
}

/// Renders the bare value, without quoting.
///
/// A [`FieldValue::Duration`] renders as whole seconds, a dot, exactly nine
/// digits of nanoseconds and a trailing `s`, e.g. `1.500000000s`.
impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Str(v) => f.write_str(v),
            FieldValue::Int(v) => write!(f, "{v}"),
            FieldValue::Bool(v) => write!(f, "{v}"),
            FieldValue::Duration(v) => write!(f, "{}.{:09}s", v.as_secs(), v.subsec_nanos()),
        }
    }
}

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        FieldValue::Str(value.to_owned())
    }
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        FieldValue::Str(value)
    }
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        FieldValue::Int(value)
    }
}

impl From<u32> for FieldValue {
    fn from(value: u32) -> Self {
        FieldValue::Int(i64::from(value))
    }
}

impl From<bool> for FieldValue {
    fn from(value: bool) -> Self {
        FieldValue::Bool(value)
    }
}

impl From<Duration> for FieldValue {
    fn from(value: Duration) -> Self {
        FieldValue::Duration(value)
    }
}

/// One key/value pair attached to a [`Record`].
///
/// The key is `&'static str`: keys are part of the emitting code, never of the
/// data being reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// Static key.
    pub key: &'static str,
    /// Associated value.
    pub value: FieldValue,
}

impl Field {
    /// Builds a field from a key and anything convertible into a
    /// [`FieldValue`].
    ///
    /// ```
    /// use kernel_core::telemetry::{Field, FieldValue};
    ///
    /// let field = Field::new("attempt", 2i64);
    /// assert_eq!(field.value, FieldValue::Int(2));
    /// ```
    pub fn new(key: &'static str, value: impl Into<FieldValue>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}

/// A single telemetry event: a level, a static event name and ordered fields.
///
/// ```
/// use kernel_core::telemetry::{Level, Record};
/// use std::time::Duration;
///
/// let record = Record::new(Level::Info, "unit.started")
///     .with("unit", "alpha")
///     .with("elapsed", Duration::from_millis(3));
/// assert_eq!(record.fields.len(), 2);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// Severity of the event.
    pub level: Level,
    /// Static event name, e.g. `"phase.entered"`.
    pub event: &'static str,
    /// Fields in insertion order; never sorted, never deduplicated.
    pub fields: Vec<Field>,
}

impl Record {
    /// Creates a record with no field.
    #[must_use]
    pub const fn new(level: Level, event: &'static str) -> Self {
        Self {
            level,
            event,
            fields: Vec::new(),
        }
    }

    /// Appends a field and returns the record, for chained construction.
    ///
    /// Appending twice with the same key keeps both fields, in order.
    #[must_use]
    pub fn with(mut self, key: &'static str, value: impl Into<FieldValue>) -> Self {
        self.fields.push(Field::new(key, value));
        self
    }

    /// Returns the value of the first field with this key, if any.
    #[must_use]
    pub fn field(&self, key: &str) -> Option<&FieldValue> {
        self.fields
            .iter()
            .find(|field| field.key == key)
            .map(|field| &field.value)
    }
}

/// Sink the kernel emits into.
///
/// Implementations must be cheap enough to call on every lifecycle transition,
/// must never panic and must never block indefinitely: an emission failure is
/// the sink's problem, never the caller's.
///
/// ```
/// use kernel_core::telemetry::{Record, Telemetry};
/// use std::sync::atomic::{AtomicUsize, Ordering};
///
/// struct Counter(AtomicUsize);
///
/// impl Telemetry for Counter {
///     fn record(&self, _record: Record) {
///         self.0.fetch_add(1, Ordering::Relaxed);
///     }
/// }
/// ```
pub trait Telemetry: Send + Sync + 'static {
    /// Consumes one record.
    fn record(&self, record: Record);
}

impl<T: Telemetry + ?Sized> Telemetry for Arc<T> {
    fn record(&self, record: Record) {
        (**self).record(record);
    }
}

/// Sink that discards every record.
///
/// This is the default when nothing else is configured, and costs one dynamic
/// call per emission.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn record(&self, _record: Record) {}
}

/// Sink that writes one line per record on the standard error stream.
///
/// The format is stable and is exactly:
///
/// ```text
/// LEVEL event key=value key=value
/// ```
///
/// * `LEVEL` is the upper-case level name ([`Level::as_str`]).
/// * `event` is the record's event name.
/// * fields follow in insertion order, separated by a single space.
/// * a [`FieldValue::Str`] is written verbatim unless it is empty or contains
///   whitespace, `"`, `=` or `\`; in that case it is wrapped in double quotes,
///   with `"` and `\` backslash-escaped and newline, carriage return and tab
///   written as `\n`, `\r` and `\t`. A record therefore always occupies
///   exactly one line.
/// * every other value is written by its [`Display`](fmt::Display)
///   implementation.
///
/// The line is written with a single `write_all`, so records from concurrent
/// threads do not interleave. A write failure is ignored: telemetry never
/// breaks the caller.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StderrTelemetry;

impl Telemetry for StderrTelemetry {
    fn record(&self, record: Record) {
        write_record(&mut std::io::stderr(), &record);
    }
}

/// Writes one record to `out`, newline included, in a single `write_all`.
///
/// A write failure is ignored: telemetry never breaks the caller.
fn write_record(out: &mut dyn std::io::Write, record: &Record) {
    let mut line = format_record(record);
    line.push('\n');
    let _ = out.write_all(line.as_bytes());
}

/// Renders a record in the [`StderrTelemetry`] line format, without the
/// trailing newline.
fn format_record(record: &Record) -> String {
    use fmt::Write as _;

    let mut out = String::new();
    out.push_str(record.level.as_str());
    out.push(' ');
    out.push_str(record.event);
    for field in &record.fields {
        out.push(' ');
        out.push_str(field.key);
        out.push('=');
        match &field.value {
            FieldValue::Str(value) if needs_quotes(value) => push_quoted(&mut out, value),
            value => {
                let _ = write!(out, "{value}");
            }
        }
    }
    out
}

/// True when a textual value cannot be written bare without breaking the
/// `key=value` line format.
fn needs_quotes(value: &str) -> bool {
    value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '"' | '=' | '\\'))
}

/// Appends a quoted, escaped rendering of `value`.
fn push_quoted(out: &mut String, value: &str) {
    out.push('"');
    for c in value.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
}

/// Sink that keeps every record in memory.
///
/// It serves both as a test double and as the kernel's own diagnostic default.
/// Cloning is cheap and shares the same buffer, so a clone handed to the kernel
/// observes what the original sees:
///
/// ```
/// use kernel_core::telemetry::{Level, Record, RecordingTelemetry, Telemetry};
///
/// let sink = RecordingTelemetry::new();
/// let handle = sink.clone();
/// handle.record(Record::new(Level::Info, "unit.started"));
/// assert!(sink.contains("unit.started"));
/// ```
///
/// The internal lock is never poisoned from the caller's point of view: if a
/// thread panicked while holding it, the guard is recovered and the buffer
/// keeps being used.
#[derive(Clone, Debug, Default)]
pub struct RecordingTelemetry {
    records: Arc<Mutex<Vec<Record>>>,
}

impl RecordingTelemetry {
    /// Creates an empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot clone of the records seen so far, in emission order.
    #[must_use]
    pub fn records(&self) -> Vec<Record> {
        self.lock().clone()
    }

    /// Returns how many records have been kept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Returns true when nothing has been recorded yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Drops every kept record.
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Returns true when at least one kept record carries this event name.
    #[must_use]
    pub fn contains(&self, event: &str) -> bool {
        self.lock().iter().any(|record| record.event == event)
    }

    /// Locks the buffer, recovering from a poisoned lock instead of panicking.
    fn lock(&self) -> MutexGuard<'_, Vec<Record>> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Telemetry for RecordingTelemetry {
    fn record(&self, record: Record) {
        self.lock().push(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_order() {
        assert!(Level::Debug < Level::Info);
        assert!(Level::Info < Level::Warn);
        assert!(Level::Warn < Level::Error);
        assert_eq!(Level::Error.to_string(), "ERROR");
    }

    #[test]
    fn value_conversions() {
        assert_eq!(FieldValue::from("a"), FieldValue::Str("a".to_owned()));
        assert_eq!(
            FieldValue::from("a".to_owned()),
            FieldValue::Str("a".to_owned())
        );
        assert_eq!(FieldValue::from(7i64), FieldValue::Int(7));
        assert_eq!(FieldValue::from(7u32), FieldValue::Int(7));
        assert_eq!(FieldValue::from(true), FieldValue::Bool(true));
        assert_eq!(
            FieldValue::from(Duration::from_secs(1)),
            FieldValue::Duration(Duration::from_secs(1))
        );
        assert_eq!(FieldValue::Int(0).kind_name(), "int");
    }

    #[test]
    fn keeps_field_order() {
        let record = Record::new(Level::Info, "e")
            .with("b", 1i64)
            .with("a", 2i64)
            .with("b", 3i64);
        let keys: Vec<&str> = record.fields.iter().map(|f| f.key).collect();
        assert_eq!(keys, ["b", "a", "b"]);
        assert_eq!(record.field("b"), Some(&FieldValue::Int(1)));
        assert_eq!(record.field("missing"), None);
    }

    #[test]
    fn formats_line() {
        let record = Record::new(Level::Warn, "unit.restarted")
            .with("unit", "alpha")
            .with("attempt", 2u32)
            .with("forced", false);
        assert_eq!(
            format_record(&record),
            "WARN unit.restarted unit=alpha attempt=2 forced=false"
        );
    }

    #[test]
    fn formats_duration() {
        let record = Record::new(Level::Debug, "e").with("took", Duration::from_millis(1500));
        assert_eq!(format_record(&record), "DEBUG e took=1.500000000s");
    }

    #[test]
    fn quotes_when_needed() {
        let record = Record::new(Level::Error, "e")
            .with("plain", "value")
            .with("spaced", "two words")
            .with("empty", "")
            .with("tricky", "a\"b\\c\nd");
        assert_eq!(
            format_record(&record),
            r#"ERROR e plain=value spaced="two words" empty="" tricky="a\"b\\c\nd""#
        );
        assert!(!format_record(&record).contains('\n'));
    }

    #[test]
    fn writes_one_line() {
        let record = Record::new(Level::Warn, "unit.restarted")
            .with("unit", "alpha")
            .with("attempt", 2u32);
        let mut out: Vec<u8> = Vec::new();
        write_record(&mut out, &record);

        let written = String::from_utf8(out).expect("utf-8");
        assert_eq!(written, "WARN unit.restarted unit=alpha attempt=2\n");
        assert_eq!(written.lines().count(), 1);
    }

    #[test]
    fn writes_quoted_values() {
        let record = Record::new(Level::Error, "e")
            .with("plain", "value")
            .with("spaced", "two words")
            .with("empty", "")
            .with("tricky", "a\"b\\c\nd");
        let mut out: Vec<u8> = Vec::new();
        write_record(&mut out, &record);

        let written = String::from_utf8(out).expect("utf-8");
        assert_eq!(
            written,
            concat!(
                r#"ERROR e plain=value spaced="two words" empty="" tricky="a\"b\\c\nd""#,
                "\n"
            )
        );
        assert_eq!(written.lines().count(), 1);
    }

    #[test]
    fn noop_stays_silent() {
        let sink = NoopTelemetry;
        sink.record(Record::new(Level::Info, "e"));
    }

    #[test]
    fn records_in_order() {
        let sink = RecordingTelemetry::new();
        assert!(sink.is_empty());
        sink.record(Record::new(Level::Info, "first"));
        sink.record(Record::new(Level::Error, "second"));

        let snapshot = sink.records();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].event, "first");
        assert_eq!(snapshot[1].event, "second");
        assert_eq!(sink.len(), 2);
        assert!(sink.contains("second"));
        assert!(!sink.contains("third"));
    }

    #[test]
    fn snapshot_is_detached() {
        let sink = RecordingTelemetry::new();
        sink.record(Record::new(Level::Info, "first"));
        let mut snapshot = sink.records();
        snapshot.clear();
        assert_eq!(sink.len(), 1);
    }

    #[test]
    fn clone_shares_buffer() {
        let sink = RecordingTelemetry::new();
        let handle = sink.clone();
        handle.record(Record::new(Level::Info, "e"));
        assert_eq!(sink.len(), 1);
        sink.clear();
        assert!(handle.is_empty());
    }

    #[test]
    fn shared_through_arc() {
        let sink: Arc<dyn Telemetry> = Arc::new(RecordingTelemetry::new());
        sink.record(Record::new(Level::Info, "e"));
    }

    #[test]
    fn survives_poison() {
        let sink = RecordingTelemetry::new();
        sink.record(Record::new(Level::Info, "kept"));

        let poisoner = sink.clone();
        let joined = std::thread::spawn(move || {
            let _guard = poisoner.records.lock().expect("fresh lock");
            panic!("poison the lock");
        })
        .join();
        assert!(joined.is_err());
        assert!(sink.records.is_poisoned());

        sink.record(Record::new(Level::Warn, "after"));
        let snapshot = sink.records();
        assert_eq!(snapshot.len(), 2);
        assert!(sink.contains("kept"));
        assert!(sink.contains("after"));
        sink.clear();
        assert!(sink.is_empty());
    }
}
