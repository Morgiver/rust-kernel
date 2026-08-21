//! The run's own timeline, and the lines written on it.
//!
//! Every line this application prints is stamped against one instant: the
//! moment the stop was asked for. Before that instant the stamp is a dot;
//! after it, milliseconds.
//!
//! That is not decoration. The thing this example exists to show is an
//! ordering — the door shuts, a new caller is refused, and a request accepted
//! *before* the door shut still gets a real answer — and an ordering is only
//! legible against a clock both halves of the program read. The narration
//! ([`crate::console`]) and the scripted caller ([`crate::caller`]) share this
//! one, so their lines interleave into a single account instead of two.

use std::io::{self, Write};
use std::sync::OnceLock;
use std::time::Instant;

/// What every line of this application is prefixed with.
const NAME: &str = "service";

/// The instant the stop was asked for, once somebody has asked.
///
/// [`OnceLock`] rather than a mutex because the value is written once and read
/// from several tasks: the first writer wins, every later one is a no-op, and
/// no reader can observe a half-written instant.
#[derive(Debug, Default)]
pub struct Clock {
    /// When the stop was requested. `None` until it is.
    stopping_from: OnceLock<Instant>,
}

impl Clock {
    /// A clock that has not started.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts the timeline. Idempotent.
    ///
    /// Called from the `ShutdownRequested` listener, which is the earliest
    /// point in the process at which the stop is a fact — earlier than
    /// `Draining`, and earlier than anything the caller can observe through a
    /// socket.
    pub fn start(&self) {
        let _ = self.stopping_from.set(Instant::now());
    }

    /// How long since the stop was requested, right-aligned so the column
    /// holds still.
    #[must_use]
    pub fn stamp(&self) -> String {
        match self.stopping_from.get() {
            Some(from) => format!("{:>6}", format!("+{}ms", from.elapsed().as_millis())),
            None => format!("{:>6}", "."),
        }
    }

    /// Writes one stamped line to standard output, and flushes it.
    ///
    /// Two deliberate choices, both about a reader:
    ///
    /// * **standard output**, because these are the program's own lines. Every
    ///   feature writes its failures to telemetry, which goes to standard
    ///   error, and the two streams stay separable.
    /// * **flushed**, because a pipe makes standard output block-buffered while
    ///   standard error stays unbuffered. Without the flush, `cargo run |
    ///   tee run.log` defers this whole account to exit and interleaves it with
    ///   the kernel's records in the wrong order — and the order is the only
    ///   thing this example is trying to show.
    ///
    /// Writing blocks the executor thread it runs on. That is acceptable here
    /// and only here: this is the application layer, and there are a few dozen
    /// lines in the whole run. A unit that writes an unbounded number of them
    /// hands them to something that owns the writing instead.
    pub fn say(&self, line: &str) {
        println!("[{NAME} {}] {line}", self.stamp());
        let _ = io::stdout().flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_before_the_stop() {
        assert_eq!(Clock::new().stamp().trim(), ".");
    }

    #[test]
    fn stamp_after_the_stop() {
        let clock = Clock::new();
        clock.start();

        let stamp = clock.stamp();

        assert!(stamp.starts_with(' ') || stamp.starts_with('+'), "{stamp}");
        assert!(stamp.trim().ends_with("ms"), "{stamp}");
    }

    #[test]
    fn start_is_idempotent() {
        let clock = Clock::new();
        clock.start();
        clock.start();

        // A second start would reset the origin, and every line printed after
        // it would understate how long the ladder took.
        assert!(clock.stamp().trim().starts_with('+'));
    }
}
