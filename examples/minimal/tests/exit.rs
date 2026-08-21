//! The example, run as a process, and asked to stop.
//!
//! `main` is the one part of this crate a reader copies before understanding
//! it, so what it does after [`kernel::Kernel::run`] returns is worth a test of
//! its own. The claim is not that the kernel stops — the kernel's suites make
//! that claim — but that the shell gets its prompt back: the binary is started,
//! signalled, and given a bounded number of seconds to end by itself.
//!
//! A child rather than a kernel in-process, because the thing under test is the
//! teardown of a runtime nothing else can drive, and a runtime torn down inside
//! a test would take the test with it. The wait is bounded and the child is
//! killed if it overruns, so a regression fails this test rather than wedging
//! the suite.

#![cfg(unix)]

use core::time::Duration;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::Instant;

/// How long the process runs before it is asked to stop.
const BEFORE_SIGNAL: Duration = Duration::from_millis(400);

/// How long the process is given to end on its own once it has been asked.
///
/// Well above the `TEARDOWN` bound `main` gives the runtime, and bounded on
/// purpose: a process that never exits must fail this test, not hang it.
const PATIENCE: Duration = Duration::from_secs(15);

/// How often the child is looked at again.
const POLL: Duration = Duration::from_millis(20);

/// Signalled, the example ends by itself.
#[test]
fn binary_exits_on_signal() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_minimal"))
        // Fast rounds, so the feed is doing work when the signal lands.
        .env("MINIMAL_ORDERS__EVERY", "50ms")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the example binary is built as a dependency of this test");

    sleep(BEFORE_SIGNAL);
    let signalled = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("kill(1) is on the path of every unix this test runs on");
    assert!(signalled.success(), "could not signal the child");

    let started = Instant::now();
    loop {
        match child.try_wait().expect("the child is ours to wait for") {
            Some(status) => {
                assert!(status.success(), "the example exited with {status}");
                return;
            }
            None if started.elapsed() < PATIENCE => sleep(POLL),
            None => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("the example did not exit within {PATIENCE:?} of being signalled");
            }
        }
    }
}
