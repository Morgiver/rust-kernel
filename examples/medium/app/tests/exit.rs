//! The example, run as a process, and left to stop itself.
//!
//! The counterpart of `examples/minimal/tests/exit.rs`, and the same claim: the
//! shell gets its prompt back. This one needs no signal, because `app` already
//! reads `app.run_for` and asks its own kernel to stop — the trigger differs,
//! what is asserted does not.
//!
//! Both are worth having. `minimal` exits on a signal the kernel captured;
//! `app` exits on a [`kernel::KernelHandle`] its own `main` armed. A teardown
//! that hangs would hang either, and neither would have been noticed by a test
//! that only checked that `run` returned.

use core::time::Duration;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::Instant;

/// How long the application is told to run before stopping itself.
const RUN_FOR: &str = "300ms";

/// How long the process is given to end on its own.
///
/// Well above the `TEARDOWN` bound `main` gives the runtime, and bounded on
/// purpose: a process that never exits must fail this test, not hang it.
const PATIENCE: Duration = Duration::from_secs(15);

/// How often the child is looked at again.
const POLL: Duration = Duration::from_millis(20);

/// Told how long to run, the example ends by itself.
#[test]
fn binary_exits_unaided() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_app"))
        .env("MEDIUM_APP__RUN_FOR", RUN_FOR)
        .env("MEDIUM_ORDERS__EVERY", "50ms")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the example binary is built as a dependency of this test");

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
                panic!("the example did not exit within {PATIENCE:?}");
            }
        }
    }
}
