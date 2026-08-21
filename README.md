# rust-kernel

**A state machine that owns an object graph and a set of long-running tasks, and
guarantees the order in which they are born, run and die.**

That sentence is the whole scope. Everything the kernel does serves it, and
everything it refuses to do is refused because it does not.

> **Status: pre-release, unproven.** Version `0.1.0`, never published, never run
> against a real workload. The public surface is still moving. See
> [Where it actually stands](#where-it-actually-stands) before you build on it.

---

## What it is

An application hands the kernel a list of *bundles*. Each bundle declares what
it provides and what it needs. The kernel then:

- validates the whole graph before anything is built — missing contract, cycle,
  ambiguous binding, invalid configuration;
- instantiates and boots the *components* in topological order;
- starts the *runnables* and supervises them, with a criticality and a restart
  policy per task;
- stops everything in two stages, in the reverse of the boot order that was
  actually observed, under a bounded budget;
- returns an `Outcome` to your `main`.

It is a `Future`. Your application keeps its `main`, chooses its runtime and
awaits the kernel.

## What it is not

- **Not a runtime.** It never creates one. It spawns onto the runtime you
  already started.
- **Not a web framework, ORM, router or CLI parser.** None of that is in the
  kernel; all of it is a bundle.
- **Not a process manager.** A kernel lives inside one process. It does not
  launch processes, and nothing coordinates two kernels.

The kernel names no domain entity, no transport and no technology. That is an
altitude rule, and CI enforces it with a word list rather than trusting review.

---

## Seven phases

```
Configure → Register → Resolve → Boot → Run → Shutdown → Terminated
```

| # | Phase | Kind | What happens | Failure means |
|---|---|---|---|---|
| 1 | Configure | sync | Config sources are loaded and merged into one frozen tree | the kernel is never built |
| 2 | Register | sync | Each bundle fills the `Registry`. It is blind: no container, no view of any other bundle | the kernel is never built |
| 3 | Resolve | sync | The graph is validated — contracts satisfied, no cycle, topological order computed | the kernel is never built |
| 4 | Boot | async | Components are instantiated and booted in topological order | rollback, then exit |
| 5 | Run | async | The supervisor starts the runnables and watches them | depends on criticality |
| 6 | Shutdown | async | Two stages — `Draining`, then `Stopping` — in reverse of the observed boot order | logged, reflected in the exit code |
| 7 | Terminated | — | An `Outcome` is returned to `main` | — |

### The rule that makes them worth something

> **Every graph error appears by phase three at the latest. No resolution is
> deferred to run time.**

`build()` is the barrier. If it returns `Ok`, the configuration is valid, every
contract is satisfied and the graph is sound — and nothing has run yet. After
boot, the container is *sealed*: a first shared instantiation attempted during
`Run` is an error, not a lazy convenience. A resolution that would have failed on
the first request in production is a design defect, not an operational one.

That is the price paid for dynamic, type-erased resolution, and it is the reason
the price is worth paying.

---

## A kernel that starts and stops

```rust
use std::process::ExitCode;
use std::sync::Arc;

use kernel::core::{BundleManifest, RegisterError, RunError, RunnableDescriptor};
use kernel::{BoxFuture, Bundle, Kernel, Provider, Registry, RunContext, Runnable};

/// A long-running task the kernel supervises.
struct Heartbeat;

impl Runnable for Heartbeat {
    fn name() -> &'static str {
        "heartbeat"
    }

    fn descriptor(&self) -> RunnableDescriptor {
        RunnableDescriptor::new()
    }

    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>> {
        Box::pin(async move {
            cx.shutdown().stopping().await;
            Ok(())
        })
    }
}

/// A bundle declares. It never builds.
struct Beat;

impl Bundle for Beat {
    fn manifest(&self) -> BundleManifest {
        BundleManifest::new("beat", "0.1.0")
    }

    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError> {
        registry.runnable(Provider::from_fn(|_container| {
            Box::pin(async { Ok(Arc::new(Heartbeat)) })
        }));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // Phases 1 to 3. No I/O, no instantiation. `?` is unavailable here:
    // `ExitCode` does not implement `FromResidual`.
    let kernel = match Kernel::builder().bundle(Beat).build().await {
        Ok(kernel) => kernel,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };

    // Ctrl-C reaches the same place; the handle is the programmatic path, and
    // any component can resolve one from the container.
    let handle = kernel.handle();
    tokio::spawn(async move {
        handle.shutdown();
    });

    // Phases 4 to 7.
    kernel.run().await.into_exit_code()
}
```

A fuller assembly lives in [`examples/minimal`](examples/minimal). It is the one
place in this repository where domain vocabulary is allowed.

---

## The workspace

| Crate | What it is for | Direct dependencies |
|---|---|---|
| [`kernel-core`](crates/kernel-core) | The runtime-free surfaces a `*-contracts` crate may need: ids, the error model, `ConfigTree`, `BoxFuture`, telemetry, health, descriptors | none, of any kind, and it must keep none |
| [`kernel`](crates/kernel) | `Registry`, `Container`, dispatcher, supervisor, `Kernel` — the phases themselves | `kernel-core`, `tokio` |
| [`kernel-macros`](crates/kernel-macros) | Optional derives and declaration sugar. Its own expansion code parses tokens by hand: no `syn`, no `quote` | none normal; three dev-only (`kernel-core`, `kernel`, `tokio`), which is what lets its tests prove an expansion compiles |
| [`kernel-testkit`](crates/kernel-testkit) | Test harness: binding substitution, a driven lifecycle, doubles | `kernel-core`, `kernel`, `tokio` |

Direct and transitive are different numbers, and only the first is small.
`cargo tree -p kernel --edges normal` resolves to **eleven third-party crates**:
`tokio` and, underneath it, `libc`, `mio`, `pin-project-lite`,
`signal-hook-registry`, `errno`, and — pulled in by tokio's `macros` feature —
`tokio-macros`, `proc-macro2`, `quote`, `unicode-ident` and `syn`. So `syn` is
in the graph. `kernel-macros` does not use it, and that is the whole of the
claim above: the derives expand tokens without a parser crate. It says nothing
about what tokio brings. `--no-default-features` drops the signal handler and
leaves seven: `tokio`, `pin-project-lite`, `tokio-macros`, `proc-macro2`,
`quote`, `unicode-ident`, `syn`.

`ci/check-dependencies.sh` enforces the direct column as an allowlist, and
checks `kernel-core` transitively: it must resolve to itself alone.

The split exists so that a `*-contracts` crate depends on `kernel-core` alone —
light, stable, buildable without a runtime. That is what makes the isolation rule
practical: **a `*-bundle` crate never depends on another `*-bundle` crate.**

No macro is ever required. Every macro expands to a public API you could have
written by hand, and CI builds and tests the whole suite with the macros crate
removed.

---

## Guards

A boundary that is asserted but not verified does not exist. Seven of them are
executable scripts you can run and watch fail:

| Boundary | Script |
|---|---|
| The kernel depends on nothing outside its allowlist | `ci/check-dependencies.sh` |
| No domain, transport or technology name in the kernel | `ci/check_vocabulary.py` against `ci/forbidden-words.txt` |
| No bundle depends on another bundle | `ci/check-bundle-graph.sh` |
| No production dependency enables `kernel/testing` | `ci/check-testing-feature.sh` |
| Macros are never load-bearing | `ci/check-without-macros.sh` |
| The lint configuration that holds the rest is still declared | `ci/check-lints.sh` |
| The licence files match what `Cargo.toml` claims | `ci/check-licenses.sh` |

`ci/msrv.sh` is not a guard: it reads `rust-version` out of `Cargo.toml` so that
no other script hardcodes it.

Two more boundaries are held by the code itself. They are real, they fail a run,
and there is nothing to execute that reports them green:

| Boundary | Held by |
|---|---|
| A bundle's `requires` matches what it actually resolves | container instrumentation, panics in `debug_assertions` builds |
| No lazy resolution during `Run` | `Container::seal` — a late first shared instantiation is `ContainerError::Sealed` |

`missing_docs = "deny"` sits in both halves: `ci/check-lints.sh` verifies the
declaration is still there, and the rustdoc job is what actually fails on an
undocumented item.

The [CI workflow](.github/workflows/ci.yml) runs those plus formatting, clippy
with `-D warnings` (all features and none), the test suite, a no-default-features
build and test, a build and test on the MSRV read from `Cargo.toml`, warning-free
rustdoc, and `cargo-deny` for advisories, licences and bans.

---

## Running it

```sh
cargo test --workspace --all-features --locked      # the suite
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check

./ci/check-lints.sh          # lint configuration is still declared
./ci/check-licenses.sh       # licence files match what Cargo.toml claims
./ci/check-dependencies.sh   # dependency allowlist
./ci/check_vocabulary.py     # altitude rule
./ci/check-bundle-graph.sh   # bundle isolation
./ci/check-testing-feature.sh # kernel/testing is off in every production graph
./ci/check-without-macros.sh # the suite without kernel-macros
```

`cargo-deny` is not vendored; install it with `cargo install cargo-deny --locked`
if you want to run `cargo deny --all-features check --deny warnings` locally.

---

## Where it actually stands

Verified against the current tree, not remembered:

- **Four crates**, phases one to seven implemented.
- **599 tests pass** on `cargo test --workspace`: 525 unit and integration, 74
  doctests, plus two doctests ignored on purpose. Per binary — `kernel` 279
  unit, 7 (`audit`), 11 (`lifecycle`); `kernel-core` 103 unit and no
  integration target; `kernel-macros` 14 (`from_config`), 4 (`listener`), 12
  (`provider`), 2 (`surface`); `kernel-testkit` 5 (`doubles`), 11 (`harness`),
  5 (`missing`), 11 (`substitution`); `minimal` 1; the medium example — `app` 6
  unit, 7 (`isolation`), 5 (`standalone`), `ledger-component` 15,
  `ledger-contracts` 3, `ledger-bundle` 1, `orders-contracts` 3,
  `orders-bundle` 8, `audit-contracts` 3, `audit-bundle` 9. Doctests: `kernel`
  24, `kernel-core` 36, `kernel-macros` 4, `kernel-testkit` 1, and 9 across the
  example crates.
- **Seven guards run green**, all executable scripts under `ci/`. Two further
  boundaries — `requires` conformance and the sealed container — are held by the
  code and have nothing to run; see [Guards](#guards).
- **Rust edition 2024, MSRV 1.88.0.**
- **One direct third-party dependency: `tokio`.** `kernel-core` has none of any
  kind; `kernel-macros` has none normal and three dev-only. Transitively,
  `cargo tree -p kernel --edges normal` resolves to eleven third-party crates —
  `syn` among them, through tokio, not through `kernel-macros`.
- Licensed **MIT OR Apache-2.0**.

What is missing, plainly:

- **It has never run a real workload.** Every claim above comes from its own test
  suite. No production, no staging, no benchmark. There are no performance
  numbers here because none have been taken.
- **Nothing is released.** `0.1.0` is not on crates.io, there is no tag, and the
  public surface is still moving. Expect breaking changes without ceremony.
- **Eight capabilities are deliberately out of scope for a first version**, and
  each was validated as an exclusion rather than forgotten: hot configuration
  reload, dynamic bundle loading, multiple or nested kernels, events crossing the
  process boundary, nested or named scopes, component restart, a built-in
  serialization bridge, and native metrics. Design document, section 19, states
  what each costs to reopen.
- **The testkit boundary is narrower than it was once described.** In a
  production graph — one that reaches `kernel` without a dev-dependency on
  `kernel-testkit` — `kernel/testing` is off, `KernelBuilder::__register_hook`
  does not exist, and no substitution is reachable;
  `ci/check-testing-feature.sh` fails the build if any workspace member enables
  the feature through a normal dependency, parks it in its own feature table, or
  reaches it by taking `kernel-testkit` as a normal dependency. Inside
  `cargo test` of a crate that dev-depends on the testkit, cargo unifies the
  feature across the build and any test there can call the hook directly. Rust
  offers no way to scope a feature to one crate's dev graph, so the type system
  does not hold that half and nothing here claims it does.

---

## Design document

[`docs/ideation.md`](docs/ideation.md) is the reference: vocabulary as a closed
list, every phase, every public surface, the decisions that were taken and the
ones that were refused. This README is the door; that document is the house.

It is written in French. The code, its identifiers and its documentation are
English, without exception.

---

## Licence

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT licence ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you state otherwise, any contribution you intentionally
submit for inclusion in this work is dual-licensed as above, with no additional
terms.
