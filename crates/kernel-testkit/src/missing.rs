//! What a bundle needs and nobody provides.

use kernel::{Bundle, KernelBuilder, MemorySource};
use kernel_core::{ConfigSource, ContractRef, KernelError, ResolveError};

/// What [`missing_contracts`] resolves a bundle with: nothing at all.
///
/// A typed empty array rather than an iterator of an inferred type, so the
/// delegation names the case instead of leaving a turbofish at the call site.
const NO_SOURCES: [MemorySource; 0] = [];

/// Resolves `bundle` alone, with no configuration, and returns the contracts
/// nothing satisfies.
///
/// Design section 18: a bundle can be booted on its own, and phase three
/// reports the unsatisfied contracts as a list. That list IS the list of
/// doubles the test has to write — so this function turns "what do I have to
/// stub?" from guesswork into an answer the kernel already computed.
///
/// `Ok(vec![])` means the bundle stands alone. `Err` means the assembly never
/// reached phase three, so no list exists to give.
///
/// A bundle that reads configuration in `register` needs
/// [`missing_contracts_with`]; with no source to read it fails before phase
/// three and this reports the failure rather than a list.
///
/// # Errors
///
/// The bundle is resolved with NO configuration source, so a bundle that reads
/// configuration in `register` fails there and this reports
/// [`KernelError::Register`]. Any other pre-resolution failure is reported the
/// same way. Answering `Ok(vec![])` to those would read as "this bundle stands
/// alone", which is the one thing that is not known.
pub fn missing_contracts(bundle: impl Bundle) -> Result<Vec<ContractRef>, KernelError> {
    missing_contracts_with(bundle, NO_SOURCES)
}

/// The same question, asked of a bundle that reads its configuration.
///
/// [`missing_contracts`] builds with no source at all, which answers `Err` for
/// every bundle that reads a key in `register` — the shape most bundles have,
/// and the one the list is worth most for. The sources are appended in
/// iteration order and later ones win leaf by leaf, exactly as
/// [`KernelBuilder::config_source`] appends them.
///
/// The list still describes contracts, not configuration: a key the bundle
/// reads and the sources do not carry is a `register` failure and comes back
/// as `Err`, because a bundle that never registered has nothing to resolve.
///
/// # Errors
///
/// Whatever kept the assembly from reaching phase three — a `register` that
/// failed on a key none of `sources` carries, or a manifest phase three
/// refused. Only a graph that was walked can say what is missing from it.
pub fn missing_contracts_with<S: ConfigSource>(
    bundle: impl Bundle,
    sources: impl IntoIterator<Item = S>,
) -> Result<Vec<ContractRef>, KernelError> {
    // Collected here rather than inside the worker: the iterator itself is the
    // caller's, and only the sources have to cross the thread.
    let sources: Vec<S> = sources.into_iter().collect();

    // Phases one to three touch nothing outside the process, but `build` is
    // still a future and this is a synchronous call. A runtime of its own, on a
    // thread of its own, answers it from inside a `#[tokio::test]` — where
    // blocking on the ambient runtime would panic — exactly as from outside
    // one, and a paused clock in the calling test cannot reach it.
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime to resolve the bundle on");
        runtime.block_on(unsatisfied(bundle, sources))
    });

    match worker.join() {
        Ok(contracts) => contracts,
        // The resolution unwound. Re-raising the original payload keeps the
        // panic the caller sees the panic that happened.
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Every contract phase three reported as unsatisfied, once each.
///
/// The same contract is reported once per unit that asked for it — a provider
/// and the manifest of the bundle it belongs to both count — because three
/// units that need it are three units to change. A list of doubles to write is
/// the other question: what matters there is how many doubles, so the
/// duplicates collapse and the first mention fixes the order.
///
/// A build that succeeded, or one that failed on something other than
/// resolution, yields no list of missing contracts: the first stands alone, and
/// the second is handed back to the caller as the error it is.
async fn unsatisfied<S: ConfigSource>(
    bundle: impl Bundle,
    sources: Vec<S>,
) -> Result<Vec<ContractRef>, KernelError> {
    let mut builder = KernelBuilder::new();
    for source in sources {
        builder = builder.config_source(source);
    }

    let errors = match builder.bundle(bundle).build().await {
        Ok(_) => return Ok(Vec::new()),
        Err(KernelError::Resolve(errors)) => errors,
        Err(error) => return Err(error),
    };

    let mut contracts: Vec<ContractRef> = Vec::new();
    for error in &errors {
        if let ResolveError::MissingContract { contract, .. } = error
            && !contracts.contains(contract)
        {
            contracts.push(*contract);
        }
    }
    Ok(contracts)
}
