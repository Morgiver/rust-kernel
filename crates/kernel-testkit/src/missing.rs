//! What a bundle needs and nobody provides.

use kernel::{Bundle, KernelBuilder};
use kernel_core::{ContractRef, KernelError, ResolveError};

/// Resolves `bundle` alone and returns the contracts nothing satisfies.
///
/// Design section 18: a bundle can be booted on its own, and phase three
/// reports the unsatisfied contracts as a list. That list IS the list of
/// doubles the test has to write — so this function turns "what do I have to
/// stub?" from guesswork into an answer the kernel already computed.
///
/// `Ok(vec![])` means the bundle stands alone. `Err` means the assembly never
/// reached phase three, so no list exists to give.
///
/// # Errors
///
/// The bundle is resolved with NO configuration source, so a bundle that reads
/// configuration in `register` fails there and this reports
/// [`KernelError::Register`]. Any other pre-resolution failure is reported the
/// same way. Answering `Ok(vec![])` to those would read as "this bundle stands
/// alone", which is the one thing that is not known.
pub fn missing_contracts(bundle: impl Bundle) -> Result<Vec<ContractRef>, KernelError> {
    // Phases one to three touch nothing outside the process, but `build` is
    // still a future and this is a synchronous call. A runtime of its own, on a
    // thread of its own, answers it from inside a `#[tokio::test]` — where
    // blocking on the ambient runtime would panic — exactly as from outside
    // one, and a paused clock in the calling test cannot reach it.
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime to resolve the bundle on");
        runtime.block_on(unsatisfied(bundle))
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
async fn unsatisfied(bundle: impl Bundle) -> Result<Vec<ContractRef>, KernelError> {
    let errors = match KernelBuilder::new().bundle(bundle).build().await {
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
