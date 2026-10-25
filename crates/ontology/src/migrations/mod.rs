//! Schema-migration ledger and drift detection against a committed fingerprint
//! snapshot. Shared by the `orbit-server` unit test and the `xtask` tooling. Runtime
//! SDLC plan lowering isn't fingerprinted, and each entry carries one scope.

mod edge_rows;
mod fingerprint;
mod ledger;
mod scope;
mod verify;

pub use edge_rows::{EdgeRows, derive_edge_rows};
pub use fingerprint::{
    FINGERPRINT_FILE, Fingerprints, embedded_sources, hash_edge_row_sources, sha256_hex,
    source_fingerprints,
};
pub use ledger::{LEDGER_FILE, MigrationEntry, MigrationLedger};
pub use scope::{LedgerScope, MigrationScope, code_entity_names, derive_scope, sdlc_entity_names};
pub use verify::verify_snapshot;
