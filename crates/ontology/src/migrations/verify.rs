use std::collections::BTreeSet;

use super::{Fingerprints, MigrationLedger};
use crate::Ontology;

/// Compares `current` fingerprints against the `committed` snapshot and
/// validates the ledger. Shared by `gkg-server`'s build script and
/// `cargo xtask migration-ledger check`.
pub fn verify_snapshot(
    ontology: &Ontology,
    current: &Fingerprints,
    committed: &Fingerprints,
    ledger: &MigrationLedger,
    schema_version: u32,
) -> Result<(), String> {
    if current != committed {
        let (sources, ddl) = current.diff(committed);
        return Err(format!(
            "ontology drift not reflected in the fingerprint snapshot.\n  \
             changed sources: {}\n  changed tables: {}\n\
             Run `mise schema:bump` to record the change.",
            format_set(&sources),
            format_set(&ddl),
        ));
    }
    ledger.validate(ontology, schema_version)
}

fn format_set(set: &BTreeSet<String>) -> String {
    if set.is_empty() {
        "(none)".to_string()
    } else {
        set.iter().cloned().collect::<Vec<_>>().join(", ")
    }
}
