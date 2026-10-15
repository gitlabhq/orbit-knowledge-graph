use std::path::Path;

use ontology::Ontology;
use ontology::archive::OntologyArchive;
use ontology::migrations::{
    self, FINGERPRINT_FILE, Fingerprints, LEDGER_FILE, MigrationLedger, embedded_sources,
};

use crate::fingerprint::current_fingerprints;

const REMEDIATION: &str = "Run `mise schema:bump` to record the change and re-snapshot.";

/// Committed state that passed [`verify_committed_snapshot`].
pub struct VerifiedSnapshot {
    pub committed: Fingerprints,
    pub ledger: MigrationLedger,
}

/// Verifies the committed fingerprint snapshot, migration ledger and current
/// ontology archive in `config_dir` against the embedded ontology.
pub fn verify_committed_snapshot(
    ontology: &Ontology,
    config_dir: &Path,
    schema_version: u32,
) -> Result<VerifiedSnapshot, String> {
    let current = current_fingerprints(ontology).map_err(|error| error.to_string())?;

    let fingerprint_path = config_dir.join(FINGERPRINT_FILE);
    let committed_text = std::fs::read_to_string(&fingerprint_path).map_err(|error| {
        format!(
            "reading fingerprint snapshot {}: {error}. {REMEDIATION}",
            fingerprint_path.display()
        )
    })?;
    let committed = Fingerprints::parse(&committed_text)?;

    let ledger_path = config_dir.join(LEDGER_FILE);
    let ledger_text = std::fs::read_to_string(&ledger_path)
        .map_err(|error| format!("reading {}: {error}", ledger_path.display()))?;
    let ledger = MigrationLedger::parse(&ledger_text)?;

    migrations::verify_snapshot(ontology, &current, &committed, &ledger, schema_version)?;

    let archive_path = OntologyArchive::path(config_dir, schema_version);
    let archive_bytes = std::fs::read(&archive_path).map_err(|error| {
        format!(
            "reading {}: {error}. Run `mise schema:snapshot` to recreate a missing archive.",
            archive_path.display()
        )
    })?;
    let archive = OntologyArchive::from_bytes(schema_version, &archive_bytes)
        .map_err(|error| error.to_string())?;
    if !archive.matches_sources(&embedded_sources()) {
        return Err(format!("ontology archive is stale. {REMEDIATION}"));
    }

    Ok(VerifiedSnapshot { committed, ledger })
}
