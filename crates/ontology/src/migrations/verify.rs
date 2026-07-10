use std::collections::BTreeSet;

use super::{Fingerprints, MigrationLedger};
use crate::Ontology;

pub fn verify_snapshot(
    ontology: &Ontology,
    current: &Fingerprints,
    committed: &Fingerprints,
    ledger: &MigrationLedger,
    schema_version: u32,
) -> Result<(), String> {
    if !current.has_same_versioned_fingerprints_as(committed) {
        let (sources, ddl) = current.get_versioned_diff_keys_between(committed);
        return Err(format!(
            "versioned schema drift not reflected in the fingerprint snapshot.\n  \
             changed sources: {}\n  changed tables: {}\n\
             Run `mise schema:bump` to record the change.",
            format_set(&sources),
            format_set(&ddl),
        ));
    }
    let active_objects = current.get_active_object_diff_keys_between(committed);
    if !active_objects.is_empty() {
        return Err(format!(
            "active schema object drift not reflected in the fingerprint snapshot.\n  \
             changed objects: {}\n\
             Run `mise schema:snapshot` to record the change without re-indexing.",
            format_set(&active_objects),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::migrations::{MigrationEntry, Scope};

    const SCHEMA_VERSION: u32 = 1;

    fn fingerprints() -> Fingerprints {
        Fingerprints {
            sources: BTreeMap::from([("schema.yaml".to_string(), "source".to_string())]),
            ddl: BTreeMap::from([("gl_note".to_string(), "table".to_string())]),
            active_objects: BTreeMap::from([(
                "materialized_view/v1_refresh".to_string(),
                "view".to_string(),
            )]),
        }
    }

    fn ledger() -> MigrationLedger {
        MigrationLedger {
            migrations: vec![MigrationEntry {
                version: SCHEMA_VERSION,
                scope: Scope::All,
                entities: BTreeSet::new(),
                note: None,
            }],
        }
    }

    #[test]
    fn active_object_drift_requests_snapshot_without_schema_bump() {
        let committed = fingerprints();
        let mut current = committed.clone();
        current.active_objects.insert(
            "materialized_view/v1_refresh".to_string(),
            "changed".to_string(),
        );

        let error = verify_snapshot(
            &Ontology::new(),
            &current,
            &committed,
            &ledger(),
            SCHEMA_VERSION,
        )
        .unwrap_err();

        assert!(error.contains("mise schema:snapshot"), "{error}");
        assert!(!error.contains("mise schema:bump"), "{error}");
    }

    #[test]
    fn versioned_ddl_drift_requests_schema_bump() {
        let committed = fingerprints();
        let mut current = committed.clone();
        current
            .ddl
            .insert("gl_note".to_string(), "changed".to_string());

        let error = verify_snapshot(
            &Ontology::new(),
            &current,
            &committed,
            &ledger(),
            SCHEMA_VERSION,
        )
        .unwrap_err();

        assert!(error.contains("mise schema:bump"), "{error}");
        assert!(!error.contains("mise schema:snapshot"), "{error}");
    }
}
