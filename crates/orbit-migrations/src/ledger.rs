use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::scope::{LedgerScope, MigrationScope, sdlc_entity_names};

pub const LEDGER_FILE: &str = "schema-migrations.yaml";

const EMBEDDED_LEDGER: &str = include_str!(concat!(env!("CONFIG_DIR"), "/schema-migrations.yaml"));

const LEDGER_HEADER: &str = "\
# yaml-language-server: $schema=schemas/schema-migrations.schema.json
#
# One entry per SCHEMA_VERSION bump, newest first (prepended by `mise schema:bump`).
# Each entry declares how much of the graph a given version invalidates:
#   scope: \"*\"    full rebuild (fail-safe; the default for anything unmapped)
#   scope: sdlc   SDLC-sourced tables; optional `entities:` narrows to a subset
#   scope: code   the code-graph tables (Definition, File, Directory, Branch,
#                 ImportedSymbol) and their edge table
#   scope: none   re-index NOTHING; the source text changed but the produced
#                 output is byte-identical. Requires a `note:` certifying
#                 output-neutrality. This deliberately bypasses the
#                 under-declaration guard, so the note is mandatory and audited.
#
# Humans may WIDEN an entry (scope or entities), never narrow it below the
# drift detected from the fingerprint snapshot — CI enforces this, except for a
# note-gated `scope: none` entry, the certified output-neutral escape hatch. A
# version with no entry is treated as \"*\", which also makes pruning old entries safe.
";

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationLedger {
    #[serde(default)]
    pub migrations: Vec<MigrationEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationEntry {
    pub version: u32,
    pub scope: LedgerScope,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub entities: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl MigrationEntry {
    #[must_use]
    pub fn to_migration_scope(&self) -> MigrationScope {
        match self.scope {
            LedgerScope::All => MigrationScope::Full,
            LedgerScope::Code => MigrationScope::Code,
            LedgerScope::Sdlc => MigrationScope::Sdlc(self.entities.clone()),
            LedgerScope::None => MigrationScope::None,
        }
    }
}

impl MigrationLedger {
    pub fn parse(content: &str) -> Result<Self, String> {
        orbit_utils::yaml::from_str(content)
            .map_err(|error| format!("parsing migration ledger: {error}"))
    }

    pub fn load_embedded() -> Result<Self, String> {
        Self::parse(EMBEDDED_LEDGER)
    }

    #[must_use]
    pub fn latest_entry(&self) -> Option<&MigrationEntry> {
        self.migrations.first()
    }

    #[must_use]
    pub fn render_as_yaml(&self) -> String {
        let body = orbit_utils::yaml::to_string(self).expect("ledger serializes");
        format!("{LEDGER_HEADER}{body}")
    }

    #[must_use]
    pub fn resolve_scope_between(
        &self,
        active_version: u32,
        target_version: u32,
    ) -> MigrationScope {
        if active_version >= target_version {
            return MigrationScope::Full;
        }

        let mut scope: Option<MigrationScope> = None;
        for version in (active_version + 1)..=target_version {
            let entry_scope = match self.migrations.iter().find(|e| e.version == version) {
                Some(entry) => entry.to_migration_scope(),
                None => return MigrationScope::Full,
            };
            scope = Some(match scope {
                Some(accumulated) => accumulated.merged_with(&entry_scope),
                None => entry_scope,
            });
        }
        scope.unwrap_or(MigrationScope::Full)
    }

    pub fn validate(
        &self,
        ontology: &ontology::Ontology,
        schema_version: u32,
    ) -> Result<(), String> {
        if self.migrations.is_empty() {
            return Err("migration ledger has no entries".into());
        }

        let mut previous_version: Option<u32> = None;
        for entry in &self.migrations {
            if let Some(previous) = previous_version
                && entry.version >= previous
            {
                return Err(format!(
                    "ledger versions must strictly decrease (newest first): \
                     {previous} is followed by {}",
                    entry.version
                ));
            }
            previous_version = Some(entry.version);

            if !entry.entities.is_empty() && entry.scope != LedgerScope::Sdlc {
                return Err(format!(
                    "version {}: `entities:` is only valid with `scope: sdlc`",
                    entry.version
                ));
            }

            if entry.scope == LedgerScope::None
                && entry.note.as_ref().is_none_or(|n| n.trim().is_empty())
            {
                return Err(format!(
                    "version {}: `scope: none` requires a non-empty `note:` \
                     certifying the source change is output-neutral",
                    entry.version
                ));
            }
        }

        let latest = self.latest_entry().expect("non-empty checked above");
        if latest.version != schema_version {
            return Err(format!(
                "top ledger version {} must equal SCHEMA_VERSION {schema_version}",
                latest.version
            ));
        }

        let names = sdlc_entity_names(ontology);
        for entry in &self.migrations {
            for entity in &entry.entities {
                if !names.contains(entity) {
                    return Err(format!(
                        "version {}: entity '{entity}' is not an SDLC entity or relationship kind",
                        entry.version
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::entity_set;

    fn entry(version: u32, scope: LedgerScope, entity_names: &[&str]) -> MigrationEntry {
        MigrationEntry {
            version,
            scope,
            entities: entity_set(entity_names),
            note: None,
        }
    }

    fn ledger(entries: Vec<MigrationEntry>) -> MigrationLedger {
        MigrationLedger {
            migrations: entries,
        }
    }

    #[test]
    fn embedded_ledger_loads() {
        let ledger = MigrationLedger::load_embedded().expect("embedded ledger must parse");
        assert!(!ledger.migrations.is_empty());
    }

    #[test]
    fn scope_resolution_unions_entries_and_widens_on_gaps_or_rollback() {
        let l = ledger(vec![
            entry(83, LedgerScope::Sdlc, &["Issue"]),
            entry(82, LedgerScope::Sdlc, &["Note"]),
            entry(81, LedgerScope::Sdlc, &["User"]),
        ]);

        assert_eq!(
            l.resolve_scope_between(80, 81),
            MigrationScope::Sdlc(entity_set(&["User"])),
        );
        assert_eq!(
            l.resolve_scope_between(81, 83),
            MigrationScope::Sdlc(entity_set(&["Issue", "Note"])),
        );
        assert_eq!(l.resolve_scope_between(83, 82), MigrationScope::Full);
        assert_eq!(l.resolve_scope_between(82, 82), MigrationScope::Full);

        let with_gap = ledger(vec![entry(83, LedgerScope::Sdlc, &["Note"])]);
        assert_eq!(with_gap.resolve_scope_between(81, 83), MigrationScope::Full);
    }

    #[test]
    fn scope_resolution_none_entries_are_absorbed() {
        let l = ledger(vec![
            entry(83, LedgerScope::None, &[]),
            entry(82, LedgerScope::Sdlc, &["Note"]),
        ]);
        assert_eq!(
            l.resolve_scope_between(81, 83),
            MigrationScope::Sdlc(entity_set(&["Note"])),
        );

        let all_none = ledger(vec![
            entry(83, LedgerScope::None, &[]),
            entry(82, LedgerScope::None, &[]),
        ]);
        assert_eq!(all_none.resolve_scope_between(81, 83), MigrationScope::None);
    }

    #[test]
    fn validation_rejects_malformed_ledgers() {
        let ontology = ontology::Ontology::new();

        let dup = ledger(vec![
            entry(5, LedgerScope::All, &[]),
            entry(5, LedgerScope::All, &[]),
        ]);
        assert!(
            dup.validate(&ontology, 5)
                .unwrap_err()
                .contains("strictly decrease")
        );

        let wrong_scope = ledger(vec![entry(1, LedgerScope::Code, &["Note"])]);
        assert!(
            wrong_scope
                .validate(&ontology, 1)
                .unwrap_err()
                .contains("only valid with")
        );

        let no_note = ledger(vec![MigrationEntry {
            version: 1,
            scope: LedgerScope::None,
            entities: BTreeSet::new(),
            note: None,
        }]);
        assert!(
            no_note
                .validate(&ontology, 1)
                .unwrap_err()
                .contains("requires a non-empty")
        );

        let wrong_ver = ledger(vec![entry(5, LedgerScope::All, &[])]);
        assert!(
            wrong_ver
                .validate(&ontology, 6)
                .unwrap_err()
                .contains("must equal")
        );

        let ghost = ledger(vec![entry(1, LedgerScope::Sdlc, &["Ghost"])]);
        assert!(
            ghost
                .validate(&ontology, 1)
                .unwrap_err()
                .contains("is not an SDLC entity")
        );
    }
}
