use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use query_data_model::{Catalog, ClickHouse, Model, Packages};
use serde::{Deserialize, Serialize};

use crate::archive::ModelArchive;
use crate::diff::{DerivedScope, derive_scope};
use crate::scope::{EdgeRows, LedgerScope, MigrationScope, sdlc_entity_names};

pub const LEDGER_FILE: &str = "schema-migrations.yaml";

const EMBEDDED_LEDGER: &str =
    include_str!(concat!(env!("ORBIT_CONFIG_DIR"), "/schema-migrations.yaml"));

const LEDGER_HEADER: &str = "\
# yaml-language-server: $schema=schemas/schema-migrations.schema.json
#
# One entry per SCHEMA_VERSION bump, newest first (prepended by `mise schema:bump`).
# Each entry declares how much of the graph a given version invalidates:
#   scope: \"*\"    full rebuild (fail-safe; the default for anything unmapped)
#   scope: sdlc   SDLC-sourced tables; optional `entities:` narrows to a subset
#   scope: code   the code-graph tables (Definition, File, Directory, Branch,
#                 ImportedSymbol) and their edge table
#   scope: none   re-index nothing; no table changes its layout or contents
#
# `mise schema:bump` derives the scope by diffing the physical catalogs of the
# previous and new model archives. Humans may WIDEN an entry (scope or entities),
# for example for an indexer code change the model cannot show, never narrow it
# below the derived scope; CI enforces this. A version with no entry is treated
# as \"*\", which also makes pruning old entries safe.
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
    #[serde(default, skip_serializing_if = "EdgeRows::is_changed")]
    pub edge_rows: EdgeRows,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl MigrationEntry {
    #[must_use]
    pub fn new(version: u32, scope: &MigrationScope, note: Option<String>) -> Self {
        let (scope, entities, edge_rows) = match scope {
            MigrationScope::Full => (LedgerScope::All, BTreeSet::new(), EdgeRows::Changed),
            MigrationScope::Code => (LedgerScope::Code, BTreeSet::new(), EdgeRows::Changed),
            MigrationScope::Sdlc {
                entities,
                edge_rows,
            } => (LedgerScope::Sdlc, entities.clone(), *edge_rows),
            MigrationScope::None => (LedgerScope::None, BTreeSet::new(), EdgeRows::Changed),
        };
        Self {
            version,
            scope,
            entities,
            edge_rows,
            note,
        }
    }

    #[must_use]
    pub fn to_migration_scope(&self) -> MigrationScope {
        match self.scope {
            LedgerScope::All => MigrationScope::Full,
            LedgerScope::Code => MigrationScope::Code,
            LedgerScope::Sdlc => MigrationScope::Sdlc {
                entities: self.entities.clone(),
                edge_rows: self.edge_rows,
            },
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
        catalog: &Catalog<ClickHouse>,
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
        }

        let latest = self.latest_entry().expect("non-empty checked above");
        if latest.version != schema_version {
            return Err(format!(
                "top ledger version {} must equal SCHEMA_VERSION {schema_version}",
                latest.version
            ));
        }

        let names = sdlc_entity_names(catalog);
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

const REMEDIATION: &str = "Run `mise schema:bump` to record the change.";

/// Checks the committed ledger and the current version's archives against the embedded packages.
pub fn verify_committed(config_dir: &Path, schema_version: u32) -> Result<MigrationLedger, String> {
    let path = config_dir.join(LEDGER_FILE);
    let ledger = MigrationLedger::parse(
        &std::fs::read_to_string(&path)
            .map_err(|error| format!("reading {}: {error}", path.display()))?,
    )?;
    let catalog = Catalog::<ClickHouse>::embedded().map_err(|error| error.to_string())?;
    ledger.validate(&catalog, schema_version)?;

    let read = |path: &Path| {
        std::fs::read(path).map_err(|error| {
            format!(
                "reading {}: {error}. Run `mise schema:bump` to write the current archives.",
                path.display()
            )
        })
    };
    let ontology = OntologyArchive::from_bytes(
        schema_version,
        &read(&OntologyArchive::path(config_dir, schema_version))?,
    )
    .map_err(|error| error.to_string())?;
    if !ontology.matches_sources(&embedded_sources()) {
        return Err(format!(
            "ontology archive v{schema_version} is stale. {REMEDIATION}"
        ));
    }
    let model = ModelArchive::from_bytes(
        schema_version,
        &read(&ModelArchive::path(config_dir, schema_version))?,
    )
    .map_err(|error| error.to_string())?;
    if !model.matches(&Packages::embedded()) {
        return Err(format!(
            "model archive v{schema_version} does not match the packages. {REMEDIATION}"
        ));
    }
    Ok(ledger)
}

/// Fails when the entry for `schema_version` is narrower than the scope derived from `previous`.
pub fn verify_entry_covers_derived_scope(
    ledger: &MigrationLedger,
    previous: &Arc<Model>,
    schema_version: u32,
) -> Result<DerivedScope, String> {
    let next = Packages::embedded()
        .load()
        .map_err(|error| error.to_string())?;
    let derived = derive_scope(previous, &next).map_err(|error| error.to_string())?;
    let entry = ledger
        .migrations
        .iter()
        .find(|entry| entry.version == schema_version)
        .ok_or_else(|| format!("no ledger entry for version {schema_version}. {REMEDIATION}"))?;
    if !entry.to_migration_scope().covers(&derived.scope) {
        return Err(format!(
            "ledger entry for version {schema_version} is narrower than the derived {}; \
             changed tables {:?}. Widen the entry or run `mise schema:bump`.",
            derived.scope, derived.tables
        ));
    }
    Ok(derived)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{entity_set, sdlc_scope};

    fn entry(version: u32, scope: LedgerScope, entity_names: &[&str]) -> MigrationEntry {
        MigrationEntry {
            version,
            scope,
            entities: entity_set(entity_names),
            edge_rows: EdgeRows::Changed,
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

        assert_eq!(l.resolve_scope_between(80, 81), sdlc_scope(&["User"]),);
        assert_eq!(
            l.resolve_scope_between(81, 83),
            sdlc_scope(&["Issue", "Note"]),
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
        assert_eq!(l.resolve_scope_between(81, 83), sdlc_scope(&["Note"]),);

        let all_none = ledger(vec![
            entry(83, LedgerScope::None, &[]),
            entry(82, LedgerScope::None, &[]),
        ]);
        assert_eq!(all_none.resolve_scope_between(81, 83), MigrationScope::None);
    }

    #[test]
    fn scope_resolution_keeps_edge_rows_unchanged_only_when_every_sdlc_entry_does() {
        let l = MigrationLedger::parse(
            "migrations:\n\
             - {version: 84, scope: sdlc, entities: [Issue]}\n\
             - {version: 83, scope: none, note: output-neutral}\n\
             - {version: 82, scope: sdlc, entities: [Note], edge_rows: unchanged}\n\
             - {version: 81, scope: sdlc, entities: [User], edge_rows: unchanged}\n",
        )
        .unwrap();

        assert_eq!(
            l.resolve_scope_between(80, 83),
            MigrationScope::Sdlc {
                entities: entity_set(&["Note", "User"]),
                edge_rows: EdgeRows::Unchanged,
            },
        );
        assert_eq!(
            l.resolve_scope_between(80, 84),
            sdlc_scope(&["Issue", "Note", "User"]),
        );
    }

    #[test]
    fn validation_rejects_malformed_ledgers() {
        let catalog = Catalog::<ClickHouse>::embedded().unwrap();

        let dup = ledger(vec![
            entry(5, LedgerScope::All, &[]),
            entry(5, LedgerScope::All, &[]),
        ]);
        assert!(
            dup.validate(&catalog, 5)
                .unwrap_err()
                .contains("strictly decrease")
        );

        let wrong_scope = ledger(vec![entry(1, LedgerScope::Code, &["Note"])]);
        assert!(
            wrong_scope
                .validate(&catalog, 1)
                .unwrap_err()
                .contains("only valid with")
        );

        let wrong_ver = ledger(vec![entry(5, LedgerScope::All, &[])]);
        assert!(
            wrong_ver
                .validate(&catalog, 6)
                .unwrap_err()
                .contains("must equal")
        );

        let ghost = ledger(vec![entry(1, LedgerScope::Sdlc, &["Ghost"])]);
        assert!(
            ghost
                .validate(&catalog, 1)
                .unwrap_err()
                .contains("is not an SDLC entity")
        );
    }
}
