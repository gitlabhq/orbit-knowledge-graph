use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::scope::{Scope, ScopeDeclaration, sdlc_entity_names};
use crate::Ontology;

/// Ledger path relative to `CONFIG_DIR`.
pub const LEDGER_FILE: &str = "schema-migrations.yaml";

const LEDGER_HEADER: &str = "\
# yaml-language-server: $schema=schemas/schema-migrations.schema.json
#
# One entry per SCHEMA_VERSION bump, newest first (prepended by `mise schema:bump`).
# Each entry declares how much of the graph a given version invalidates:
#   scope: \"*\"    full rebuild (fail-safe; the default for anything unmapped)
#   scope: sdlc   SDLC-sourced tables; optional `entities:` narrows to a subset
#   scope: code   the code-graph tables (Definition, File, Directory, Branch,
#                 ImportedSymbol) and their edge table
#
# Humans may WIDEN an entry (scope or entities), never narrow it below the
# drift detected from the fingerprint snapshot — CI enforces this. A version
# with no entry is treated as \"*\", which also makes pruning old entries safe.
";

const EMBEDDED_LEDGER: &str = include_str!(concat!(env!("CONFIG_DIR"), "/schema-migrations.yaml"));

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationLedger {
    #[serde(default)]
    pub migrations: Vec<MigrationEntry>,
}

impl MigrationLedger {
    pub fn parse(content: &str) -> Result<Self, String> {
        serde_yaml::from_str(content).map_err(|e| format!("parsing migration ledger: {e}"))
    }

    pub fn load_embedded() -> Result<Self, String> {
        Self::parse(EMBEDDED_LEDGER)
    }

    /// Union of ledger scopes between versions; gaps and rollbacks widen to [`Scope::All`].
    #[must_use]
    pub fn invalidation_scope_between(
        &self,
        active_version: u32,
        target_version: u32,
    ) -> ScopeDeclaration {
        if active_version >= target_version {
            return ScopeDeclaration::all();
        }

        let mut scope: Option<ScopeDeclaration> = None;
        for version in (active_version + 1)..=target_version {
            let entry = self
                .migrations
                .iter()
                .find(|entry| entry.version == version);
            let declaration = match entry {
                Some(entry) => entry.declaration(),
                None => return ScopeDeclaration::all(),
            };
            scope = Some(match scope {
                Some(accumulated) => accumulated.widened_with(&declaration),
                None => declaration,
            });
        }
        scope.unwrap_or_else(ScopeDeclaration::all)
    }

    /// Serialized YAML with the usage header.
    #[must_use]
    pub fn render(&self) -> String {
        let body = serde_yaml::to_string(self).expect("ledger serializes");
        format!("{LEDGER_HEADER}{body}")
    }

    /// The current entry: newest first, so the top of the list.
    #[must_use]
    pub fn latest(&self) -> Option<&MigrationEntry> {
        self.migrations.first()
    }

    pub fn validate(&self, ontology: &Ontology, schema_version: u32) -> Result<(), String> {
        if self.migrations.is_empty() {
            return Err("migration ledger has no entries".into());
        }

        let mut prev: Option<u32> = None;
        for entry in &self.migrations {
            if let Some(prev) = prev
                && entry.version >= prev
            {
                return Err(format!(
                    "ledger versions must strictly decrease (newest first): {prev} is followed by {}",
                    entry.version
                ));
            }
            prev = Some(entry.version);

            if !entry.entities.is_empty() && entry.scope != Scope::Sdlc {
                return Err(format!(
                    "version {}: `entities:` is only valid with `scope: sdlc`",
                    entry.version
                ));
            }
        }

        let latest = self.latest().expect("non-empty checked above");
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

/// One `SCHEMA_VERSION` bump.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationEntry {
    pub version: u32,
    pub scope: Scope,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub entities: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl MigrationEntry {
    #[must_use]
    pub fn declaration(&self) -> ScopeDeclaration {
        ScopeDeclaration {
            scope: self.scope,
            entities: self.entities.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entities(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn versions_must_strictly_decrease() {
        let ledger = MigrationLedger {
            migrations: vec![
                MigrationEntry {
                    version: 5,
                    scope: Scope::All,
                    entities: BTreeSet::new(),
                    note: None,
                },
                MigrationEntry {
                    version: 5,
                    scope: Scope::All,
                    entities: BTreeSet::new(),
                    note: None,
                },
            ],
        };
        let ontology = Ontology::new();
        let err = ledger.validate(&ontology, 5).unwrap_err();
        assert!(err.contains("strictly decrease"), "{err}");
    }

    #[test]
    fn newest_first_ordering_validates() {
        let ledger = MigrationLedger {
            migrations: vec![
                MigrationEntry {
                    version: 82,
                    scope: Scope::Code,
                    entities: BTreeSet::new(),
                    note: None,
                },
                MigrationEntry {
                    version: 81,
                    scope: Scope::All,
                    entities: BTreeSet::new(),
                    note: None,
                },
            ],
        };
        let ontology = Ontology::new();
        ledger.validate(&ontology, 82).unwrap();
    }

    #[test]
    fn top_version_must_equal_schema_version() {
        let ledger = MigrationLedger {
            migrations: vec![MigrationEntry {
                version: 5,
                scope: Scope::All,
                entities: BTreeSet::new(),
                note: None,
            }],
        };
        let ontology = Ontology::new();
        let err = ledger.validate(&ontology, 6).unwrap_err();
        assert!(err.contains("must equal SCHEMA_VERSION"), "{err}");
    }

    #[test]
    fn entities_require_sdlc_scope() {
        let ledger = MigrationLedger {
            migrations: vec![MigrationEntry {
                version: 1,
                scope: Scope::Code,
                entities: entities(&["Note"]),
                note: None,
            }],
        };
        let ontology = Ontology::new();
        let err = ledger.validate(&ontology, 1).unwrap_err();
        assert!(err.contains("only valid with `scope: sdlc`"), "{err}");
    }

    fn ledger(entries: Vec<MigrationEntry>) -> MigrationLedger {
        MigrationLedger {
            migrations: entries,
        }
    }

    fn entry(version: u32, scope: Scope, entity_names: &[&str]) -> MigrationEntry {
        MigrationEntry {
            version,
            scope,
            entities: entities(entity_names),
            note: None,
        }
    }

    #[test]
    fn invalidation_scope_single_entry_in_range() {
        let ledger = ledger(vec![entry(81, Scope::Sdlc, &["Note"])]);
        assert_eq!(
            ledger.invalidation_scope_between(80, 81),
            ScopeDeclaration {
                scope: Scope::Sdlc,
                entities: entities(&["Note"]),
            }
        );
    }

    #[test]
    fn invalidation_scope_unions_across_range() {
        let ledger = ledger(vec![
            entry(83, Scope::Sdlc, &["Issue"]),
            entry(82, Scope::Sdlc, &["Note"]),
        ]);
        assert_eq!(
            ledger.invalidation_scope_between(81, 83),
            ScopeDeclaration {
                scope: Scope::Sdlc,
                entities: entities(&["Issue", "Note"]),
            }
        );
    }

    #[test]
    fn invalidation_scope_gap_in_range_widens_to_all() {
        let ledger = ledger(vec![entry(83, Scope::Sdlc, &["Note"])]);
        assert_eq!(
            ledger.invalidation_scope_between(81, 83),
            ScopeDeclaration::all()
        );
    }

    #[test]
    fn invalidation_scope_rollback_direction_is_all() {
        let ledger = ledger(vec![entry(82, Scope::Sdlc, &["Note"])]);
        assert_eq!(
            ledger.invalidation_scope_between(83, 82),
            ScopeDeclaration::all()
        );
        assert_eq!(
            ledger.invalidation_scope_between(82, 82),
            ScopeDeclaration::all()
        );
    }

    #[test]
    fn invalidation_scope_mixing_code_and_sdlc_widens_to_all() {
        let ledger = ledger(vec![
            entry(82, Scope::Code, &[]),
            entry(81, Scope::Sdlc, &["Note"]),
        ]);
        assert_eq!(
            ledger.invalidation_scope_between(80, 82),
            ScopeDeclaration::all()
        );
    }

    #[test]
    fn embedded_ledger_loads() {
        let ledger = MigrationLedger::load_embedded().expect("embedded ledger must parse");
        assert!(!ledger.migrations.is_empty());
    }

    #[test]
    fn entity_names_must_resolve() {
        let ledger = MigrationLedger {
            migrations: vec![MigrationEntry {
                version: 1,
                scope: Scope::Sdlc,
                entities: entities(&["Ghost"]),
                note: None,
            }],
        };
        let ontology = Ontology::new();
        let err = ledger.validate(&ontology, 1).unwrap_err();
        assert!(err.contains("is not an SDLC entity"), "{err}");
    }
}
