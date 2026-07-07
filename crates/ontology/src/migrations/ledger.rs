use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::scope::{InvalidationScope, Scope, ScopeDeclaration, SdlcScope, sdlc_entity_names};
use crate::Ontology;

/// Ledger path relative to `CONFIG_DIR`.
pub const LEDGER_FILE: &str = "schema-migrations.yaml";

const LEDGER_HEADER: &str = "\
# yaml-language-server: $schema=schemas/schema-migrations.schema.json
#
# One entry per SCHEMA_VERSION bump, appended by `mise schema:bump`.
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

const LEDGER_EMBEDDED: &str = include_str!(concat!(env!("CONFIG_DIR"), "/schema-migrations.yaml"));

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

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationLedger {
    #[serde(default)]
    pub migrations: Vec<MigrationEntry>,
}

impl MigrationLedger {
    pub fn load_embedded() -> Result<Self, String> {
        Self::parse(LEDGER_EMBEDDED)
    }

    pub fn parse(content: &str) -> Result<Self, String> {
        serde_yaml::from_str(content).map_err(|e| format!("parsing migration ledger: {e}"))
    }

    /// Serialized YAML with the usage header.
    #[must_use]
    pub fn render(&self) -> String {
        let body = serde_yaml::to_string(self).expect("ledger serializes");
        format!("{LEDGER_HEADER}{body}")
    }

    #[must_use]
    pub fn last(&self) -> Option<&MigrationEntry> {
        self.migrations.last()
    }

    pub fn validate(&self, ontology: &Ontology, schema_version: u32) -> Result<(), String> {
        if self.migrations.is_empty() {
            return Err("migration ledger has no entries".into());
        }

        let mut prev: Option<u32> = None;
        for entry in &self.migrations {
            if let Some(prev) = prev
                && entry.version <= prev
            {
                return Err(format!(
                    "ledger versions must strictly increase: {prev} is followed by {}",
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

        let last = self.last().expect("non-empty checked above");
        if last.version != schema_version {
            return Err(format!(
                "last ledger version {} must equal SCHEMA_VERSION {schema_version}",
                last.version
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

    /// Union of entry scopes over `(active_version, embedded_version]`; a version
    /// with no entry invalidates the whole graph.
    #[must_use]
    pub fn invalidation_between(
        &self,
        active_version: u32,
        embedded_version: u32,
    ) -> InvalidationScope {
        if embedded_version <= active_version {
            return InvalidationScope::none();
        }

        let entries_by_version: BTreeMap<u32, &MigrationEntry> =
            self.migrations.iter().map(|e| (e.version, e)).collect();

        let mut scope = InvalidationScope::none();
        for version in (active_version + 1)..=embedded_version {
            match entries_by_version.get(&version) {
                Some(entry) => scope = scope.union(&entry.invalidation_scope()),
                None => return InvalidationScope::full(),
            }
        }
        scope
    }
}

impl MigrationEntry {
    #[must_use]
    pub fn declaration(&self) -> ScopeDeclaration {
        ScopeDeclaration {
            scope: self.scope,
            entities: self.entities.clone(),
        }
    }

    #[must_use]
    fn invalidation_scope(&self) -> InvalidationScope {
        match self.scope {
            Scope::All => InvalidationScope::full(),
            Scope::Code => InvalidationScope {
                sdlc: SdlcScope::None,
                code: true,
            },
            Scope::Sdlc if self.entities.is_empty() => InvalidationScope {
                sdlc: SdlcScope::All,
                code: false,
            },
            Scope::Sdlc => InvalidationScope {
                sdlc: SdlcScope::Entities(self.entities.clone()),
                code: false,
            },
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
    fn embedded_ledger_parses_and_validates() {
        let ledger = MigrationLedger::load_embedded().expect("ledger parses");
        let ontology = Ontology::load_embedded().expect("ontology loads");
        let last_version = ledger.last().expect("ledger has entries").version;
        ledger
            .validate(&ontology, last_version)
            .expect("embedded ledger is well-formed and its entities resolve");
    }

    #[test]
    fn versions_must_strictly_increase() {
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
        assert!(err.contains("strictly increase"), "{err}");
    }

    #[test]
    fn last_version_must_equal_schema_version() {
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

    #[test]
    fn invalidated_missing_version_is_full() {
        let ledger = MigrationLedger {
            migrations: vec![MigrationEntry {
                version: 3,
                scope: Scope::Sdlc,
                entities: entities(&["Note"]),
                note: None,
            }],
        };
        assert_eq!(ledger.invalidation_between(2, 5), InvalidationScope::full());
    }

    #[test]
    fn invalidated_unions_entities_across_range() {
        let ledger = MigrationLedger {
            migrations: vec![
                MigrationEntry {
                    version: 3,
                    scope: Scope::Sdlc,
                    entities: entities(&["Note"]),
                    note: None,
                },
                MigrationEntry {
                    version: 4,
                    scope: Scope::Sdlc,
                    entities: entities(&["Issue"]),
                    note: None,
                },
            ],
        };
        let scope = ledger.invalidation_between(2, 4);
        assert_eq!(
            scope.sdlc,
            SdlcScope::Entities(entities(&["Issue", "Note"]))
        );
        assert!(!scope.code);
    }

    #[test]
    fn invalidated_code_and_sdlc_compose() {
        let ledger = MigrationLedger {
            migrations: vec![
                MigrationEntry {
                    version: 3,
                    scope: Scope::Code,
                    entities: BTreeSet::new(),
                    note: None,
                },
                MigrationEntry {
                    version: 4,
                    scope: Scope::Sdlc,
                    entities: entities(&["Note"]),
                    note: None,
                },
            ],
        };
        let scope = ledger.invalidation_between(2, 4);
        assert!(scope.code);
        assert_eq!(scope.sdlc, SdlcScope::Entities(entities(&["Note"])));
    }

    #[test]
    fn invalidated_all_absorbs_entities() {
        let ledger = MigrationLedger {
            migrations: vec![
                MigrationEntry {
                    version: 3,
                    scope: Scope::Sdlc,
                    entities: entities(&["Note"]),
                    note: None,
                },
                MigrationEntry {
                    version: 4,
                    scope: Scope::All,
                    entities: BTreeSet::new(),
                    note: None,
                },
            ],
        };
        assert_eq!(ledger.invalidation_between(2, 4), InvalidationScope::full());
    }

    #[test]
    fn invalidated_empty_range_is_none() {
        let ledger = MigrationLedger::default();
        assert_eq!(ledger.invalidation_between(5, 5), InvalidationScope::none());
    }
}
