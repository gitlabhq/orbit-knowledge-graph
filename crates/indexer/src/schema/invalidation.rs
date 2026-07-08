//! Table cloning decisions for narrowed schema migrations. Entity edge-key
//! changes must be in the ledger, or cloned shared edge tables can retain old-key rows.

use std::collections::{BTreeMap, BTreeSet};

use ontology::Ontology;
use ontology::migrations::{Scope, ScopeDeclaration, code_entity_names, sdlc_entity_names};
use query_engine::compiler::generate_graph_tables_with_prefix;

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMigrationAction {
    RebuildEmpty,
    CloneFromActive,
}

#[must_use]
pub fn classify_tables_for_scope(
    ontology: &Ontology,
    scope: &ScopeDeclaration,
) -> BTreeMap<String, TableMigrationAction> {
    let invalidated = invalidated_entities(ontology, scope);
    generate_graph_tables_with_prefix(ontology, "")
        .into_iter()
        .map(|table| {
            let action = action_for_table(ontology, &table.name, scope, &invalidated);
            (table.name, action)
        })
        .collect()
}

fn invalidated_entities(ontology: &Ontology, scope: &ScopeDeclaration) -> BTreeSet<String> {
    match scope.scope {
        Scope::All => sdlc_entity_names(ontology)
            .into_iter()
            .chain(code_entity_names(ontology))
            .collect(),
        Scope::Sdlc if scope.entities.is_empty() => sdlc_entity_names(ontology),
        Scope::Sdlc => scope.entities.clone(),
        Scope::Code => code_entity_names(ontology),
    }
}

fn action_for_table(
    ontology: &Ontology,
    table: &str,
    scope: &ScopeDeclaration,
    invalidated: &BTreeSet<String>,
) -> TableMigrationAction {
    if table == CODE_INDEXING_CHECKPOINT_TABLE {
        return if scope.scope == Scope::Code {
            TableMigrationAction::RebuildEmpty
        } else {
            TableMigrationAction::CloneFromActive
        };
    }

    let writers = writers_of_table(ontology, table);
    if !writers.is_empty() && writers.iter().all(|writer| invalidated.contains(writer)) {
        TableMigrationAction::RebuildEmpty
    } else {
        TableMigrationAction::CloneFromActive
    }
}

fn writers_of_table(ontology: &Ontology, table: &str) -> BTreeSet<String> {
    let mut writers = BTreeSet::new();

    for node in ontology.nodes() {
        if node.destination_table == table || emits_to_table(ontology, &node.name, table) {
            writers.insert(node.name.clone());
        }
    }
    for derived in ontology.derived_entities() {
        if emits_to_table(ontology, &derived.name, table) {
            writers.insert(derived.name.clone());
        }
    }
    for kind in ontology.edge_names() {
        if ontology.edge_table_for_relationship(kind) == table {
            writers.insert(kind.to_string());
        }
    }

    writers
}

fn emits_to_table(ontology: &Ontology, entity: &str, table: &str) -> bool {
    ontology
        .relationship_kinds_emitted_by(entity)
        .iter()
        .any(|kind| ontology.edge_table_for_relationship(kind) == table)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entities(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn classify(scope: ScopeDeclaration) -> BTreeMap<String, TableMigrationAction> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        classify_tables_for_scope(&ontology, &scope)
    }

    fn action(map: &BTreeMap<String, TableMigrationAction>, table: &str) -> TableMigrationAction {
        *map.get(table)
            .unwrap_or_else(|| panic!("table '{table}' missing from classification: {map:?}"))
    }

    #[test]
    fn narrowed_note_scope_rebuilds_only_its_node_table() {
        let map = classify(ScopeDeclaration {
            scope: Scope::Sdlc,
            entities: entities(&["Note"]),
        });
        assert_eq!(action(&map, "gl_note"), TableMigrationAction::RebuildEmpty);
        assert_eq!(
            action(&map, "gl_edge"),
            TableMigrationAction::CloneFromActive
        );
        assert_eq!(
            action(&map, "checkpoint"),
            TableMigrationAction::CloneFromActive
        );
    }

    #[test]
    fn full_sdlc_scope_rebuilds_sdlc_tables_and_clones_code() {
        let map = classify(ScopeDeclaration {
            scope: Scope::Sdlc,
            entities: BTreeSet::new(),
        });
        assert_eq!(action(&map, "gl_note"), TableMigrationAction::RebuildEmpty);
        assert_eq!(action(&map, "gl_edge"), TableMigrationAction::RebuildEmpty);
        assert_eq!(
            action(&map, "gl_definition"),
            TableMigrationAction::CloneFromActive
        );
        assert_eq!(
            action(&map, "gl_code_edge"),
            TableMigrationAction::CloneFromActive
        );
        assert_eq!(
            action(&map, "checkpoint"),
            TableMigrationAction::CloneFromActive
        );
        assert_eq!(
            action(&map, "code_indexing_checkpoint"),
            TableMigrationAction::CloneFromActive
        );
    }

    #[test]
    fn code_scope_rebuilds_code_tables_and_checkpoint() {
        let map = classify(ScopeDeclaration {
            scope: Scope::Code,
            entities: BTreeSet::new(),
        });
        for table in ["gl_definition", "gl_file", "gl_directory", "gl_code_edge"] {
            assert_eq!(
                action(&map, table),
                TableMigrationAction::RebuildEmpty,
                "{table} should rebuild under code scope"
            );
        }
        assert_eq!(
            action(&map, "code_indexing_checkpoint"),
            TableMigrationAction::RebuildEmpty
        );
        assert_eq!(
            action(&map, "gl_edge"),
            TableMigrationAction::CloneFromActive
        );
        assert_eq!(
            action(&map, "gl_note"),
            TableMigrationAction::CloneFromActive
        );
        assert_eq!(
            action(&map, "checkpoint"),
            TableMigrationAction::CloneFromActive
        );
    }
}
