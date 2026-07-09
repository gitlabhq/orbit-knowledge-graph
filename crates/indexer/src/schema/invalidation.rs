//! Clone-based schema migration invalidation. Entity edge-key changes must be in
//! the ledger, or cloned shared edge tables can retain old-key rows.

use std::collections::{BTreeMap, BTreeSet};

use ontology::migrations::{MigrationScope, code_entity_names, sdlc_entity_names};
use ontology::{EtlScope, Ontology};
use query_engine::compiler::generate_graph_tables_with_prefix;
use tracing::warn;

// TODO: move to the ontology as the single source for checkpoint table names.
pub(crate) const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMigrationAction {
    RebuildEmpty,
    CloneFromActive,
}

#[derive(Debug)]
pub struct InvalidatedPipelines {
    pub namespaced: Vec<String>,
    pub global: Vec<String>,
}

pub fn classify_tables_for_scope(
    ontology: &Ontology,
    scope: &MigrationScope,
) -> BTreeMap<String, TableMigrationAction> {
    let invalidated = invalidated_entities(ontology, scope);
    let tables = generate_graph_tables_with_prefix(ontology, "");

    tables
        .into_iter()
        .map(|table| {
            let action =
                compute_migration_action_for_table(ontology, &table.name, scope, &invalidated);
            (table.name, action)
        })
        .collect()
}

/// FK-derived edge kinds invalidate the pipelines that emit them.
pub fn find_invalidated_pipelines(
    ontology: &Ontology,
    scope: &MigrationScope,
) -> InvalidatedPipelines {
    let invalidated = invalidated_entities(ontology, scope);
    let descriptors = ontology.pipeline_descriptors();
    for entity in &invalidated {
        if !descriptors
            .iter()
            .any(|d| d.reindex_targets.contains(entity))
        {
            warn!(entity = %entity, "invalidated entity is emitted by no pipeline — orphan, excluded from seeding and gating");
        }
    }

    let mut namespaced = Vec::new();
    let mut global = Vec::new();
    for descriptor in descriptors {
        if descriptor.reindex_targets.is_disjoint(&invalidated) {
            continue;
        }
        match descriptor.scope {
            EtlScope::Namespaced => namespaced.push(descriptor.name),
            EtlScope::Global => global.push(descriptor.name),
        }
    }
    InvalidatedPipelines { namespaced, global }
}

fn invalidated_entities(ontology: &Ontology, scope: &MigrationScope) -> BTreeSet<String> {
    match scope {
        MigrationScope::Full => sdlc_entity_names(ontology)
            .into_iter()
            .chain(code_entity_names(ontology))
            .collect(),
        MigrationScope::Sdlc(entities) if entities.is_empty() => sdlc_entity_names(ontology),
        MigrationScope::Sdlc(entities) => entities.clone(),
        MigrationScope::Code => code_entity_names(ontology),
    }
}

fn compute_migration_action_for_table(
    ontology: &Ontology,
    table: &str,
    scope: &MigrationScope,
    invalidated: &BTreeSet<String>,
) -> TableMigrationAction {
    if table == CODE_INDEXING_CHECKPOINT_TABLE {
        return if matches!(scope, MigrationScope::Code) {
            TableMigrationAction::RebuildEmpty
        } else {
            TableMigrationAction::CloneFromActive
        };
    }

    let writers = entities_writing_to_table(ontology, table);
    if !writers.is_empty() && writers.iter().all(|writer| invalidated.contains(writer)) {
        TableMigrationAction::RebuildEmpty
    } else {
        TableMigrationAction::CloneFromActive
    }
}

fn entities_writing_to_table(ontology: &Ontology, table: &str) -> BTreeSet<String> {
    let mut writers = BTreeSet::new();

    for node in ontology.nodes() {
        if node.destination_table == table || emits_edge_to_table(ontology, &node.name, table) {
            writers.insert(node.name.clone());
        }
    }
    for derived in ontology.derived_entities() {
        if emits_edge_to_table(ontology, &derived.name, table) {
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

fn emits_edge_to_table(ontology: &Ontology, entity: &str, table: &str) -> bool {
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

    fn classify(scope: MigrationScope) -> BTreeMap<String, TableMigrationAction> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        classify_tables_for_scope(&ontology, &scope)
    }

    fn action(map: &BTreeMap<String, TableMigrationAction>, table: &str) -> TableMigrationAction {
        *map.get(table)
            .unwrap_or_else(|| panic!("table '{table}' missing from classification: {map:?}"))
    }

    fn sdlc_scope(names: &[&str]) -> MigrationScope {
        MigrationScope::Sdlc(entities(names))
    }

    #[test]
    fn invalidated_pipelines_split_by_scope() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let note = find_invalidated_pipelines(&ontology, &sdlc_scope(&["Note"]));
        assert!(note.namespaced.contains(&"Note".to_string()));
        assert!(note.global.is_empty());

        let user = find_invalidated_pipelines(&ontology, &sdlc_scope(&["User"]));
        assert!(user.global.contains(&"User".to_string()));
        assert!(user.namespaced.is_empty());
    }

    #[test]
    fn invalidated_pipelines_ignore_unknown_entities() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let pipelines = find_invalidated_pipelines(&ontology, &sdlc_scope(&["Ghost"]));
        assert!(pipelines.namespaced.is_empty());
        assert!(pipelines.global.is_empty());
    }

    #[test]
    fn invalidated_pipelines_expand_fk_edge_kind_to_emitting_node() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let pipelines = find_invalidated_pipelines(&ontology, &sdlc_scope(&["HAS_NOTE"]));
        assert!(
            pipelines.namespaced.contains(&"Note".to_string()),
            "HAS_NOTE must match the Note pipeline: {:?}",
            pipelines.namespaced
        );
    }

    #[test]
    fn whole_sdlc_scope_invalidates_every_pipeline() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let whole = find_invalidated_pipelines(&ontology, &sdlc_scope(&[]));
        assert_eq!(
            whole.namespaced.len() + whole.global.len(),
            ontology.pipeline_descriptors().len()
        );
    }

    #[test]
    fn orphan_sdlc_entities_are_exactly_the_known_set() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let descriptors = ontology.pipeline_descriptors();
        let orphans: BTreeSet<String> = sdlc_entity_names(&ontology)
            .into_iter()
            .filter(|entity| {
                !descriptors
                    .iter()
                    .any(|d| d.reindex_targets.contains(entity))
            })
            .collect();
        assert_eq!(
            orphans,
            BTreeSet::new(),
            "declared entities that no pipeline emits; if this set changes, confirm the new \
             entry is a genuine orphan and not a missed emitter mapping"
        );
    }

    #[test]
    fn note_scope_migration_rebuilds_only_its_node_table() {
        let map = classify(MigrationScope::Sdlc(entities(&["Note"])));
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
        let map = classify(MigrationScope::Sdlc(BTreeSet::new()));
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
        let map = classify(MigrationScope::Code);
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
