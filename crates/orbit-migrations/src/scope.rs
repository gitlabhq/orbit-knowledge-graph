use std::collections::{BTreeMap, BTreeSet};

use ontology::{EtlScope, Ontology};
use serde::{Deserialize, Serialize};

pub const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerScope {
    #[serde(rename = "*")]
    All,
    #[serde(rename = "sdlc")]
    Sdlc,
    #[serde(rename = "code")]
    Code,
    #[serde(rename = "none")]
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationScope {
    Full,
    Code,
    Sdlc(BTreeSet<String>),
    None,
}

impl MigrationScope {
    #[must_use]
    pub fn includes(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Full, _) | (_, Self::None) => true, // full covers all, none needs nothing
            (Self::Code, Self::Code) => true,          // same domain
            (Self::Sdlc(covering), Self::Sdlc(required)) => {
                covering.is_empty() || (!required.is_empty() && required.is_subset(covering))
            }
            _ => false, // cross-domain or insufficient
        }
    }

    #[must_use]
    pub fn merged_with(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Full, _) | (_, Self::Full) => Self::Full, // full absorbs all
            (Self::None, kept) | (kept, Self::None) => kept.clone(), // none is identity
            (Self::Code, Self::Code) => Self::Code,          // same domain
            (Self::Sdlc(left), Self::Sdlc(right)) if left.is_empty() || right.is_empty() => {
                Self::Sdlc(BTreeSet::new()) // "all sdlc" absorbs subsets
            }
            (Self::Sdlc(left), Self::Sdlc(right)) => {
                Self::Sdlc(left.union(right).cloned().collect()) // union subsets
            }
            _ => Self::Full, // mixed domains escalate
        }
    }
}

impl std::fmt::Display for MigrationScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "scope \"*\""),
            Self::None => write!(f, "scope none"),
            Self::Code => write!(f, "scope code"),
            Self::Sdlc(entities) if entities.is_empty() => write!(f, "scope sdlc"),
            Self::Sdlc(entities) => {
                let list = entities.iter().cloned().collect::<Vec<_>>().join(", ");
                write!(f, "scope sdlc, entities [{list}]")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMigrationAction {
    RebuildEmpty,
    CloneFromActive,
}

#[derive(Debug, Default)]
pub struct InvalidatedPipelines {
    pub namespaced: Vec<String>,
    pub global: Vec<String>,
}

#[must_use]
pub fn sdlc_entity_names(ontology: &Ontology) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for node in ontology.nodes() {
        if !node.pipelines.is_empty() {
            names.insert(node.name.clone());
        }
    }
    for derived in ontology.derived_entities() {
        names.insert(derived.name.clone());
    }
    for kind in ontology.edge_names() {
        if !is_code_domain_table(ontology, ontology.edge_table_for_relationship(kind)) {
            names.insert(kind.to_string());
        }
    }
    names
}

#[must_use]
pub fn code_entity_names(ontology: &Ontology) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for node in ontology.nodes() {
        if node.pipelines.is_empty() {
            names.insert(node.name.clone());
        }
    }
    for kind in ontology.edge_names() {
        if is_code_domain_table(ontology, ontology.edge_table_for_relationship(kind)) {
            names.insert(kind.to_string());
        }
    }
    names
}

pub fn widen_scope_for_shared_table_writers(
    ontology: &Ontology,
    requested_scope: &MigrationScope,
) -> MigrationScope {
    if matches!(requested_scope, MigrationScope::Full) {
        return MigrationScope::Full;
    }

    let invalidated = invalidated_entities(ontology, requested_scope);

    for table in versioned_table_names(ontology) {
        if matches!(requested_scope, MigrationScope::Code) && table == ontology.edge_table() {
            continue;
        }

        let writers = entities_writing_to_table(ontology, &table);
        let scope_writes_to_table = writers.iter().any(|writer| invalidated.contains(writer));
        let has_writer_outside_scope = writers.iter().any(|writer| !invalidated.contains(writer));

        if table == ontology.edge_table() && scope_writes_to_table {
            return MigrationScope::Full;
        }
        if scope_writes_to_table && has_writer_outside_scope {
            return MigrationScope::Full;
        }
    }

    requested_scope.clone()
}

pub fn classify_tables_for_scope(
    ontology: &Ontology,
    scope: &MigrationScope,
) -> BTreeMap<String, TableMigrationAction> {
    let invalidated = invalidated_entities(ontology, scope);

    versioned_table_names(ontology)
        .into_iter()
        .map(|table| {
            let action = migration_action_for_table(ontology, &table, scope, &invalidated);
            (table, action)
        })
        .collect()
}

pub fn find_invalidated_pipelines(
    ontology: &Ontology,
    scope: &MigrationScope,
) -> InvalidatedPipelines {
    let invalidated = invalidated_entities(ontology, scope);
    let descriptors = ontology.pipeline_descriptors();

    for entity in &invalidated {
        if !descriptors
            .iter()
            .any(|descriptor| descriptor.reindex_targets.contains(entity))
        {
            tracing::warn!(
                entity = %entity,
                "invalidated entity is emitted by no pipeline — orphan, excluded from seeding and gating"
            );
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
        MigrationScope::None => BTreeSet::new(),
    }
}

fn versioned_table_names(ontology: &Ontology) -> Vec<String> {
    let mut names = Vec::new();

    for auxiliary_table in ontology.auxiliary_tables() {
        if auxiliary_table.versioned {
            names.push(auxiliary_table.name.clone());
        }
    }
    for node in ontology.nodes() {
        names.push(node.destination_table.clone());
    }
    for table_name in ontology.edge_tables() {
        names.push(table_name.to_string());
    }
    for join in ontology.denormalized_joins() {
        names.push(join.table.clone());
    }

    names
}

fn migration_action_for_table(
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

fn is_code_domain_table(ontology: &Ontology, table: &str) -> bool {
    ontology
        .nodes()
        .any(|node| node.pipelines.is_empty() && node.destination_table == table)
        || table == "gl_code_edge"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::entity_set;

    fn sdlc_scope(names: &[&str]) -> MigrationScope {
        MigrationScope::Sdlc(entity_set(names))
    }

    fn classify(scope: MigrationScope) -> BTreeMap<String, TableMigrationAction> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        classify_tables_for_scope(&ontology, &scope)
    }

    fn action(map: &BTreeMap<String, TableMigrationAction>, table: &str) -> TableMigrationAction {
        *map.get(table)
            .unwrap_or_else(|| panic!("table '{table}' missing from classification: {map:?}"))
    }

    #[test]
    fn shared_edge_writers_force_scope_widening() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        assert_eq!(
            widen_scope_for_shared_table_writers(&ontology, &sdlc_scope(&["User"])),
            sdlc_scope(&["User"]),
        );
        assert_eq!(
            widen_scope_for_shared_table_writers(&ontology, &sdlc_scope(&["SystemNote"])),
            MigrationScope::Full,
        );
        assert_eq!(
            widen_scope_for_shared_table_writers(&ontology, &sdlc_scope(&[])),
            MigrationScope::Full,
        );
        assert_eq!(
            widen_scope_for_shared_table_writers(&ontology, &MigrationScope::Code),
            MigrationScope::Code,
        );
    }

    #[test]
    fn note_scope_rebuilds_only_its_node_table() {
        let map = classify(sdlc_scope(&["Note"]));
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
    fn code_scope_rebuilds_code_tables_and_clones_sdlc() {
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

    #[test]
    fn none_scope_clones_everything_and_invalidates_nothing() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let map = classify(MigrationScope::None);
        for (table, table_action) in &map {
            assert_eq!(
                *table_action,
                TableMigrationAction::CloneFromActive,
                "{table} should clone under none scope"
            );
        }

        let pipelines = find_invalidated_pipelines(&ontology, &MigrationScope::None);
        assert!(
            pipelines.namespaced.is_empty(),
            "{:?}",
            pipelines.namespaced
        );
        assert!(pipelines.global.is_empty(), "{:?}", pipelines.global);
    }

    #[test]
    fn fk_edge_kind_invalidates_its_emitting_pipeline() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let pipelines = find_invalidated_pipelines(&ontology, &sdlc_scope(&["HAS_NOTE"]));
        assert!(
            pipelines.namespaced.contains(&"Note".to_string()),
            "HAS_NOTE must match the Note pipeline: {:?}",
            pipelines.namespaced
        );
    }

    #[test]
    fn every_sdlc_entity_is_reachable_from_a_pipeline() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let descriptors = ontology.pipeline_descriptors();
        let orphans: BTreeSet<String> = sdlc_entity_names(&ontology)
            .into_iter()
            .filter(|entity| {
                !descriptors
                    .iter()
                    .any(|descriptor| descriptor.reindex_targets.contains(entity))
            })
            .collect();
        assert_eq!(orphans, BTreeSet::new());
    }
}
