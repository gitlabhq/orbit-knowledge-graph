use std::collections::{BTreeMap, BTreeSet};

use crate::schema::Schema;
use query_data_model::catalog::sources::{EtlScope, PipelineDescriptor, Writer};
use query_data_model::{Catalog, ClickHouse, RelationshipId};
use serde::{Deserialize, Serialize};

pub const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeRows {
    #[default]
    Changed,
    Unchanged,
}

impl EdgeRows {
    pub fn is_changed(&self) -> bool {
        *self == Self::Changed
    }

    #[must_use]
    pub fn widened_with(self, other: Self) -> Self {
        if self.is_changed() || other.is_changed() {
            Self::Changed
        } else {
            Self::Unchanged
        }
    }
}

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
    Sdlc {
        entities: BTreeSet<String>,
        edge_rows: EdgeRows,
    },
    None,
}

impl MigrationScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Code => "code",
            Self::Sdlc { .. } => "sdlc",
            Self::None => "none",
        }
    }

    pub fn sdlc_entities(&self) -> Option<String> {
        match self {
            Self::Sdlc { entities, .. } if !entities.is_empty() => {
                Some(entities.iter().cloned().collect::<Vec<_>>().join(","))
            }
            _ => None,
        }
    }

    pub fn keeps_edge_rows(&self) -> bool {
        matches!(
            self,
            Self::Sdlc {
                edge_rows: EdgeRows::Unchanged,
                ..
            }
        )
    }

    /// Whether migrating with `self` rebuilds at least everything `required` rebuilds.
    pub fn covers(&self, required: &Self) -> bool {
        match (self, required) {
            (Self::Full, _) | (_, Self::None) => true,
            (Self::Code, Self::Code) => true,
            (
                Self::Sdlc {
                    entities: covering,
                    edge_rows: covering_edge_rows,
                },
                Self::Sdlc {
                    entities: required,
                    edge_rows: required_edge_rows,
                },
            ) => {
                (covering.is_empty() || (!required.is_empty() && required.is_subset(covering)))
                    && (covering_edge_rows.is_changed() || !required_edge_rows.is_changed())
            }
            _ => false,
        }
    }

    #[must_use]
    pub fn merged_with(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Full, _) | (_, Self::Full) => Self::Full, // full absorbs all
            (Self::None, kept) | (kept, Self::None) => kept.clone(), // none is identity
            (Self::Code, Self::Code) => Self::Code,          // same domain
            (
                Self::Sdlc {
                    entities: left,
                    edge_rows: left_edge_rows,
                },
                Self::Sdlc {
                    entities: right,
                    edge_rows: right_edge_rows,
                },
            ) => {
                let entities = if left.is_empty() || right.is_empty() {
                    BTreeSet::new() // "all sdlc" absorbs subsets
                } else {
                    left.union(right).cloned().collect() // union subsets
                };
                Self::Sdlc {
                    entities,
                    edge_rows: left_edge_rows.widened_with(*right_edge_rows),
                }
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
            Self::Sdlc { entities, .. } if entities.is_empty() => write!(f, "scope sdlc"),
            Self::Sdlc { entities, .. } => {
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
pub fn sdlc_entity_names(catalog: &Catalog<ClickHouse>) -> BTreeSet<String> {
    let sources = catalog.model().sources();
    let graph = catalog.graph();
    let code_tables = code_domain_tables(catalog);
    let mut names = BTreeSet::new();
    for pipeline in sources.pipelines() {
        if matches!(pipeline.writer, Writer::Entity(_) | Writer::Derived(_)) {
            names.insert(sources.writer_name(graph, pipeline.writer).to_string());
        }
    }
    for relationship in graph.relationships() {
        if relationship_tables(catalog, relationship.id)
            .iter()
            .any(|table| !code_tables.contains(table))
        {
            names.insert(relationship.name.clone());
        }
    }
    names
}

#[must_use]
pub fn code_entity_names(catalog: &Catalog<ClickHouse>) -> BTreeSet<String> {
    let sources = catalog.model().sources();
    let graph = catalog.graph();
    let code_tables = code_domain_tables(catalog);
    let mut names: BTreeSet<String> = graph
        .entities()
        .filter(|entity| sources.entity_pipelines(entity.id).next().is_none())
        .map(|entity| entity.name.clone())
        .collect();
    for relationship in graph.relationships() {
        let tables = relationship_tables(catalog, relationship.id);
        if !tables.is_empty() && tables.iter().all(|table| code_tables.contains(table)) {
            names.insert(relationship.name.clone());
        }
    }
    names
}

pub(crate) fn relationship_tables(
    catalog: &Catalog<ClickHouse>,
    relationship: RelationshipId,
) -> BTreeSet<String> {
    let variants = &catalog.graph().relationship(relationship).variants;
    if variants.is_empty() {
        return catalog
            .shared_edge_table()
            .map(str::to_string)
            .into_iter()
            .collect();
    }
    variants
        .iter()
        .filter_map(|variant| catalog.edge_table(*variant))
        .map(str::to_string)
        .collect()
}

pub(crate) fn code_domain_tables(catalog: &Catalog<ClickHouse>) -> BTreeSet<String> {
    let sources = catalog.model().sources();
    catalog
        .graph()
        .entities()
        .filter(|entity| sources.entity_pipelines(entity.id).next().is_none())
        .filter_map(|entity| catalog.table_of(entity.id))
        .map(str::to_string)
        .chain(std::iter::once("gl_code_edge".to_string()))
        .collect()
}

pub(crate) fn edge_tables(catalog: &Catalog<ClickHouse>) -> Vec<String> {
    let mut tables = catalog.edge_tables(&[]);
    tables.extend(catalog.shared_edge_table().map(str::to_string));
    tables.sort();
    tables.dedup();
    tables
}

pub fn widen_scope_for_shared_table_writers(
    catalog: &Catalog<ClickHouse>,
    schema: &Schema,
    requested_scope: &MigrationScope,
) -> MigrationScope {
    if matches!(requested_scope, MigrationScope::Full) {
        return MigrationScope::Full;
    }

    let invalidated = invalidated_entities(catalog, requested_scope);

    if schema.join_sources().any(|table| {
        schema
            .writers(table)
            .is_some_and(|writers| !writers.is_disjoint(&invalidated))
    }) {
        return MigrationScope::Full;
    }

    let edge_tables = edge_tables(catalog);
    let shared_edge_table = catalog.shared_edge_table();
    for table in schema.versioned_tables().map(|table| &table.name) {
        if matches!(requested_scope, MigrationScope::Code)
            && Some(table.as_str()) == shared_edge_table
        {
            continue;
        }
        if requested_scope.keeps_edge_rows() && edge_tables.contains(table) {
            continue;
        }

        let Some(writers) = schema.writers(table) else {
            continue;
        };
        let scope_writes_to_table = writers.iter().any(|writer| invalidated.contains(writer));
        let has_writer_outside_scope = writers.iter().any(|writer| !invalidated.contains(writer));

        if Some(table.as_str()) == shared_edge_table && scope_writes_to_table {
            return MigrationScope::Full;
        }
        if scope_writes_to_table && has_writer_outside_scope {
            return MigrationScope::Full;
        }
    }

    requested_scope.clone()
}

pub fn classify_tables_for_scope(
    catalog: &Catalog<ClickHouse>,
    schema: &Schema,
    scope: &MigrationScope,
) -> BTreeMap<String, TableMigrationAction> {
    let invalidated = invalidated_entities(catalog, scope);
    let edge_tables = edge_tables(catalog);

    let mut actions: BTreeMap<_, _> = schema
        .versioned_tables()
        .map(|table| table.name.clone())
        .map(|table| {
            let action =
                migration_action_for_table(schema, &table, scope, &invalidated, &edge_tables);
            (table, action)
        })
        .collect();
    let dependencies = schema.dependencies();
    loop {
        let mut changed = false;
        for (table, sources) in dependencies {
            if sources
                .iter()
                .any(|source| actions.get(source) == Some(&TableMigrationAction::RebuildEmpty))
                && actions.get(table) != Some(&TableMigrationAction::RebuildEmpty)
            {
                actions.insert(table.clone(), TableMigrationAction::RebuildEmpty);
                changed = true;
            }
        }
        if !changed {
            return actions;
        }
    }
}

pub fn find_invalidated_pipelines(
    catalog: &Catalog<ClickHouse>,
    scope: &MigrationScope,
) -> InvalidatedPipelines {
    let invalidated = invalidated_entities(catalog, scope);
    let descriptors = catalog.model().sources().descriptors(catalog.graph());

    for entity in orphaned_sdlc_entities(catalog, &invalidated, &descriptors) {
        orbit_log_fields::warn!(
            safe(ENTITY) = entity.as_str(),
            "invalidated SDLC entity is emitted by no pipeline — orphan, excluded from seeding and gating"
        );
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

fn orphaned_sdlc_entities(
    catalog: &Catalog<ClickHouse>,
    invalidated: &BTreeSet<String>,
    descriptors: &[PipelineDescriptor],
) -> BTreeSet<String> {
    let code = code_entity_names(catalog);
    invalidated
        .iter()
        .filter(|entity| !code.contains(*entity))
        .filter(|entity| {
            !descriptors
                .iter()
                .any(|descriptor| descriptor.reindex_targets.contains(*entity))
        })
        .cloned()
        .collect()
}

fn invalidated_entities(catalog: &Catalog<ClickHouse>, scope: &MigrationScope) -> BTreeSet<String> {
    match scope {
        MigrationScope::Full => sdlc_entity_names(catalog)
            .into_iter()
            .chain(code_entity_names(catalog))
            .collect(),
        MigrationScope::Sdlc { entities, .. } if entities.is_empty() => sdlc_entity_names(catalog),
        MigrationScope::Sdlc { entities, .. } => {
            let mut invalidated = entities.clone();
            for descriptor in catalog.model().sources().descriptors(catalog.graph()) {
                if !descriptor.reindex_targets.is_disjoint(entities) {
                    invalidated.insert(descriptor.entity);
                }
            }
            invalidated
        }
        MigrationScope::Code => code_entity_names(catalog),
        MigrationScope::None => BTreeSet::new(),
    }
}

fn migration_action_for_table(
    schema: &Schema,
    table: &str,
    scope: &MigrationScope,
    invalidated: &BTreeSet<String>,
    edge_tables: &[String],
) -> TableMigrationAction {
    if scope.keeps_edge_rows() && edge_tables.iter().any(|edge_table| edge_table == table) {
        return TableMigrationAction::CloneFromActive;
    }

    if table == CODE_INDEXING_CHECKPOINT_TABLE {
        return if matches!(scope, MigrationScope::Code) {
            TableMigrationAction::RebuildEmpty
        } else {
            TableMigrationAction::CloneFromActive
        };
    }

    if schema.writers(table).is_some_and(|writers| {
        !writers.is_empty() && writers.iter().all(|writer| invalidated.contains(writer))
    }) {
        TableMigrationAction::RebuildEmpty
    } else {
        TableMigrationAction::CloneFromActive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{entity_set, sdlc_scope};

    fn sdlc_scope_keeping_edge_rows(names: &[&str]) -> MigrationScope {
        MigrationScope::Sdlc {
            entities: entity_set(names),
            edge_rows: EdgeRows::Unchanged,
        }
    }

    fn classify(scope: MigrationScope) -> BTreeMap<String, TableMigrationAction> {
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        classify_tables_for_scope(&catalog, &embedded_schema(), &scope)
    }

    fn embedded_schema() -> Schema {
        Schema::from_model(&query_data_model::Packages::embedded().load().unwrap()).unwrap()
    }

    fn action(map: &BTreeMap<String, TableMigrationAction>, table: &str) -> TableMigrationAction {
        *map.get(table)
            .unwrap_or_else(|| panic!("table '{table}' missing from classification: {map:?}"))
    }

    #[test]
    fn shared_edge_writers_force_scope_widening() {
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        let layout = embedded_schema();

        assert_eq!(
            widen_scope_for_shared_table_writers(&catalog, &layout, &sdlc_scope(&["User"])),
            sdlc_scope(&["User"]),
        );
        assert_eq!(
            widen_scope_for_shared_table_writers(&catalog, &layout, &sdlc_scope(&["SystemNote"])),
            MigrationScope::Full,
        );
        assert_eq!(
            widen_scope_for_shared_table_writers(&catalog, &layout, &sdlc_scope(&["HAS_NOTE"])),
            MigrationScope::Full,
        );
        assert_eq!(
            widen_scope_for_shared_table_writers(&catalog, &layout, &sdlc_scope(&[])),
            MigrationScope::Full,
        );
        assert_eq!(
            widen_scope_for_shared_table_writers(&catalog, &layout, &MigrationScope::Code),
            MigrationScope::Code,
        );
    }

    #[test]
    fn unchanged_edge_rows_clone_every_edge_table_and_rebuild_sdlc_node_tables() {
        let map = classify(sdlc_scope_keeping_edge_rows(&[]));
        for table in [
            "gl_edge",
            "gl_ci_edge",
            "gl_sec_edge",
            "gl_diff_edge",
            "gl_code_edge",
        ] {
            assert_eq!(
                action(&map, table),
                TableMigrationAction::CloneFromActive,
                "{table} should clone when edge rows are unchanged"
            );
        }
        for table in ["gl_note", "gl_job", "gl_merge_request"] {
            assert_eq!(
                action(&map, table),
                TableMigrationAction::RebuildEmpty,
                "{table} should rebuild under whole sdlc scope"
            );
        }
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
        let map = classify(sdlc_scope(&[]));
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
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");

        let map = classify(MigrationScope::None);
        for (table, table_action) in &map {
            assert_eq!(
                *table_action,
                TableMigrationAction::CloneFromActive,
                "{table} should clone under none scope"
            );
        }

        let pipelines = find_invalidated_pipelines(&catalog, &MigrationScope::None);
        assert!(
            pipelines.namespaced.is_empty(),
            "{:?}",
            pipelines.namespaced
        );
        assert!(pipelines.global.is_empty(), "{:?}", pipelines.global);
    }

    #[test]
    fn fk_edge_kind_invalidates_its_emitting_pipeline() {
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        let pipelines = find_invalidated_pipelines(&catalog, &sdlc_scope(&["HAS_NOTE"]));
        assert!(
            pipelines.namespaced.contains(&"Note".to_string()),
            "HAS_NOTE must match the Note pipeline: {:?}",
            pipelines.namespaced
        );
    }

    #[test]
    fn full_scope_has_no_orphans_and_skips_code_entities() {
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        let descriptors = catalog.model().sources().descriptors(catalog.graph());
        let invalidated = invalidated_entities(&catalog, &MigrationScope::Full);
        assert!(!code_entity_names(&catalog).is_empty());
        assert_eq!(
            orphaned_sdlc_entities(&catalog, &invalidated, &descriptors),
            BTreeSet::new()
        );
    }
}
