//! Derives a migration scope by diffing the physical ClickHouse catalogs of two models.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use query_data_model::catalog::sources::{
    EdgeMapping, Endpoint, EndpointKind, ExtractQuery, Pipeline, Transform, Writer,
};
use query_data_model::{
    Catalog, ClickHouse, DataModelError, Model, PropertyId, RelationshipId, RelationshipVariantId,
};

use crate::schema::{GraphSchema, Schema};
use crate::scope::{
    EdgeRows, MigrationScope, code_domain_tables, edge_tables, relationship_tables,
    sdlc_entity_names, widen_scope_for_shared_table_writers,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableChange {
    Added,
    Dropped,
    Layout,
    /// The table keeps its DDL, but these writers produce different rows.
    Contents(BTreeSet<String>),
}

#[derive(Debug)]
pub struct DerivedScope {
    pub scope: MigrationScope,
    pub tables: BTreeMap<String, TableChange>,
}

/// The scope that rebuilds every table whose layout or contents differ between the models.
pub fn derive_scope(
    previous: &Arc<Model>,
    next: &Arc<Model>,
) -> Result<DerivedScope, DataModelError> {
    let previous = PhysicalCatalog::new(previous)?;
    let next = PhysicalCatalog::new(next)?;
    let tables = previous.diff(&next);
    Ok(DerivedScope {
        scope: next.scope_for(&tables),
        tables,
    })
}

struct PhysicalCatalog {
    catalog: Catalog<ClickHouse>,
    schema: Schema,
    tables: BTreeMap<String, PhysicalTable>,
}

#[derive(Default)]
struct PhysicalTable {
    layout: Vec<String>,
    writers: BTreeMap<String, Vec<String>>,
}

impl PhysicalCatalog {
    fn new(model: &Arc<Model>) -> Result<Self, DataModelError> {
        let catalog = Catalog::<ClickHouse>::from_model(model)?;
        let schema = Schema::from_model(model)?;
        let mut tables = BTreeMap::<String, PhysicalTable>::new();

        let graph_schema = GraphSchema::new(schema.clone(), false);
        for table in &graph_schema.tables {
            tables
                .entry(table.name.clone())
                .or_default()
                .layout
                .push(table.to_create_sql(""));
        }
        let table_names: Vec<String> = schema.table_names().map(String::from).collect();
        for view in graph_schema.views.iter().filter(|view| view.versioned) {
            let target = view.to_table.clone().unwrap_or_else(|| view.name.clone());
            let sql = view
                .clone()
                .with_schema_version_prefix("", &table_names)
                .to_create_sql();
            tables.entry(target).or_default().layout.push(sql);
        }

        let contents = Contents { catalog: &catalog };
        for (table, writer, rendered) in contents.rows() {
            tables
                .entry(table)
                .or_default()
                .writers
                .entry(writer)
                .or_default()
                .push(rendered);
        }
        for table in tables.values_mut() {
            for rendered in table.writers.values_mut() {
                rendered.sort();
            }
        }

        Ok(Self {
            catalog,
            schema,
            tables,
        })
    }

    fn diff(&self, next: &Self) -> BTreeMap<String, TableChange> {
        let names: BTreeSet<&String> = self.tables.keys().chain(next.tables.keys()).collect();
        names
            .into_iter()
            .filter_map(|name| {
                let change = match (self.tables.get(name), next.tables.get(name)) {
                    (None, _) => TableChange::Added,
                    (_, None) => TableChange::Dropped,
                    (Some(previous), Some(next)) if previous.layout != next.layout => {
                        TableChange::Layout
                    }
                    (Some(previous), Some(next)) => {
                        let writers: BTreeSet<String> = previous
                            .writers
                            .keys()
                            .chain(next.writers.keys())
                            .filter(|writer| {
                                previous.writers.get(*writer) != next.writers.get(*writer)
                            })
                            .cloned()
                            .collect();
                        if writers.is_empty() {
                            return None;
                        }
                        TableChange::Contents(writers)
                    }
                };
                Some((name.clone(), change))
            })
            .collect()
    }

    fn scope_for(&self, tables: &BTreeMap<String, TableChange>) -> MigrationScope {
        if tables.is_empty() {
            return MigrationScope::None;
        }
        let code_tables = code_domain_tables(&self.catalog);
        let edge_tables = edge_tables(&self.catalog);
        let mut code = false;
        let mut entities = BTreeSet::new();
        let mut edge_rows = EdgeRows::Unchanged;
        for (table, change) in tables {
            if edge_tables.contains(table) {
                edge_rows = EdgeRows::Changed;
            }
            if code_tables.contains(table) {
                code = true;
                continue;
            }
            let owners = match change {
                TableChange::Added | TableChange::Dropped => None,
                TableChange::Layout => self.owners(table),
                TableChange::Contents(writers) => Some(writers.clone()),
            };
            let Some(owners) = owners else {
                return MigrationScope::Full;
            };
            entities.extend(owners);
        }
        let scope = match (code, entities.is_empty()) {
            (true, true) => MigrationScope::Code,
            (false, false) if entities.is_subset(&sdlc_entity_names(&self.catalog)) => {
                MigrationScope::Sdlc {
                    entities,
                    edge_rows,
                }
            }
            _ => return MigrationScope::Full,
        };
        widen_scope_for_shared_table_writers(&self.catalog, &self.schema, &scope)
    }

    /// Every writer whose rows a rebuilt `table` must reproduce, following copies and joins to their sources.
    fn owners(&self, table: &str) -> Option<BTreeSet<String>> {
        if let Some(writers) = self.schema.writers(table).filter(|w| !w.is_empty()) {
            return Some(writers.clone());
        }
        let sources = self.schema.dependencies().get(table)?;
        let mut owners = BTreeSet::new();
        for source in sources {
            owners.extend(self.owners(source)?);
        }
        Some(owners)
    }
}

/// Renders what the indexer reads to write each table, by name, so equal output renders equal.
struct Contents<'a> {
    catalog: &'a Catalog<ClickHouse>,
}

impl Contents<'_> {
    fn rows(&self) -> Vec<(String, String, String)> {
        let graph = self.catalog.graph();
        let sources = self.catalog.model().sources();
        let mut rows = Vec::new();
        for entity in graph.entities() {
            if let Some(table) = self.catalog.table_of(entity.id) {
                let mut properties: Vec<String> = entity
                    .properties
                    .iter()
                    .map(|property| self.property(*property))
                    .collect();
                properties.sort();
                rows.push((
                    table.to_string(),
                    entity.name.clone(),
                    properties.join("\n"),
                ));
            }
        }
        for variant in graph.variants() {
            if let Some(table) = self.catalog.edge_table(variant.id) {
                let relationship = graph.relationship(variant.relationship);
                rows.push((
                    table.to_string(),
                    relationship.name.clone(),
                    self.relationship(relationship.id),
                ));
            }
        }
        for pipeline in sources.pipelines() {
            let writer = sources.writer_name(graph, pipeline.writer).to_string();
            let rendered = self.pipeline(pipeline);
            for table in self.destinations(pipeline) {
                rows.push((table, writer.clone(), rendered.clone()));
            }
        }
        rows
    }

    fn destinations(&self, pipeline: &Pipeline) -> BTreeSet<String> {
        let sources = self.catalog.model().sources();
        let mut tables = BTreeSet::new();
        if let Writer::Entity(entity) = pipeline.writer {
            tables.extend(self.catalog.table_of(entity).map(str::to_string));
        }
        let mapped: BTreeSet<RelationshipId> = pipeline
            .transform
            .edges()
            .iter()
            .map(|edge| edge.relationship)
            .collect();
        for edge in pipeline.transform.edges() {
            tables.extend(
                edge.variants
                    .iter()
                    .filter_map(|variant| self.catalog.edge_table(*variant))
                    .map(str::to_string),
            );
        }
        for relationship in sources.emitted_relationships(pipeline) {
            if !mapped.contains(&relationship) {
                tables.extend(relationship_tables(self.catalog, relationship));
            }
        }
        tables
    }

    fn property(&self, id: PropertyId) -> String {
        let graph = self.catalog.graph();
        let property = graph.property(id);
        format!(
            "{}.{} {:?} nullable={} mutable={} enum={:?} {:?}",
            graph.entity(property.entity).name,
            property.name,
            property.data_type,
            property.nullable,
            !self.catalog.model().immutable(id),
            self.catalog.model().enum_type(id),
            property.enum_values,
        )
    }

    fn variant(&self, id: RelationshipVariantId) -> String {
        let graph = self.catalog.graph();
        let variant = graph.variant(id);
        format!(
            "{}->{}@{}",
            graph.entity(variant.source).name,
            graph.entity(variant.target).name,
            self.catalog.edge_table(id).unwrap_or_default(),
        )
    }

    fn relationship(&self, id: RelationshipId) -> String {
        let graph = self.catalog.graph();
        let relationship = graph.relationship(id);
        // A variant only a foreign key carries writes no edge rows, so it changes no table.
        let mut variants: Vec<String> = relationship
            .variants
            .iter()
            .filter(|variant| self.catalog.edge_table(**variant).is_some())
            .map(|variant| self.variant(*variant))
            .collect();
        variants.sort();
        let mut denormalized: Vec<String> = self
            .catalog
            .denormalized_properties()
            .filter(|(relationship, _, _)| *relationship == id)
            .map(|(_, key, property)| {
                format!(
                    "{} {:?} -> {} tag={} nullable={}",
                    self.property(key.property),
                    key.direction,
                    property.edge_column,
                    property.tag_key,
                    property.nullable,
                )
            })
            .collect();
        denormalized.sort();
        format!(
            "{} variants={variants:?} denormalized={denormalized:?}",
            relationship.name
        )
    }

    fn endpoint(&self, endpoint: &Endpoint) -> String {
        let graph = self.catalog.graph();
        let kind = match &endpoint.kind {
            EndpointKind::Entity(entity) => graph.entity(*entity).name.clone(),
            EndpointKind::Column { column, mapping } => {
                let mapping: BTreeMap<&String, &String> = mapping
                    .iter()
                    .map(|(raw, entity)| (raw, &graph.entity(*entity).name))
                    .collect();
                format!("{column}{mapping:?}")
            }
        };
        let properties: Vec<String> = endpoint
            .properties
            .iter()
            .map(|(property, input)| format!("{} <- {input}", self.property(*property)))
            .collect();
        format!("{} {kind} {properties:?}", endpoint.field)
    }

    fn edge(&self, edge: &EdgeMapping) -> String {
        let variants: Vec<String> = edge
            .variants
            .iter()
            .map(|variant| self.variant(*variant))
            .collect();
        format!(
            "{} [{}] [{}] mutable={} variants={variants:?}",
            self.relationship(edge.relationship),
            self.endpoint(&edge.source),
            self.endpoint(&edge.target),
            edge.mutable,
        )
    }

    fn pipeline(&self, pipeline: &Pipeline) -> String {
        let graph = self.catalog.graph();
        let sources = self.catalog.model().sources();
        let writer = sources.writer_name(graph, pipeline.writer);
        let columns: Vec<String> = pipeline
            .columns
            .iter()
            .map(|column| {
                format!(
                    "{} <- {} binary={}",
                    self.property(column.property),
                    column.column,
                    column.binary
                )
            })
            .collect();
        let query = match &pipeline.query {
            ExtractQuery::Generated { filter } => format!("generated {filter:?}"),
            ExtractQuery::Template(sql) => format!("template {sql}"),
        };
        let lookups: Vec<String> = pipeline
            .lookups
            .iter()
            .map(|lookup| {
                format!(
                    "{} {} {:?} {} {}",
                    graph.entity(lookup.entity).name,
                    lookup.id_column,
                    lookup.fields,
                    lookup.table,
                    lookup.namespaced
                )
            })
            .collect();
        let transform = match &pipeline.transform {
            Transform::Rust(name) => format!("rust {name}"),
            Transform::DataFusion(edges) => edges
                .iter()
                .map(|edge| self.edge(edge))
                .collect::<Vec<_>>()
                .join("; "),
        };
        let mut emits: Vec<String> = sources
            .emitted_relationships(pipeline)
            .into_iter()
            .map(|relationship| self.relationship(relationship))
            .collect();
        emits.sort();
        let reindex: Vec<String> = sources
            .reindex_triggers()
            .iter()
            .filter(|trigger| trigger.target == writer)
            .map(|trigger| format!("{trigger:?}"))
            .collect();
        format!(
            "{writer} {:?} source={:?} tables={:?} columns={columns:?} fields={:?} \
             order={:?} query={query} lookups={lookups:?} transform={transform} \
             emits={emits:?} reindex={reindex:?}",
            pipeline.scope,
            sources.data_source(pipeline),
            pipeline.tables,
            pipeline.fields,
            pipeline.order_by,
        )
    }
}

#[cfg(test)]
mod tests {
    use query_data_model::Packages;

    use super::*;
    use crate::archive::ModelArchive;
    use crate::ledger::MigrationLedger;
    use crate::test_helpers::entity_set;

    /// Ledger entries wider than any catalog change between the archives.
    const WIDENED_BY_HAND: [(u32, &str); 4] = [
        (
            94,
            "the DDL generator changed in code; the archives are identical",
        ),
        (109, "a forced re-index; the archives are identical"),
        (
            113,
            "skip indexes on seven single-writer node tables; no edge or shared table",
        ),
        (
            115,
            "epoch nulls and indexes on node tables only; no edge or shared table",
        ),
    ];

    fn bundled_model(version: u32) -> Arc<Model> {
        ModelArchive::bundled(version)
            .unwrap()
            .unwrap_or_else(|| panic!("v{version} model archive is bundled"))
            .packages()
            .deployment("server")
            .unwrap_or_else(|error| panic!("v{version} model loads: {error}"))
    }

    fn derive_embedded_edit(path: &str, from: &str, to: &str) -> DerivedScope {
        let embedded = Packages::embedded();
        let mut sources = embedded.sources().clone();
        let source = sources.get_mut(path).unwrap();
        assert!(source.contains(from), "{path} contains {from:?}");
        *source = source.replacen(from, to, 1);
        derive_scope(
            &embedded.deployment("server").unwrap(),
            &Packages::new(query_data_model::Tier::Platform, sources)
                .deployment("server")
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn identical_models_derive_no_scope() {
        let model = Packages::embedded().deployment("server").unwrap();
        let derived = derive_scope(&model, &model).unwrap();
        assert_eq!(derived.scope, MigrationScope::None);
        assert!(derived.tables.is_empty(), "{:?}", derived.tables);
    }

    #[test]
    fn query_metadata_change_derives_no_scope() {
        let derived = derive_embedded_edit(
            "gitlab.core/graph.yaml",
            "A GitLab group (top-level group or subgroup)",
            "A group",
        );
        assert_eq!(derived.scope, MigrationScope::None, "{:?}", derived.tables);
    }

    #[test]
    fn node_index_change_rebuilds_its_entity_and_keeps_edge_rows() {
        let derived = derive_embedded_edit(
            "gitlab.plan/storage/clickhouse.yaml",
            "        description: text\n",
            "",
        );
        assert_eq!(
            derived.tables,
            BTreeMap::from([("gl_label".to_string(), TableChange::Layout)])
        );
        assert_eq!(
            derived.scope,
            MigrationScope::Sdlc {
                entities: entity_set(&["Label"]),
                edge_rows: EdgeRows::Unchanged,
            }
        );
    }

    #[test]
    fn extract_template_change_rebuilds_its_writer() {
        let derived = derive_embedded_edit(
            "gitlab.core/sources/group.sql.j2",
            "SELECT",
            "SELECT DISTINCT",
        );
        assert_eq!(
            derived.tables.get("gl_group"),
            Some(&TableChange::Contents(entity_set(&["Group"])))
        );
        assert!(derived.scope.covers(&MigrationScope::Sdlc {
            entities: entity_set(&["Group"]),
            edge_rows: EdgeRows::Changed,
        }));
    }

    #[test]
    fn code_table_change_derives_code_scope() {
        let derived = derive_embedded_edit(
            "gitlab.source_code/storage/clickhouse.yaml",
            "fqn: {immutable: true, backend: {type: String, codec: [ZSTD(9)]}}",
            "fqn: {immutable: true, backend: {type: String, codec: [ZSTD(3)]}}",
        );
        assert_eq!(derived.scope, MigrationScope::Code, "{:?}", derived.tables);
    }

    #[test]
    fn derived_scope_covers_every_bundled_ledger_entry() {
        let ledger = MigrationLedger::load_embedded().unwrap();
        let mut versions = ModelArchive::bundled_versions().unwrap();
        versions.sort_unstable();
        let mut models = versions
            .iter()
            .map(|version| (*version, bundled_model(*version)));
        let (mut previous_version, mut previous) = models.next().expect("bundled archives");
        let mut pairs = Vec::new();
        for (version, next) in models {
            assert_eq!(
                version,
                previous_version + 1,
                "bundled archives skip a version"
            );
            let derived = derive_scope(&previous, &next).unwrap();
            let entry = ledger
                .migrations
                .iter()
                .find(|entry| entry.version == version)
                .unwrap_or_else(|| panic!("v{version} has a ledger entry"))
                .to_migration_scope();
            assert_eq!(
                derived.scope.covers(&entry),
                !WIDENED_BY_HAND
                    .iter()
                    .any(|(widened, _)| *widened == version),
                "v{version}: derived {} against the ledger's {entry}; changed tables {:?}",
                derived.scope,
                derived.tables
            );
            let relation = if derived.scope == entry {
                "equal".to_string()
            } else if derived.scope.covers(&entry) {
                format!("wider {:?}", derived.tables)
            } else {
                "widened by hand".to_string()
            };
            pairs.push(format!(
                "v{previous_version}->v{version}: ledger {entry}, derived {}, {relation}",
                derived.scope
            ));
            (previous_version, previous) = (version, next);
        }
        eprintln!("{}", pairs.join("\n"));
    }
}
