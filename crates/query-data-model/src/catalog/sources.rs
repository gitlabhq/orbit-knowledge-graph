use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use indexmap::IndexMap;

use super::Diagnostics;
pub use crate::spec::{EdgeMapping, Endpoint, EndpointKind, ExtractLookup, ReindexTrigger};
use crate::spec::{
    GENERATED_QUERY, PipelineSpec, Raw, Short, SourceFragment, TEMPLATE_EXTENSION, snake_case,
};
use crate::{EntityId, GraphCatalog, PropertyId, RelationshipId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EtlScope {
    Global,
    Namespaced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Writer {
    Entity(EntityId),
    Relationship(RelationshipId),
    Derived(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSource {
    pub name: String,
    pub connection: String,
    pub version: String,
    pub watermark: String,
    pub deleted: String,
    pub enabled_namespaces: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub name: String,
    pub source: usize,
    pub writer: Writer,
    pub scope: EtlScope,
    pub tables: Vec<String>,
    pub columns: Vec<SourceColumn>,
    pub fields: Vec<String>,
    pub order_by: Vec<String>,
    pub query: ExtractQuery,
    pub lookups: Vec<ExtractLookup>,
    pub partition_count: Option<u32>,
    pub transform: Transform,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceColumn {
    pub property: PropertyId,
    pub column: String,
    pub binary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractQuery {
    Generated { filter: Option<String> },
    Template(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Transform {
    DataFusion(Vec<EdgeMapping>),
    Rust(String),
}

impl Transform {
    pub fn edges(&self) -> &[EdgeMapping] {
        match self {
            Self::DataFusion(edges) => edges,
            Self::Rust(_) => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derived {
    pub name: String,
    pub emits: Vec<RelationshipId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineDescriptor {
    pub name: String,
    pub scope: EtlScope,
    pub entity: String,
    pub reindex_targets: BTreeSet<String>,
}

#[derive(Debug, Default)]
pub struct Sources {
    sources: Vec<DataSource>,
    pipelines: Vec<Pipeline>,
    derived: Vec<Derived>,
    reindex: BTreeSet<ReindexTrigger>,
}

impl Sources {
    pub fn data_source(&self, pipeline: &Pipeline) -> &DataSource {
        &self.sources[pipeline.source]
    }

    pub fn source_named(&self, name: &str) -> Option<&DataSource> {
        self.sources.iter().find(|source| source.name == name)
    }

    /// The one source that lists the namespaces enabled for indexing, with that table.
    pub fn enabled_namespaces(&self) -> Option<(&DataSource, &str)> {
        self.sources
            .iter()
            .find_map(|source| Some((source, source.enabled_namespaces.as_deref()?)))
    }

    pub fn pipelines(&self) -> &[Pipeline] {
        &self.pipelines
    }

    pub fn pipeline_named(&self, name: &str) -> Option<&Pipeline> {
        self.pipelines.iter().find(|pipeline| pipeline.name == name)
    }

    pub fn entity_pipelines(&self, entity: EntityId) -> impl Iterator<Item = &Pipeline> {
        self.pipelines
            .iter()
            .filter(move |pipeline| pipeline.writer == Writer::Entity(entity))
    }

    pub fn derived(&self) -> &[Derived] {
        &self.derived
    }

    pub fn reindex_triggers(&self) -> &BTreeSet<ReindexTrigger> {
        &self.reindex
    }

    pub fn writer_name<'a>(&'a self, graph: &'a GraphCatalog, writer: Writer) -> &'a str {
        match writer {
            Writer::Entity(entity) => &graph.entity(entity).name,
            Writer::Relationship(relationship) => &graph.relationship(relationship).name,
            Writer::Derived(index) => &self.derived[index].name,
        }
    }

    pub fn emitted_relationships(&self, pipeline: &Pipeline) -> BTreeSet<RelationshipId> {
        match pipeline.writer {
            Writer::Relationship(relationship) => BTreeSet::from([relationship]),
            Writer::Derived(index) => self.derived[index].emits.iter().copied().collect(),
            Writer::Entity(_) => pipeline
                .transform
                .edges()
                .iter()
                .map(|edge| edge.relationship)
                .collect(),
        }
    }

    pub fn descriptors(&self, graph: &GraphCatalog) -> Vec<PipelineDescriptor> {
        self.pipelines
            .iter()
            .map(|pipeline| {
                let entity = self.writer_name(graph, pipeline.writer).to_string();
                let mut reindex_targets: BTreeSet<String> = self
                    .emitted_relationships(pipeline)
                    .into_iter()
                    .map(|relationship| graph.relationship(relationship).name.clone())
                    .collect();
                reindex_targets.insert(entity.clone());
                PipelineDescriptor {
                    name: pipeline.name.clone(),
                    scope: pipeline.scope,
                    entity,
                    reindex_targets,
                }
            })
            .collect()
    }

    /// `global` tells the entities whose authorization scope is global, which index once for
    /// every namespace.
    pub(super) fn resolve(
        graph: &GraphCatalog,
        global: &dyn Fn(EntityId) -> bool,
        fragments: &[&SourceFragment],
        served: &HashSet<PropertyId>,
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let mut resolver = Resolver {
            graph,
            global,
            served,
            diagnostics,
            sources: Sources::default(),
            entity_columns: HashMap::new(),
        };
        resolver.declare_sources(fragments);
        resolver.resolve(fragments);
        resolver.sources
    }
}

struct Resolver<'a> {
    graph: &'a GraphCatalog,
    global: &'a dyn Fn(EntityId) -> bool,
    served: &'a HashSet<PropertyId>,
    diagnostics: &'a mut Diagnostics,
    sources: Sources,
    entity_columns: HashMap<EntityId, EntityColumns>,
}

struct EntityColumns {
    columns: HashMap<PropertyId, String>,
    enrich: Vec<(PropertyId, String)>,
    table: String,
    namespaced: bool,
}

#[derive(Clone, Copy)]
struct Pending<'a> {
    fragment: &'a SourceFragment,
    source: usize,
}

fn sorted<'a, T>(
    pending: &[Pending<'a>],
    section: impl Fn(&'a SourceFragment) -> &'a IndexMap<String, T>,
) -> Vec<(&'a str, &'a T, Pending<'a>)> {
    let mut items: Vec<_> = pending
        .iter()
        .flat_map(|item| {
            section(item.fragment)
                .iter()
                .map(|(name, spec)| (name.as_str(), spec, *item))
        })
        .collect();
    items.sort_by_key(|(name, ..)| *name);
    items
}

impl<'a> Resolver<'a> {
    fn declare_sources(&mut self, fragments: &[&SourceFragment]) {
        for fragment in fragments {
            let Some(declaration) = &fragment.declare else {
                continue;
            };
            if declaration.enabled_namespaces.is_some()
                && let Some((other, _)) = self.sources.enabled_namespaces()
            {
                self.diagnostics.error(
                    &fragment.package,
                    format!(
                        "sources {} and {} both list the enabled namespaces",
                        other.name, fragment.source
                    ),
                );
            }
            if self.sources.source_named(&fragment.source).is_some() {
                self.diagnostics.error(
                    &fragment.package,
                    format!("source {} is declared twice", fragment.source),
                );
                continue;
            }
            self.sources.sources.push(DataSource {
                name: fragment.source.clone(),
                connection: declaration.connection.clone(),
                version: declaration.version.clone(),
                watermark: declaration.watermark.clone(),
                deleted: declaration.deleted.clone(),
                enabled_namespaces: declaration.enabled_namespaces.clone(),
            });
        }
    }

    fn resolve(&mut self, fragments: &[&'a SourceFragment]) {
        let mut pending = Vec::new();
        for fragment in fragments {
            let found = self
                .sources
                .sources
                .iter()
                .position(|source| source.name == fragment.source);
            let Some(source) = self.diagnostics.require(found, &fragment.package, || {
                format!("source {} is never declared", fragment.source)
            }) else {
                continue;
            };
            pending.push(Pending { fragment, source });
        }
        let mut work = Vec::new();
        for (name, spec, item) in sorted(&pending, |fragment| &fragment.entities) {
            let package = item.fragment.package.as_str();
            let Some(entity) =
                self.diagnostics
                    .require(self.graph.entity_id(name), package, || {
                        format!(
                            "source {} names unknown entity {name}",
                            item.fragment.source
                        )
                    })
            else {
                continue;
            };
            if self.entity_columns.contains_key(&entity) {
                self.diagnostics
                    .error(package, format!("entity {name} has two sources"));
                continue;
            }
            let columns = self.entity_columns(package, entity, spec);
            self.entity_columns.insert(entity, columns);
            work.push((Writer::Entity(entity), name, spec, item));
        }
        for (name, spec, item) in sorted(&pending, |fragment| &fragment.relationships) {
            let package = item.fragment.package.as_str();
            let Some(relationship) =
                self.diagnostics
                    .require(self.graph.relationship_id(name), package, || {
                        format!(
                            "source {} names unknown relationship {name}",
                            item.fragment.source
                        )
                    })
            else {
                continue;
            };
            self.reindex(package, name, &spec.reindex, item.source);
            let writer = Writer::Relationship(relationship);
            work.extend(
                spec.pipelines
                    .iter()
                    .map(|(pipeline, spec)| (writer, pipeline.as_str(), spec, item)),
            );
        }
        for (name, spec, item) in sorted(&pending, |fragment| &fragment.derived) {
            let package = item.fragment.package.as_str();
            if self.graph.entity_id(name).is_some() {
                self.diagnostics.error(
                    package,
                    format!("derived source {name} redeclares a graph entity"),
                );
                continue;
            }
            let emits = spec
                .emits
                .iter()
                .flatten()
                .filter_map(|emitted| {
                    self.diagnostics
                        .require(self.graph.relationship_id(emitted), package, || {
                            format!("derived source {name} emits unknown relationship {emitted}")
                        })
                })
                .collect();
            let index = self.sources.derived.len();
            self.sources.derived.push(Derived {
                name: name.to_string(),
                emits,
            });
            work.push((Writer::Derived(index), name, spec, item));
        }
        let pipelines: Vec<_> = work
            .into_iter()
            .map(|(writer, name, spec, item)| self.pipeline(writer, name, spec, item))
            .collect();
        let mut names = HashSet::new();
        for pipeline in &pipelines {
            if !names.insert(pipeline.name.as_str()) {
                self.diagnostics.error(
                    &fragments[0].package,
                    format!("pipeline {} is declared twice", pipeline.name),
                );
            }
        }
        self.sources.pipelines = pipelines;
    }

    fn pipeline(
        &mut self,
        writer: Writer,
        name: &str,
        spec: &PipelineSpec,
        item: Pending<'_>,
    ) -> Pipeline {
        let package = item.fragment.package.as_str();
        let query = self.query(item.fragment, name, spec);
        let lookups = self.lookups(package, name, spec.lookups.as_deref().unwrap_or_default());
        let reindex = spec.reindex.as_deref().unwrap_or_default();
        self.reindex(package, name, reindex, item.source);
        let columns = match writer {
            Writer::Entity(entity) => self.extracted_columns(entity, spec),
            Writer::Relationship(_) | Writer::Derived(_) => Vec::new(),
        };
        let transform = match &spec.transform {
            Some(transform) => {
                if matches!(query, ExtractQuery::Generated { .. }) {
                    self.diagnostics.error(
                        package,
                        format!("derived source {name} needs a template, not a generated query"),
                    );
                }
                Transform::Rust(transform.clone())
            }
            None => {
                let specs = self.edge_specs(writer, spec);
                Transform::DataFusion(self.edges(package, name, &specs))
            }
        };
        Pipeline {
            name: name.to_string(),
            source: item.source,
            writer,
            scope: match writer {
                Writer::Entity(entity) if (self.global)(entity) => EtlScope::Global,
                _ => EtlScope::Namespaced,
            },
            tables: spec.tables.clone(),
            columns,
            fields: spec.fields.clone().unwrap_or_default(),
            order_by: spec.order_by.clone(),
            query,
            lookups,
            partition_count: spec.partition_count,
            transform,
        }
    }

    fn edge_specs(&self, writer: Writer, spec: &PipelineSpec) -> Vec<EdgeMapping<Raw>> {
        let Writer::Relationship(relationship) = writer else {
            return spec.edges.clone().unwrap_or_default();
        };
        let label = &self.graph.relationship(relationship).name;
        spec.from
            .clone()
            .zip(spec.to.clone())
            .map(|(source, target)| EdgeMapping {
                relationship: label.clone(),
                source,
                target,
                array_field: None,
                mutable: false,
                variants: (),
            })
            .into_iter()
            .collect()
    }

    fn entity_columns(
        &mut self,
        package: &str,
        entity: EntityId,
        spec: &PipelineSpec,
    ) -> EntityColumns {
        let name = self.graph.entity(entity).name.clone();
        let mut columns = HashMap::new();
        for (property, column) in spec.columns.iter().flatten() {
            if let Some(property) = self.stored_property(package, entity, property) {
                columns.insert(property, column.clone());
            }
        }
        for property in spec.binary.iter().flatten() {
            self.stored_property(package, entity, property);
        }
        let enrich = spec
            .enrich
            .iter()
            .flatten()
            .filter_map(|property| {
                let id = self.stored_property(package, entity, property)?;
                let column = columns
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| property.clone());
                Some((id, column))
            })
            .collect();
        let table = self.diagnostics.require(spec.tables.first(), package, || {
            format!("source for {name} lists no tables")
        });
        EntityColumns {
            columns,
            enrich,
            table: table.cloned().unwrap_or_default(),
            namespaced: !(self.global)(entity),
        }
    }

    fn stored_property(
        &mut self,
        package: &str,
        entity: EntityId,
        property: &str,
    ) -> Option<PropertyId> {
        let name = &self.graph.entity(entity).name;
        let id =
            self.diagnostics
                .require(self.graph.property_id(entity, property), package, || {
                    format!("source names unknown property {name}.{property}")
                })?;
        if self.served.contains(&id) {
            self.diagnostics.error(
                package,
                format!("source extracts {name}.{property}, which a service provides"),
            );
            return None;
        }
        Some(id)
    }

    fn extracted_columns(&self, entity: EntityId, spec: &PipelineSpec) -> Vec<SourceColumn> {
        let columns = &self.entity_columns[&entity].columns;
        let binary = spec.binary.as_deref().unwrap_or_default();
        self.graph
            .entity(entity)
            .properties
            .iter()
            .filter(|property| !self.served.contains(property))
            .map(|property| {
                let name = &self.graph.property(*property).name;
                SourceColumn {
                    property: *property,
                    column: columns
                        .get(property)
                        .cloned()
                        .unwrap_or_else(|| name.clone()),
                    binary: binary.contains(name),
                }
            })
            .collect()
    }

    fn query(
        &mut self,
        fragment: &SourceFragment,
        name: &str,
        spec: &PipelineSpec,
    ) -> ExtractQuery {
        let package = fragment.package.as_str();
        if spec.tables.is_empty() {
            self.diagnostics
                .error(package, format!("pipeline {name} lists no tables"));
        }
        if spec.order_by.is_empty() {
            self.diagnostics
                .error(package, format!("pipeline {name} has an empty order_by"));
        }
        if spec.query == GENERATED_QUERY {
            if spec.tables.len() > 1 {
                self.diagnostics.error(
                    package,
                    format!("pipeline {name} generates its query from more than one table"),
                );
            }
            return ExtractQuery::Generated {
                filter: spec.filter.clone(),
            };
        }
        for (set, key) in [
            (spec.filter.is_some(), "filter"),
            (spec.fields.is_some(), "fields"),
            (spec.lookups.is_some(), "lookups"),
        ] {
            if set {
                self.diagnostics.error(
                    package,
                    format!("pipeline {name} sets {key}, which needs a generated query"),
                );
            }
        }
        let template = spec
            .query
            .ends_with(TEMPLATE_EXTENSION)
            .then(|| fragment.templates.get(&spec.query))
            .flatten();
        match self.diagnostics.require(template, package, || {
            format!("pipeline {name} names unknown template {}", spec.query)
        }) {
            Some(template) => ExtractQuery::Template(template.clone()),
            None => ExtractQuery::Generated { filter: None },
        }
    }

    fn edge(
        &mut self,
        package: &str,
        pipeline: &str,
        spec: &EdgeMapping<Raw>,
    ) -> Option<EdgeMapping> {
        let label = &spec.relationship;
        let relationship =
            self.diagnostics
                .require(self.graph.relationship_id(label), package, || {
                    format!("pipeline {pipeline} emits unknown relationship {label}")
                })?;
        let source = self.endpoint(package, pipeline, &spec.source)?;
        let target = self.endpoint(package, pipeline, &spec.target)?;
        let variants: Vec<_> = self
            .graph
            .relationship(relationship)
            .variants
            .iter()
            .copied()
            .filter(|variant| {
                let variant = self.graph.variant(*variant);
                source.kind.matches(variant.source) && target.kind.matches(variant.target)
            })
            .collect();
        for from in source.kind.entities() {
            for to in target.kind.entities() {
                let exists = variants.iter().any(|variant| {
                    let variant = self.graph.variant(*variant);
                    variant.source == from && variant.target == to
                });
                if !exists {
                    self.diagnostics.error(
                        package,
                        format!(
                            "pipeline {pipeline} emits unknown variant {} -[{label}]-> {}",
                            self.graph.entity(from).name,
                            self.graph.entity(to).name
                        ),
                    );
                }
            }
        }
        if variants.is_empty() {
            self.diagnostics.error(
                package,
                format!("pipeline {pipeline} emits no declared {label} variant"),
            );
            return None;
        }
        Some(EdgeMapping {
            relationship,
            source,
            target,
            array_field: spec.array_field.clone(),
            mutable: spec.mutable,
            variants,
        })
    }

    fn endpoint(
        &mut self,
        package: &str,
        pipeline: &str,
        spec: &Endpoint<Raw>,
    ) -> Option<Endpoint> {
        let entity = |name: &str, diagnostics: &mut Diagnostics| {
            diagnostics.require(self.graph.entity_id(name), package, || {
                format!("pipeline {pipeline} names unknown entity {name}")
            })
        };
        let kind = match &spec.kind {
            EndpointKind::Entity(name) => EndpointKind::Entity(entity(name, self.diagnostics)?),
            EndpointKind::Column { column, mapping } => {
                if spec.properties {
                    self.diagnostics.error(
                        package,
                        format!(
                            "pipeline {pipeline} endpoint {} derives its kind and cannot enrich",
                            spec.field
                        ),
                    );
                }
                let mut resolved = BTreeMap::new();
                for (raw, name) in mapping {
                    resolved.insert(raw.clone(), entity(name, self.diagnostics)?);
                }
                EndpointKind::Column {
                    column: column.clone(),
                    mapping: resolved,
                }
            }
        };
        Some(Endpoint {
            field: spec.field.clone(),
            kind,
            properties: IndexMap::new(),
        })
    }

    fn edges(
        &mut self,
        package: &str,
        pipeline: &str,
        specs: &[EdgeMapping<Raw>],
    ) -> Vec<EdgeMapping> {
        let mut edges: Vec<_> = specs
            .iter()
            .filter_map(|spec| Some((self.edge(package, pipeline, spec)?, spec)))
            .collect();
        let mut counts = HashMap::<EntityId, usize>::new();
        for (edge, spec) in &edges {
            for (endpoint, written) in [(&edge.source, &spec.source), (&edge.target, &spec.target)]
            {
                if let Some(entity) = endpoint.kind.entity().filter(|_| written.properties) {
                    *counts.entry(entity).or_default() += 1;
                }
            }
        }
        for (edge, spec) in &mut edges {
            let endpoints = [
                (&mut edge.source, &spec.source),
                (&mut edge.target, &spec.target),
            ];
            for (endpoint, written) in endpoints {
                if let Some(entity) = endpoint.kind.entity().filter(|_| written.properties) {
                    self.enrich(package, pipeline, endpoint, entity, counts[&entity]);
                }
            }
        }
        edges.into_iter().map(|(edge, _)| edge).collect()
    }

    fn enrich(
        &mut self,
        package: &str,
        pipeline: &str,
        endpoint: &mut Endpoint,
        entity: EntityId,
        references: usize,
    ) {
        let name = &self.graph.entity(entity).name;
        let Some(columns) = self.entity_columns.get(&entity) else {
            self.diagnostics.error(
                package,
                format!("pipeline {pipeline} enriches from {name}, which has no source"),
            );
            return;
        };
        if columns.enrich.is_empty() {
            self.diagnostics.error(
                package,
                format!("pipeline {pipeline}: {name} declares no enrich properties"),
            );
            return;
        }
        let prefix = enrichment_prefix(name, &endpoint.field, references);
        for (property, column) in &columns.enrich {
            endpoint
                .properties
                .insert(*property, format!("{prefix}{column}"));
        }
    }

    fn lookups(
        &mut self,
        package: &str,
        pipeline: &str,
        specs: &[ExtractLookup<Raw>],
    ) -> Vec<ExtractLookup> {
        let mut counts = HashMap::<&str, usize>::new();
        for spec in specs.iter().filter(|spec| spec.fields.is_empty()) {
            *counts.entry(spec.entity.as_str()).or_default() += 1;
        }
        let mut lookups = Vec::new();
        let mut outputs = HashSet::new();
        for spec in specs {
            let found = self.graph.entity_id(&spec.entity);
            let Some(entity) = self.diagnostics.require(found, package, || {
                format!(
                    "pipeline {pipeline} looks up unknown entity {}",
                    spec.entity
                )
            }) else {
                continue;
            };
            let Some(columns) = self.entity_columns.get(&entity) else {
                self.diagnostics.error(
                    package,
                    format!(
                        "pipeline {pipeline} looks up {}, which has no source",
                        spec.entity
                    ),
                );
                continue;
            };
            let mut fields = spec.fields.clone();
            if fields.is_empty() {
                if columns.enrich.is_empty() {
                    self.diagnostics.error(
                        package,
                        format!(
                            "pipeline {pipeline}: {} declares no enrich properties",
                            spec.entity
                        ),
                    );
                }
                let prefix =
                    enrichment_prefix(&spec.entity, &spec.id_column, counts[spec.entity.as_str()]);
                for (_, column) in &columns.enrich {
                    fields.insert(column.clone(), format!("{prefix}{column}"));
                }
            }
            for output in fields.values() {
                if !outputs.insert(output.clone()) {
                    self.diagnostics.error(
                        package,
                        format!("pipeline {pipeline} looks up output field {output} twice"),
                    );
                }
            }
            lookups.push(ExtractLookup {
                entity,
                id_column: spec.id_column.clone(),
                fields,
                table: columns.table.clone(),
                namespaced: columns.namespaced,
            });
        }
        lookups
    }

    fn reindex(
        &mut self,
        package: &str,
        target: &str,
        specs: &[Short<ReindexTrigger<Raw>>],
        source: usize,
    ) {
        let watermark = &self.sources.sources[source].watermark;
        for Short(spec) in specs {
            let trigger = ReindexTrigger {
                table: spec.table.clone(),
                target: target.to_string(),
                traversal_path: spec.traversal_path.clone(),
                watermark: watermark.clone(),
            };
            if !self.sources.reindex.insert(trigger) {
                let table = &spec.table;
                self.diagnostics
                    .error(package, format!("{target} reindexes on {table} twice"));
            }
        }
    }
}

fn enrichment_prefix(entity: &str, field: &str, references: usize) -> String {
    if references > 1 {
        return format!("{}_", field.strip_suffix("_id").unwrap_or(field));
    }
    format!("{}_", snake_case(entity))
}
