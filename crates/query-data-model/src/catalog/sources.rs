use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use indexmap::IndexMap;

use super::Diagnostics;
use crate::spec::{
    EndpointKindSpec, EndpointSpec, ExtractLookupSpec, GENERATED_QUERY, PathSpec, ReindexSpec,
    SourceFragment, TEMPLATE_EXTENSION, snake_case,
};
use crate::{EntityId, GraphCatalog, PropertyId, RelationshipId, RelationshipVariantId};

const PATH_COLUMN: &str = "traversal_path";

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractLookup {
    pub entity: EntityId,
    pub id_column: String,
    pub fields: IndexMap<String, String>,
    pub table: String,
    pub namespaced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
pub struct EdgeMapping {
    pub relationship: RelationshipId,
    pub source: Endpoint,
    pub target: Endpoint,
    pub array_field: Option<String>,
    pub mutable: bool,
    pub variants: Vec<RelationshipVariantId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub field: String,
    pub kind: EndpointKind,
    pub properties: IndexMap<PropertyId, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    Entity(EntityId),
    Column {
        column: String,
        mapping: BTreeMap<String, EntityId>,
    },
}

impl EndpointKind {
    pub fn entity(&self) -> Option<EntityId> {
        match self {
            Self::Entity(entity) => Some(*entity),
            Self::Column { .. } => None,
        }
    }

    pub fn matches(&self, entity: EntityId) -> bool {
        self.entity().is_none_or(|kind| kind == entity)
    }

    fn entities(&self) -> Vec<EntityId> {
        match self {
            Self::Entity(entity) => vec![*entity],
            Self::Column { mapping, .. } => mapping.values().copied().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derived {
    pub name: String,
    pub emits: Vec<RelationshipId>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReindexTrigger {
    pub table: String,
    pub target: String,
    pub traversal_path: PathResolution,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PathResolution {
    Column(String),
    Dictionary {
        dictionary: String,
        key_column: String,
    },
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
    pub fn data_sources(&self) -> &[DataSource] {
        &self.sources
    }

    pub fn data_source(&self, pipeline: &Pipeline) -> &DataSource {
        &self.sources[pipeline.source]
    }

    pub fn source_named(&self, name: &str) -> Option<&DataSource> {
        self.sources.iter().find(|source| source.name == name)
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

    pub(super) fn resolve(
        graph: &GraphCatalog,
        fragments: &[&SourceFragment],
        served: &HashSet<PropertyId>,
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let mut resolver = Resolver {
            graph,
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

struct Pending<'a> {
    fragment: &'a SourceFragment,
    source: usize,
}

impl<'a> Resolver<'a> {
    fn declare_sources(&mut self, fragments: &[&SourceFragment]) {
        for fragment in fragments {
            let Some(declaration) = &fragment.declare else {
                continue;
            };
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
        let mut entities = Vec::new();
        for item in &pending {
            for (name, spec) in &item.fragment.entities {
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
                entities.push((name.as_str(), entity, spec, item));
            }
        }
        let mut pipelines = Vec::new();
        entities.sort_by_key(|(name, ..)| *name);
        for (name, entity, spec, item) in entities {
            let package = item.fragment.package.as_str();
            let extract = Extract {
                name,
                tables: &spec.tables,
                fields: &[],
                order_by: &spec.order_by,
                query: &spec.query,
                filter: spec.filter.as_ref(),
                lookups: &[],
            };
            let query = self.query(item.fragment, &extract);
            let edges = spec
                .edges
                .iter()
                .filter_map(|edge| {
                    self.edge(package, name, &edge.label, &edge.from, &edge.to)
                        .map(|(relationship, source, target, variants)| EdgeMapping {
                            relationship,
                            source,
                            target,
                            array_field: edge.array_field.clone(),
                            mutable: edge.mutable,
                            variants,
                        })
                })
                .collect::<Vec<_>>();
            let edges = self.enrich(package, name, edges, &spec.edges);
            let columns = self.extracted_columns(entity, spec);
            self.reindex(package, name, &spec.reindex);
            pipelines.push(Pipeline {
                name: name.to_string(),
                source: item.source,
                writer: Writer::Entity(entity),
                scope: if spec.global {
                    EtlScope::Global
                } else {
                    EtlScope::Namespaced
                },
                tables: spec.tables.clone(),
                columns,
                fields: Vec::new(),
                order_by: spec.order_by.clone(),
                query,
                lookups: Vec::new(),
                partition_count: spec.partition_count,
                transform: Transform::DataFusion(edges),
            });
        }
        let mut relationships: Vec<_> = pending
            .iter()
            .flat_map(|item| {
                item.fragment
                    .relationships
                    .iter()
                    .map(move |(name, spec)| (name.as_str(), spec, item))
            })
            .collect();
        relationships.sort_by_key(|(name, ..)| *name);
        for (name, spec, item) in relationships {
            let package = item.fragment.package.as_str();
            if self
                .diagnostics
                .require(self.graph.relationship_id(name), package, || {
                    format!(
                        "source {} names unknown relationship {name}",
                        item.fragment.source
                    )
                })
                .is_none()
            {
                continue;
            }
            self.reindex(package, name, &spec.reindex);
            for (pipeline, edge) in &spec.pipelines {
                let extract = Extract {
                    name: pipeline,
                    tables: &edge.tables,
                    fields: &edge.fields,
                    order_by: &edge.order_by,
                    query: &edge.query,
                    filter: edge.filter.as_ref(),
                    lookups: &edge.lookups,
                };
                let query = self.query(item.fragment, &extract);
                let lookups = self.lookups(package, pipeline, &edge.lookups);
                let Some((relationship, source, target, variants)) =
                    self.edge(package, pipeline, name, &edge.from, &edge.to)
                else {
                    continue;
                };
                let mapping = EdgeMapping {
                    relationship,
                    source,
                    target,
                    array_field: edge.array_field.clone(),
                    mutable: edge.mutable,
                    variants,
                };
                let mut specs = [(edge.from.clone(), edge.to.clone())];
                let edges = self.enrich_endpoints(package, pipeline, vec![mapping], &mut specs);
                pipelines.push(Pipeline {
                    name: pipeline.clone(),
                    source: item.source,
                    writer: Writer::Relationship(relationship),
                    scope: EtlScope::Namespaced,
                    tables: edge.tables.clone(),
                    columns: Vec::new(),
                    fields: edge.fields.clone(),
                    order_by: edge.order_by.clone(),
                    query,
                    lookups,
                    partition_count: None,
                    transform: Transform::DataFusion(edges),
                });
            }
        }
        let mut derived: Vec<_> = pending
            .iter()
            .flat_map(|item| {
                item.fragment
                    .derived
                    .iter()
                    .map(move |(name, spec)| (name.as_str(), spec, item))
            })
            .collect();
        derived.sort_by_key(|(name, ..)| *name);
        for (name, spec, item) in derived {
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
                .filter_map(|emitted| {
                    self.diagnostics
                        .require(self.graph.relationship_id(emitted), package, || {
                            format!("derived source {name} emits unknown relationship {emitted}")
                        })
                })
                .collect();
            let extract = Extract {
                name,
                tables: &spec.tables,
                fields: &[],
                order_by: &spec.order_by,
                query: &spec.query,
                filter: None,
                lookups: &[],
            };
            let query = self.query(item.fragment, &extract);
            if matches!(query, ExtractQuery::Generated { .. }) {
                self.diagnostics.error(
                    package,
                    format!("derived source {name} needs a template, not a generated query"),
                );
            }
            self.reindex(package, name, &spec.reindex);
            let index = self.sources.derived.len();
            self.sources.derived.push(Derived {
                name: name.to_string(),
                emits,
            });
            pipelines.push(Pipeline {
                name: name.to_string(),
                source: item.source,
                writer: Writer::Derived(index),
                scope: EtlScope::Namespaced,
                tables: spec.tables.clone(),
                columns: Vec::new(),
                fields: Vec::new(),
                order_by: spec.order_by.clone(),
                query,
                lookups: Vec::new(),
                partition_count: None,
                transform: Transform::Rust(spec.transform.clone()),
            });
        }
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

    fn entity_columns(
        &mut self,
        package: &str,
        entity: EntityId,
        spec: &crate::spec::EntitySourceSpec,
    ) -> EntityColumns {
        let name = self.graph.entity(entity).name.clone();
        let mut columns = HashMap::new();
        for (property, column) in &spec.columns {
            if let Some(property) = self.stored_property(package, entity, property) {
                columns.insert(property, column.clone());
            }
        }
        for property in &spec.binary {
            self.stored_property(package, entity, property);
        }
        let enrich = spec
            .enrich
            .iter()
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
            namespaced: !spec.global,
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

    fn extracted_columns(
        &self,
        entity: EntityId,
        spec: &crate::spec::EntitySourceSpec,
    ) -> Vec<SourceColumn> {
        let columns = &self.entity_columns[&entity].columns;
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
                    binary: spec.binary.contains(name),
                }
            })
            .collect()
    }

    fn query(&mut self, fragment: &SourceFragment, extract: &Extract<'_>) -> ExtractQuery {
        let package = fragment.package.as_str();
        let name = extract.name;
        if extract.tables.is_empty() {
            self.diagnostics
                .error(package, format!("pipeline {name} lists no tables"));
        }
        if extract.order_by.is_empty() {
            self.diagnostics
                .error(package, format!("pipeline {name} has an empty order_by"));
        }
        if extract.query == GENERATED_QUERY {
            if extract.tables.len() > 1 {
                self.diagnostics.error(
                    package,
                    format!("pipeline {name} generates its query from more than one table"),
                );
            }
            return ExtractQuery::Generated {
                filter: extract.filter.cloned(),
            };
        }
        for (set, key) in [
            (extract.filter.is_some(), "filter"),
            (!extract.fields.is_empty(), "fields"),
            (!extract.lookups.is_empty(), "lookups"),
        ] {
            if set {
                self.diagnostics.error(
                    package,
                    format!("pipeline {name} sets {key}, which needs a generated query"),
                );
            }
        }
        let template = extract
            .query
            .ends_with(TEMPLATE_EXTENSION)
            .then(|| fragment.templates.get(extract.query))
            .flatten();
        match self.diagnostics.require(template, package, || {
            format!("pipeline {name} names unknown template {}", extract.query)
        }) {
            Some(template) => ExtractQuery::Template(template.clone()),
            None => ExtractQuery::Generated { filter: None },
        }
    }

    fn edge(
        &mut self,
        package: &str,
        pipeline: &str,
        label: &str,
        from: &EndpointSpec,
        to: &EndpointSpec,
    ) -> Option<(
        RelationshipId,
        Endpoint,
        Endpoint,
        Vec<RelationshipVariantId>,
    )> {
        let relationship =
            self.diagnostics
                .require(self.graph.relationship_id(label), package, || {
                    format!("pipeline {pipeline} emits unknown relationship {label}")
                })?;
        let source = self.endpoint(package, pipeline, from)?;
        let target = self.endpoint(package, pipeline, to)?;
        let declared = &self.graph.relationship(relationship).variants;
        for from in source.kind.entities() {
            for to in target.kind.entities() {
                let exists = declared.iter().any(|variant| {
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
        let variants: Vec<_> = declared
            .iter()
            .copied()
            .filter(|variant| {
                let variant = self.graph.variant(*variant);
                source.kind.matches(variant.source) && target.kind.matches(variant.target)
            })
            .collect();
        if variants.is_empty() {
            self.diagnostics.error(
                package,
                format!("pipeline {pipeline} emits no declared {label} variant"),
            );
            return None;
        }
        Some((relationship, source, target, variants))
    }

    fn endpoint(&mut self, package: &str, pipeline: &str, spec: &EndpointSpec) -> Option<Endpoint> {
        let entity = |name: &str, diagnostics: &mut Diagnostics| {
            diagnostics.require(self.graph.entity_id(name), package, || {
                format!("pipeline {pipeline} names unknown entity {name}")
            })
        };
        let kind = match &spec.kind {
            EndpointKindSpec::Entity(name) => EndpointKind::Entity(entity(name, self.diagnostics)?),
            EndpointKindSpec::Derived { derive, mapping } => {
                if spec.enrich || !spec.properties.is_empty() {
                    self.diagnostics.error(
                        package,
                        format!(
                            "pipeline {pipeline} endpoint {} derives its kind and cannot carry properties",
                            spec.field
                        ),
                    );
                }
                let mut resolved = BTreeMap::new();
                for (raw, name) in mapping {
                    resolved.insert(raw.clone(), entity(name, self.diagnostics)?);
                }
                EndpointKind::Column {
                    column: derive.clone(),
                    mapping: resolved,
                }
            }
        };
        if spec.enrich && !spec.properties.is_empty() {
            self.diagnostics.error(
                package,
                format!(
                    "pipeline {pipeline} endpoint {} sets both enrich and properties",
                    spec.field
                ),
            );
        }
        let mut properties = IndexMap::new();
        if let EndpointKind::Entity(entity) = kind {
            for (property, input) in &spec.properties {
                let found = self.graph.property_id(entity, property);
                if let Some(property) = self.diagnostics.require(found, package, || {
                    format!(
                        "pipeline {pipeline} names unknown property {}.{property}",
                        self.graph.entity(entity).name
                    )
                }) {
                    properties.insert(property, input.clone());
                }
            }
        }
        Some(Endpoint {
            field: spec.field.clone(),
            kind,
            properties,
        })
    }

    fn enrich(
        &mut self,
        package: &str,
        pipeline: &str,
        edges: Vec<EdgeMapping>,
        specs: &[crate::spec::EdgeSpec],
    ) -> Vec<EdgeMapping> {
        let mut specs: Vec<_> = specs
            .iter()
            .map(|edge| (edge.from.clone(), edge.to.clone()))
            .collect();
        self.enrich_endpoints(package, pipeline, edges, &mut specs)
    }

    fn enrich_endpoints(
        &mut self,
        package: &str,
        pipeline: &str,
        mut edges: Vec<EdgeMapping>,
        specs: &mut [(EndpointSpec, EndpointSpec)],
    ) -> Vec<EdgeMapping> {
        let mut counts = HashMap::<EntityId, usize>::new();
        let enriched: Vec<(usize, bool, EntityId)> = edges
            .iter()
            .zip(specs.iter())
            .enumerate()
            .flat_map(|(index, (edge, (from, to)))| {
                [(true, &edge.source, from), (false, &edge.target, to)]
                    .into_iter()
                    .filter(|(_, _, spec)| spec.enrich)
                    .filter_map(move |(source, endpoint, _)| {
                        endpoint.kind.entity().map(|entity| (index, source, entity))
                    })
            })
            .collect();
        for (_, _, entity) in &enriched {
            *counts.entry(*entity).or_default() += 1;
        }
        for (index, source, entity) in enriched {
            let Some(columns) = self.entity_columns.get(&entity) else {
                self.diagnostics.error(
                    package,
                    format!(
                        "pipeline {pipeline} enriches from {}, which has no source",
                        self.graph.entity(entity).name
                    ),
                );
                continue;
            };
            if columns.enrich.is_empty() {
                self.diagnostics.error(
                    package,
                    format!(
                        "pipeline {pipeline}: {} declares no enrich properties",
                        self.graph.entity(entity).name
                    ),
                );
                continue;
            }
            let endpoint = if source {
                &mut edges[index].source
            } else {
                &mut edges[index].target
            };
            let prefix = enrichment_prefix(
                &self.graph.entity(entity).name,
                &endpoint.field,
                counts[&entity],
            );
            for (property, column) in &columns.enrich {
                endpoint
                    .properties
                    .insert(*property, format!("{prefix}{column}"));
            }
        }
        edges
    }

    fn lookups(
        &mut self,
        package: &str,
        pipeline: &str,
        specs: &[ExtractLookupSpec],
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
                    enrichment_prefix(&spec.entity, &spec.id, counts[spec.entity.as_str()]);
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
                id_column: spec.id.clone(),
                fields,
                table: columns.table.clone(),
                namespaced: columns.namespaced,
            });
        }
        lookups
    }

    fn reindex(&mut self, package: &str, target: &str, specs: &[ReindexSpec]) {
        for spec in specs {
            let (table, traversal_path) = match spec {
                ReindexSpec::Table(table) => (table, PathResolution::Column(PATH_COLUMN.into())),
                ReindexSpec::Detail {
                    table,
                    traversal_path: PathSpec::Column { column },
                } => (table, PathResolution::Column(column.clone())),
                ReindexSpec::Detail {
                    table,
                    traversal_path:
                        PathSpec::Dictionary {
                            dictionary,
                            key_column,
                        },
                } => (
                    table,
                    PathResolution::Dictionary {
                        dictionary: dictionary.clone(),
                        key_column: key_column.clone(),
                    },
                ),
            };
            let trigger = ReindexTrigger {
                table: table.clone(),
                target: target.to_string(),
                traversal_path,
            };
            if !self.sources.reindex.insert(trigger) {
                self.diagnostics
                    .error(package, format!("{target} reindexes on {table} twice"));
            }
        }
    }
}

struct Extract<'a> {
    name: &'a str,
    tables: &'a [String],
    fields: &'a [String],
    order_by: &'a [String],
    query: &'a str,
    filter: Option<&'a String>,
    lookups: &'a [ExtractLookupSpec],
}

fn enrichment_prefix(entity: &str, field: &str, references: usize) -> String {
    if references > 1 {
        return format!("{}_", field.strip_suffix("_id").unwrap_or(field));
    }
    format!("{}_", snake_case(entity))
}
