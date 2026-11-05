use std::collections::HashSet;

use indexmap::IndexMap;
use ontology::FieldSelectivity;
use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::Diagnostics;
use super::access::Guard;
use crate::spec::{
    EngineSpec, RouteSpec, SourceSpec, StorageFragment, format_variant, parse_variant, parse_word,
    snake_case,
};
use crate::{
    EntityId, ForeignKey, GraphCatalog, PropertyId, RelationshipVariant, RelationshipVariantId,
    VariantRoute,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    Clickhouse,
    Duckdb,
    Search,
    Service,
}

impl Engine {
    pub fn is_relational(self) -> bool {
        matches!(self, Self::Clickhouse | Self::Duckdb)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Text,
    Bloom,
    Set,
    MinMax,
}

impl IndexKind {
    fn parse(word: &str) -> Option<Self> {
        match word.split('(').next()? {
            "text" => Some(Self::Text),
            "bloom" | "bloom_filter" => Some(Self::Bloom),
            "set" => Some(Self::Set),
            "minmax" => Some(Self::MinMax),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Versioning {
    pub version: String,
    pub deleted: String,
}

#[derive(Debug, Clone)]
pub struct EdgeTable {
    pub name: String,
    pub sort_key: Vec<String>,
    pub domains: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Source {
    pub entity: EntityId,
    pub table: String,
    pub key: String,
    pub path_column: Option<String>,
    pub sort_key: Vec<String>,
    pub high_fan_out: bool,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub indexes: Vec<IndexKind>,
    pub selectivity: FieldSelectivity,
}

#[derive(Debug, Clone)]
pub struct Lookup {
    pub name: String,
    pub operation: String,
    pub entity: EntityId,
    pub provides: Vec<PropertyId>,
    pub keys: Vec<PropertyId>,
    pub operators: Vec<String>,
    pub disabled: bool,
}

#[derive(Debug, Clone)]
pub enum Realization {
    Column(Column),
    Lookup(usize),
}

#[derive(Debug)]
pub struct Store {
    pub name: String,
    pub engine: Engine,
    pub trusted: bool,
    pub versioning: Option<Versioning>,
    pub edge_tables: Vec<EdgeTable>,
    default_edge_table: Option<usize>,
    sources: Vec<Option<Source>>,
    realizations: Vec<Option<Realization>>,
    routes: Vec<Option<VariantRoute>>,
    lookups: Vec<Lookup>,
    pub(super) guards: Vec<Option<Guard>>,
}

impl Store {
    pub fn source(&self, entity: EntityId) -> Option<&Source> {
        self.sources[entity.index()].as_ref()
    }

    pub fn sources(&self) -> impl Iterator<Item = &Source> {
        self.sources.iter().flatten()
    }

    pub fn realization(&self, property: PropertyId) -> Option<&Realization> {
        self.realizations[property.index()].as_ref()
    }

    pub fn column(&self, property: PropertyId) -> Option<&Column> {
        match self.realization(property)? {
            Realization::Column(column) => Some(column),
            Realization::Lookup(_) => None,
        }
    }

    pub fn lookup(&self, property: PropertyId) -> Option<&Lookup> {
        match self.realization(property)? {
            Realization::Lookup(lookup) => Some(&self.lookups[*lookup]),
            Realization::Column(_) => None,
        }
    }

    pub fn lookups(&self) -> &[Lookup] {
        &self.lookups
    }

    pub fn route(&self, variant: RelationshipVariantId) -> Option<&VariantRoute> {
        self.routes[variant.index()].as_ref()
    }

    pub fn guard(&self, entity: EntityId) -> Option<&Guard> {
        self.guards[entity.index()].as_ref()
    }

    pub fn default_edge_table(&self) -> Option<&EdgeTable> {
        self.default_edge_table
            .map(|index| &self.edge_tables[index])
    }
}

pub(super) fn resolve(
    graph: &GraphCatalog,
    fragments: &[&StorageFragment],
    diagnostics: &mut Diagnostics,
) -> Vec<Store> {
    let mut grouped = IndexMap::<&str, Vec<&StorageFragment>>::new();
    for fragment in fragments {
        grouped.entry(&fragment.store).or_default().push(fragment);
    }
    let mut stores: Vec<(Store, Vec<&StorageFragment>)> = grouped
        .into_iter()
        .filter_map(|(name, fragments)| {
            Some((
                declare_store(name, &fragments, graph, diagnostics)?,
                fragments,
            ))
        })
        .collect();
    for (store, fragments) in &mut stores {
        for fragment in fragments.iter() {
            store.declare_lookups(fragment, graph, diagnostics);
        }
    }
    let served: HashSet<PropertyId> = stores
        .iter()
        .flat_map(|(store, _)| {
            store
                .lookups
                .iter()
                .flat_map(|lookup| lookup.provides.iter().copied())
        })
        .collect();
    for (store, fragments) in &mut stores {
        let engine = fragments
            .iter()
            .find_map(|fragment| fragment.engine.as_ref());
        for fragment in fragments.iter() {
            store.declare_sources(fragment, engine, &served, graph, diagnostics);
        }
        if let Some(engine) = engine.filter(|_| store.engine.is_relational()) {
            store.declare_routes(fragments, engine, graph, diagnostics);
        } else if let Some(fragment) = fragments.iter().find(|f| !f.relationships.is_empty()) {
            diagnostics.error(
                &fragment.package,
                format!("store {} cannot route relationships", store.name),
            );
        }
    }
    stores.into_iter().map(|(store, _)| store).collect()
}

fn declare_store(
    name: &str,
    fragments: &[&StorageFragment],
    graph: &GraphCatalog,
    diagnostics: &mut Diagnostics,
) -> Option<Store> {
    let mut engines = fragments
        .iter()
        .filter_map(|fragment| Some((fragment, fragment.engine.as_ref()?)));
    let Some((fragment, spec)) = engines.next() else {
        diagnostics.error(
            &fragments[0].package,
            format!("store {name} declares no engine"),
        );
        return None;
    };
    if let Some((duplicate, _)) = engines.next() {
        diagnostics.error(
            &duplicate.package,
            format!("store {name} declares its engine twice"),
        );
    }
    let Some(engine) = parse_word::<Engine>(&spec.kind) else {
        diagnostics.error(
            &fragment.package,
            format!("store {name} has unknown engine `{}`", spec.kind),
        );
        return None;
    };
    let edge_tables: Vec<EdgeTable> = spec
        .edge_tables
        .iter()
        .map(|(name, table)| EdgeTable {
            name: name.clone(),
            sort_key: table.sort_key.clone(),
            domains: table.domains.clone(),
        })
        .collect();
    let defaults: Vec<usize> = spec
        .edge_tables
        .values()
        .enumerate()
        .filter_map(|(index, table)| table.default.then_some(index))
        .collect();
    let mut domains = HashSet::new();
    for domain in spec.edge_tables.values().flat_map(|table| &table.domains) {
        if !domains.insert(domain) {
            diagnostics.error(
                &fragment.package,
                format!("store {name} assigns domain {domain} to two edge tables"),
            );
        }
    }
    if defaults.len() > 1 {
        diagnostics.error(
            &fragment.package,
            format!("store {name} has more than one default edge table"),
        );
    }
    let versioning = match (&spec.version_column, &spec.deleted_column) {
        (Some(version), Some(deleted)) => Some(Versioning {
            version: version.clone(),
            deleted: deleted.clone(),
        }),
        (None, None) => None,
        _ => {
            diagnostics.error(
                &fragment.package,
                format!("store {name} must name both version and deleted columns"),
            );
            None
        }
    };
    Some(Store {
        name: name.to_string(),
        engine,
        trusted: spec.trusted,
        versioning,
        edge_tables,
        default_edge_table: defaults.first().copied(),
        sources: (0..graph.entities().count()).map(|_| None).collect(),
        realizations: vec![None; graph.properties().count()],
        routes: vec![None; graph.variants().count()],
        lookups: Vec::new(),
        guards: vec![None; graph.entities().count()],
    })
}

impl Store {
    fn declare_lookups(
        &mut self,
        fragment: &StorageFragment,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        if !fragment.lookups.is_empty() && self.engine != Engine::Service {
            diagnostics.error(
                package,
                format!(
                    "store {} is not a service and cannot declare lookups",
                    self.name
                ),
            );
            return;
        }
        for (name, spec) in &fragment.lookups {
            let Some(entity) = graph.entity_id(&spec.entity) else {
                diagnostics.error(
                    package,
                    format!("lookup {name} names unknown entity {}", spec.entity),
                );
                continue;
            };
            let mut properties = |names: &[String]| -> Vec<PropertyId> {
                names
                    .iter()
                    .filter_map(|property| {
                        diagnostics.require(graph.property_id(entity, property), package, || {
                            format!(
                                "lookup {name} names unknown property {}.{property}",
                                spec.entity
                            )
                        })
                    })
                    .collect()
            };
            let provides = properties(&spec.provides);
            let keys = properties(&spec.keys);
            if let Some(shared) = provides.iter().find(|property| keys.contains(property)) {
                diagnostics.error(
                    package,
                    format!(
                        "lookup {name} uses {} as both key and output",
                        graph.property(*shared).name
                    ),
                );
            }
            let index = self.lookups.len();
            for property in &provides {
                self.realizations[property.index()] = Some(Realization::Lookup(index));
            }
            self.lookups.push(Lookup {
                name: name.clone(),
                operation: spec.operation.clone().unwrap_or_else(|| name.clone()),
                entity,
                provides,
                keys,
                operators: spec.operators.clone(),
                disabled: spec.disabled,
            });
        }
    }

    fn declare_sources(
        &mut self,
        fragment: &StorageFragment,
        engine: Option<&EngineSpec>,
        served: &HashSet<PropertyId>,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for (name, spec) in &fragment.entities {
            let Some(entity) = graph.entity_id(name) else {
                diagnostics.error(
                    package,
                    format!("store {} binds unknown entity {name}", self.name),
                );
                continue;
            };
            if self.sources[entity.index()].is_some() {
                diagnostics.error(package, format!("store {} binds {name} twice", self.name));
                continue;
            }
            let Some(table) = Self::table_name(name, spec, engine) else {
                diagnostics.error(
                    package,
                    format!("store {} has no table for {name}", self.name),
                );
                continue;
            };
            let store = self.name.clone();
            let resolve = |property: &str, diagnostics: &mut Diagnostics| {
                diagnostics.require(graph.property_id(entity, property), package, || {
                    format!("store {store} names unknown property {name}.{property}")
                })
            };
            let stored = self.stored_properties(entity, spec, served, graph, &resolve, diagnostics);
            for (property, column) in stored {
                let data_type = graph.property(property).data_type;
                self.realizations[property.index()] = Some(Realization::Column(Column {
                    name: column,
                    indexes: Vec::new(),
                    selectivity: FieldSelectivity::from_data_type(data_type),
                }));
            }
            self.annotate_columns(name, spec, &resolve, package, diagnostics);
            let high_fan_out = spec
                .high_fan_out
                .iter()
                .filter_map(|property| resolve(property, diagnostics))
                .count()
                > 0;
            let key = graph
                .property_id(entity, DEFAULT_PRIMARY_KEY)
                .and_then(|id| self.column(id))
                .map(|column| column.name.clone());
            let Some(key) = key else {
                diagnostics.error(
                    package,
                    format!("store {} does not store the id of {name}", self.name),
                );
                continue;
            };
            let path_column = match &spec.path_column {
                Some(column) => column.clone(),
                None => engine.and_then(|engine| engine.path_column.clone()),
            };
            let sort_key = match (&spec.sort_key[..], engine) {
                ([], Some(engine)) => engine.sort_key.clone(),
                (sort_key, _) => sort_key.to_vec(),
            };
            self.sources[entity.index()] = Some(Source {
                entity,
                table,
                key,
                path_column,
                sort_key,
                high_fan_out,
            });
        }
    }

    fn table_name(entity: &str, spec: &SourceSpec, engine: Option<&EngineSpec>) -> Option<String> {
        spec.table.clone().or_else(|| {
            let pattern = engine?.table.as_ref()?;
            Some(pattern.replace("{entity}", &snake_case(entity)))
        })
    }

    fn annotate_columns(
        &mut self,
        entity: &str,
        spec: &SourceSpec,
        resolve: &impl Fn(&str, &mut Diagnostics) -> Option<PropertyId>,
        package: &str,
        diagnostics: &mut Diagnostics,
    ) {
        for (property, kinds) in &spec.indexes {
            let Some(id) = resolve(property, diagnostics) else {
                continue;
            };
            for kind in kinds.kinds() {
                let parsed = diagnostics.require(IndexKind::parse(kind), package, || {
                    format!("{entity}.{property} has unknown index kind `{kind}`")
                });
                if let (Some(parsed), Some(column)) = (parsed, self.column_mut(id)) {
                    column.indexes.push(parsed);
                }
            }
        }
        for property in &spec.low_selectivity {
            if let Some(column) = resolve(property, diagnostics).and_then(|id| self.column_mut(id))
            {
                column.selectivity = FieldSelectivity::Low;
            }
        }
    }

    fn stored_properties(
        &self,
        entity: EntityId,
        spec: &SourceSpec,
        served: &HashSet<PropertyId>,
        graph: &GraphCatalog,
        resolve: &impl Fn(&str, &mut Diagnostics) -> Option<PropertyId>,
        diagnostics: &mut Diagnostics,
    ) -> Vec<(PropertyId, String)> {
        let renamed: Vec<(PropertyId, &String)> = spec
            .columns
            .iter()
            .filter_map(|(property, column)| Some((resolve(property, diagnostics)?, column)))
            .collect();
        let excluded: HashSet<PropertyId> = spec
            .exclude
            .iter()
            .filter_map(|property| resolve(property, diagnostics))
            .collect();
        let candidates: Vec<PropertyId> = if spec.only.is_empty() {
            graph.entity(entity).properties.clone()
        } else {
            spec.only
                .iter()
                .filter_map(|property| resolve(property, diagnostics))
                .collect()
        };
        candidates
            .into_iter()
            .filter(|property| !excluded.contains(property))
            .filter_map(|property| {
                let renamed = renamed.iter().find(|(id, _)| *id == property);
                if renamed.is_none()
                    && (served.contains(&property) || self.realizations[property.index()].is_some())
                {
                    return None;
                }
                let column = renamed.map_or_else(
                    || graph.property(property).name.clone(),
                    |(_, column)| (*column).clone(),
                );
                Some((property, column))
            })
            .collect()
    }

    fn column_mut(&mut self, property: PropertyId) -> Option<&mut Column> {
        match self.realizations[property.index()].as_mut()? {
            Realization::Column(column) => Some(column),
            Realization::Lookup(_) => None,
        }
    }

    fn declare_routes(
        &mut self,
        fragments: &[&StorageFragment],
        engine: &EngineSpec,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) {
        let mut specs = IndexMap::<&str, (&str, &RouteSpec)>::new();
        for fragment in fragments {
            for (name, spec) in &fragment.relationships {
                if graph.relationship_id(name).is_none() {
                    diagnostics.error(
                        &fragment.package,
                        format!("store {} routes unknown relationship {name}", self.name),
                    );
                } else if specs.insert(name, (&fragment.package, spec)).is_some() {
                    diagnostics.error(
                        &fragment.package,
                        format!("store {} routes {name} twice", self.name),
                    );
                }
            }
        }
        for (name, (package, spec)) in &specs {
            for variant in spec.variants.keys() {
                let known = parse_variant(variant)
                    .and_then(|(source, target)| graph.variant_named(name, source, target));
                if known.is_none() {
                    diagnostics.error(
                        package,
                        format!(
                            "store {} routes unknown variant {name}({variant})",
                            self.name
                        ),
                    );
                }
            }
        }
        for variant in graph.variants() {
            if self.source(variant.source).is_none() || self.source(variant.target).is_none() {
                continue;
            }
            let name = graph.relationship(variant.relationship).name.as_str();
            let (package, spec) = specs.get(name).copied().unzip();
            let package = package.unwrap_or(&fragments[0].package);
            let key = format_variant(
                &graph.entity(variant.source).name,
                &graph.entity(variant.target).name,
            );
            let narrow = spec.and_then(|spec| spec.variants.get(&key));
            let table = narrow
                .and_then(|spec| spec.table.as_deref())
                .or_else(|| spec?.table.as_deref())
                .or_else(|| {
                    engine.edge_table_for(
                        graph.entity(variant.source).domain.as_deref(),
                        graph.entity(variant.target).domain.as_deref(),
                    )
                })
                .map(str::to_string);
            let Some(table) = table else {
                continue;
            };
            if !self.edge_tables.iter().any(|edge| edge.name == table) {
                diagnostics.error(
                    package,
                    format!(
                        "store {} routes {name}({key}) to undeclared edge table {table}",
                        self.name
                    ),
                );
                continue;
            }
            let fk = narrow
                .and_then(|spec| spec.fk.as_deref())
                .or_else(|| spec?.fk.as_deref());
            let foreign_key = fk.and_then(|column| {
                let found = ForeignKey::resolve(graph, variant.source, variant.target, column);
                match found {
                    None => diagnostics.error(package, format!("{name}({key}) foreign key {column} is on neither endpoint")),
                    Some(found) if !references_other_endpoint(graph, variant, found) => diagnostics.error(
                        package,
                        format!("{name}({key}) foreign key {column} does not reference the other endpoint in the graph"),
                    ),
                    Some(_) => {}
                }
                found
            });
            self.routes[variant.id.index()] = Some(VariantRoute { table, foreign_key });
        }
    }
}

fn references_other_endpoint(
    graph: &GraphCatalog,
    variant: &RelationshipVariant,
    key: ForeignKey,
) -> bool {
    let other = key.referenced_entity(variant.source, variant.target);
    graph.property(key.property).references.contains(&other)
}
