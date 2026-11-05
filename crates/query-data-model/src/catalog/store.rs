use std::collections::HashSet;

use indexmap::IndexMap;
use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::Diagnostics;
use super::access::Guard;
use crate::spec::{
    BackendOptions, EngineSpec, RouteSpec, SourceSpec, StorageFragment, format_variant,
    parse_variant, snake_case,
};
use crate::{
    EntityId, ForeignKey, GraphCatalog, PropertyId, RelationshipId, RelationshipVariant,
    RelationshipVariantId,
};

#[derive(Debug, Clone)]
pub struct Source {
    pub entity: EntityId,
    pub table: String,
    pub key: String,
    pub path_column: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeclaredRoute {
    pub relationship: RelationshipId,
    pub package: String,
    pub spec: RouteSpec,
}

impl DeclaredRoute {
    pub fn for_variant(
        &self,
        graph: &GraphCatalog,
        variant: &RelationshipVariant,
    ) -> Option<&RouteSpec> {
        let key = format_variant(
            &graph.entity(variant.source).name,
            &graph.entity(variant.target).name,
        );
        self.spec.variants.get(&key)
    }
}

#[derive(Debug)]
pub struct Store {
    pub name: String,
    pub connection: String,
    pub kind: String,
    pub trusted: bool,
    pub path_column: Option<String>,
    pub package: String,
    pub(crate) options: BackendOptions,
    pub(crate) source_options: Vec<(EntityId, String, BackendOptions)>,
    pub(crate) routes: Vec<DeclaredRoute>,
    sources: Vec<Option<Source>>,
    columns: Vec<Option<String>>,
    excluded: HashSet<PropertyId>,
    foreign_keys: Vec<Option<ForeignKey>>,
    pub(super) guards: Vec<Option<Guard>>,
}

impl Store {
    pub fn source(&self, entity: EntityId) -> Option<&Source> {
        self.sources[entity.index()].as_ref()
    }

    pub fn sources(&self) -> impl Iterator<Item = &Source> {
        self.sources.iter().flatten()
    }

    pub fn column(&self, property: PropertyId) -> Option<&str> {
        self.columns[property.index()].as_deref()
    }

    pub fn excludes(&self, property: PropertyId) -> bool {
        self.excluded.contains(&property)
    }

    pub fn foreign_key(&self, variant: RelationshipVariantId) -> Option<ForeignKey> {
        self.foreign_keys[variant.index()]
    }

    pub fn guard(&self, entity: EntityId) -> Option<&Guard> {
        self.guards[entity.index()].as_ref()
    }

    pub fn route(&self, relationship: RelationshipId) -> Option<&DeclaredRoute> {
        self.routes
            .iter()
            .find(|route| route.relationship == relationship)
    }
}

pub(super) fn resolve(
    graph: &GraphCatalog,
    fragments: &[&StorageFragment],
    served: &HashSet<PropertyId>,
    diagnostics: &mut Diagnostics,
) -> Vec<Store> {
    let mut grouped = IndexMap::<&str, Vec<&StorageFragment>>::new();
    for fragment in fragments {
        grouped.entry(&fragment.store).or_default().push(fragment);
    }
    let mut stores: Vec<Store> = grouped
        .into_iter()
        .filter_map(|(name, fragments)| {
            let (package, engine) = declare_engine(name, &fragments, diagnostics)?;
            let mut store = Store::new(name, package, engine, graph);
            for fragment in &fragments {
                store.declare_sources(fragment, engine, served, graph, diagnostics);
            }
            store.source_options.sort_by_key(|(entity, _, _)| *entity);
            store.declare_routes(&fragments, graph, diagnostics);
            Some(store)
        })
        .collect();
    let keys: Vec<Vec<Option<ForeignKey>>> = stores
        .iter()
        .map(|store| store.resolve_foreign_keys(&stores, graph, diagnostics))
        .collect();
    for (store, keys) in stores.iter_mut().zip(keys) {
        store.foreign_keys = keys;
    }
    check_connections(&stores, diagnostics);
    stores
}

fn check_connections(stores: &[Store], diagnostics: &mut Diagnostics) {
    for (index, store) in stores.iter().enumerate() {
        let Some(first) = stores[..index]
            .iter()
            .find(|other| other.connection == store.connection)
        else {
            continue;
        };
        if first.kind != store.kind {
            diagnostics.error(
                &store.package,
                format!(
                    "store {} is {} but connection {} is {}",
                    store.name, store.kind, store.connection, first.kind
                ),
            );
        }
        if first.trusted != store.trusted {
            diagnostics.error(
                &store.package,
                format!(
                    "store {} and store {} share connection {} but only one is trusted",
                    store.name, first.name, store.connection
                ),
            );
        }
    }
}

fn declare_engine<'a>(
    name: &str,
    fragments: &[&'a StorageFragment],
    diagnostics: &mut Diagnostics,
) -> Option<(&'a str, &'a EngineSpec)> {
    let mut engines = fragments
        .iter()
        .filter_map(|fragment| Some((fragment.package.as_str(), fragment.engine.as_ref()?)));
    let Some(first) = engines.next() else {
        diagnostics.error(
            &fragments[0].package,
            format!("store {name} declares no engine"),
        );
        return None;
    };
    if let Some((package, _)) = engines.next() {
        diagnostics.error(package, format!("store {name} declares its engine twice"));
    }
    Some(first)
}

impl Store {
    fn new(name: &str, package: &str, engine: &EngineSpec, graph: &GraphCatalog) -> Self {
        Self {
            name: name.to_string(),
            connection: engine
                .connection
                .clone()
                .unwrap_or_else(|| engine.kind.clone()),
            kind: engine.kind.clone(),
            trusted: engine.trusted,
            path_column: engine.path_column.clone(),
            package: package.to_string(),
            options: engine.backend.clone(),
            source_options: Vec::new(),
            routes: Vec::new(),
            sources: (0..graph.entities().count()).map(|_| None).collect(),
            columns: vec![None; graph.properties().count()],
            excluded: HashSet::new(),
            foreign_keys: vec![None; graph.variants().count()],
            guards: vec![None; graph.entities().count()],
        }
    }

    fn declare_sources(
        &mut self,
        fragment: &StorageFragment,
        engine: &EngineSpec,
        served: &HashSet<PropertyId>,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for (name, spec) in &fragment.entities {
            let Some(entity) = diagnostics.require(graph.entity_id(name), package, || {
                format!("store {} binds unknown entity {name}", self.name)
            }) else {
                continue;
            };
            if self.sources[entity.index()].is_some() {
                diagnostics.error(package, format!("store {} binds {name} twice", self.name));
                continue;
            }
            let table = spec.table.clone().or_else(|| {
                let pattern = engine.table.as_ref()?;
                Some(pattern.replace("{entity}", &snake_case(name)))
            });
            let Some(table) = diagnostics.require(table, package, || {
                format!("store {} has no table for {name}", self.name)
            }) else {
                continue;
            };
            let columns = self.stored_columns(entity, spec, served, graph, package, diagnostics);
            for (property, column) in columns {
                self.columns[property.index()] = Some(column);
            }
            let key = graph
                .property_id(entity, DEFAULT_PRIMARY_KEY)
                .and_then(|id| self.column(id))
                .map(str::to_string);
            let Some(key) = diagnostics.require(key, package, || {
                format!("store {} does not store the id of {name}", self.name)
            }) else {
                continue;
            };
            let path_column = match &spec.path_column {
                Some(column) => column.clone(),
                None => engine.path_column.clone(),
            };
            self.sources[entity.index()] = Some(Source {
                entity,
                table,
                key,
                path_column,
            });
            self.source_options
                .push((entity, package.to_string(), spec.backend.clone()));
        }
    }

    fn stored_columns(
        &mut self,
        entity: EntityId,
        spec: &SourceSpec,
        served: &HashSet<PropertyId>,
        graph: &GraphCatalog,
        package: &str,
        diagnostics: &mut Diagnostics,
    ) -> Vec<(PropertyId, String)> {
        let name = &graph.entity(entity).name;
        let resolve = |property: &str, diagnostics: &mut Diagnostics| {
            diagnostics.require(graph.property_id(entity, property), package, || {
                format!(
                    "store {} names unknown property {name}.{property}",
                    self.name
                )
            })
        };
        let renamed: Vec<(PropertyId, &String)> = spec
            .columns
            .iter()
            .filter_map(|(property, column)| Some((resolve(property, diagnostics)?, column)))
            .collect();
        let excluded: HashSet<PropertyId> = spec
            .exclude
            .iter()
            .filter_map(|p| resolve(p, diagnostics))
            .collect();
        let candidates: Vec<PropertyId> = if spec.only.is_empty() {
            graph.entity(entity).properties.clone()
        } else {
            spec.only
                .iter()
                .filter_map(|p| resolve(p, diagnostics))
                .collect()
        };
        self.excluded.extend(
            graph
                .entity(entity)
                .properties
                .iter()
                .filter(|property| excluded.contains(property) || !candidates.contains(property)),
        );
        candidates
            .into_iter()
            .filter(|property| !excluded.contains(property))
            .filter_map(|property| {
                let renamed = renamed.iter().find(|(id, _)| *id == property);
                if served.contains(&property) {
                    if renamed.is_some() {
                        diagnostics.error(
                            package,
                            format!(
                                "store {} stores {name}.{}, which a lookup provides",
                                self.name,
                                graph.property(property).name
                            ),
                        );
                    }
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

    fn declare_routes(
        &mut self,
        fragments: &[&StorageFragment],
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) {
        for fragment in fragments {
            let package = fragment.package.as_str();
            for (name, spec) in &fragment.relationships {
                let relationship =
                    diagnostics.require(graph.relationship_id(name), package, || {
                        format!("store {} routes unknown relationship {name}", self.name)
                    });
                let Some(relationship) = relationship else {
                    continue;
                };
                if self.route(relationship).is_some() {
                    diagnostics.error(package, format!("store {} routes {name} twice", self.name));
                    continue;
                }
                for variant in spec.variants.keys() {
                    let known = parse_variant(variant)
                        .and_then(|(source, target)| graph.variant_named(name, source, target));
                    diagnostics.require(known, package, || {
                        format!(
                            "store {} routes unknown variant {name}({variant})",
                            self.name
                        )
                    });
                }
                self.routes.push(DeclaredRoute {
                    relationship,
                    package: package.to_string(),
                    spec: spec.clone(),
                });
            }
        }
    }

    fn resolve_foreign_keys(
        &self,
        stores: &[Store],
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) -> Vec<Option<ForeignKey>> {
        let on_connection = |entity: EntityId| {
            stores
                .iter()
                .any(|store| store.connection == self.connection && store.source(entity).is_some())
        };
        let mut keys = vec![None; graph.variants().count()];
        for route in &self.routes {
            let mut matched = false;
            for id in &graph.relationship(route.relationship).variants {
                let variant = graph.variant(*id);
                let narrow = route.for_variant(graph, variant);
                let explicit = narrow.and_then(|spec| spec.fk.as_deref());
                let Some(column) = explicit.or(route.spec.fk.as_deref()) else {
                    continue;
                };
                let label = format!(
                    "{}({})",
                    graph.relationship(route.relationship).name,
                    format_variant(
                        &graph.entity(variant.source).name,
                        &graph.entity(variant.target).name
                    )
                );
                let found = ForeignKey::resolve(graph, variant.source, variant.target, column);
                matched |= found.is_some();
                let joinable = found.filter(|key| {
                    let holder = key.holder_entity(variant.source, variant.target);
                    let referenced = key.referenced_entity(variant.source, variant.target);
                    self.source(holder).is_some()
                        && self.column(key.property).is_some()
                        && on_connection(referenced)
                });
                let Some(key) = joinable else {
                    if explicit.is_some() {
                        diagnostics.error(
                            &route.package,
                            format!(
                                "{label} foreign key {column} is not joinable on connection {}",
                                self.connection
                            ),
                        );
                    }
                    continue;
                };
                let referenced = key.referenced_entity(variant.source, variant.target);
                if !graph
                    .property(key.property)
                    .references
                    .contains(&referenced)
                {
                    diagnostics.error(
                        &route.package,
                        format!(
                            "{label} foreign key {column} does not reference the other endpoint in the graph"
                        ),
                    );
                    continue;
                }
                keys[id.index()] = Some(key);
            }
            if let Some(column) = route.spec.fk.as_deref().filter(|_| !matched) {
                diagnostics.error(
                    &route.package,
                    format!(
                        "{} foreign key {column} is on no endpoint of any variant",
                        graph.relationship(route.relationship).name
                    ),
                );
            }
        }
        keys
    }
}
