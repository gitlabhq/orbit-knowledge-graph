use std::collections::HashSet;

use indexmap::IndexMap;

use super::Diagnostics;
use super::backend::{EntityBlocks, StoreBlocks};
use super::store::{Source, Store, StoredColumn};
use crate::spec::{AccessHints, EngineSpec, EntityStorage, Resolved, StorageFragment};
use crate::{DataType, EntityId, GraphCatalog, PropertyId};

/// Resolves every storage fragment into one store per store name, each holding its entities and
/// variant bindings, with the backend blocks its fragments wrote.
pub(super) fn resolve<'a>(
    graph: &GraphCatalog,
    fragments: &[&'a StorageFragment],
    served: &HashSet<PropertyId>,
    elsewhere: &crate::spec::Elsewhere,
    diagnostics: &mut Diagnostics,
) -> (Vec<Store>, Vec<StoreBlocks<'a>>) {
    let mut grouped = IndexMap::<&str, Vec<&'a StorageFragment>>::new();
    for fragment in fragments {
        grouped.entry(&fragment.store).or_default().push(fragment);
    }
    let (stores, blocks): (Vec<Store>, Vec<StoreBlocks>) = grouped
        .into_iter()
        .filter_map(|(name, fragments)| {
            let (package, engine) = declare_engine(name, &fragments, diagnostics)?;
            let mut store = Store::new(name, package, engine, graph);
            let mut entities = Vec::new();
            for fragment in &fragments {
                for (entity, spec) in &fragment.entities {
                    entities.extend(store.declare_entity(
                        &fragment.package,
                        entity,
                        spec,
                        served,
                        graph,
                        diagnostics,
                    ));
                }
            }
            entities.sort_by_key(|blocks| blocks.entity);
            for fragment in &fragments {
                store.declare_bindings(fragment, graph, diagnostics);
            }
            let options = engine.backend.as_ref();
            Some((store, StoreBlocks { options, entities }))
        })
        .unzip();
    check_connections(&stores, diagnostics);
    super::bindings::check(graph, &stores, served, elsewhere, diagnostics);
    (stores, blocks)
}

fn check_connections(stores: &[Store], diagnostics: &mut Diagnostics) {
    for (index, store) in stores.iter().enumerate() {
        let Some(first) = stores[..index]
            .iter()
            .find(|other| other.connection_name == store.connection_name)
        else {
            continue;
        };
        if first.kind != store.kind {
            diagnostics.error(
                &store.package,
                format!(
                    "store {} is {} but connection {} is {}",
                    store.name, store.kind, store.connection_name, first.kind
                ),
            );
        }
        if first.trusted != store.trusted {
            diagnostics.error(
                &store.package,
                format!(
                    "store {} and store {} share connection {} but only one is trusted",
                    store.name, first.name, store.connection_name
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
            connection_name: engine.connection.clone(),
            connection_id: crate::ConnectionId::default(),
            id: crate::StoreId::default(),
            kind: engine.kind.clone(),
            trusted: engine.trusted,
            path_column: engine.path_column.clone(),
            package: package.to_string(),
            sources: (0..graph.entities().count()).map(|_| None).collect(),
            columns: vec![None; graph.properties().count()],
            excluded: HashSet::new(),
            bindings: vec![None; graph.variants().count()],
            declared: HashSet::new(),
            guards: vec![None; graph.entities().count()],
            edge_guards: Default::default(),
            facts: None,
        }
    }

    fn declare_entity<'a>(
        &mut self,
        package: &'a str,
        name: &str,
        spec: &'a EntityStorage,
        served: &HashSet<PropertyId>,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) -> Option<EntityBlocks<'a>> {
        let entity = diagnostics.require(graph.entity_id(name), package, || {
            format!("store {} binds unknown entity {name}", self.name)
        })?;
        if self.sources[entity.index()].is_some() {
            diagnostics.error(package, format!("store {} binds {name} twice", self.name));
            return None;
        }
        let property = |property: &str, diagnostics: &mut Diagnostics| {
            diagnostics.require(graph.property_id(entity, property), package, || {
                format!(
                    "store {} names unknown property {name}.{property}",
                    self.name
                )
            })
        };
        let mut columns = Vec::new();
        for (written, column) in &spec.columns {
            let Some(id) = property(written, diagnostics) else {
                continue;
            };
            if served.contains(&id) {
                diagnostics.error(
                    package,
                    format!(
                        "store {} stores {name}.{written}, which a lookup provides",
                        self.name
                    ),
                );
                continue;
            }
            let stored = column.column.as_deref().unwrap_or(written);
            if column.stored_as_text && graph.property(id).data_type != DataType::Enum {
                diagnostics.error(
                    package,
                    format!(
                        "store {} stores {name}.{written} as text, but it is not an enum",
                        self.name
                    ),
                );
            }
            self.columns[id.index()] = Some(StoredColumn {
                name: stored.to_string(),
                immutable: column.immutable,
                selectivity: column.selectivity,
                stored_as_text: column.stored_as_text,
            });
            columns.push((id, stored, column.backend.as_ref()));
        }
        if !spec.complete && !spec.exclude.is_empty() {
            diagnostics.error(
                package,
                format!(
                    "store {} excludes properties of {name} but is not complete; a partial store lists only what it holds",
                    self.name
                ),
            );
        }
        for written in &spec.exclude {
            if let Some(id) = property(written, diagnostics) {
                self.excluded.insert(id);
            }
        }
        let access = self.access(entity, name, &spec.access, package, graph, diagnostics);
        let accepts = self.resolve_accepts(
            entity,
            name,
            spec.accepts.as_deref(),
            package,
            graph,
            diagnostics,
        );
        let identity = graph.entity(entity).identity.clone();
        if identity.is_empty() {
            return None;
        }
        if identity.len() > 1 && spec.table.is_some() {
            diagnostics.error(
                package,
                format!(
                    "store {} keys table rows by one column, but {name} has a composite identity",
                    self.name
                ),
            );
            return None;
        }
        let stored: Vec<Option<&str>> = identity.iter().map(|part| self.column(*part)).collect();
        let key: Vec<String> = match stored.iter().all(Option::is_some) {
            true => stored.into_iter().flatten().map(str::to_string).collect(),
            false if spec.table.is_some() => {
                let part = identity[stored.iter().position(Option::is_none)?];
                diagnostics.error(
                    package,
                    format!(
                        "store {} does not store {name}.{}, its identity",
                        self.name,
                        graph.property(part).name
                    ),
                );
                return None;
            }
            false => Vec::new(),
        };
        let path_column = spec
            .path_column
            .clone()
            .unwrap_or_else(|| self.path_column.clone());
        if let Some(path) = &path_column
            && !columns.iter().any(|(_, stored, _)| stored == path)
        {
            diagnostics.error(
                package,
                format!(
                    "store {} reads {name} paths from {path}, which it does not store",
                    self.name
                ),
            );
        }
        self.sources[entity.index()] = Some(Source {
            entity,
            package: package.to_string(),
            complete: spec.complete,
            accepts,
            returned: columns.iter().map(|(id, _, _)| *id).collect(),
            table: spec.table.clone(),
            key,
            path_column,
            access,
        });
        Some(EntityBlocks {
            entity,
            package,
            options: spec.backend.as_ref(),
            columns,
        })
    }

    fn resolve_accepts(
        &self,
        entity: EntityId,
        name: &str,
        written: Option<&[String]>,
        package: &str,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) -> Vec<PropertyId> {
        let Some(written) = diagnostics.require(written, package, || {
            format!("store {} declares no accepts for {name}", self.name)
        }) else {
            return Vec::new();
        };
        written
            .iter()
            .filter_map(|property| {
                let id = graph
                    .property_id(entity, property)
                    .filter(|id| self.column(*id).is_some());
                diagnostics.require(id, package, || {
                    format!(
                        "store {} accepts keys on {name}.{property}, which it does not store",
                        self.name
                    )
                })
            })
            .collect()
    }

    fn access(
        &self,
        entity: EntityId,
        name: &str,
        hints: &AccessHints<crate::spec::Raw>,
        package: &str,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) -> AccessHints<Resolved> {
        let mut stored = |written: &Vec<String>| -> Vec<PropertyId> {
            written
                .iter()
                .filter_map(|property| {
                    let id = graph
                        .property_id(entity, property)
                        .filter(|id| self.column(*id).is_some());
                    diagnostics.require(id, package, || {
                        format!(
                            "store {} access names {name}.{property}, which it does not store",
                            self.name
                        )
                    })
                })
                .collect()
        };
        AccessHints {
            filter: stored(&hints.filter),
            text: stored(&hints.text),
            order: stored(&hints.order),
        }
    }
}
