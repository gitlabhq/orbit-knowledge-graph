use std::collections::HashSet;

use super::layout::{build_indexes, merge, projections};
use super::resolve::Resolver;
use super::{Column, GraphTable, Table, storage_type};
use crate::EntityId;
use crate::catalog::Scope;
use crate::implementations::clickhouse::spec::ClickHouseSource;

impl Resolver<'_, '_> {
    pub(super) fn entity_tables(&mut self) {
        let input = self.input;
        let (graph, store) = (input.graph, input.store);
        for source in &input.sources {
            let entity = source.entity;
            let Some(bound) = store.source(entity) else {
                continue;
            };
            let options = &source.options;
            let context = format!("{} {}", self.context(), graph.entity(entity).name);
            let mut columns = self.entity_columns(entity, source.package, options);
            self.shared_columns(&mut columns);
            let shared = &self.layout.columns;
            let column_of = |target: &str| {
                graph
                    .property_id(entity, target)
                    .and_then(|property| store.column(property))
                    .or_else(|| shared.iter().any(|c| c.name == target).then_some(target))
                    .map(str::to_string)
            };
            let indexes = build_indexes(&options.indexes, column_of);
            let engine = self.engine(source.package, &context, options.engine.as_deref());
            if self.creates() {
                let has_path_column = bound
                    .path_column
                    .as_ref()
                    .is_some_and(|path| columns.iter().any(|column| &column.name == path));
                self.schema.graph_tables.push(GraphTable {
                    name: bound.table.clone(),
                    global: input
                        .access
                        .entity(entity)
                        .is_some_and(|access| access.scope == Scope::Global),
                    has_path_column,
                });
                self.schema
                    .writers
                    .entry(bound.table.clone())
                    .or_default()
                    .insert(graph.entity(entity).name.clone());
            }
            let sort_key = match options.sort_key.as_slice() {
                [] => input.options.sort_key.clone(),
                declared => declared.to_vec(),
            };
            self.tables.push(Table {
                indexes,
                projections: projections(&options.projections),
                engine,
                primary_key: options.primary_key.clone(),
                settings: merge(&self.layout.settings, &options.settings),
                ..Table::of_source(bound, sort_key, columns)
            });
        }
    }

    /// The stored properties, those the source lays out first and in its order.
    fn entity_columns(
        &mut self,
        entity: EntityId,
        package: &str,
        options: &ClickHouseSource,
    ) -> Vec<Column> {
        let (graph, store) = (self.input.graph, self.input.store);
        let name = &graph.entity(entity).name;
        let declared = &options.columns;
        let epoch: HashSet<_> = options
            .epoch_nulls
            .iter()
            .filter_map(|property| graph.property_id(entity, property))
            .collect();
        let stored: Vec<_> = graph
            .entity(entity)
            .properties
            .iter()
            .copied()
            .filter(|property| store.column(*property).is_some())
            .collect();
        let mut ordered = Vec::with_capacity(stored.len());
        for (property, spec) in declared {
            let id = graph
                .property_id(entity, property)
                .filter(|id| stored.contains(id));
            let Some(id) = self.diagnostics.require(id, package, || {
                format!(
                    "store {} lays out {name}.{property}, which it does not store",
                    store.name
                )
            }) else {
                continue;
            };
            ordered.push((id, Some(spec)));
        }
        ordered.extend(
            stored
                .iter()
                .filter(|id| !declared.contains_key(&graph.property(**id).name))
                .map(|id| (*id, None)),
        );
        ordered
            .into_iter()
            .map(|(id, spec)| {
                let property = graph.property(id);
                let nullable = property.nullable && !epoch.contains(&id);
                Column {
                    name: store.column(id).unwrap_or_default().to_string(),
                    property: Some(id),
                    data_type: Some(property.data_type),
                    storage_type: spec
                        .and_then(|spec| spec.storage_type.clone())
                        .unwrap_or_else(|| storage_type(property.data_type, nullable)),
                    nullable,
                    default: spec.and_then(|spec| spec.default.clone()),
                    codecs: spec.map(|spec| spec.codec.clone()).unwrap_or_default(),
                    null_as_epoch: epoch.contains(&id),
                }
            })
            .collect()
    }
}
