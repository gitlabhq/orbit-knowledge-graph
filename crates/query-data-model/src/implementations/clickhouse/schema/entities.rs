use std::collections::HashSet;

use indexmap::IndexMap;

use super::layout::{build_indexes, merge, projections};
use super::resolve::Resolver;
use super::{Column, GraphTable, Table};
use crate::catalog::{Scope, SourceInput};
use crate::implementations::clickhouse::ClickHouse;
use crate::implementations::clickhouse::spec::{IndexDetail, IndexSpec, IndexSpecs};
use crate::implementations::relational::entity_layout;
use crate::spec::{AccessHints, Resolved};
use crate::{DataType, GraphCatalog};

impl Resolver<'_, '_> {
    pub(super) fn entity_tables(&mut self) {
        let input = self.input;
        let (graph, store) = (input.graph, input.store);
        for source in &input.entities {
            let entity = source.entity;
            let options = &source.options;
            let Some((bound, table, sort_key)) =
                entity_layout(input, source, &options.sort_key, self.diagnostics)
            else {
                continue;
            };
            let context = format!("{} {}", self.context(), graph.entity(entity).name);
            let mut columns = self.entity_columns(source);
            self.shared_columns(&mut columns);
            let shared = &self.layout.columns;
            let column_of = |target: &str| {
                graph
                    .property_id(entity, target)
                    .and_then(|property| store.column(property))
                    .or_else(|| shared.iter().any(|c| c.name == target).then_some(target))
                    .map(str::to_string)
            };
            let declared = options
                .indexes
                .clone()
                .unwrap_or_else(|| derived_indexes(graph, &bound.access));
            let indexes = build_indexes(&declared, column_of);
            let engine = self.engine(source.package, &context, options.engine.as_deref());
            if self.creates() {
                let has_path_column = bound
                    .path_column
                    .as_ref()
                    .is_some_and(|path| columns.iter().any(|column| &column.name == path));
                self.schema.graph_tables.push(GraphTable {
                    name: table.to_string(),
                    global: input
                        .access
                        .entity(entity)
                        .is_some_and(|access| access.scope == Scope::Global),
                    has_path_column,
                });
                self.schema
                    .writers
                    .entry(table.to_string())
                    .or_default()
                    .insert(graph.entity(entity).name.clone());
            }
            self.tables.push(Table {
                indexes,
                projections: projections(&options.projections),
                engine,
                primary_key: options.primary_key.clone(),
                settings: merge(&self.layout.settings, &options.settings),
                ..Table::of_source(bound, table, sort_key, columns)
            });
        }
    }

    /// The stored properties in the order the store lists them.
    fn entity_columns(&mut self, source: &SourceInput<'_, ClickHouse>) -> Vec<Column> {
        let (graph, store) = (self.input.graph, self.input.store);
        let epoch: HashSet<_> = source
            .options
            .epoch_nulls
            .iter()
            .filter_map(|property| graph.property_id(source.entity, property))
            .collect();
        let mut columns = Vec::with_capacity(source.columns.len());
        for column in &source.columns {
            let (id, property) = (column.property, graph.property(column.property));
            let spec = self
                .diagnostics
                .require(column.options.as_ref(), source.package, || {
                    format!(
                        "store {} column {}.{} declares no storage type",
                        store.name,
                        graph.entity(source.entity).name,
                        property.name
                    )
                });
            let Some(spec) = spec else {
                continue;
            };
            columns.push(Column {
                name: column.name.to_string(),
                property: Some(id),
                data_type: Some(property.data_type),
                storage_type: spec.storage_type.clone(),
                nullable: property.nullable && !epoch.contains(&id),
                default: spec
                    .default
                    .clone()
                    .or_else(|| property.default.as_ref().map(|value| value.literal())),
                codecs: spec.codec.clone(),
                null_as_epoch: epoch.contains(&id),
            });
        }
        columns
    }
}

/// The skip indexes an entity's access hints ask for, when its backend block declares none.
fn derived_indexes(
    graph: &GraphCatalog,
    access: &AccessHints<Resolved>,
) -> IndexMap<String, IndexSpecs> {
    let mut indexes = IndexMap::new();
    for id in &access.filter {
        let property = graph.property(*id);
        let spec = match property.data_type {
            DataType::Bool | DataType::Date | DataType::DateTime => {
                IndexSpec::Kind("minmax".into())
            }
            DataType::Enum => IndexSpec::Detail(IndexDetail {
                kind: format!(
                    "set({})",
                    property
                        .enum_values
                        .as_ref()
                        .map_or(0, |values| values.len())
                ),
                granularity: 2,
                name: None,
            }),
            _ => IndexSpec::Kind("bloom_filter(0.0001)".into()),
        };
        indexes.insert(property.name.clone(), IndexSpecs::One(spec));
    }
    for id in &access.text {
        indexes.insert(
            graph.property(*id).name.clone(),
            IndexSpecs::One(IndexSpec::Kind("text".into())),
        );
    }
    indexes
}
