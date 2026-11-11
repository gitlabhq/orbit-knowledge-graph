use std::collections::{BTreeMap, BTreeSet, HashSet};

use super::layout::{declared_columns, merge, projections, table_indexes};
use super::resolve::Resolver;
use super::{Column, GraphTable, Table, joins};
use crate::catalog::sources::Writer;
use crate::catalog::{Diagnostics, StoreInput};
use crate::implementations::clickhouse::ClickHouse;
use crate::implementations::relational::RelationalStore;

/// The writers of each edge table: the pipelines that emit a variant bound to it, and for a
/// variant no pipeline emits, the endpoints that no pipeline writes, which their own indexer
/// writes.
fn writers<'a>(
    input: &'a StoreInput<'_, ClickHouse>,
    diagnostics: &mut Diagnostics,
) -> BTreeMap<&'a str, BTreeSet<String>> {
    let (graph, sources, store) = (input.graph, input.sources, input.store);
    let mut writers = BTreeMap::<&str, BTreeSet<String>>::new();
    let mut emitted = HashSet::new();
    for pipeline in sources.pipelines() {
        let name = sources.writer_name(graph, pipeline.writer);
        let variants: Vec<_> = match pipeline.writer {
            Writer::Derived(index) => sources.derived()[index]
                .emits
                .iter()
                .flat_map(|relationship| graph.relationship(*relationship).variants.iter().copied())
                .collect(),
            _ => pipeline
                .transform
                .edges()
                .iter()
                .flat_map(|edge| edge.variants.iter().copied())
                .collect(),
        };
        for variant in variants {
            emitted.insert(variant);
            if let Some(table) = store.edge_table(variant) {
                writers.entry(table).or_default().insert(name.to_string());
            }
        }
    }
    for (variant, package, binding) in store.bindings() {
        if binding.index.is_some() && !emitted.contains(&variant) {
            let variant = graph.variant(variant);
            diagnostics.error(
                package,
                format!(
                    "store {} derives an edge index for {}({} -> {}), which no pipeline writes",
                    store.name,
                    graph.relationship(variant.relationship).name,
                    graph.entity(variant.source).name,
                    graph.entity(variant.target).name
                ),
            );
        }
    }
    for variant in graph
        .variants()
        .filter(|variant| !emitted.contains(&variant.id))
    {
        let Some(table) = store.edge_table(variant.id) else {
            continue;
        };
        for entity in [variant.source, variant.target] {
            if sources.entity_pipelines(entity).next().is_none() {
                writers
                    .entry(table)
                    .or_default()
                    .insert(graph.entity(entity).name.clone());
            }
        }
    }
    writers
}

impl Resolver<'_, '_> {
    pub(super) fn edge_tables(&mut self) {
        let package = self.package().to_string();
        let input = self.input;
        let creates = self.creates();
        let mut writers = writers(input, self.diagnostics);
        for (name, spec) in &input.options.edge_tables {
            let context = format!("{} edge table {name}", self.context());
            let mut columns =
                declared_columns(&package, &context, &spec.columns, creates, self.diagnostics);
            if !creates {
                for denormalized in &input.options.denormalized {
                    if !columns
                        .iter()
                        .any(|column| column.name == denormalized.column)
                    {
                        columns.push(Column::new(&denormalized.column, None));
                    }
                }
            }
            self.shared_columns(&mut columns);
            let indexes = table_indexes(
                &package,
                &context,
                &spec.indexes,
                &columns,
                self.diagnostics,
            );
            let path = input.store.path_column.as_ref();
            if creates {
                self.schema.graph_tables.push(GraphTable {
                    name: name.clone(),
                    global: false,
                    has_path_column: path
                        .is_some_and(|path| columns.iter().any(|column| &column.name == path)),
                });
                if let Some(writers) = writers.remove(name.as_str()) {
                    self.schema.writers.insert(name.clone(), writers);
                }
            }
            let engine = self.engine(&package, &context, spec.engine.as_deref());
            self.tables.push(Table {
                indexes,
                projections: projections(&spec.projections),
                engine,
                primary_key: spec.primary_key.clone(),
                settings: merge(&self.layout.settings, &spec.settings),
                ..Table::edge(name, input.store, spec.sort_key.clone(), columns)
            });
        }
    }

    pub(super) fn joins(&mut self) {
        let package = self.package().to_string();
        let input = self.input;
        for (name, spec) in &input.options.joins {
            let context = format!("{} join {name}", self.context());
            let Some((mut table, views)) = joins::resolve(
                name,
                spec,
                &self.tables,
                &self.layout,
                &package,
                &context,
                self.diagnostics,
            ) else {
                continue;
            };
            self.shared_columns(&mut table.columns);
            if self.creates() {
                let sources = spec.sources.iter().map(|source| source.table.clone());
                self.schema
                    .dependencies
                    .insert(name.clone(), sources.collect());
                self.schema.joins.push(name.clone());
                self.schema.views.extend(views);
            }
            self.tables.push(table);
        }
    }

    pub(super) fn copies(&mut self, relational: &mut RelationalStore) {
        for (name, copy) in &self.input.options.copies {
            let source = self.tables.iter().find(|table| table.name == copy.source);
            let Some(source) =
                self.diagnostics
                    .require(source.cloned(), &self.input.store.package, || {
                        format!("copy {name} names unknown table {}", copy.source)
                    })
            else {
                continue;
            };
            let copied = relational
                .copies
                .iter()
                .any(|(copy_name, _)| *copy_name == copy.source);
            if self.creates() && !copied {
                self.schema
                    .dependencies
                    .insert(name.clone(), BTreeSet::from([copy.source.clone()]));
            }
            self.tables.push(Table {
                name: name.clone(),
                sort_key: copy.sort_key.clone(),
                ..source
            });
            relational.copies.push((name.clone(), copy.source.clone()));
        }
    }
}
