use std::collections::{BTreeMap, BTreeSet, HashSet};

use indexmap::IndexMap;

use super::{
    AuxiliaryTable, Column, Dictionary, Engine, GraphTable, Index, MaterializedView, Projection,
    RefreshableView, Schema, Table, joins, storage_type,
};
use crate::EntityId;
use crate::catalog::{Diagnostics, Scope, StoreInput};
use crate::implementations::clickhouse::ClickHouse;
use crate::implementations::clickhouse::backend::IndexKind;
use crate::implementations::clickhouse::spec::{
    ClickHouseSource, ColumnSpec, IndexSpec, IndexSpecs, ProjectionSpec, Settings, TableSpec,
};
use crate::implementations::relational::RelationalStore;

const TEXT_INDEX: &str = "text";

pub(super) struct Layout {
    pub columns: Vec<Column>,
    pub settings: Vec<(String, String)>,
    pub engine: Engine,
    pub versioning: Option<(String, String)>,
}

pub(in crate::implementations::clickhouse) fn resolve(
    input: &StoreInput<'_, ClickHouse>,
    relational: &RelationalStore,
    diagnostics: &mut Diagnostics,
) -> Option<Schema> {
    let engine = input.options.engine.as_deref()?;
    let package = input.store.package.as_str();
    let context = format!("store {}", input.store.name);
    let layout = Layout {
        columns: declared_columns(package, &context, &input.options.columns, diagnostics),
        settings: merge(&[], &input.options.settings),
        engine: parse_engine(package, &context, engine, diagnostics)?,
        versioning: relational
            .versioning
            .as_ref()
            .map(|versioning| (versioning.version.clone(), versioning.deleted.clone())),
    };
    let mut resolver = Resolver {
        input,
        layout,
        diagnostics,
        schema: Schema {
            tables: Vec::new(),
            auxiliary: Vec::new(),
            dictionaries: Vec::new(),
            views: Vec::new(),
            refreshable_views: Vec::new(),
            graph_tables: Vec::new(),
            joins: Vec::new(),
            dependencies: BTreeMap::new(),
            writers: BTreeMap::new(),
        },
    };
    resolver.entity_tables(relational);
    resolver.edge_tables();
    resolver.joins();
    resolver.copies();
    resolver.auxiliary_tables();
    resolver.dictionaries();
    resolver.views();
    resolver.refreshable_views();
    Some(resolver.schema)
}

struct Resolver<'a, 'd> {
    input: &'a StoreInput<'a, ClickHouse>,
    layout: Layout,
    diagnostics: &'d mut Diagnostics,
    schema: Schema,
}

impl Resolver<'_, '_> {
    fn package(&self) -> &str {
        &self.input.store.package
    }

    fn context(&self) -> String {
        format!("store {}", self.input.store.name)
    }

    fn entity_tables(&mut self, relational: &RelationalStore) {
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
            columns.extend(self.layout.columns.iter().cloned());
            let shared = &self.layout.columns;
            let column_of = |target: &str| {
                graph
                    .property_id(entity, target)
                    .and_then(|property| store.column(property))
                    .or_else(|| shared.iter().any(|c| c.name == target).then_some(target))
                    .map(str::to_string)
            };
            let indexes = build_indexes(&options.indexes, column_of);
            let projections = projections(&options.projections);
            let engine = self.engine(source.package, &context, options.engine.as_deref());
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
            self.schema.tables.push(Table {
                name: bound.table.clone(),
                columns,
                indexes,
                projections,
                engine,
                order_by: relational.sort_key(entity).unwrap_or_default().to_vec(),
                primary_key: options.primary_key.clone(),
                settings: merge(&self.layout.settings, &options.settings),
                ttl: None,
            });
        }
    }

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
                let default_type = || {
                    storage_type(
                        property.data_type,
                        property.nullable && !epoch.contains(&id),
                    )
                };
                Column {
                    name: store.column(id).unwrap_or_default().to_string(),
                    storage_type: spec
                        .and_then(|spec| spec.storage_type.clone())
                        .unwrap_or_else(default_type),
                    default: spec.and_then(|spec| spec.default.clone()),
                    codecs: spec.map(|spec| spec.codec.clone()).unwrap_or_default(),
                }
            })
            .collect()
    }

    fn engine(&mut self, package: &str, context: &str, declared: Option<&str>) -> Engine {
        declared
            .and_then(|text| parse_engine(package, context, text, self.diagnostics))
            .unwrap_or_else(|| self.layout.engine.clone())
    }

    fn edge_tables(&mut self) {
        let package = self.package().to_string();
        let input = self.input;
        for (name, spec) in &input.options.edge_tables {
            let context = format!("{} edge table {name}", self.context());
            let mut columns = declared_columns(&package, &context, &spec.columns, self.diagnostics);
            columns.extend(self.layout.columns.iter().cloned());
            let indexes = table_indexes(
                &package,
                &context,
                &spec.indexes,
                &columns,
                self.diagnostics,
            );
            let path = input.store.path_column.as_ref();
            self.schema.graph_tables.push(GraphTable {
                name: name.clone(),
                global: false,
                has_path_column: path
                    .is_some_and(|path| columns.iter().any(|column| &column.name == path)),
            });
            if !spec.writers.is_empty() {
                self.schema
                    .writers
                    .entry(name.clone())
                    .or_default()
                    .extend(spec.writers.iter().cloned());
            }
            let table = Table {
                name: name.clone(),
                indexes,
                projections: projections(&spec.projections),
                engine: self.engine(&package, &context, spec.engine.as_deref()),
                order_by: spec.sort_key.clone(),
                primary_key: spec.primary_key.clone(),
                settings: merge(&self.layout.settings, &spec.settings),
                ttl: None,
                columns,
            };
            self.schema.tables.push(table);
        }
    }

    fn joins(&mut self) {
        let package = self.package().to_string();
        let input = self.input;
        for (name, spec) in &input.options.joins {
            let context = format!("{} join {name}", self.context());
            let Some((table, views)) = joins::resolve(
                name,
                spec,
                &self.schema.tables,
                &self.layout,
                &package,
                &context,
                self.diagnostics,
            ) else {
                continue;
            };
            self.schema.dependencies.insert(
                name.clone(),
                spec.sources
                    .iter()
                    .map(|source| source.table.clone())
                    .collect(),
            );
            self.schema.joins.push(name.clone());
            self.schema.tables.push(table);
            self.schema.views.extend(views);
        }
    }

    fn copies(&mut self) {
        for (name, copy) in &self.input.options.copies {
            if self.schema.table(&copy.source).is_some() {
                self.schema
                    .dependencies
                    .insert(name.clone(), BTreeSet::from([copy.source.clone()]));
            }
        }
    }

    fn auxiliary_tables(&mut self) {
        let package = self.package().to_string();
        let input = self.input;
        for (name, spec) in &input.options.tables {
            let context = format!("{} table {name}", self.context());
            let Some(table) = auxiliary_table(&package, &context, name, spec, self.diagnostics)
            else {
                continue;
            };
            self.schema.auxiliary.push(AuxiliaryTable {
                table,
                versioned: spec.versioned,
            });
        }
    }

    fn known_table(&self, name: &str) -> bool {
        self.schema.table_names().any(|table| table == name)
    }

    fn dictionaries(&mut self) {
        let package = self.package().to_string();
        let input = self.input;
        for (name, spec) in &input.options.dictionaries {
            let context = format!("{} dictionary {name}", self.context());
            if !self.known_table(&spec.source) {
                self.diagnostics.error(
                    &package,
                    format!("{context} reads unknown table {}", spec.source),
                );
            }
            if !spec.attributes.contains_key(&spec.key) {
                self.diagnostics.error(
                    &package,
                    format!("{context} key {} is not an attribute", spec.key),
                );
            }
            let Some((version, deleted)) =
                self.diagnostics
                    .require(self.layout.versioning.clone(), &package, || {
                        format!("{context} reads an unversioned store")
                    })
            else {
                continue;
            };
            self.schema.dictionaries.push(Dictionary {
                name: name.clone(),
                source_table: spec.source.clone(),
                key: spec.key.clone(),
                attributes: spec
                    .attributes
                    .iter()
                    .map(|(name, storage_type)| Column {
                        name: name.clone(),
                        storage_type: storage_type.clone(),
                        default: None,
                        codecs: Vec::new(),
                    })
                    .collect(),
                layout: spec.layout.clone(),
                size_in_cells: spec.size_in_cells,
                lifetime_min: spec.lifetime.min,
                lifetime_max: spec.lifetime.max,
                version_column: version,
                deleted_column: deleted,
            });
        }
    }

    fn views(&mut self) {
        let package = self.package().to_string();
        let mut declared = Vec::new();
        let input = self.input;
        for (name, spec) in &input.options.views {
            let context = format!("{} view {name}", self.context());
            if let Some(to) = spec.to.as_ref().filter(|to| !self.known_table(to)) {
                self.diagnostics
                    .error(&package, format!("{context} writes unknown table {to}"));
            }
            let engine = spec
                .engine
                .as_deref()
                .and_then(|text| parse_engine(&package, &context, text, self.diagnostics));
            if spec.to.is_none() && engine.is_none() {
                self.diagnostics.error(
                    &package,
                    format!("{context} names neither a destination table nor an engine"),
                );
                continue;
            }
            declared.push(MaterializedView {
                name: name.clone(),
                versioned: spec.versioned,
                to_table: spec.to.clone(),
                select_query: spec.select.clone(),
                engine,
                order_by: spec.order_by.clone(),
                populate: spec.populate,
            });
        }
        self.schema.views.splice(0..0, declared);
    }

    fn refreshable_views(&mut self) {
        let package = self.package().to_string();
        let context = self.context();
        for (name, spec) in &self.input.options.refreshable_views {
            if !self.known_table(&spec.append_to) {
                self.diagnostics.error(
                    &package,
                    format!(
                        "{context} refreshable view {name} appends to unknown table {}",
                        spec.append_to
                    ),
                );
            }
            self.schema.refreshable_views.push(RefreshableView {
                name: name.clone(),
                versioned: spec.versioned,
                select_query: spec.select.clone(),
                append_to: spec.append_to.clone(),
                refresh: spec.refresh.clone(),
            });
        }
    }
}

fn auxiliary_table(
    package: &str,
    context: &str,
    name: &str,
    spec: &TableSpec,
    diagnostics: &mut Diagnostics,
) -> Option<Table> {
    let engine = parse_engine(package, context, &spec.engine, diagnostics)?;
    let columns = declared_columns(package, context, &spec.columns, diagnostics);
    Some(Table {
        name: name.to_string(),
        indexes: table_indexes(package, context, &spec.indexes, &columns, diagnostics),
        projections: projections(&spec.projections),
        engine,
        order_by: spec.sort_key.clone(),
        primary_key: spec.primary_key.clone(),
        settings: merge(&[], &spec.settings),
        ttl: spec.ttl.clone(),
        columns,
    })
}

fn declared_columns(
    package: &str,
    context: &str,
    columns: &IndexMap<String, ColumnSpec>,
    diagnostics: &mut Diagnostics,
) -> Vec<Column> {
    columns
        .iter()
        .filter_map(|(name, spec)| {
            let storage_type = diagnostics.require(spec.storage_type.clone(), package, || {
                format!("{context} column {name} declares no type")
            })?;
            Some(Column {
                name: name.clone(),
                storage_type,
                default: spec.default.clone(),
                codecs: spec.codec.clone(),
            })
        })
        .collect()
}

fn table_indexes(
    package: &str,
    context: &str,
    indexes: &IndexMap<String, IndexSpecs>,
    columns: &[Column],
    diagnostics: &mut Diagnostics,
) -> Vec<Index> {
    let has_column = |target: &str| columns.iter().any(|column| column.name == target);
    check_indexes(package, context, indexes, has_column, diagnostics);
    build_indexes(indexes, |target| {
        has_column(target).then(|| target.to_string())
    })
}

fn parse_engine(
    package: &str,
    context: &str,
    text: &str,
    diagnostics: &mut Diagnostics,
) -> Option<Engine> {
    diagnostics.require(Engine::parse(text), package, || {
        format!("{context} has malformed engine `{text}`")
    })
}

pub(super) fn merge(base: &[(String, String)], overrides: &Settings) -> Vec<(String, String)> {
    let mut settings = base.to_vec();
    for (name, value) in overrides {
        match settings.iter_mut().find(|(key, _)| key == name) {
            Some((_, previous)) => previous.clone_from(value),
            None => settings.push((name.clone(), value.clone())),
        }
    }
    settings
}

pub fn default_index_name(target: &str, position: usize, kind: &str) -> String {
    let column = target.trim_start_matches('_');
    match position {
        0 => format!("idx_{column}"),
        _ => format!(
            "idx_{column}_{}",
            kind.split('(').next().unwrap_or(kind).trim()
        ),
    }
}

fn named<'a>(
    target: &'a str,
    specs: &'a IndexSpecs,
) -> impl Iterator<Item = (String, &'a IndexSpec)> + 'a {
    specs
        .specs()
        .iter()
        .enumerate()
        .map(move |(position, spec)| {
            let name = spec.name().map_or_else(
                || default_index_name(target, position, spec.kind()),
                str::to_string,
            );
            (name, spec)
        })
}

pub(in crate::implementations::clickhouse) fn check_indexes(
    package: &str,
    context: &str,
    indexes: &IndexMap<String, IndexSpecs>,
    has_column: impl Fn(&str) -> bool,
    diagnostics: &mut Diagnostics,
) {
    let mut names = HashSet::new();
    for (target, specs) in indexes {
        if !has_column(target) {
            diagnostics.error(
                package,
                format!("{context} indexes unknown column {target}"),
            );
        }
        for (name, spec) in named(target, specs) {
            if IndexKind::parse(spec.kind()).is_none() {
                diagnostics.error(
                    package,
                    format!(
                        "{context} {target} has unknown index kind `{}`",
                        spec.kind()
                    ),
                );
            }
            if !names.insert(name.clone()) {
                diagnostics.error(package, format!("{context} declares index {name} twice"));
            }
        }
    }
}

fn build_indexes(
    indexes: &IndexMap<String, IndexSpecs>,
    column_of: impl Fn(&str) -> Option<String>,
) -> Vec<Index> {
    let mut built = Vec::new();
    for (target, specs) in indexes {
        let Some(column) = column_of(target) else {
            continue;
        };
        for (name, spec) in named(target, specs) {
            let index = Index {
                name,
                column: column.clone(),
                lowercase: false,
                kind: spec.kind().to_string(),
                granularity: spec.granularity(),
            };
            if index.kind == TEXT_INDEX {
                built.extend(text_indexes(index));
            } else {
                built.push(index);
            }
        }
    }
    built
}

fn text_indexes(index: Index) -> [Index; 2] {
    [
        Index {
            lowercase: true,
            kind: "text(tokenizer = splitByNonAlpha)".into(),
            ..index.clone()
        },
        Index {
            name: format!("{}_ngram", index.name),
            lowercase: true,
            kind: "ngrambf_v1(3, 512, 2, 0)".into(),
            ..index
        },
    ]
}

fn projections(specs: &IndexMap<String, ProjectionSpec>) -> Vec<Projection> {
    specs
        .iter()
        .map(|(name, spec)| match spec {
            ProjectionSpec::Ordered(ordered) if ordered.lightweight => Projection::Lightweight {
                name: name.clone(),
                order_by: ordered.order_by.clone(),
            },
            ProjectionSpec::Ordered(ordered) => Projection::Reorder {
                name: name.clone(),
                order_by: ordered.order_by.clone(),
            },
            ProjectionSpec::Aggregate(aggregate) => Projection::Aggregate {
                name: name.clone(),
                select: aggregate.select.clone(),
                group_by: aggregate.group_by.clone(),
            },
        })
        .collect()
}
