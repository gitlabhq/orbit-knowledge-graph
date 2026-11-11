use super::layout::{auxiliary_table, declared_columns, merge, parse_engine};
use super::{
    AuxiliaryTable, Column, Dictionary, Engine, MaterializedView, RefreshableView, Schema, Table,
};
use crate::DataType;
use crate::catalog::{Diagnostics, StoreInput};
use crate::implementations::clickhouse::ClickHouse;
use crate::implementations::relational::{ReadPaths, RelationalStore};

pub(super) struct Layout {
    pub columns: Vec<Column>,
    pub settings: Vec<(String, String)>,
    pub engine: Option<Engine>,
    pub versioning: Option<(String, String)>,
}

/// Resolves the store's tables into `relational` and, for a store that creates them, its schema.
pub(in crate::implementations::clickhouse) fn resolve(
    input: &StoreInput<'_, ClickHouse>,
    relational: &mut RelationalStore,
    paths: &mut ReadPaths,
    diagnostics: &mut Diagnostics,
) -> Option<Schema> {
    let package = input.store.package.as_str();
    let context = format!("store {}", input.store.name);
    let declared = input.options.engine.as_deref();
    let layout = Layout {
        columns: declared_columns(&input.options.columns),
        settings: merge(&[], &input.options.settings),
        engine: declared.and_then(|text| parse_engine(package, &context, text, diagnostics)),
        versioning: relational
            .versioning
            .as_ref()
            .map(|versioning| (versioning.version.clone(), versioning.deleted.clone())),
    };
    let creates = layout.engine.is_some();
    let mut resolver = Resolver {
        input,
        layout,
        diagnostics,
        tables: Vec::new(),
        schema: Schema::default(),
    };
    resolver.entity_tables();
    resolver.edge_tables();
    resolver.joins();
    let created = resolver.tables.len();
    resolver.copies(paths);
    if creates {
        resolver.schema.tables = resolver.tables[..created].to_vec();
        resolver.auxiliary_tables();
        resolver.dictionaries();
        resolver.views();
        resolver.refreshable_views();
    }
    let Resolver {
        mut tables, schema, ..
    } = resolver;
    tables.extend(schema.auxiliary.iter().map(|table| table.table.clone()));
    relational.tables = tables;
    creates.then_some(schema)
}

pub(super) struct Resolver<'a, 'd> {
    pub input: &'a StoreInput<'a, ClickHouse>,
    pub layout: Layout,
    pub diagnostics: &'d mut Diagnostics,
    pub tables: Vec<Table>,
    pub schema: Schema,
}

impl Resolver<'_, '_> {
    pub(super) fn package(&self) -> &str {
        &self.input.store.package
    }

    pub(super) fn context(&self) -> String {
        format!("store {}", self.input.store.name)
    }

    pub(super) fn creates(&self) -> bool {
        self.layout.engine.is_some()
    }

    /// The shared columns, then the version and deleted columns a store that creates no
    /// tables leaves undeclared.
    pub(super) fn shared_columns(&self, columns: &mut Vec<Column>) {
        columns.extend(self.layout.columns.iter().cloned());
        if self.creates() {
            return;
        }
        let system = self
            .layout
            .versioning
            .iter()
            .flat_map(|(version, deleted)| {
                [(version, DataType::DateTime), (deleted, DataType::Bool)]
            });
        for (name, data_type) in system {
            if !columns.iter().any(|column| &column.name == name) {
                columns.push(Column::new(name, Some(data_type)));
            }
        }
    }

    pub(super) fn engine(
        &mut self,
        package: &str,
        context: &str,
        declared: Option<&str>,
    ) -> Option<Engine> {
        let default = self.layout.engine.clone()?;
        Some(
            declared
                .and_then(|text| parse_engine(package, context, text, self.diagnostics))
                .unwrap_or(default),
        )
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
            if let Some(role) = spec.role
                && self.schema.engine_table(role).is_ok()
            {
                self.diagnostics
                    .error(&package, format!("{context} repeats the table role {role}"));
            }
            self.schema.auxiliary.push(AuxiliaryTable {
                table,
                versioned: spec.versioned,
                role: spec.role,
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
                        storage_type: storage_type.clone(),
                        ..Column::new(name, None)
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
