//! Turns a pipeline's declarative `Extract` into the templated ClickHouse query the runtime consumes.

mod generated;
mod lookup;
mod sql;

use std::collections::HashSet;

use indexmap::IndexSet;
use query_data_model::catalog::sources::{EtlScope, ExtractQuery, Pipeline, Writer};
use query_data_model::{
    Catalog, ClickHouse, DEFAULT_PRIMARY_KEY, DELETED_COLUMN, DataType, TRAVERSAL_PATH_COLUMN,
    VERSION_COLUMN, sql_template,
};

use super::build::PlanError;
use lookup::PointLookupJoin;

pub(super) const FILTERS_MARKER: &str = "{{filters}}";
pub(super) const BATCH_SIZE_MARKER: &str = "{{batch_size}}";

#[derive(Debug)]
pub(in crate::modules::sdlc) struct ExtractSpec {
    pub template: ExtractTemplate,
    pub watermark: String,
    pub deleted: String,
}

/// Validated template — the only way a `Plan` gets its `extract_template`.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct ExtractTemplate(String);

pub(super) struct ClickHouseExtractDeclaration {
    pub entity: String,
    pub scope: EtlScope,
    pub table: String,
    pub source_columns: Vec<SourceColumn>,
    pub order_by: Vec<String>,
    pub version: String,
    pub watermark: String,
    pub deleted: String,
    pub query: ExtractQuery,
    pub lookup_joins: Vec<PointLookupJoin>,
}

pub(super) struct SourceColumn {
    pub name: String,
    pub data_type: DataType,
    pub binary: bool,
}

pub(super) fn compile_extract_spec(
    declaration: &ClickHouseExtractDeclaration,
) -> Result<ExtractSpec, PlanError> {
    match &declaration.query {
        ExtractQuery::Generated { filter } => {
            generated::compile_generated_extract(declaration, filter.as_deref())
        }
        ExtractQuery::Template(raw) => sql::compile_authored_extract(declaration, raw),
    }
}

impl ExtractTemplate {
    pub fn new(sql: String) -> Result<Self, PlanError> {
        let undeclared = sql_template::undeclared_variables(&sql)
            .map_err(|e| PlanError::MalformedTemplate(format!("template parse failed: {e}")))?;
        let expected: HashSet<String> = [FILTERS_MARKER, BATCH_SIZE_MARKER]
            .iter()
            .map(|marker| marker.trim_matches(|c| c == '{' || c == '}').to_string())
            .collect();
        if undeclared != expected {
            return Err(PlanError::MalformedTemplate(format!(
                "template variables must be exactly {{{{filters}}}} and {{{{batch_size}}}}, found {undeclared:?}"
            )));
        }
        for (marker, name) in [
            (FILTERS_MARKER, "filters"),
            (BATCH_SIZE_MARKER, "batch_size"),
        ] {
            if sql.matches(marker).count() != 1 {
                return Err(PlanError::MalformedTemplate(format!(
                    "template must contain exactly one {{{{{name}}}}} marker"
                )));
            }
        }
        for alias in [VERSION_COLUMN, DELETED_COLUMN] {
            if !sql.contains(&format!(" AS {alias}")) {
                return Err(PlanError::MalformedTemplate(format!(
                    "template must select a column ` AS {alias}`"
                )));
            }
        }
        Ok(Self(sql))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl SourceColumn {
    fn from_field_names(fields: impl IntoIterator<Item = String>) -> Vec<SourceColumn> {
        fields
            .into_iter()
            .map(|name| SourceColumn {
                name,
                data_type: DataType::String,
                binary: false,
            })
            .collect()
    }
}

impl ClickHouseExtractDeclaration {
    pub(super) fn from_pipeline(catalog: &Catalog<ClickHouse>, pipeline: &Pipeline) -> Self {
        let source = catalog.model().sources().data_source(pipeline);
        let (source_columns, lookup_joins) = match pipeline.writer {
            Writer::Entity(_) => (
                pipeline
                    .columns
                    .iter()
                    .map(|column| SourceColumn {
                        name: column.column.clone(),
                        data_type: catalog.graph().property(column.property).data_type,
                        binary: column.binary,
                    })
                    .collect(),
                Vec::new(),
            ),
            Writer::Relationship(_) => {
                let lookup_joins = PointLookupJoin::get_from_extract_declaration(pipeline);
                let source_columns = get_edge_source_columns(pipeline, lookup_joins.is_empty());
                (source_columns, lookup_joins)
            }
            Writer::Derived(_) => (Vec::new(), Vec::new()),
        };
        Self {
            entity: pipeline.name.clone(),
            scope: pipeline.scope,
            table: pipeline.tables.first().cloned().unwrap_or_default(),
            source_columns,
            version: source.version.clone(),
            watermark: source.watermark.clone(),
            deleted: source.deleted.clone(),
            order_by: pipeline.order_by.clone(),
            query: pipeline.query.clone(),
            lookup_joins,
        }
    }

    fn build_spec(&self, sql: String) -> Result<ExtractSpec, PlanError> {
        Ok(ExtractSpec {
            template: ExtractTemplate::new(sql)?,
            watermark: self.watermark.clone(),
            deleted: self.deleted.clone(),
        })
    }
}

fn get_edge_source_columns(
    pipeline: &Pipeline,
    uses_single_table_query: bool,
) -> Vec<SourceColumn> {
    let mut source_columns: IndexSet<String> = pipeline.fields.iter().cloned().collect();
    if uses_single_table_query {
        source_columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
        source_columns.insert(DEFAULT_PRIMARY_KEY.to_string());
    } else {
        source_columns.extend(pipeline.order_by.iter().cloned());
        source_columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
    }
    SourceColumn::from_field_names(source_columns)
}
