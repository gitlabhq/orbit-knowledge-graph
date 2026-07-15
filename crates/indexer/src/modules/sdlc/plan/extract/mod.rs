//! Turns a pipeline's declarative `Extract` into the templated ClickHouse query the runtime consumes.

mod generated;
mod lookup;
mod sql;

use std::collections::HashSet;

use indexmap::IndexSet;
use ontology::sql_template;
use ontology::{
    ClickHouseExtract, DataType, EtlScope, Extract, ExtractQuery, NodeEntity, Pipeline,
    constants::{DEFAULT_PRIMARY_KEY, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN},
};

use super::build::PlanError;
use lookup::PointLookupJoin;

pub(super) const FILTERS_MARKER: &str = "{{filters}}";
pub(super) const BATCH_SIZE_MARKER: &str = "{{batch_size}}";

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct ClickHouseExtractPlan {
    pub template: ExtractTemplate,
    pub watermark_column: String,
    pub deleted_column: String,
    pub sort_key: Vec<String>,
    pub batch_size: u64,
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) enum ExtractPlan {
    ClickHouse(ClickHouseExtractPlan),
}

impl ExtractPlan {
    pub fn get_clickhouse_plan(&self) -> &ClickHouseExtractPlan {
        match self {
            Self::ClickHouse(plan) => plan,
        }
    }
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
) -> Result<ClickHouseExtractPlan, PlanError> {
    match &declaration.query {
        ExtractQuery::Generated { filter } => {
            generated::compile_generated_extract(declaration, filter.as_deref())
        }
        ExtractQuery::Sql(raw) => sql::compile_authored_extract(declaration, raw),
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
    pub(super) fn from_node(node: &NodeEntity) -> Vec<SourceColumn> {
        node.fields
            .iter()
            .filter_map(|field| {
                field.column_name().map(|name| SourceColumn {
                    name: name.to_string(),
                    data_type: field.data_type,
                    binary: field.binary,
                })
            })
            .collect()
    }

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
    pub(super) fn from_node_pipeline(node: &NodeEntity, pipeline: &Pipeline) -> Self {
        let Extract::ClickHouse(extract) = &pipeline.extract;
        Self::from_pipeline_and_source_columns(
            pipeline,
            extract,
            SourceColumn::from_node(node),
            Vec::new(),
        )
    }

    pub(super) fn from_edge_pipeline(pipeline: &Pipeline) -> Self {
        let Extract::ClickHouse(extract) = &pipeline.extract;
        let lookup_joins = PointLookupJoin::get_from_extract_declaration(extract, pipeline.scope);
        let source_columns = get_edge_source_columns(extract, lookup_joins.is_empty());
        Self::from_pipeline_and_source_columns(pipeline, extract, source_columns, lookup_joins)
    }

    pub(super) fn from_derived_pipeline(pipeline: &Pipeline) -> Self {
        let Extract::ClickHouse(extract) = &pipeline.extract;
        Self::from_pipeline_and_source_columns(pipeline, extract, Vec::new(), Vec::new())
    }

    fn from_pipeline_and_source_columns(
        pipeline: &Pipeline,
        extract: &ClickHouseExtract,
        source_columns: Vec<SourceColumn>,
        lookup_joins: Vec<PointLookupJoin>,
    ) -> Self {
        Self {
            entity: pipeline.name.clone(),
            scope: pipeline.scope,
            table: extract.tables.first().cloned().unwrap_or_default(),
            source_columns,
            watermark: extract.watermark.clone(),
            deleted: extract.deleted.clone(),
            order_by: extract.order_by.clone(),
            query: extract.query.clone(),
            lookup_joins,
        }
    }

    fn build_spec(&self, sql: String) -> Result<ClickHouseExtractPlan, PlanError> {
        Ok(ClickHouseExtractPlan {
            template: ExtractTemplate::new(sql)?,
            watermark_column: self.watermark.clone(),
            deleted_column: self.deleted.clone(),
            sort_key: self.order_by.clone(),
            batch_size: 0,
        })
    }
}

fn get_edge_source_columns(
    extract: &ClickHouseExtract,
    uses_single_table_query: bool,
) -> Vec<SourceColumn> {
    let mut source_columns: IndexSet<String> = extract.fields.iter().cloned().collect();
    if uses_single_table_query {
        source_columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
        source_columns.insert(DEFAULT_PRIMARY_KEY.to_string());
    } else {
        source_columns.extend(extract.order_by.iter().cloned());
        source_columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
    }
    SourceColumn::from_field_names(source_columns)
}
