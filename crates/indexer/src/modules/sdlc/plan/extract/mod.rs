//! Turns a pipeline's declarative `Extract` into the templated ClickHouse query the runtime consumes.

mod generated;
mod lookup;
mod sql;

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
pub(super) const FINAL_MARKER: &str = "{{final}}";

#[derive(Debug)]
pub(in crate::modules::sdlc) struct ExtractSpec {
    pub template: ExtractTemplate,
    pub watermark: String,
    pub deleted: String,
}

/// Validated template — the only way a `Plan` gets its `extract_template`. `final` is the one
/// mode switch: `{{final}}` reads `FINAL` in full mode and `{% if final %}` keeps SQL to full mode.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct ExtractTemplate {
    full: String,
    incremental: String,
}

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
        ExtractQuery::Sql(raw) => sql::compile_authored_extract(declaration, raw),
    }
}

impl ExtractTemplate {
    pub fn new(source: &str, columns: sql_template::Value) -> Result<Self, PlanError> {
        for (marker, name) in [
            (FILTERS_MARKER, "filters"),
            (BATCH_SIZE_MARKER, "batch_size"),
            (FINAL_MARKER, "final"),
        ] {
            if source.matches(marker).count() != 1 {
                return Err(PlanError::MalformedTemplate(format!(
                    "template must contain exactly one {{{{{name}}}}} marker"
                )));
            }
        }
        let render = |read: &str| {
            sql_template::render(
                source,
                sql_template::context! {
                    final => read,
                    filters => FILTERS_MARKER,
                    batch_size => BATCH_SIZE_MARKER,
                    ..columns.clone()
                },
            )
            .map_err(|e| PlanError::MalformedTemplate(format!("template render failed: {e}")))
        };
        let full = render("FINAL")?;
        let incremental = render("")?;
        for alias in [VERSION_COLUMN, DELETED_COLUMN] {
            if !incremental.contains(&format!(" AS {alias}")) {
                return Err(PlanError::MalformedTemplate(format!(
                    "template must select a column ` AS {alias}`"
                )));
            }
        }
        Ok(Self { full, incremental })
    }

    pub fn full(&self) -> &str {
        &self.full
    }

    pub fn incremental(&self) -> &str {
        &self.incremental
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
            version: extract.version.clone(),
            watermark: extract.watermark.clone(),
            deleted: extract.deleted.clone(),
            order_by: extract.order_by.clone(),
            query: extract.query.clone(),
            lookup_joins,
        }
    }

    pub(super) fn template_columns(&self) -> sql_template::Value {
        sql_template::context! {
            version_column => self.version,
            watermark_column => self.watermark,
            deleted_column => self.deleted,
        }
    }

    fn build_spec(&self, sql: String) -> Result<ExtractSpec, PlanError> {
        Ok(ExtractSpec {
            template: ExtractTemplate::new(&sql, self.template_columns())?,
            watermark: self.watermark.clone(),
            deleted: self.deleted.clone(),
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
