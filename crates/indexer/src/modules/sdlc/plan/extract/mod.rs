//! Turns a pipeline's declarative `Extract` into the templated ClickHouse query the runtime consumes.

pub(super) mod generated;
mod lookup;
pub(super) mod sql;

use std::collections::HashSet;

use ontology::sql_template;
use ontology::{
    DataType, EtlScope, Extract, NodeEntity, Pipeline,
    constants::{DELETED_COLUMN, VERSION_COLUMN},
};

use super::build::PlanError;
pub(in crate::modules::sdlc) use lookup::PointLookupJoin;

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

/// A typed source column; `data_type` drives dialect rendering (uuid → string, date clamp)
/// and `binary` marks a bytea source that must be UTF8-guarded.
pub(super) struct SourceColumn {
    pub name: String,
    pub data_type: DataType,
    pub binary: bool,
}

impl SourceColumn {
    /// A column selected verbatim — the name is already the source expression.
    pub(super) fn bare(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: DataType::String,
            binary: false,
        }
    }

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

    pub(super) fn bare_all(columns: &[String]) -> Vec<SourceColumn> {
        columns
            .iter()
            .map(|c| SourceColumn::bare(c.as_str()))
            .collect()
    }
}

/// The narrow view strategies receive; deliberately excludes the pipeline's `query` and transform.
pub(super) struct ExtractDecl {
    pub entity: String,
    pub scope: EtlScope,
    pub table: String,
    pub watermark: String,
    pub deleted: String,
    pub order_by: Vec<String>,
}

impl ExtractDecl {
    /// The single ontology→indexer conversion point — nothing below the `*_plan` fns sees the pipeline.
    pub(super) fn of(pipeline: &Pipeline) -> Self {
        let Extract::ClickHouse(extract) = &pipeline.extract;
        ExtractDecl {
            entity: pipeline.name.clone(),
            scope: pipeline.scope,
            table: extract.tables.first().cloned().unwrap_or_default(),
            watermark: extract.watermark.clone(),
            deleted: extract.deleted.clone(),
            order_by: extract.order_by.clone(),
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
