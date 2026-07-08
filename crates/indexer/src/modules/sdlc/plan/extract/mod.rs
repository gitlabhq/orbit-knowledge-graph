//! Turns a pipeline's declarative `Extract` into the templated ClickHouse query the runtime consumes.

mod enrichment;
pub(super) mod generated;
pub(super) mod sql;

use ontology::{
    DataType, EtlScope, Extract, NodeEntity, Pipeline,
    constants::{DELETED_COLUMN, VERSION_COLUMN},
};

pub(in crate::modules::sdlc) use enrichment::EnrichmentJoin;

use super::build::PlanError;

pub(super) const FILTERS_MARKER: &str = "{{filters}}";
pub(super) const BATCH_SIZE_MARKER: &str = "{{batch_size}}";

#[derive(Debug)]
pub(in crate::modules::sdlc) struct ExtractSpec {
    pub template: ExtractTemplate,
    /// Settings default for generated extracts, or recovered from authored SQL's `AS _version`/`AS _deleted`.
    pub watermark: String,
    pub deleted: String,
    pub order_by: Vec<String>,
}

/// Validated template — the only way a `Plan` gets its `extract_template`.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct ExtractTemplate(String);

impl ExtractTemplate {
    pub fn new(sql: String) -> Result<Self, PlanError> {
        for marker in double_brace_markers(&sql) {
            if marker != "filters" && marker != "batch_size" {
                return Err(PlanError::MalformedTemplate(format!(
                    "unresolved marker {{{{{marker}}}}}"
                )));
            }
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
pub(super) struct ExtractDecl<'a> {
    pub entity: &'a str,
    pub scope: EtlScope,
    pub table: &'a str,
    pub watermark: &'a str,
    pub deleted: &'a str,
    pub order_by: &'a [String],
}

impl<'a> ExtractDecl<'a> {
    /// The single ontology→indexer conversion point — nothing below the `*_plan` fns sees the pipeline.
    pub(super) fn of(pipeline: &'a Pipeline) -> Self {
        let Extract::ClickHouse(extract) = &pipeline.extract;
        ExtractDecl {
            entity: &pipeline.name,
            scope: pipeline.scope,
            table: extract
                .tables
                .first()
                .map(String::as_str)
                .unwrap_or_default(),
            watermark: &extract.watermark,
            deleted: &extract.deleted,
            order_by: &extract.order_by,
        }
    }

    fn build_spec(&self, sql: String) -> Result<ExtractSpec, PlanError> {
        Ok(ExtractSpec {
            template: ExtractTemplate::new(sql)?,
            watermark: self.watermark.to_string(),
            deleted: self.deleted.to_string(),
            order_by: self.order_by.to_vec(),
        })
    }
}

fn double_brace_markers(raw: &str) -> Vec<&str> {
    let mut markers = Vec::new();
    let mut rest = raw;
    while let Some(start) = rest.find("{{") {
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find("}}") else {
            markers.push(after_start.trim());
            break;
        };
        markers.push(after_start[..end].trim());
        rest = &after_start[end + 2..];
    }
    markers
}
