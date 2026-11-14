//! The result columns of a stage that feeds Combine: exactly its contract's columns, in order.

use query_data_model::EdgeField;

use super::Matched;
use super::pattern::Pattern;
use crate::constants::{
    EDGE_DST_KIND_SUFFIX, EDGE_DST_SUFFIX, EDGE_KIND_SUFFIX, EDGE_SRC_KIND_SUFFIX, EDGE_SRC_SUFFIX,
};
use crate::dag::{Contract, Handle, Output};
use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::passes::codegen::SqlBackend;

pub(super) fn outputs<B: SqlBackend>(
    mut matched: Matched,
    pattern: &Pattern<'_, B>,
    query: Query<'_>,
    contract: &Contract,
) -> Result<Matched> {
    let graph = query.model.graph();
    let relation = |step: usize| {
        pattern
            .relations
            .iter()
            .find(|relation| relation.input_index == step)
            .ok_or_else(|| QueryError::PipelineInvariant("a contract edge has no step".into()))
    };
    let name = |output: &Output| -> Result<String> {
        Ok(match output {
            Output::Node { binding, handle } => {
                let entity = query
                    .binding(binding)
                    .ok_or_else(|| {
                        QueryError::PipelineInvariant("a contract node has no binding".into())
                    })?
                    .entity;
                let property = match handle {
                    Handle::Identity(part) => graph.entity(entity).identity[*part],
                    Handle::Property(property) => *property,
                };
                format!("{binding}_{}", graph.property(property).name)
            }
            Output::Edge { step, field } => {
                let suffix = match field {
                    EdgeField::Kind => EDGE_KIND_SUFFIX,
                    EdgeField::SourceId => EDGE_SRC_SUFFIX,
                    EdgeField::SourceKind => EDGE_SRC_KIND_SUFFIX,
                    EdgeField::TargetId => EDGE_DST_SUFFIX,
                    EdgeField::TargetKind => EDGE_DST_KIND_SUFFIX,
                };
                format!("{}{suffix}", relation(*step)?.column_prefix())
            }
            Output::Path { step } => relation(*step)?.path_nodes_column(),
            Output::Line | Output::Capture(_) => {
                return Err(QueryError::PipelineInvariant(
                    "a relational stage returns no match column".into(),
                ));
            }
        })
    };
    matched.outputs = contract
        .columns
        .iter()
        .map(|column| {
            let name = name(&column.output)?;
            matched
                .outputs
                .iter()
                .find(|projection| projection.name == name)
                .cloned()
                .ok_or_else(|| {
                    QueryError::Validation(format!(
                        "a stage that feeds another cannot return {name} inline"
                    ))
                })
        })
        .collect::<Result<_>>()?;
    Ok(matched)
}
