//! Indexer transform-query frontend.
//!
//! Builds `Rel` trees for DataFusion transform queries that run against
//! the in-memory `source_data` MemTable. Two shapes:
//!
//! 1. Node transform: `SELECT cols FROM source_data`
//! 2. FK edge transform: `SELECT edge_cols FROM source_data WHERE filters`

use super::types::*;
use llqm::ir::expr::{self, DataType, Expr};
use llqm::ir::plan::{Plan, Rel};
use llqm::pipeline::Frontend;

const SOURCE_DATA_TABLE: &str = "source_data";
const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";

// ---------------------------------------------------------------------------
// Node Transform Frontend
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    #[error("node transform has no columns")]
    NoColumns,
}

pub struct NodeTransformFrontend;

impl Frontend for NodeTransformFrontend {
    type Input = NodeTransformInput;
    type Error = TransformError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        if input.columns.is_empty() {
            return Err(TransformError::NoColumns);
        }

        let source = source_data_read(&input.columns);
        let rel = build_node_projection(source, &input.columns);
        Ok(rel.into_plan())
    }
}

fn source_data_read(columns: &[NodeColumn]) -> Rel {
    let mut cols: Vec<(&str, DataType)> = columns
        .iter()
        .map(|c| {
            let name = match c {
                NodeColumn::Identity(n) => n.as_str(),
                NodeColumn::Rename { source, .. } => source.as_str(),
                NodeColumn::IntEnum { source, .. } => source.as_str(),
            };
            (name, DataType::String)
        })
        .collect();
    cols.push((VERSION_ALIAS, DataType::String));
    cols.push((DELETED_ALIAS, DataType::Bool));
    Rel::read(SOURCE_DATA_TABLE, "", &cols)
}

fn build_node_projection(rel: Rel, columns: &[NodeColumn]) -> Rel {
    let mut items: Vec<(Expr, &str)> = columns.iter().map(lower_node_column).collect();
    items.push((expr::col("", VERSION_ALIAS), VERSION_ALIAS));
    items.push((expr::col("", DELETED_ALIAS), DELETED_ALIAS));
    rel.project(&items)
}

fn lower_node_column(column: &NodeColumn) -> (Expr, &str) {
    match column {
        NodeColumn::Identity(name) => (expr::col("", name), name.as_str()),
        NodeColumn::Rename { source, target } => (expr::col("", source), target.as_str()),
        NodeColumn::IntEnum {
            source,
            target,
            values,
        } => {
            let ifs: Vec<(Expr, Expr)> = values
                .iter()
                .map(|(key, value)| {
                    (
                        expr::col("", source).eq(expr::int(*key)),
                        expr::string(value),
                    )
                })
                .collect();
            let case = expr::if_then(ifs, Some(expr::string("unknown")));
            (case, target.as_str())
        }
    }
}

// ---------------------------------------------------------------------------
// FK Edge Transform Frontend
// ---------------------------------------------------------------------------

pub struct FkEdgeTransformFrontend;

impl Frontend for FkEdgeTransformFrontend {
    type Input = FkEdgeTransformInput;
    type Error = TransformError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        let source = edge_source_data_read();
        let rel = build_edge_query(source, &input);
        Ok(rel.into_plan())
    }
}

fn edge_source_data_read() -> Rel {
    Rel::read(
        SOURCE_DATA_TABLE,
        "",
        &[
            ("id", DataType::Int64),
            ("traversal_path", DataType::String),
            (VERSION_ALIAS, DataType::String),
            (DELETED_ALIAS, DataType::Bool),
        ],
    )
}

fn build_edge_query(rel: Rel, input: &FkEdgeTransformInput) -> Rel {
    let mut rel = rel;

    if let Some(filter) = build_edge_filter(&input.filters) {
        rel = rel.filter(filter);
    }

    let traversal_path_expr = if input.namespaced {
        expr::col("", "traversal_path")
    } else {
        expr::raw("'0/'")
    };

    let items: Vec<(Expr, &str)> = vec![
        (traversal_path_expr, "traversal_path"),
        (lower_edge_id(&input.source_id), "source_id"),
        (lower_edge_kind(&input.source_kind), "source_kind"),
        (
            expr::raw(&format!("'{}'", input.relationship_kind)),
            "relationship_kind",
        ),
        (lower_edge_id(&input.target_id), "target_id"),
        (lower_edge_kind(&input.target_kind), "target_kind"),
        (expr::col("", VERSION_ALIAS), VERSION_ALIAS),
        (expr::col("", DELETED_ALIAS), DELETED_ALIAS),
    ];

    rel.project(&items)
}

fn lower_edge_id(id: &EdgeId) -> Expr {
    match id {
        EdgeId::Column(column) => expr::col("", column),
        EdgeId::Exploded { column, delimiter } => Expr::Cast {
            expr: Box::new(expr::func(
                "NULLIF",
                vec![
                    expr::func(
                        "unnest",
                        vec![expr::func(
                            "string_to_array",
                            vec![expr::col("", column), expr::raw(&format!("'{delimiter}'"))],
                        )],
                    ),
                    expr::raw("''"),
                ],
            )),
            target_type: DataType::Int64,
        },
    }
}

fn lower_edge_kind(kind: &EdgeKind) -> Expr {
    match kind {
        EdgeKind::Literal(value) => expr::raw(&format!("'{value}'")),
        EdgeKind::Column(column) => expr::col("", column),
        EdgeKind::TypeMapping { column, mapping } => {
            let ifs: Vec<(Expr, Expr)> = mapping
                .iter()
                .map(|(from, to)| {
                    (
                        expr::col("", column).eq(expr::string(from)),
                        expr::string(to),
                    )
                })
                .collect();
            let fallback = expr::col("", column);
            expr::if_then(ifs, Some(fallback))
        }
    }
}

fn build_edge_filter(filters: &[EdgeFilter]) -> Option<Expr> {
    let exprs: Vec<Expr> = filters.iter().map(lower_edge_filter).collect();
    if exprs.is_empty() {
        None
    } else {
        Some(expr::and(exprs))
    }
}

fn lower_edge_filter(filter: &EdgeFilter) -> Expr {
    match filter {
        EdgeFilter::IsNotNull(column) => expr::col("", column).is_not_null(),
        EdgeFilter::NotEmpty(column) => expr::col("", column).ne(expr::raw("''")),
        EdgeFilter::TypeIn { column, types } => {
            let list: Vec<Expr> = types.iter().map(|t| expr::string(t)).collect();
            expr::col("", column).in_list(list)
        }
    }
}
