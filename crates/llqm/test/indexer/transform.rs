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

// ---------------------------------------------------------------------------
// Tests — all use Pipeline for construction AND emission
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use llqm::backend::clickhouse::ClickHouseBackend;
    use llqm::pipeline::Pipeline;
    use std::collections::BTreeMap;

    /// Build + emit through the pipeline.
    fn emit_node(input: NodeTransformInput) -> String {
        Pipeline::new()
            .input(NodeTransformFrontend, input)
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish()
            .sql
    }

    fn emit_edge(input: FkEdgeTransformInput) -> String {
        Pipeline::new()
            .input(FkEdgeTransformFrontend, input)
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish()
            .sql
    }

    // -- Node transforms --

    #[test]
    fn node_transform_identity_columns() {
        let sql = emit_node(NodeTransformInput {
            columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::Identity("name".into()),
            ],
        });

        assert!(sql.contains("id"), "sql: {sql}");
        assert!(sql.contains("name"), "sql: {sql}");
        assert!(sql.contains(SOURCE_DATA_TABLE), "sql: {sql}");
        assert!(sql.contains(VERSION_ALIAS), "sql: {sql}");
        assert!(sql.contains(DELETED_ALIAS), "sql: {sql}");
    }

    #[test]
    fn node_transform_column_renaming() {
        let sql = emit_node(NodeTransformInput {
            columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::Identity("name".into()),
                NodeColumn::Rename {
                    source: "admin".into(),
                    target: "is_admin".into(),
                },
            ],
        });

        assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
    }

    #[test]
    fn node_transform_int_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".into());
        values.insert(1, "blocked".into());

        let sql = emit_node(NodeTransformInput {
            columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::Identity("name".into()),
                NodeColumn::IntEnum {
                    source: "state".into(),
                    target: "state".into(),
                    values,
                },
            ],
        });

        assert!(sql.contains("CASE"), "sql: {sql}");
        assert!(sql.contains("WHEN"), "sql: {sql}");
        assert!(sql.contains("ELSE"), "sql: {sql}");
        assert!(sql.contains("END AS state"), "sql: {sql}");
    }

    #[test]
    fn node_transform_rejects_empty_columns() {
        let result = Pipeline::new()
            .input(
                NodeTransformFrontend,
                NodeTransformInput { columns: vec![] },
            )
            .lower();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no columns"));
    }

    // -- FK edge transforms --

    #[test]
    fn fk_edge_outgoing_literal() {
        let sql = emit_edge(FkEdgeTransformInput {
            relationship_kind: "owns".into(),
            source_id: EdgeId::Column("id".into()),
            source_kind: EdgeKind::Literal("Group".into()),
            target_id: EdgeId::Column("owner_id".into()),
            target_kind: EdgeKind::Literal("User".into()),
            filters: vec![EdgeFilter::IsNotNull("owner_id".into())],
            namespaced: true,
        });

        assert!(sql.contains("id AS source_id"), "sql: {sql}");
        assert!(sql.contains("'Group' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("owner_id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'User' AS target_kind"), "sql: {sql}");
        assert!(sql.contains("'owns' AS relationship_kind"), "sql: {sql}");
        assert!(sql.contains("IS NOT NULL"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_incoming_literal() {
        let sql = emit_edge(FkEdgeTransformInput {
            relationship_kind: "authored".into(),
            source_id: EdgeId::Column("author_id".into()),
            source_kind: EdgeKind::Literal("User".into()),
            target_id: EdgeId::Column("id".into()),
            target_kind: EdgeKind::Literal("Note".into()),
            filters: vec![EdgeFilter::IsNotNull("author_id".into())],
            namespaced: true,
        });

        assert!(sql.contains("author_id AS source_id"), "sql: {sql}");
        assert!(sql.contains("'User' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'Note' AS target_kind"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_multi_value_exploded() {
        let sql = emit_edge(FkEdgeTransformInput {
            relationship_kind: "assigned".into(),
            source_id: EdgeId::Exploded {
                column: "assignee_ids".into(),
                delimiter: "/".into(),
            },
            source_kind: EdgeKind::Literal("User".into()),
            target_id: EdgeId::Column("id".into()),
            target_kind: EdgeKind::Literal("WorkItem".into()),
            filters: vec![
                EdgeFilter::IsNotNull("assignee_ids".into()),
                EdgeFilter::NotEmpty("assignee_ids".into()),
            ],
            namespaced: true,
        });

        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS Int64)"),
            "sql: {sql}"
        );
        assert!(sql.contains("'User' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'WorkItem' AS target_kind"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_global_uses_literal_traversal_path() {
        let sql = emit_edge(FkEdgeTransformInput {
            relationship_kind: "owns".into(),
            source_id: EdgeId::Column("id".into()),
            source_kind: EdgeKind::Literal("User".into()),
            target_id: EdgeId::Column("project_id".into()),
            target_kind: EdgeKind::Literal("Project".into()),
            filters: vec![EdgeFilter::IsNotNull("project_id".into())],
            namespaced: false,
        });

        assert!(
            sql.contains("'0/' AS traversal_path"),
            "global edges should use literal '0/': {sql}"
        );
    }

    #[test]
    fn fk_edge_namespaced_uses_column_traversal_path() {
        let sql = emit_edge(FkEdgeTransformInput {
            relationship_kind: "owns".into(),
            source_id: EdgeId::Column("id".into()),
            source_kind: EdgeKind::Literal("Group".into()),
            target_id: EdgeId::Column("owner_id".into()),
            target_kind: EdgeKind::Literal("User".into()),
            filters: vec![EdgeFilter::IsNotNull("owner_id".into())],
            namespaced: true,
        });

        assert!(!sql.contains("'0/'"), "sql: {sql}");
        assert!(sql.contains("traversal_path"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_not_empty_filter() {
        let sql = emit_edge(FkEdgeTransformInput {
            relationship_kind: "test".into(),
            source_id: EdgeId::Column("id".into()),
            source_kind: EdgeKind::Literal("A".into()),
            target_id: EdgeId::Column("fk".into()),
            target_kind: EdgeKind::Literal("B".into()),
            filters: vec![
                EdgeFilter::IsNotNull("fk".into()),
                EdgeFilter::NotEmpty("fk".into()),
            ],
            namespaced: false,
        });

        assert!(sql.contains("IS NOT NULL"), "sql: {sql}");
        assert!(sql.contains("!="), "sql: {sql}");
    }

    #[test]
    fn fk_edge_type_in_filter() {
        let sql = emit_edge(FkEdgeTransformInput {
            relationship_kind: "test".into(),
            source_id: EdgeId::Column("id".into()),
            source_kind: EdgeKind::Column("source_type".into()),
            target_id: EdgeId::Column("fk".into()),
            target_kind: EdgeKind::Literal("B".into()),
            filters: vec![
                EdgeFilter::IsNotNull("fk".into()),
                EdgeFilter::TypeIn {
                    column: "source_type".into(),
                    types: vec!["Issue".into(), "MergeRequest".into()],
                },
            ],
            namespaced: false,
        });

        assert!(sql.contains("IN ("), "sql: {sql}");
    }
}
