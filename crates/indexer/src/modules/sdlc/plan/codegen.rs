//! ClickHouse SQL codegen for SDLC extract and transform queries via llqm.

use llqm::codegen::emit_clickhouse_sql;
use llqm::expr::*;
use llqm::plan::PlanBuilder;

use super::input::{
    EdgeFilter, EdgeId, EdgeKind, ExtractColumn, ExtractPlan, ExtractSource, FkEdgeTransform,
    NodeColumn,
};
use super::SOURCE_DATA_TABLE;

const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";

/// Generate extract-query SQL from an `ExtractPlan` using llqm.
pub(in crate::modules::sdlc) fn emit_extract_sql(
    input: &ExtractPlan,
    batch_size: u64,
    sort_key_columns: &[String],
    cursor_values: &[String],
) -> String {
    let mut b = PlanBuilder::new();

    // -- Schema columns (only used for PlanBuilder bookkeeping) ---------------
    let schema_cols: Vec<(&str, DataType)> = input
        .columns
        .iter()
        .map(|c| (extract_column_name(c), DataType::String))
        .chain([
            ("_version", DataType::String),
            ("_deleted", DataType::String),
        ])
        .collect();

    // -- FROM ------------------------------------------------------------------
    let rel = match &input.source {
        ExtractSource::Table(table) => b.read(table, table, &schema_cols),
        ExtractSource::Raw(raw_from) => b.read_raw(raw_from, &schema_cols),
    };

    // -- WHERE -----------------------------------------------------------------
    let watermark_filter = and([
        gt(
            raw(&input.watermark),
            param("last_watermark", DataType::String),
        ),
        le(raw(&input.watermark), param("watermark", DataType::String)),
    ]);

    let traversal_filter = build_traversal_filter(input);
    let additional_where = input.additional_where.as_deref().map(raw);
    let cursor_expr = build_cursor_expr(sort_key_columns, cursor_values);

    let where_expr = and_opt([
        Some(watermark_filter),
        traversal_filter,
        additional_where,
        cursor_expr,
    ])
    .expect("at least watermark filter is always present");

    let rel = b.filter(rel, where_expr);

    // -- ORDER BY --------------------------------------------------------------
    let sort_keys: Vec<(Expr, SortDir)> = sort_key_columns
        .iter()
        .map(|c| (raw(c.as_str()), SortDir::Asc))
        .collect();
    let rel = b.sort(rel, &sort_keys);

    // -- SELECT ----------------------------------------------------------------
    let select_items: Vec<(Expr, &str)> = input
        .columns
        .iter()
        .map(select_item)
        .chain([
            (raw(&input.watermark), VERSION_ALIAS),
            (raw(&input.deleted), DELETED_ALIAS),
        ])
        .collect();
    let rel = b.project(rel, &select_items);

    // -- LIMIT -----------------------------------------------------------------
    let rel = b.fetch(rel, batch_size, None);

    // -- Build & emit ----------------------------------------------------------
    let plan = b.build(rel);
    emit_clickhouse_sql(&plan)
        .expect("codegen should not fail for extract queries")
        .sql
}

/// Map an `ExtractColumn` to a `(Expr, alias)` pair for llqm's project.
fn select_item(col: &ExtractColumn) -> (Expr, &str) {
    match col {
        ExtractColumn::Bare(name) => {
            if let Some((expr_part, alias_part)) = name.split_once(" AS ") {
                (raw(expr_part), alias_part)
            } else {
                (raw(name.as_str()), extract_column_name(col))
            }
        }
        ExtractColumn::ToString(name) => {
            (func("toString", vec![raw(name.as_str())]), name.as_str())
        }
    }
}

/// Return the output name of an extract column (for schema bookkeeping).
fn extract_column_name(col: &ExtractColumn) -> &str {
    match col {
        ExtractColumn::Bare(name) => {
            if let Some((_expr, alias)) = name.split_once(" AS ") {
                alias
            } else if let Some((_table, col_name)) = name.rsplit_once('.') {
                col_name
            } else {
                name.as_str()
            }
        }
        ExtractColumn::ToString(name) => name.as_str(),
    }
}

fn build_traversal_filter(input: &ExtractPlan) -> Option<Expr> {
    if !input.namespaced {
        return None;
    }
    match &input.traversal_path_filter {
        Some(filter) => Some(raw(filter.as_str())),
        None => Some(starts_with(
            raw("traversal_path"),
            param("traversal_path", DataType::String),
        )),
    }
}

/// Build DNF cursor pagination expression.
///
/// For keys `[c1, c2]` with values `[v1, v2]`:
/// `(c1 > 'v1') OR ((c1 = 'v1') AND (c2 > 'v2'))`
fn build_cursor_expr(sort_key_columns: &[String], cursor_values: &[String]) -> Option<Expr> {
    if cursor_values.is_empty() {
        return None;
    }

    or_opt((0..sort_key_columns.len()).map(|depth| {
        let mut conjuncts: Vec<Option<Expr>> = Vec::with_capacity(depth + 1);
        for prefix in 0..depth {
            conjuncts.push(Some(eq(
                raw(&sort_key_columns[prefix]),
                raw(&format!("'{}'", cursor_values[prefix])),
            )));
        }
        conjuncts.push(Some(gt(
            raw(&sort_key_columns[depth]),
            raw(&format!("'{}'", cursor_values[depth])),
        )));
        and_opt(conjuncts)
    }))
}

// ---------------------------------------------------------------------------
// Transform queries (Phase 2)
// ---------------------------------------------------------------------------

/// Generate a node transform query.
///
/// Reads from `source_data` (DataFusion in-memory table) and projects
/// node columns with optional renames and CASE expressions.
pub(in crate::modules::sdlc) fn emit_node_transform_sql(columns: &[NodeColumn]) -> String {
    let mut b = PlanBuilder::new();

    let schema_cols: Vec<(&str, DataType)> = columns
        .iter()
        .map(|c| (node_column_name(c), DataType::String))
        .chain([
            (VERSION_ALIAS, DataType::String),
            (DELETED_ALIAS, DataType::String),
        ])
        .collect();

    let rel = b.read(SOURCE_DATA_TABLE, SOURCE_DATA_TABLE, &schema_cols);

    let select_items: Vec<(Expr, &str)> = columns
        .iter()
        .map(node_column_item)
        .chain([
            (raw(VERSION_ALIAS), VERSION_ALIAS),
            (raw(DELETED_ALIAS), DELETED_ALIAS),
        ])
        .collect();
    let rel = b.project(rel, &select_items);

    let plan = b.build(rel);
    emit_clickhouse_sql(&plan)
        .expect("codegen should not fail for node transforms")
        .sql
}

/// Generate an FK edge transform query.
///
/// Reads from `source_data` and projects the fixed edge schema with
/// optional WHERE filters.
pub(in crate::modules::sdlc) fn emit_fk_edge_transform_sql(fk_edge: &FkEdgeTransform) -> String {
    emit_edge_transform_sql(
        &fk_edge.source_id,
        &fk_edge.source_kind,
        &fk_edge.relationship_kind,
        &fk_edge.target_id,
        &fk_edge.target_kind,
        &fk_edge.filters,
        fk_edge.namespaced,
    )
}

/// Generate an edge transform query (shared by FK and standalone edges).
///
/// Produces: `SELECT edge_columns FROM source_data [WHERE filters]`
pub(in crate::modules::sdlc) fn emit_edge_transform_sql(
    source_id: &EdgeId,
    source_kind: &EdgeKind,
    relationship_kind: &str,
    target_id: &EdgeId,
    target_kind: &EdgeKind,
    filters: &[EdgeFilter],
    namespaced: bool,
) -> String {
    let mut b = PlanBuilder::new();

    // Dummy schema — all expressions use raw(), so no column resolution needed.
    let schema_cols = [("_d", DataType::String)];
    let rel = b.read(SOURCE_DATA_TABLE, SOURCE_DATA_TABLE, &schema_cols);

    // WHERE
    let filter_expr = and_opt(filters.iter().map(|f| Some(edge_filter_expr(f))));
    let rel = if let Some(expr) = filter_expr {
        b.filter(rel, expr)
    } else {
        rel
    };

    // SELECT — fixed edge schema
    let traversal_path_item: (Expr, &str) = if namespaced {
        (raw("traversal_path"), "traversal_path")
    } else {
        (raw("'0/'"), "traversal_path")
    };

    let select_items: Vec<(Expr, &str)> = vec![
        traversal_path_item,
        (edge_id_expr(source_id), "source_id"),
        (edge_kind_expr(source_kind), "source_kind"),
        (raw(&format!("'{relationship_kind}'")), "relationship_kind"),
        (edge_id_expr(target_id), "target_id"),
        (edge_kind_expr(target_kind), "target_kind"),
        (raw(VERSION_ALIAS), VERSION_ALIAS),
        (raw(DELETED_ALIAS), DELETED_ALIAS),
    ];
    let rel = b.project(rel, &select_items);

    let plan = b.build(rel);
    emit_clickhouse_sql(&plan)
        .expect("codegen should not fail for edge transforms")
        .sql
}

fn node_column_item(col: &NodeColumn) -> (Expr, &str) {
    match col {
        NodeColumn::Identity(name) => (raw(name), name.as_str()),
        NodeColumn::Rename { source, target } => (raw(source), target.as_str()),
        NodeColumn::IntEnum {
            source,
            target,
            values,
        } => {
            let cases: Vec<String> = values
                .iter()
                .map(|(key, value)| format!("WHEN {source} = {key} THEN '{value}'"))
                .collect();
            (
                raw(&format!("CASE {} ELSE 'unknown' END", cases.join(" "))),
                target.as_str(),
            )
        }
    }
}

fn node_column_name(col: &NodeColumn) -> &str {
    match col {
        NodeColumn::Identity(name) => name.as_str(),
        NodeColumn::Rename { target, .. } | NodeColumn::IntEnum { target, .. } => target.as_str(),
    }
}

fn edge_id_expr(id: &EdgeId) -> Expr {
    match id {
        EdgeId::Column(column) => raw(column),
        EdgeId::Exploded { column, delimiter } => raw(&format!(
            "CAST(NULLIF(unnest(string_to_array({column}, '{delimiter}')), '') AS BIGINT)"
        )),
    }
}

fn edge_kind_expr(kind: &EdgeKind) -> Expr {
    match kind {
        EdgeKind::Literal(value) => raw(&format!("'{value}'")),
        EdgeKind::Column(column) => raw(column),
        EdgeKind::TypeMapping { column, mapping } => {
            let cases: Vec<String> = mapping
                .iter()
                .map(|(from, to)| format!("WHEN {column} = '{from}' THEN '{to}'"))
                .collect();
            raw(&format!("CASE {} ELSE {column} END", cases.join(" ")))
        }
    }
}

fn edge_filter_expr(filter: &EdgeFilter) -> Expr {
    match filter {
        EdgeFilter::IsNotNull(column) => is_not_null(raw(column)),
        EdgeFilter::NotEmpty(column) => ne(raw(column), raw("''")),
        EdgeFilter::TypeIn { column, types } => {
            let types_list = types
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ");
            raw(&format!("{column} IN ({types_list})"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::input::ExtractSource;
    use super::*;

    fn simple_extract() -> ExtractPlan {
        ExtractPlan {
            destination_table: "gl_user".to_string(),
            columns: vec![
                ExtractColumn::Bare("id".to_string()),
                ExtractColumn::Bare("username".to_string()),
            ],
            source: ExtractSource::Table("siphon_users".to_string()),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
        }
    }

    #[test]
    fn first_page_table_etl() {
        let input = simple_extract();
        let sql = emit_extract_sql(&input, 1000, &input.order_by.clone(), &[]);

        assert!(
            sql.contains("SELECT id AS id, username AS username,"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("_siphon_replicated_at AS _version"),
            "sql: {sql}"
        );
        assert!(sql.contains("_siphon_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("FROM siphon_users"), "sql: {sql}");
        assert!(
            sql.contains("(_siphon_replicated_at > {last_watermark:String})"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("(_siphon_replicated_at <= {watermark:String})"),
            "sql: {sql}"
        );
        assert!(sql.contains("ORDER BY id ASC"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000"), "sql: {sql}");
        assert!(!sql.contains("(id >"), "no cursor on first page: {sql}");
    }

    #[test]
    fn cursor_pagination_single_key() {
        let input = simple_extract();
        let sort_keys = vec!["id".to_string()];
        let cursor = vec!["42".to_string()];
        let sql = emit_extract_sql(&input, 1000, &sort_keys, &cursor);

        assert!(sql.contains("(id > '42')"), "sql: {sql}");
    }

    #[test]
    fn cursor_pagination_composite_key() {
        let input = ExtractPlan {
            namespaced: true,
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
            ..simple_extract()
        };
        let sort_keys = input.order_by.clone();
        let cursor = vec!["1/2/".to_string(), "42".to_string()];
        let sql = emit_extract_sql(&input, 1000, &sort_keys, &cursor);

        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(
            sql.contains("(traversal_path = '1/2/')") && sql.contains("(id > '42')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn namespaced_default_traversal_filter() {
        let input = ExtractPlan {
            namespaced: true,
            ..simple_extract()
        };
        let sql = emit_extract_sql(&input, 1000, &input.order_by.clone(), &[]);

        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
    }

    #[test]
    fn namespaced_custom_traversal_filter() {
        let input = ExtractPlan {
            namespaced: true,
            traversal_path_filter: Some(
                "id IN (SELECT id FROM tp WHERE startsWith(traversal_path, {traversal_path:String}))"
                    .to_string(),
            ),
            ..simple_extract()
        };
        let sql = emit_extract_sql(&input, 500, &input.order_by.clone(), &[]);

        assert!(
            sql.contains(
                "id IN (SELECT id FROM tp WHERE startsWith(traversal_path, {traversal_path:String}))"
            ),
            "sql: {sql}"
        );
    }

    #[test]
    fn additional_where_clause() {
        let input = ExtractPlan {
            additional_where: Some("system = false".to_string()),
            ..simple_extract()
        };
        let sql = emit_extract_sql(&input, 1000, &input.order_by.clone(), &[]);

        assert!(sql.contains("system = false"), "sql: {sql}");
    }

    #[test]
    fn query_etl_raw_from() {
        let input = ExtractPlan {
            destination_table: "gl_project".to_string(),
            columns: vec![
                ExtractColumn::Bare("project.id AS id".to_string()),
                ExtractColumn::Bare(
                    "traversal_paths.traversal_path AS traversal_path".to_string(),
                ),
            ],
            source: ExtractSource::Raw(
                "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id"
                    .to_string(),
            ),
            watermark: "project._siphon_replicated_at".to_string(),
            deleted: "project._siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
            namespaced: true,
            traversal_path_filter: Some(
                "startsWith(traversal_path, {traversal_path:String})".to_string(),
            ),
            additional_where: None,
        };
        let sql = emit_extract_sql(&input, 500, &input.order_by.clone(), &[]);

        assert!(sql.contains("project.id AS id"), "sql: {sql}");
        assert!(
            sql.contains("traversal_paths.traversal_path AS traversal_path"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("project._siphon_replicated_at AS _version"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("project._siphon_deleted AS _deleted"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("ORDER BY traversal_path ASC, id ASC"),
            "sql: {sql}"
        );
        assert!(sql.contains("LIMIT 500"), "sql: {sql}");
    }

    #[test]
    fn tostring_column() {
        let input = ExtractPlan {
            columns: vec![
                ExtractColumn::Bare("id".to_string()),
                ExtractColumn::ToString("uuid".to_string()),
            ],
            ..simple_extract()
        };
        let sql = emit_extract_sql(&input, 1000, &input.order_by.clone(), &[]);

        assert!(sql.contains("toString(uuid) AS uuid"), "sql: {sql}");
    }

    #[test]
    fn ontology_driven_plans_produce_valid_sql() {
        let ontology = ontology::Ontology::load_embedded().expect("should load ontology");
        let plan_input = super::super::input::from_ontology(&ontology);

        for node_plan in &plan_input.node_plans {
            let extract = &node_plan.extract;
            let sql = emit_extract_sql(extract, 1000, &extract.order_by, &[]);

            assert!(
                sql.contains("SELECT "),
                "{}: missing SELECT: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("FROM "),
                "{}: missing FROM: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("WHERE "),
                "{}: missing WHERE: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("AS _version"),
                "{}: missing _version: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("AS _deleted"),
                "{}: missing _deleted: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("LIMIT 1000"),
                "{}: missing LIMIT: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("{last_watermark:String}"),
                "{}: missing last_watermark param: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("{watermark:String}"),
                "{}: missing watermark param: {sql}",
                node_plan.name
            );
        }

        for edge_plan in &plan_input.standalone_edge_plans {
            let extract = &edge_plan.extract;
            let sql = emit_extract_sql(extract, 1000, &extract.order_by, &[]);

            assert!(
                sql.contains("SELECT "),
                "{}: missing SELECT: {sql}",
                edge_plan.relationship_kind
            );
            assert!(
                sql.contains("LIMIT 1000"),
                "{}: missing LIMIT: {sql}",
                edge_plan.relationship_kind
            );
        }
    }

    // -----------------------------------------------------------------------
    // Transform tests (Phase 2)
    // -----------------------------------------------------------------------

    #[test]
    fn node_transform_identity_columns() {
        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::Identity("name".to_string()),
        ];
        let sql = emit_node_transform_sql(&columns);

        assert!(sql.contains("SELECT id AS id, name AS name"), "sql: {sql}");
        assert!(sql.contains("_version AS _version"), "sql: {sql}");
        assert!(sql.contains("_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("FROM source_data"), "sql: {sql}");
        assert!(
            !sql.contains("WHERE"),
            "no WHERE for node transforms: {sql}"
        );
    }

    #[test]
    fn node_transform_column_rename() {
        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::Rename {
                source: "admin".to_string(),
                target: "is_admin".to_string(),
            },
        ];
        let sql = emit_node_transform_sql(&columns);

        assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
    }

    #[test]
    fn node_transform_int_enum() {
        use std::collections::BTreeMap;
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "blocked".to_string());

        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::IntEnum {
                source: "state".to_string(),
                target: "state".to_string(),
                values,
            },
        ];
        let sql = emit_node_transform_sql(&columns);

        assert!(sql.contains("CASE"), "sql: {sql}");
        assert!(sql.contains("WHEN state = 0 THEN 'active'"), "sql: {sql}");
        assert!(sql.contains("WHEN state = 1 THEN 'blocked'"), "sql: {sql}");
        assert!(sql.contains("ELSE 'unknown' END AS state"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_transform_outgoing_literal() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "owns".to_string(),
            source_id: EdgeId::Column("id".to_string()),
            source_kind: EdgeKind::Literal("Group".to_string()),
            target_id: EdgeId::Column("owner_id".to_string()),
            target_kind: EdgeKind::Literal("User".to_string()),
            filters: vec![EdgeFilter::IsNotNull("owner_id".to_string())],
            namespaced: true,
        };
        let sql = emit_fk_edge_transform_sql(&fk_edge);

        assert!(sql.contains("id AS source_id"), "sql: {sql}");
        assert!(sql.contains("'Group' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("'owns' AS relationship_kind"), "sql: {sql}");
        assert!(sql.contains("owner_id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'User' AS target_kind"), "sql: {sql}");
        assert!(
            sql.contains("traversal_path AS traversal_path"),
            "sql: {sql}"
        );
        assert!(sql.contains("_version AS _version"), "sql: {sql}");
        assert!(sql.contains("_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("(owner_id IS NOT NULL)"), "sql: {sql}");
        assert!(sql.contains("FROM source_data"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_transform_incoming_literal() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "authored".to_string(),
            source_id: EdgeId::Column("author_id".to_string()),
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("Note".to_string()),
            filters: vec![EdgeFilter::IsNotNull("author_id".to_string())],
            namespaced: true,
        };
        let sql = emit_fk_edge_transform_sql(&fk_edge);

        assert!(sql.contains("author_id AS source_id"), "sql: {sql}");
        assert!(sql.contains("'User' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'Note' AS target_kind"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_transform_multi_value_exploded() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "assigned".to_string(),
            source_id: EdgeId::Exploded {
                column: "assignee_ids".to_string(),
                delimiter: "/".to_string(),
            },
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("WorkItem".to_string()),
            filters: vec![
                EdgeFilter::IsNotNull("assignee_ids".to_string()),
                EdgeFilter::NotEmpty("assignee_ids".to_string()),
            ],
            namespaced: true,
        };
        let sql = emit_fk_edge_transform_sql(&fk_edge);

        assert!(
            sql.contains(
                "CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS BIGINT) AS source_id"
            ),
            "sql: {sql}"
        );
        assert!(sql.contains("'User' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'WorkItem' AS target_kind"), "sql: {sql}");
        assert!(sql.contains("(assignee_ids IS NOT NULL)"), "sql: {sql}");
        assert!(sql.contains("(assignee_ids != '')"), "sql: {sql}");
    }

    #[test]
    fn edge_transform_global_traversal_path() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "owns".to_string(),
            source_id: EdgeId::Column("id".to_string()),
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("key_id".to_string()),
            target_kind: EdgeKind::Literal("Key".to_string()),
            filters: vec![EdgeFilter::IsNotNull("key_id".to_string())],
            namespaced: false,
        };
        let sql = emit_fk_edge_transform_sql(&fk_edge);

        assert!(sql.contains("'0/' AS traversal_path"), "sql: {sql}");
    }

    #[test]
    fn edge_transform_type_in_filter() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "targets".to_string(),
            source_id: EdgeId::Column("id".to_string()),
            source_kind: EdgeKind::Literal("Note".to_string()),
            target_id: EdgeId::Column("noteable_id".to_string()),
            target_kind: EdgeKind::Column("noteable_type".to_string()),
            filters: vec![
                EdgeFilter::IsNotNull("noteable_id".to_string()),
                EdgeFilter::TypeIn {
                    column: "noteable_type".to_string(),
                    types: vec!["Issue".to_string(), "MergeRequest".to_string()],
                },
            ],
            namespaced: true,
        };
        let sql = emit_fk_edge_transform_sql(&fk_edge);

        assert!(sql.contains("noteable_type AS target_kind"), "sql: {sql}");
        assert!(
            sql.contains("noteable_type IN ('Issue', 'MergeRequest')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn edge_transform_type_mapping() {
        use std::collections::BTreeMap;
        let mut mapping = BTreeMap::new();
        mapping.insert("Vulnerability".to_string(), "SecurityFinding".to_string());

        let fk_edge = FkEdgeTransform {
            relationship_kind: "references".to_string(),
            source_id: EdgeId::Column("id".to_string()),
            source_kind: EdgeKind::TypeMapping {
                column: "source_type".to_string(),
                mapping,
            },
            target_id: EdgeId::Column("target_id".to_string()),
            target_kind: EdgeKind::Literal("Issue".to_string()),
            filters: vec![EdgeFilter::IsNotNull("target_id".to_string())],
            namespaced: true,
        };
        let sql = emit_fk_edge_transform_sql(&fk_edge);

        assert!(sql.contains("CASE"), "sql: {sql}");
        assert!(
            sql.contains("WHEN source_type = 'Vulnerability' THEN 'SecurityFinding'"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("ELSE source_type END AS source_kind"),
            "sql: {sql}"
        );
    }

    #[test]
    fn edge_transform_no_filters() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "contains".to_string(),
            source_id: EdgeId::Column("group_id".to_string()),
            source_kind: EdgeKind::Literal("Group".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("Project".to_string()),
            filters: vec![],
            namespaced: true,
        };
        let sql = emit_fk_edge_transform_sql(&fk_edge);

        assert!(!sql.contains("WHERE"), "no WHERE when no filters: {sql}");
        assert!(sql.contains("FROM source_data"), "sql: {sql}");
    }

    #[test]
    fn ontology_driven_transforms_produce_valid_sql() {
        let ontology = ontology::Ontology::load_embedded().expect("should load ontology");
        let plan_input = super::super::input::from_ontology(&ontology);

        for node_plan in &plan_input.node_plans {
            // Node transform
            let sql = emit_node_transform_sql(&node_plan.columns);
            assert!(
                sql.contains("SELECT "),
                "{}: missing SELECT in node transform: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("FROM source_data"),
                "{}: missing FROM source_data in node transform: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("AS _version"),
                "{}: missing _version in node transform: {sql}",
                node_plan.name
            );
            assert!(
                sql.contains("AS _deleted"),
                "{}: missing _deleted in node transform: {sql}",
                node_plan.name
            );

            // FK edge transforms
            for fk_edge in &node_plan.edges {
                let sql = emit_fk_edge_transform_sql(fk_edge);
                assert!(
                    sql.contains("SELECT "),
                    "{}/{}: missing SELECT in FK edge transform: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("FROM source_data"),
                    "{}/{}: missing FROM source_data: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS source_id"),
                    "{}/{}: missing source_id: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS source_kind"),
                    "{}/{}: missing source_kind: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS relationship_kind"),
                    "{}/{}: missing relationship_kind: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS target_id"),
                    "{}/{}: missing target_id: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS target_kind"),
                    "{}/{}: missing target_kind: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS traversal_path"),
                    "{}/{}: missing traversal_path: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS _version"),
                    "{}/{}: missing _version: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
                assert!(
                    sql.contains("AS _deleted"),
                    "{}/{}: missing _deleted: {sql}",
                    node_plan.name,
                    fk_edge.relationship_kind
                );
            }
        }

        // Standalone edge transforms
        for edge_plan in &plan_input.standalone_edge_plans {
            let sql = emit_edge_transform_sql(
                &edge_plan.source_id,
                &edge_plan.source_kind,
                &edge_plan.relationship_kind,
                &edge_plan.target_id,
                &edge_plan.target_kind,
                &edge_plan.filters,
                edge_plan.namespaced,
            );
            assert!(
                sql.contains("SELECT "),
                "{}: missing SELECT in standalone edge transform: {sql}",
                edge_plan.relationship_kind
            );
            assert!(
                sql.contains("FROM source_data"),
                "{}: missing FROM source_data: {sql}",
                edge_plan.relationship_kind
            );
            assert!(
                sql.contains("AS source_id"),
                "{}: missing source_id: {sql}",
                edge_plan.relationship_kind
            );
            assert!(
                sql.contains("AS relationship_kind"),
                "{}: missing relationship_kind: {sql}",
                edge_plan.relationship_kind
            );
            assert!(
                sql.contains("AS target_id"),
                "{}: missing target_id: {sql}",
                edge_plan.relationship_kind
            );
        }
    }
}
