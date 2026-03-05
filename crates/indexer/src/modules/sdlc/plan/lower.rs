use ontology::{EtlScope, constants::TRAVERSAL_PATH_COLUMN};

use super::ast::{Expr, Op, Query, SelectExpr, TableRef};
use super::input::{
    EdgeFilter, EdgeId, EdgeKind, EdgePlan, ExtractColumn, ExtractPlan, ExtractSource, NodeColumn,
    NodePlan, PlanInput,
};
use super::{ExtractQuery, PipelinePlan, Plans, Transformation};

const SOURCE_DATA_TABLE: &str = "source_data";
const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";

pub(in crate::modules::sdlc) fn lower(inputs: PlanInput, batch_size: u64) -> Plans {
    let mut global = Vec::new();
    let mut namespaced = Vec::new();

    let mut push = |plan: PipelinePlan, scope: EtlScope| match scope {
        EtlScope::Global => global.push(plan),
        EtlScope::Namespaced => namespaced.push(plan),
    };

    for node in inputs.node_plans {
        let scope = node.scope;
        push(lower_node_plan(node, batch_size), scope);
    }

    for edge in inputs.edge_plans {
        let scope = edge.scope;
        push(lower_edge_plan(edge, batch_size), scope);
    }

    Plans { global, namespaced }
}

fn lower_node_plan(input: NodePlan, batch_size: u64) -> PipelinePlan {
    let destination_table = input.extract.destination_table.clone();
    let extract_query = lower_extract_plan(input.extract, batch_size);

    PipelinePlan {
        name: input.name,
        extract_query,
        transforms: vec![Transformation {
            query: lower_node_transform(&input.columns),
            destination_table,
        }],
    }
}

fn lower_node_transform(columns: &[NodeColumn]) -> Query {
    let mut select: Vec<SelectExpr> = columns.iter().map(lower_node_column).collect();
    select.push(SelectExpr::bare(Expr::col("", VERSION_ALIAS)));
    select.push(SelectExpr::bare(Expr::col("", DELETED_ALIAS)));

    Query {
        select,
        from: TableRef::scan(SOURCE_DATA_TABLE, None),
        where_clause: None,
        order_by: vec![],
        limit: None,
    }
}

fn lower_node_column(column: &NodeColumn) -> SelectExpr {
    match column {
        NodeColumn::Identity(name) => SelectExpr::bare(Expr::col("", name)),
        NodeColumn::Rename { source, target } => {
            SelectExpr::new(Expr::col("", source), target.clone())
        }
        NodeColumn::IntEnum {
            source,
            target,
            values,
        } => {
            let cases: Vec<String> = values
                .iter()
                .map(|(key, value)| format!("WHEN {source} = {key} THEN '{value}'"))
                .collect();
            SelectExpr::new(
                Expr::raw(format!("CASE {} ELSE 'unknown' END", cases.join(" "))),
                target.clone(),
            )
        }
    }
}

fn lower_edge_plan(input: EdgePlan, batch_size: u64) -> PipelinePlan {
    let destination_table = input.extract.destination_table.clone();
    let extract_query = lower_extract_plan(input.extract, batch_size);

    let transform_query = Query {
        select: lower_edge_select(
            lower_edge_id(&input.source_id),
            lower_edge_kind(&input.source_kind),
            &input.relationship_kind,
            lower_edge_id(&input.target_id),
            lower_edge_kind(&input.target_kind),
            input.namespaced,
        ),
        from: TableRef::scan(SOURCE_DATA_TABLE, None),
        where_clause: lower_filters(&input.filters),
        order_by: vec![],
        limit: None,
    };

    PipelinePlan {
        name: input.relationship_kind,
        extract_query,
        transforms: vec![Transformation {
            query: transform_query,
            destination_table,
        }],
    }
}

fn lower_edge_id(id: &EdgeId) -> Expr {
    match id {
        EdgeId::Column(column) => Expr::col("", column),
        EdgeId::Exploded { column, delimiter } => Expr::cast(
            Expr::func(
                "NULLIF",
                vec![
                    Expr::func(
                        "unnest",
                        vec![Expr::func(
                            "string_to_array",
                            vec![Expr::col("", column), Expr::raw(format!("'{delimiter}'"))],
                        )],
                    ),
                    Expr::raw("''"),
                ],
            ),
            "BIGINT",
        ),
    }
}

fn lower_edge_kind(kind: &EdgeKind) -> Expr {
    match kind {
        EdgeKind::Literal(value) => Expr::raw(format!("'{value}'")),
        EdgeKind::Column(column) => Expr::col("", column),
        EdgeKind::TypeMapping { column, mapping } => {
            let cases: Vec<String> = mapping
                .iter()
                .map(|(from, to)| format!("WHEN {column} = '{from}' THEN '{to}'"))
                .collect();
            Expr::raw(format!("CASE {} ELSE {column} END", cases.join(" ")))
        }
    }
}

fn lower_filter(filter: &EdgeFilter) -> Expr {
    match filter {
        EdgeFilter::IsNotNull(column) => Expr::is_not_null(Expr::col("", column)),
        EdgeFilter::NotEmpty(column) => {
            Expr::binary(Op::Ne, Expr::col("", column), Expr::raw("''"))
        }
        EdgeFilter::TypeIn { column, types } => {
            let types_list = types
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ");
            Expr::raw(format!("{column} IN ({types_list})"))
        }
    }
}

fn lower_filters(filters: &[EdgeFilter]) -> Option<Expr> {
    Expr::and_all(filters.iter().map(|f| Some(lower_filter(f))))
}

fn lower_edge_select(
    source_id: Expr,
    source_kind: Expr,
    relationship_kind: &str,
    target_id: Expr,
    target_kind: Expr,
    namespaced: bool,
) -> Vec<SelectExpr> {
    let traversal_path = if namespaced {
        SelectExpr::bare(Expr::col("", "traversal_path"))
    } else {
        SelectExpr::new(Expr::raw("'0/'"), "traversal_path")
    };

    vec![
        traversal_path,
        SelectExpr::new(source_id, "source_id"),
        SelectExpr::new(source_kind, "source_kind"),
        SelectExpr::new(
            Expr::raw(format!("'{relationship_kind}'")),
            "relationship_kind",
        ),
        SelectExpr::new(target_id, "target_id"),
        SelectExpr::new(target_kind, "target_kind"),
        SelectExpr::bare(Expr::col("", VERSION_ALIAS)),
        SelectExpr::bare(Expr::col("", DELETED_ALIAS)),
    ]
}

fn lower_extract_plan(input: ExtractPlan, batch_size: u64) -> ExtractQuery {
    let mut select: Vec<SelectExpr> = input.columns.iter().map(lower_extract_column).collect();

    select.push(SelectExpr::new(
        Expr::raw(input.watermark.clone()),
        VERSION_ALIAS,
    ));
    select.push(SelectExpr::new(
        Expr::raw(input.deleted.clone()),
        DELETED_ALIAS,
    ));

    let from = match &input.source {
        ExtractSource::Table(table) => TableRef::scan(table.clone(), None),
        ExtractSource::Raw(raw) => TableRef::Raw(raw.clone()),
    };

    let traversal_filter =
        lower_traversal_filter(input.namespaced, input.traversal_path_filter.as_deref());

    let where_clause = Expr::and_all([
        Some(watermark_where(&input.watermark)),
        traversal_filter,
        input.additional_where.map(Expr::raw),
    ]);

    let base_query = Query {
        select,
        from,
        where_clause,
        order_by: vec![],
        limit: None,
    };

    ExtractQuery::new(base_query, input.order_by, batch_size)
}

/// If namespaced and no custom filter, use the default `startsWith`.
/// If a custom filter is provided, use it as-is (it replaces the default).
fn lower_traversal_filter(namespaced: bool, custom_filter: Option<&str>) -> Option<Expr> {
    if !namespaced {
        return None;
    }

    match custom_filter {
        Some(filter) => Some(Expr::raw(filter.to_string())),
        None => Some(Expr::func(
            "startsWith",
            vec![
                Expr::col("", TRAVERSAL_PATH_COLUMN),
                Expr::param("traversal_path", "String"),
            ],
        )),
    }
}

fn lower_extract_column(column: &ExtractColumn) -> SelectExpr {
    match column {
        ExtractColumn::Bare(name) => SelectExpr::bare(Expr::raw(name.clone())),
        ExtractColumn::ToString(name) => SelectExpr::new(
            Expr::func("toString", vec![Expr::col("", name)]),
            name.clone(),
        ),
    }
}

fn watermark_where(watermark: &str) -> Expr {
    Expr::and_all([
        Some(Expr::binary(
            Op::Gt,
            Expr::raw(watermark.to_string()),
            Expr::param("last_watermark", "String"),
        )),
        Some(Expr::binary(
            Op::Le,
            Expr::raw(watermark.to_string()),
            Expr::param("watermark", "String"),
        )),
    ])
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::super::input;
    use super::*;
    use std::collections::BTreeMap;

    fn emit(query: &Query) -> String {
        super::super::codegen::emit_sql(query)
    }

    fn build_plans(ontology: &ontology::Ontology, batch_size: u64) -> Plans {
        lower(input::from_ontology(ontology), batch_size)
    }

    #[test]
    fn build_plans_partitions_by_scope() {
        let ontology = ontology::Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1_000_000);

        let global_names: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        let namespaced_names: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();

        assert!(global_names.contains(&"User"), "User should be global");
        assert!(
            namespaced_names.contains(&"Group"),
            "Group should be namespaced"
        );
        assert!(
            namespaced_names.contains(&"Project"),
            "Project should be namespaced"
        );
    }

    #[test]
    fn node_plan_has_single_transform() {
        let ontology = ontology::Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1_000_000);

        let note_plan = plans.namespaced.iter().find(|p| p.name == "Note").unwrap();
        assert_eq!(note_plan.transforms.len(), 1);
        assert_eq!(note_plan.transforms[0].destination_table, "gl_note");
    }

    #[test]
    fn fk_edges_produce_separate_edge_plans() {
        let ontology = ontology::Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1_000_000);

        let all_plans: Vec<_> = plans.global.iter().chain(plans.namespaced.iter()).collect();

        let edge_table = ontology.edge_table().to_string();
        let edge_plans: Vec<_> = all_plans
            .iter()
            .filter(|p| {
                p.transforms
                    .iter()
                    .any(|t| t.destination_table == edge_table)
            })
            .collect();

        assert!(
            !edge_plans.is_empty(),
            "should have edge plans writing to {edge_table}"
        );
    }

    #[test]
    fn node_transform_sql_handles_column_renaming() {
        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::Identity("name".to_string()),
            NodeColumn::Rename {
                source: "admin".to_string(),
                target: "is_admin".to_string(),
            },
        ];

        assert!(emit(&lower_node_transform(&columns)).contains("admin AS is_admin"));
    }

    #[test]
    fn node_transform_sql_handles_int_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "blocked".to_string());

        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::Identity("name".to_string()),
            NodeColumn::IntEnum {
                source: "state".to_string(),
                target: "state".to_string(),
                values,
            },
        ];

        let sql = emit(&lower_node_transform(&columns));
        assert!(sql.contains("CASE"));
        assert!(sql.contains("WHEN state = 0 THEN 'active'"));
        assert!(sql.contains("WHEN state = 1 THEN 'blocked'"));
        assert!(sql.contains("ELSE 'unknown' END AS state"));
    }

    #[test]
    fn edge_sql_outgoing_literal() {
        let edge = EdgePlan {
            relationship_kind: "owns".to_string(),
            scope: EtlScope::Namespaced,
            extract: ExtractPlan {
                destination_table: "gl_edges".to_string(),
                columns: vec![],
                source: ExtractSource::Table("siphon_groups".to_string()),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                order_by: vec!["id".to_string()],
                namespaced: false,
                traversal_path_filter: None,
                additional_where: None,
            },
            source_id: EdgeId::Column("id".to_string()),
            source_kind: EdgeKind::Literal("Group".to_string()),
            target_id: EdgeId::Column("owner_id".to_string()),
            target_kind: EdgeKind::Literal("User".to_string()),
            filters: vec![EdgeFilter::IsNotNull("owner_id".to_string())],
            namespaced: true,
        };

        let plan = lower_edge_plan(edge, 1000);
        let sql = emit(&plan.transforms[0].query);

        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
    }

    #[test]
    fn edge_sql_incoming_literal() {
        let edge = EdgePlan {
            relationship_kind: "authored".to_string(),
            scope: EtlScope::Namespaced,
            extract: ExtractPlan {
                destination_table: "gl_edges".to_string(),
                columns: vec![],
                source: ExtractSource::Table("siphon_notes".to_string()),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                order_by: vec!["id".to_string()],
                namespaced: false,
                traversal_path_filter: None,
                additional_where: None,
            },
            source_id: EdgeId::Column("author_id".to_string()),
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("Note".to_string()),
            filters: vec![EdgeFilter::IsNotNull("author_id".to_string())],
            namespaced: true,
        };

        let plan = lower_edge_plan(edge, 1000);
        let sql = emit(&plan.transforms[0].query);

        assert!(sql.contains("author_id AS source_id"));
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'Note' AS target_kind"));
    }

    #[test]
    fn edge_sql_multi_value_incoming() {
        let edge = EdgePlan {
            relationship_kind: "assigned".to_string(),
            scope: EtlScope::Namespaced,
            extract: ExtractPlan {
                destination_table: "gl_edges".to_string(),
                columns: vec![],
                source: ExtractSource::Table("siphon_work_items".to_string()),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                order_by: vec!["id".to_string()],
                namespaced: false,
                traversal_path_filter: None,
                additional_where: None,
            },
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

        let plan = lower_edge_plan(edge, 1000);
        let sql = emit(&plan.transforms[0].query);

        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS BIGINT)"),
            "sql: {sql}"
        );
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'WorkItem' AS target_kind"));
    }

    #[test]
    fn extract_query_table_etl_includes_all_columns() {
        let extract = ExtractPlan {
            destination_table: "gl_user".to_string(),
            columns: vec![
                ExtractColumn::Bare("id".to_string()),
                ExtractColumn::Bare("name".to_string()),
            ],
            source: ExtractSource::Table("siphon_user".to_string()),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
        };

        let sql = lower_extract_plan(extract, 1000).to_sql();

        assert!(sql.contains("SELECT id, name,"), "sql: {sql}");
        assert!(
            sql.contains("_siphon_replicated_at AS _version"),
            "sql: {sql}"
        );
        assert!(sql.contains("_siphon_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("FROM siphon_user"), "sql: {sql}");
        assert!(sql.contains("ORDER BY id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000"), "sql: {sql}");
    }

    #[test]
    fn extract_query_query_etl_uses_structured_fields() {
        let extract = ExtractPlan {
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

        let sql = lower_extract_plan(extract, 500).to_sql();

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
        assert!(sql.contains("INNER JOIN"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
        assert!(sql.contains("ORDER BY traversal_path, id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 500"), "sql: {sql}");
    }
}
