use ontology::{EtlScope, constants::TRAVERSAL_PATH_COLUMN};

use super::ast::{Expr, Op, Query, SelectExpr, TableRef};
use super::input::{
    EdgeFilter, EdgeId, EdgeKind, ExtractColumn, ExtractPlan, ExtractSource, FkEdgeTransform,
    NodeColumn, NodePlan, PlanInput, StandaloneEdgePlan,
};
use super::{ExtractQuery, PipelinePlan, Plans, SOURCE_DATA_TABLE, Transformation};
const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";

pub(in crate::modules::sdlc) fn lower(
    inputs: PlanInput,
    global_batch_size: u64,
    namespaced_batch_size: u64,
) -> Plans {
    let mut global = Vec::new();
    let mut namespaced = Vec::new();

    for node in inputs.node_plans {
        let batch_size = match node.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let scope = node.scope;
        let plan = lower_node_plan(node, &inputs.edge_table, batch_size);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    for edge in inputs.standalone_edge_plans {
        let batch_size = match edge.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let scope = edge.scope;
        let plan = lower_standalone_edge_plan(edge, batch_size);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    Plans { global, namespaced }
}

fn lower_node_plan(input: NodePlan, edge_table: &str, batch_size: u64) -> PipelinePlan {
    let node_destination = input.extract.destination_table.clone();
    let extract_query = lower_extract_plan(input.extract, batch_size);

    let mut transforms = vec![Transformation {
        query: lower_node_transform(&input.columns),
        destination_table: node_destination,
    }];

    for fk_edge in &input.edges {
        transforms.push(lower_fk_edge_transform(fk_edge, edge_table));
    }

    PipelinePlan {
        name: input.name,
        extract_query,
        transforms,
    }
}

fn lower_fk_edge_transform(fk_edge: &FkEdgeTransform, edge_table: &str) -> Transformation {
    let transform_query = Query {
        select: lower_edge_select(
            lower_edge_id(&fk_edge.source_id),
            lower_edge_kind(&fk_edge.source_kind),
            &fk_edge.relationship_kind,
            lower_edge_id(&fk_edge.target_id),
            lower_edge_kind(&fk_edge.target_kind),
            fk_edge.namespaced,
        ),
        from: TableRef::scan(SOURCE_DATA_TABLE, None),
        where_clause: lower_filters(&fk_edge.filters),
        order_by: vec![],
        limit: None,
    };

    Transformation {
        query: transform_query,
        destination_table: edge_table.to_string(),
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

fn lower_standalone_edge_plan(input: StandaloneEdgePlan, batch_size: u64) -> PipelinePlan {
    let destination_table = input.extract.destination_table.clone();
    let extract_query = lower_extract_plan(input.extract, batch_size);
    let where_clause = lower_filters(&input.filters);

    let forward_query = Query {
        select: lower_edge_select(
            lower_edge_id(&input.source_id),
            lower_edge_kind(&input.source_kind),
            &input.relationship_kind,
            lower_edge_id(&input.target_id),
            lower_edge_kind(&input.target_kind),
            input.namespaced,
        ),
        from: TableRef::scan(SOURCE_DATA_TABLE, None),
        where_clause: where_clause.clone(),
        order_by: vec![],
        limit: None,
    };

    let mut transforms = vec![Transformation {
        query: forward_query,
        destination_table: destination_table.clone(),
    }];

    if input.bidirectional {
        let reverse_query = Query {
            select: lower_edge_select(
                lower_edge_id(&input.target_id),
                lower_edge_kind(&input.target_kind),
                &input.relationship_kind,
                lower_edge_id(&input.source_id),
                lower_edge_kind(&input.source_kind),
                input.namespaced,
            ),
            from: TableRef::scan(SOURCE_DATA_TABLE, None),
            where_clause,
            order_by: vec![],
            limit: None,
        };
        transforms.push(Transformation {
            query: reverse_query,
            destination_table,
        });
    }

    PipelinePlan {
        name: input.relationship_kind,
        extract_query,
        transforms,
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
        EdgeId::ArrayElement { column, field } => {
            Expr::struct_field(Expr::func("unnest", vec![Expr::col("", column)]), field)
        }
        EdgeId::ArrayUnnest { column } => Expr::func("unnest", vec![Expr::col("", column)]),
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
        EdgeFilter::ArrayNotEmpty(column) => Expr::binary(
            Op::Gt,
            Expr::func("cardinality", vec![Expr::col("", column)]),
            Expr::raw("0"),
        ),
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
        lower(input::from_ontology(ontology), batch_size, batch_size)
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
    fn node_plan_includes_fk_edge_transforms() {
        let ontology = ontology::Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1_000_000);

        let note_plan = plans.namespaced.iter().find(|p| p.name == "Note").unwrap();
        assert!(
            note_plan.transforms.len() >= 2,
            "Note should have node transform + FK edge transforms"
        );
        assert_eq!(note_plan.transforms[0].destination_table, "gl_note");
        assert_eq!(
            note_plan.transforms[1].destination_table,
            ontology.edge_table().to_string(),
        );
    }

    #[test]
    fn standalone_edges_produce_separate_plans() {
        let ontology = ontology::Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1_000_000);

        let all_plans: Vec<_> = plans.global.iter().chain(plans.namespaced.iter()).collect();

        let edge_table = ontology.edge_table().to_string();
        let standalone_edge_plans: Vec<_> = all_plans
            .iter()
            .filter(|p| p.transforms.len() == 1 && p.transforms[0].destination_table == edge_table)
            .collect();

        assert!(
            !standalone_edge_plans.is_empty(),
            "should have standalone edge plans writing to {edge_table}"
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
    fn fk_edge_transform_sql_outgoing_literal() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "owns".to_string(),
            source_id: EdgeId::Column("id".to_string()),
            source_kind: EdgeKind::Literal("Group".to_string()),
            target_id: EdgeId::Column("owner_id".to_string()),
            target_kind: EdgeKind::Literal("User".to_string()),
            filters: vec![EdgeFilter::IsNotNull("owner_id".to_string())],
            namespaced: true,
        };

        let transform = lower_fk_edge_transform(&fk_edge, "gl_edge");
        let sql = emit(&transform.query);

        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
    }

    #[test]
    fn fk_edge_transform_sql_incoming_literal() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "authored".to_string(),
            source_id: EdgeId::Column("author_id".to_string()),
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("Note".to_string()),
            filters: vec![EdgeFilter::IsNotNull("author_id".to_string())],
            namespaced: true,
        };

        let transform = lower_fk_edge_transform(&fk_edge, "gl_edge");
        let sql = emit(&transform.query);

        assert!(sql.contains("author_id AS source_id"));
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'Note' AS target_kind"));
    }

    #[test]
    fn fk_edge_transform_sql_multi_value_incoming() {
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

        let transform = lower_fk_edge_transform(&fk_edge, "gl_edge");
        let sql = emit(&transform.query);

        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS BIGINT)"),
            "sql: {sql}"
        );
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'WorkItem' AS target_kind"));
    }

    #[test]
    fn fk_edge_transform_sql_array_element_incoming() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "assigned".to_string(),
            source_id: EdgeId::ArrayElement {
                column: "assignees".to_string(),
                field: "user_id".to_string(),
            },
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("MergeRequest".to_string()),
            filters: vec![EdgeFilter::ArrayNotEmpty("assignees".to_string())],
            namespaced: true,
        };

        let transform = lower_fk_edge_transform(&fk_edge, "gl_edge");
        let sql = emit(&transform.query);

        assert!(sql.contains("unnest(assignees)['user_id']"), "sql: {sql}");
        assert!(sql.contains("'User' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'MergeRequest' AS target_kind"), "sql: {sql}");
        assert!(sql.contains("cardinality(assignees) > 0"), "sql: {sql}");
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
