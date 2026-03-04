use ontology::{
    DataType, EdgeDirection, EdgeEndpointType, EdgeSourceEtlConfig, EdgeTarget, EnumType,
    EtlConfig, EtlScope, Field, NodeEntity, Ontology,
};

use super::ast::{Expr, Op, Query, SelectExpr, TableRef};
use super::{ExtractQuery, PipelinePlan, TransformOutput};

const SOURCE_DATA_TABLE: &str = "source_data";

pub(in crate::modules::sdlc_v2) struct PartitionedPlans {
    pub global: Vec<PipelinePlan>,
    pub namespaced: Vec<PipelinePlan>,
}

pub(in crate::modules::sdlc_v2) fn build_plans(
    ontology: &Ontology,
    batch_size: u64,
) -> PartitionedPlans {
    let mut global_plans = Vec::new();
    let mut namespaced_plans = Vec::new();

    let mut push = |plan: PipelinePlan, scope: EtlScope| match scope {
        EtlScope::Global => global_plans.push(plan),
        EtlScope::Namespaced => namespaced_plans.push(plan),
    };

    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };
        let Some(plan) = build_node_plan(node, etl, ontology, batch_size) else {
            continue;
        };
        push(plan, etl.scope());
    }

    for (relationship_kind, config) in ontology.edge_etl_configs() {
        let plan = build_edge_etl_plan(relationship_kind, config, ontology, batch_size);
        push(plan, config.scope);
    }

    PartitionedPlans {
        global: global_plans,
        namespaced: namespaced_plans,
    }
}

fn build_node_plan(
    node: &NodeEntity,
    etl: &EtlConfig,
    ontology: &Ontology,
    batch_size: u64,
) -> Option<PipelinePlan> {
    let namespaced = etl.scope() == EtlScope::Namespaced;
    let extract_query = build_extract_query(node, etl, batch_size)?;

    let mut transforms = vec![TransformOutput {
        query: build_node_transform(node),
        destination_table: node.destination_table.clone(),
    }];

    for (fk_column, mapping) in etl.edges() {
        transforms.push(TransformOutput {
            query: build_fk_edge_transform(&node.name, fk_column, mapping, namespaced, ontology),
            destination_table: ontology.edge_table().to_string(),
        });
    }

    Some(PipelinePlan {
        name: node.name.clone(),
        extract_query,
        transforms,
    })
}

fn build_extract_query(
    node: &NodeEntity,
    etl: &EtlConfig,
    batch_size: u64,
) -> Option<ExtractQuery> {
    match etl {
        EtlConfig::Table {
            source,
            watermark,
            deleted,
            order_by,
            edges,
            ..
        } => {
            let mut select: Vec<SelectExpr> = node
                .fields
                .iter()
                .map(|field| {
                    if field.data_type == DataType::Uuid {
                        SelectExpr::new(
                            Expr::func("toString", vec![Expr::col("", &field.source)]),
                            field.source.clone(),
                        )
                    } else {
                        SelectExpr::bare(Expr::raw(field.source.clone()))
                    }
                })
                .collect();

            append_missing_columns(&mut select, &edges.keys().cloned().collect::<Vec<_>>());
            append_missing_columns(&mut select, order_by);
            select.push(SelectExpr::new(Expr::raw(watermark.clone()), "_version"));
            select.push(SelectExpr::new(Expr::raw(deleted.clone()), "_deleted"));

            let base_query = Query {
                select,
                from: TableRef::scan(source.clone(), None),
                where_clause: Some(watermark_where(watermark)),
                order_by: vec![],
                limit: None,
            };
            Some(ExtractQuery::new(base_query, order_by.clone(), batch_size))
        }
        EtlConfig::Query {
            select,
            from,
            where_clause,
            watermark,
            deleted,
            order_by,
            ..
        } => {
            let mut select: Vec<SelectExpr> = select
                .split(", ")
                .map(|s| SelectExpr::bare(Expr::raw(s.trim().to_string())))
                .collect();

            append_missing_columns(&mut select, order_by);
            select.push(SelectExpr::new(Expr::raw(watermark.clone()), "_version"));
            select.push(SelectExpr::new(Expr::raw(deleted.clone()), "_deleted"));

            let base_query = Query {
                select,
                from: TableRef::Raw(from.clone()),
                where_clause: Expr::and_all([
                    Some(watermark_where(watermark)),
                    where_clause.as_ref().map(|w| Expr::raw(w.clone())),
                ]),
                order_by: vec![],
                limit: None,
            };
            Some(ExtractQuery::new(base_query, order_by.clone(), batch_size))
        }
    }
}

fn build_node_transform(node: &NodeEntity) -> Query {
    let field_expr = |field: &Field| -> SelectExpr {
        if field.data_type == DataType::Enum
            && field.enum_type == EnumType::Int
            && let Some(ref values) = field.enum_values
        {
            let cases: Vec<String> = values
                .iter()
                .map(|(key, value)| format!("WHEN {} = {} THEN '{}'", field.source, key, value))
                .collect();
            return SelectExpr::new(
                Expr::raw(format!("CASE {} ELSE 'unknown' END", cases.join(" "))),
                field.name.clone(),
            );
        }
        if field.source == field.name {
            SelectExpr::bare(Expr::col("", &field.name))
        } else {
            SelectExpr::new(Expr::col("", &field.source), field.name.clone())
        }
    };

    let mut select: Vec<SelectExpr> = node.fields.iter().map(field_expr).collect();
    select.push(SelectExpr::bare(Expr::col("", "_version")));
    select.push(SelectExpr::bare(Expr::col("", "_deleted")));

    Query {
        select,
        from: TableRef::scan(SOURCE_DATA_TABLE, None),
        where_clause: None,
        order_by: vec![],
        limit: None,
    }
}

fn build_fk_edge_transform(
    node_kind: &str,
    fk_column: &str,
    mapping: &ontology::EdgeMapping,
    namespaced: bool,
    ontology: &Ontology,
) -> Query {
    let node_id = Expr::col("", "id");
    let node_literal = Expr::raw(format!("'{node_kind}'"));
    let fk_ref = Expr::col("", fk_column);

    let (fk_kind, type_filter) = match &mapping.target {
        EdgeTarget::Literal(target_type) => {
            (Expr::raw(format!("'{target_type}'")), None)
        }
        EdgeTarget::Column(type_column) => {
            let allowed = ontology.get_edge_target_types(
                &mapping.relationship_kind,
                node_kind,
                mapping.direction,
            );
            (Expr::col("", type_column), type_filter_expr(type_column, &allowed))
        }
    };

    let (mut source_id, source_kind, mut target_id, target_kind) = match mapping.direction {
        EdgeDirection::Outgoing => (node_id, node_literal, fk_ref, fk_kind),
        EdgeDirection::Incoming => (fk_ref, fk_kind, node_id, node_literal),
    };

    let where_clause = if let Some(delimiter) = &mapping.delimiter {
        let exploded = Expr::cast(
            Expr::func("NULLIF", vec![
                Expr::func("unnest", vec![
                    Expr::func("string_to_array", vec![
                        Expr::col("", fk_column),
                        Expr::raw(format!("'{delimiter}'")),
                    ]),
                ]),
                Expr::raw("''"),
            ]),
            "BIGINT",
        );
        match mapping.direction {
            EdgeDirection::Outgoing => target_id = exploded,
            EdgeDirection::Incoming => source_id = exploded,
        }
        Expr::and_all([
            Some(Expr::is_not_null(Expr::col("", fk_column))),
            Some(Expr::binary(Op::Ne, Expr::col("", fk_column), Expr::raw("''"))),
        ])
    } else {
        Expr::and_all([
            Some(Expr::is_not_null(Expr::col("", fk_column))),
            type_filter,
        ])
    };

    Query {
        select: edge_select(
            source_id, source_kind,
            &mapping.relationship_kind,
            target_id, target_kind,
            namespaced,
        ),
        from: TableRef::scan(SOURCE_DATA_TABLE, None),
        where_clause,
        order_by: vec![],
        limit: None,
    }
}

fn build_edge_etl_plan(
    relationship_kind: &str,
    config: &EdgeSourceEtlConfig,
    ontology: &Ontology,
    batch_size: u64,
) -> PipelinePlan {
    let extract_query = build_edge_etl_extract(config, batch_size);
    let transform_query = build_edge_etl_transform(relationship_kind, config, ontology);

    PipelinePlan {
        name: relationship_kind.to_string(),
        extract_query,
        transforms: vec![TransformOutput {
            query: transform_query,
            destination_table: ontology.edge_table().to_string(),
        }],
    }
}

fn build_edge_etl_extract(config: &EdgeSourceEtlConfig, batch_size: u64) -> ExtractQuery {
    let mut columns = vec![config.from.id_column.clone(), config.to.id_column.clone()];

    if let EdgeEndpointType::Column { column, .. } = &config.from.node_type
        && !columns.contains(column) {
        columns.push(column.clone());
    }
    if let EdgeEndpointType::Column { column, .. } = &config.to.node_type
        && !columns.contains(column) {
        columns.push(column.clone());
    }
    for column in &config.order_by {
        if !columns.contains(column) { columns.push(column.clone()); }
    }

    let namespaced = config.scope == EtlScope::Namespaced;
    let mut extra_where = None;
    if namespaced {
        if !columns.contains(&"traversal_path".to_string()) {
            columns.push("traversal_path".to_string());
        }
        extra_where = Some(Expr::raw("startsWith(traversal_path, {traversal_path:String})"));
    }

    let mut select: Vec<SelectExpr> = columns
        .iter()
        .map(|c| SelectExpr::bare(Expr::raw(c.clone())))
        .collect();
    select.push(SelectExpr::new(Expr::raw(config.watermark.clone()), "_version"));
    select.push(SelectExpr::new(Expr::raw(config.deleted.clone()), "_deleted"));

    let base_query = Query {
        select,
        from: TableRef::scan(config.source.clone(), None),
        where_clause: Expr::and_all([Some(watermark_where(&config.watermark)), extra_where]),
        order_by: vec![],
        limit: None,
    };

    ExtractQuery::new(base_query, config.order_by.clone(), batch_size)
}

fn build_edge_etl_transform(
    relationship_kind: &str,
    config: &EdgeSourceEtlConfig,
    ontology: &Ontology,
) -> Query {
    let namespaced = config.scope == EtlScope::Namespaced;

    let resolve = |endpoint: &ontology::EdgeEndpoint, is_source: bool| -> (Expr, Expr, Option<Expr>) {
        let id_expr = Expr::col("", &endpoint.id_column);
        match &endpoint.node_type {
            EdgeEndpointType::Literal(node_type) => {
                (id_expr, Expr::raw(format!("'{node_type}'")), None)
            }
            EdgeEndpointType::Column { column, type_mapping } => {
                let allowed = if is_source {
                    ontology.get_edge_source_types(relationship_kind)
                } else {
                    ontology.get_edge_all_target_types(relationship_kind)
                };
                let filter = type_filter_expr(column, &allowed);

                let kind = if type_mapping.is_empty() {
                    Expr::col("", column)
                } else {
                    let cases: Vec<String> = type_mapping
                        .iter()
                        .map(|(from, to)| format!("WHEN {column} = '{from}' THEN '{to}'"))
                        .collect();
                    Expr::raw(format!("CASE {} ELSE {column} END", cases.join(" ")))
                };
                (id_expr, kind, filter)
            }
        }
    };

    let (source_id, source_kind, source_filter) = resolve(&config.from, true);
    let (target_id, target_kind, _) = resolve(&config.to, false);

    Query {
        select: edge_select(
            source_id.clone(), source_kind,
            relationship_kind,
            target_id.clone(), target_kind,
            namespaced,
        ),
        from: TableRef::scan(SOURCE_DATA_TABLE, None),
        where_clause: Expr::and_all([
            Some(Expr::is_not_null(source_id)),
            Some(Expr::is_not_null(target_id)),
            source_filter,
        ]),
        order_by: vec![],
        limit: None,
    }
}

// ── Shared helpers ───────────────────────────────────────────────────────────

fn edge_select(
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
        SelectExpr::new(Expr::raw(format!("'{relationship_kind}'")), "relationship_kind"),
        SelectExpr::new(target_id, "target_id"),
        SelectExpr::new(target_kind, "target_kind"),
        SelectExpr::bare(Expr::col("", "_version")),
        SelectExpr::bare(Expr::col("", "_deleted")),
    ]
}

fn watermark_where(watermark: &str) -> Expr {
    Expr::and_all([
        Some(Expr::binary(Op::Gt, Expr::raw(watermark.to_string()), Expr::raw("{last_watermark:String}"))),
        Some(Expr::binary(Op::Le, Expr::raw(watermark.to_string()), Expr::raw("{watermark:String}"))),
    ])
    .unwrap()
}

fn type_filter_expr(type_column: &str, allowed_types: &[String]) -> Option<Expr> {
    if allowed_types.is_empty() {
        return None;
    }
    let types_list = allowed_types
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    Some(Expr::raw(format!("{type_column} IN ({types_list})")))
}

fn append_missing_columns(select: &mut Vec<SelectExpr>, columns: &[String]) {
    let contains = |se: &SelectExpr, name: &str| -> bool {
        if let Some(alias) = &se.alias && alias == name { return true; }
        match &se.expr {
            Expr::Column { column, .. } => column == name,
            Expr::Raw(raw) => raw.contains(name),
            Expr::FuncCall { args, .. } => args.iter().any(|a| matches!(a, Expr::Column { column, .. } if column == name)),
            _ => false,
        }
    };

    for column in columns {
        if !select.iter().any(|se| contains(se, column)) {
            select.push(SelectExpr::bare(Expr::raw(column.clone())));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::constants::GL_TABLE_PREFIX;
    use ontology::{EdgeMapping, EnumType};
    use std::collections::BTreeMap;

    fn test_field(name: &str, data_type: DataType) -> Field {
        Field {
            name: name.to_string(),
            source: name.to_string(),
            data_type,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::default(),
        }
    }

    fn test_node_table_etl(name: &str, scope: EtlScope) -> NodeEntity {
        NodeEntity {
            name: name.to_string(),
            domain: "core".to_string(),
            fields: vec![
                test_field("id", DataType::Int),
                test_field("name", DataType::String),
            ],
            destination_table: format!("{GL_TABLE_PREFIX}{}", name.to_lowercase()),
            sort_key: vec!["traversal_path".to_string(), "id".to_string()],
            etl: Some(EtlConfig::Table {
                scope,
                source: format!("siphon_{}", name.to_lowercase()),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                order_by: vec!["id".to_string()],
                edges: BTreeMap::new(),
            }),
            ..Default::default()
        }
    }

    fn emit(query: &Query) -> String {
        super::super::codegen::emit_sql(query)
    }

    #[test]
    fn build_plans_partitions_by_scope() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1_000_000);

        let global_names: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        let namespaced_names: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();

        assert!(global_names.contains(&"User"), "User should be global");
        assert!(namespaced_names.contains(&"Group"), "Group should be namespaced");
        assert!(namespaced_names.contains(&"Project"), "Project should be namespaced");
    }

    #[test]
    fn node_plan_has_node_transform_and_edge_transforms() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1_000_000);

        let note_plan = plans.namespaced.iter().find(|p| p.name == "Note").unwrap();
        assert!(note_plan.transforms.len() >= 2);
        assert_eq!(note_plan.transforms[0].destination_table, "gl_note");
        assert_eq!(
            note_plan.transforms[1].destination_table,
            ontology.edge_table().to_string(),
        );
    }

    #[test]
    fn node_transform_sql_handles_column_renaming() {
        let mut node = test_node_table_etl("Test", EtlScope::Global);
        node.fields.push(Field {
            name: "is_admin".to_string(),
            source: "admin".to_string(),
            data_type: DataType::Bool,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::default(),
        });

        assert!(emit(&build_node_transform(&node)).contains("admin AS is_admin"));
    }

    #[test]
    fn node_transform_sql_handles_int_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "blocked".to_string());

        let mut node = test_node_table_etl("Test", EtlScope::Global);
        node.fields.push(Field {
            name: "state".to_string(),
            source: "state".to_string(),
            data_type: DataType::Enum,
            nullable: false,
            enum_values: Some(values),
            enum_type: EnumType::Int,
        });

        let sql = emit(&build_node_transform(&node));
        assert!(sql.contains("CASE"));
        assert!(sql.contains("WHEN state = 0 THEN 'active'"));
        assert!(sql.contains("WHEN state = 1 THEN 'blocked'"));
        assert!(sql.contains("ELSE 'unknown' END AS state"));
    }

    #[test]
    fn edge_sql_outgoing_literal() {
        let ontology = Ontology::new();
        let mut node = test_node_table_etl("Group", EtlScope::Namespaced);
        if let Some(EtlConfig::Table { edges, .. }) = &mut node.etl {
            edges.insert("owner_id".to_string(), EdgeMapping {
                target: EdgeTarget::Literal("User".to_string()),
                relationship_kind: "owns".to_string(),
                direction: EdgeDirection::Outgoing,
                delimiter: None,
            });
        }

        let plan = build_node_plan(&node, node.etl.as_ref().unwrap(), &ontology, 1000).unwrap();
        let sql = emit(&plan.transforms[1].query);

        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
    }

    #[test]
    fn edge_sql_incoming_literal() {
        let ontology = Ontology::new();
        let mut node = test_node_table_etl("Note", EtlScope::Namespaced);
        if let Some(EtlConfig::Table { edges, .. }) = &mut node.etl {
            edges.insert("author_id".to_string(), EdgeMapping {
                target: EdgeTarget::Literal("User".to_string()),
                relationship_kind: "authored".to_string(),
                direction: EdgeDirection::Incoming,
                delimiter: None,
            });
        }

        let plan = build_node_plan(&node, node.etl.as_ref().unwrap(), &ontology, 1000).unwrap();
        let sql = emit(&plan.transforms[1].query);

        assert!(sql.contains("author_id AS source_id"));
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'Note' AS target_kind"));
    }

    #[test]
    fn edge_sql_multi_value_incoming() {
        let ontology = Ontology::new();
        let mut node = test_node_table_etl("WorkItem", EtlScope::Namespaced);
        if let Some(EtlConfig::Table { edges, .. }) = &mut node.etl {
            edges.insert("assignee_ids".to_string(), EdgeMapping {
                target: EdgeTarget::Literal("User".to_string()),
                relationship_kind: "assigned".to_string(),
                direction: EdgeDirection::Incoming,
                delimiter: Some("/".to_string()),
            });
        }

        let plan = build_node_plan(&node, node.etl.as_ref().unwrap(), &ontology, 1000).unwrap();
        let sql = emit(&plan.transforms[1].query);

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
        let mut node = test_node_table_etl("User", EtlScope::Global);
        node.sort_key = vec!["id".to_string()];

        let sql = build_extract_query(&node, node.etl.as_ref().unwrap(), 1000)
            .unwrap()
            .to_sql();

        assert!(sql.contains("SELECT id, name,"), "sql: {sql}");
        assert!(sql.contains("_siphon_replicated_at AS _version"), "sql: {sql}");
        assert!(sql.contains("_siphon_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("FROM siphon_user"), "sql: {sql}");
        assert!(sql.contains("ORDER BY id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000"), "sql: {sql}");
    }

    #[test]
    fn extract_query_query_etl_uses_structured_fields() {
        let node = NodeEntity {
            name: "Project".to_string(),
            domain: "core".to_string(),
            fields: vec![test_field("id", DataType::Int)],
            destination_table: format!("{GL_TABLE_PREFIX}project"),
            sort_key: vec!["traversal_path".to_string(), "id".to_string()],
            etl: Some(EtlConfig::Query {
                scope: EtlScope::Namespaced,
                select: "project.id AS id, traversal_paths.traversal_path AS traversal_path"
                    .to_string(),
                from: "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id"
                    .to_string(),
                where_clause: Some(
                    "startsWith(traversal_path, {traversal_path:String})".to_string(),
                ),
                watermark: "project._siphon_replicated_at".to_string(),
                deleted: "project._siphon_deleted".to_string(),
                order_by: vec!["traversal_path".to_string(), "id".to_string()],
                edges: BTreeMap::new(),
            }),
            ..Default::default()
        };

        let sql = build_extract_query(&node, node.etl.as_ref().unwrap(), 500)
            .unwrap()
            .to_sql();

        assert!(sql.contains("project.id AS id"), "sql: {sql}");
        assert!(sql.contains("traversal_paths.traversal_path AS traversal_path"), "sql: {sql}");
        assert!(sql.contains("project._siphon_replicated_at AS _version"), "sql: {sql}");
        assert!(sql.contains("project._siphon_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("INNER JOIN"), "sql: {sql}");
        assert!(sql.contains("startsWith(traversal_path, {traversal_path:String})"), "sql: {sql}");
        assert!(sql.contains("ORDER BY traversal_path, id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 500"), "sql: {sql}");
    }
}
