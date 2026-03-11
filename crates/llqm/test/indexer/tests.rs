use std::collections::BTreeMap;

use llqm::backend::clickhouse::{ClickHouseBackend, InsertSelectPass};
use llqm::ir::expr::{self, DataType};
use llqm::ir::plan::{Plan, Rel};
use llqm::pipeline::{Frontend, IrPass, IrPhase, Pipeline};

use super::lower::*;
use super::orchestrate::*;
use super::types::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn user_entity() -> EntityDef {
    EntityDef::global(
        "siphon_users",
        "gl_user",
        vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("username", DataType::String),
        ],
        vec!["id"],
    )
}

fn project_entity() -> EntityDef {
    EntityDef::namespaced(
        "siphon_projects",
        "p",
        "gl_project",
        vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
        ],
        vec!["traversal_path", "id"],
        JoinDef {
            table: "traversal_paths".into(),
            alias: "tp".into(),
            left_key: "id".into(),
            right_key: "id".into(),
            columns: vec![ColumnDef::new("traversal_path", DataType::String)],
        },
    )
}

fn user_extract() -> ExtractDef {
    ExtractDef::Table(ExtractInput {
        entity: EntityDef::global(
            "siphon_users",
            "gl_user",
            vec![
                ColumnDef::new("id", DataType::Int64),
                ColumnDef::new("name", DataType::String),
                ColumnDef::new("username", DataType::String),
            ],
            vec!["id"],
        ),
        batch_size: 1_000_000,
        cursor_values: vec![],
    })
}

fn project_extract() -> ExtractDef {
    ExtractDef::Query(RawExtractInput {
        columns: vec![
            RawExtractColumn::Bare("project.id AS id".into()),
            RawExtractColumn::Bare("traversal_paths.traversal_path AS traversal_path".into()),
            RawExtractColumn::Bare("project.name AS name".into()),
        ],
        from:
            "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id"
                .into(),
        watermark: "project._siphon_replicated_at".into(),
        deleted: "project._siphon_deleted".into(),
        order_by: vec!["traversal_path".into(), "id".into()],
        batch_size: 500_000,
        namespaced: true,
        traversal_path_filter: Some("startsWith(traversal_path, {traversal_path:String})".into()),
        additional_where: None,
        cursor_values: vec![],
    })
}

fn table_extract(batch_size: u64) -> RawExtractInput {
    RawExtractInput {
        columns: vec![
            RawExtractColumn::Bare("id".into()),
            RawExtractColumn::Bare("name".into()),
        ],
        from: "siphon_user".into(),
        watermark: "_siphon_replicated_at".into(),
        deleted: "_siphon_deleted".into(),
        order_by: vec!["id".into()],
        batch_size,
        namespaced: false,
        traversal_path_filter: None,
        additional_where: None,
        cursor_values: vec![],
    }
}

fn query_extract(batch_size: u64) -> RawExtractInput {
    RawExtractInput {
        columns: vec![
            RawExtractColumn::Bare("project.id AS id".into()),
            RawExtractColumn::Bare("traversal_paths.traversal_path AS traversal_path".into()),
        ],
        from:
            "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id"
                .into(),
        watermark: "project._siphon_replicated_at".into(),
        deleted: "project._siphon_deleted".into(),
        order_by: vec!["traversal_path".into(), "id".into()],
        batch_size,
        namespaced: true,
        traversal_path_filter: Some("startsWith(traversal_path, {traversal_path:String})".into()),
        additional_where: None,
        cursor_values: vec![],
    }
}

fn emit_raw(input: RawExtractInput) -> String {
    Pipeline::new()
        .input(RawExtractFrontend, input)
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish()
        .sql
}

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

fn emit_pipeline(pipeline: Pipeline<IrPhase>) -> String {
    pipeline.emit(&ClickHouseBackend).unwrap().finish().sql
}

// ===========================================================================
// Extract — IndexerFrontend
// ===========================================================================

#[test]
fn extract_global() {
    let input = ExtractInput {
        entity: user_entity(),
        batch_size: 1_000_000,
        cursor_values: vec![],
    };

    let pq = Pipeline::new()
        .input(IndexerFrontend, input)
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish();

    let sql = &pq.sql;
    assert!(sql.contains("siphon_users"), "sql: {sql}");
    assert!(sql.contains("{last_watermark:String}"), "sql: {sql}");
    assert!(sql.contains("{watermark:String}"), "sql: {sql}");
    assert!(sql.contains("ORDER BY"), "sql: {sql}");
    assert!(sql.contains("LIMIT 1000000"), "sql: {sql}");
    assert!(!sql.contains("JOIN"), "should not have JOIN: {sql}");
}

#[test]
fn extract_namespaced_with_join() {
    let input = ExtractInput {
        entity: project_entity(),
        batch_size: 500_000,
        cursor_values: vec![],
    };

    let pq = Pipeline::new()
        .input(IndexerFrontend, input)
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish();

    let sql = &pq.sql;
    assert!(sql.contains("siphon_projects"), "sql: {sql}");
    assert!(sql.contains("INNER JOIN"), "sql: {sql}");
    assert!(sql.contains("traversal_paths"), "sql: {sql}");
    assert!(sql.contains("ON"), "sql: {sql}");
    assert!(sql.contains("LIMIT 500000"), "sql: {sql}");
}

#[test]
fn extract_cursor_single_key() {
    let input = ExtractInput {
        entity: user_entity(),
        batch_size: 1000,
        cursor_values: vec![("id".into(), "42".into())],
    };

    let sql = Pipeline::new()
        .input(IndexerFrontend, input)
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish()
        .sql;

    assert!(sql.contains("id > '42'"), "sql: {sql}");
}

#[test]
fn extract_cursor_composite_key() {
    let input = ExtractInput {
        entity: project_entity(),
        batch_size: 1000,
        cursor_values: vec![
            ("traversal_path".into(), "1/2/".into()),
            ("id".into(), "99".into()),
        ],
    };

    let sql = Pipeline::new()
        .input(IndexerFrontend, input)
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish()
        .sql;

    assert!(sql.contains("traversal_path > '1/2/'"), "sql: {sql}");
    assert!(sql.contains("traversal_path = '1/2/'"), "sql: {sql}");
    assert!(sql.contains("id > '99'"), "sql: {sql}");
    assert!(sql.contains("OR"), "sql: {sql}");
}

#[test]
fn extract_full_pipeline() {
    let input = ExtractInput {
        entity: project_entity(),
        batch_size: 100_000,
        cursor_values: vec![],
    };

    let sql = Pipeline::new()
        .input(IndexerFrontend, input)
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish()
        .sql;

    assert!(sql.starts_with("SELECT"), "sql: {sql}");
    assert!(sql.contains("FROM"), "sql: {sql}");
    assert!(sql.contains("WHERE"), "sql: {sql}");
    assert!(sql.contains("ORDER BY"), "sql: {sql}");
    assert!(sql.contains("LIMIT"), "sql: {sql}");
    assert!(sql.contains("_version"), "sql: {sql}");
    assert!(sql.contains("_deleted"), "sql: {sql}");
}

#[test]
fn extract_from_plan_reentry() {
    let input = ExtractInput {
        entity: user_entity(),
        batch_size: 1_000_000,
        cursor_values: vec![],
    };

    let plan = Pipeline::new()
        .input(IndexerFrontend, input)
        .lower()
        .unwrap()
        .into_plan();

    let sql = Pipeline::from_plan(plan)
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish()
        .sql;

    assert!(sql.contains("siphon_users"), "sql: {sql}");
    assert!(sql.contains("_version"), "sql: {sql}");
    assert!(sql.contains("_deleted"), "sql: {sql}");
}

#[test]
fn extract_rejects_empty_columns() {
    let mut entity = user_entity();
    entity.columns.clear();
    let input = ExtractInput {
        entity,
        batch_size: 1000,
        cursor_values: vec![],
    };

    let result = Pipeline::new().input(IndexerFrontend, input).lower();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("no columns"));
}

// ===========================================================================
// Extract — RawExtractFrontend
// ===========================================================================

#[test]
fn raw_extract_table_columns() {
    let sql = emit_raw(table_extract(1000));

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
fn raw_extract_query_fields() {
    let sql = emit_raw(query_extract(500));

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
    assert!(sql.contains("ORDER BY traversal_path"), "sql: {sql}");
    assert!(sql.contains("LIMIT 500"), "sql: {sql}");
}

#[test]
fn raw_extract_watermark() {
    let sql = emit_raw(table_extract(500));
    assert!(sql.contains("{last_watermark:String}"), "sql: {sql}");
    assert!(sql.contains("{watermark:String}"), "sql: {sql}");
}

#[test]
fn raw_extract_namespace_default() {
    let mut input = table_extract(1000);
    input.namespaced = true;
    input.traversal_path_filter = None;

    let sql = emit_raw(input);
    assert!(
        sql.contains("startsWith(traversal_path, {traversal_path:String})"),
        "sql: {sql}"
    );
}

#[test]
fn raw_extract_namespace_custom() {
    let mut input = table_extract(1000);
    input.namespaced = true;
    input.traversal_path_filter =
        Some("startsWith(traversal_path, {traversal_path:String})".into());

    let sql = emit_raw(input);
    assert!(
        sql.contains("startsWith(traversal_path, {traversal_path:String})"),
        "sql: {sql}"
    );
}

#[test]
fn raw_extract_additional_where() {
    let mut input = table_extract(1000);
    input.additional_where = Some("type = 'active'".into());

    let sql = emit_raw(input);
    assert!(sql.contains("type = 'active'"), "sql: {sql}");
}

#[test]
fn raw_extract_cursor_single() {
    let mut input = table_extract(1000);
    input.cursor_values = vec![("id".into(), "42".into())];

    let sql = emit_raw(input);
    assert!(sql.contains("id > '42'"), "sql: {sql}");
}

#[test]
fn raw_extract_cursor_composite() {
    let mut input = query_extract(1000);
    input.cursor_values = vec![
        ("traversal_path".into(), "1/2/".into()),
        ("id".into(), "42".into()),
    ];

    let sql = emit_raw(input);
    assert!(sql.contains("traversal_path > '1/2/'"), "sql: {sql}");
    assert!(sql.contains("traversal_path = '1/2/'"), "sql: {sql}");
    assert!(sql.contains("id > '42'"), "sql: {sql}");
    assert!(sql.contains("OR"), "sql: {sql}");
}

#[test]
fn raw_extract_to_string_column() {
    let input = RawExtractInput {
        columns: vec![
            RawExtractColumn::Bare("id".into()),
            RawExtractColumn::ToString("uuid".into()),
        ],
        from: "siphon_user".into(),
        watermark: "_siphon_replicated_at".into(),
        deleted: "_siphon_deleted".into(),
        order_by: vec!["id".into()],
        batch_size: 1000,
        namespaced: false,
        traversal_path_filter: None,
        additional_where: None,
        cursor_values: vec![],
    };

    let sql = emit_raw(input);
    assert!(sql.contains("toString(uuid)"), "sql: {sql}");
}

#[test]
fn raw_extract_rejects_empty_columns() {
    let input = RawExtractInput {
        columns: vec![],
        from: "t".into(),
        watermark: "w".into(),
        deleted: "d".into(),
        order_by: vec![],
        batch_size: 100,
        namespaced: false,
        traversal_path_filter: None,
        additional_where: None,
        cursor_values: vec![],
    };

    let result = Pipeline::new().input(RawExtractFrontend, input).lower();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("no columns"));
}

// ===========================================================================
// Transform — Node
// ===========================================================================

#[test]
fn node_transform_identity() {
    let sql = emit_node(NodeTransformInput {
        columns: vec![
            NodeColumn::Identity("id".into()),
            NodeColumn::Identity("name".into()),
        ],
    });

    assert!(sql.contains("id"), "sql: {sql}");
    assert!(sql.contains("name"), "sql: {sql}");
    assert!(sql.contains("source_data"), "sql: {sql}");
    assert!(sql.contains("_version"), "sql: {sql}");
    assert!(sql.contains("_deleted"), "sql: {sql}");
}

#[test]
fn node_transform_rename() {
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
fn node_transform_rejects_empty() {
    let result = Pipeline::new()
        .input(
            NodeTransformFrontend,
            NodeTransformInput { columns: vec![] },
        )
        .lower();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("no columns"));
}

// ===========================================================================
// Transform — FK Edge
// ===========================================================================

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
fn fk_edge_global_traversal_path() {
    let sql = emit_edge(FkEdgeTransformInput {
        relationship_kind: "owns".into(),
        source_id: EdgeId::Column("id".into()),
        source_kind: EdgeKind::Literal("User".into()),
        target_id: EdgeId::Column("project_id".into()),
        target_kind: EdgeKind::Literal("Project".into()),
        filters: vec![EdgeFilter::IsNotNull("project_id".into())],
        namespaced: false,
    });

    assert!(sql.contains("'0/' AS traversal_path"), "sql: {sql}");
}

#[test]
fn fk_edge_namespaced_traversal_path() {
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

// ===========================================================================
// Orchestration — lower_plans
// ===========================================================================

#[test]
fn orchestrate_partitions_by_scope() {
    let plans = lower_plans(
        vec![
            NodePlanInput {
                name: "User".into(),
                scope: Scope::Global,
                extract: user_extract(),
                node_columns: vec![
                    NodeColumn::Identity("id".into()),
                    NodeColumn::Identity("name".into()),
                    NodeColumn::Identity("username".into()),
                ],
                edges: vec![],
                edge_table: "gl_edge".into(),
            },
            NodePlanInput {
                name: "Project".into(),
                scope: Scope::Namespaced,
                extract: project_extract(),
                node_columns: vec![
                    NodeColumn::Identity("id".into()),
                    NodeColumn::Identity("name".into()),
                ],
                edges: vec![],
                edge_table: "gl_edge".into(),
            },
        ],
        vec![],
    )
    .unwrap();

    let global_names: Vec<&str> = plans.global.iter().map(|p| p.name.as_str()).collect();
    let ns_names: Vec<&str> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();

    assert!(global_names.contains(&"User"));
    assert!(ns_names.contains(&"Project"));
}

#[test]
fn orchestrate_node_with_fk_edges() {
    let plans = lower_plans(
        vec![NodePlanInput {
            name: "Note".into(),
            scope: Scope::Namespaced,
            extract: project_extract(),
            node_columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::Identity("body".into()),
            ],
            edges: vec![
                FkEdgeTransformInput {
                    relationship_kind: "authored".into(),
                    source_id: EdgeId::Column("author_id".into()),
                    source_kind: EdgeKind::Literal("User".into()),
                    target_id: EdgeId::Column("id".into()),
                    target_kind: EdgeKind::Literal("Note".into()),
                    filters: vec![EdgeFilter::IsNotNull("author_id".into())],
                    namespaced: true,
                },
                FkEdgeTransformInput {
                    relationship_kind: "belongs_to".into(),
                    source_id: EdgeId::Column("id".into()),
                    source_kind: EdgeKind::Literal("Note".into()),
                    target_id: EdgeId::Column("project_id".into()),
                    target_kind: EdgeKind::Literal("Project".into()),
                    filters: vec![EdgeFilter::IsNotNull("project_id".into())],
                    namespaced: true,
                },
            ],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let note = &plans.namespaced[0];
    assert_eq!(note.name, "Note");
    assert!(note.transforms.len() >= 3, "got {}", note.transforms.len());
    assert_eq!(note.transforms[1].destination_table, "gl_edge");
    assert_eq!(note.transforms[2].destination_table, "gl_edge");
}

#[test]
fn orchestrate_standalone_edge() {
    let plans = lower_plans(
        vec![],
        vec![StandaloneEdgePlanInput {
            name: "label_links".into(),
            scope: Scope::Namespaced,
            extract: project_extract(),
            transform: FkEdgeTransformInput {
                relationship_kind: "labeled".into(),
                source_id: EdgeId::Column("target_id".into()),
                source_kind: EdgeKind::Column("target_type".into()),
                target_id: EdgeId::Column("label_id".into()),
                target_kind: EdgeKind::Literal("Label".into()),
                filters: vec![
                    EdgeFilter::IsNotNull("target_id".into()),
                    EdgeFilter::IsNotNull("label_id".into()),
                ],
                namespaced: true,
            },
            edge_table: "gl_edge".into(),
        }],
    )
    .unwrap();

    assert_eq!(plans.namespaced.len(), 1);
    assert_eq!(plans.namespaced[0].name, "label_links");
    assert_eq!(
        plans.namespaced[0].transforms[0].destination_table,
        "gl_edge"
    );
}

#[test]
fn orchestrate_extract_emits_clickhouse() {
    let plans = lower_plans(
        vec![NodePlanInput {
            name: "User".into(),
            scope: Scope::Global,
            extract: user_extract(),
            node_columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::Identity("name".into()),
                NodeColumn::Identity("username".into()),
            ],
            edges: vec![],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let pq = plans.global[0]
        .extract
        .pipeline
        .clone()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish();
    assert!(pq.sql.contains("siphon_users"), "sql: {}", pq.sql);
    assert!(pq.sql.contains("ORDER BY"), "sql: {}", pq.sql);
    assert!(pq.sql.contains("LIMIT 1000000"), "sql: {}", pq.sql);
    assert!(
        pq.sql.contains("{last_watermark:String}"),
        "sql: {}",
        pq.sql
    );
}

#[test]
fn orchestrate_node_transform_sql() {
    let plans = lower_plans(
        vec![NodePlanInput {
            name: "User".into(),
            scope: Scope::Global,
            extract: user_extract(),
            node_columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::Rename {
                    source: "admin".into(),
                    target: "is_admin".into(),
                },
            ],
            edges: vec![],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let sql = emit_pipeline(plans.global[0].transforms[0].pipeline.clone());
    assert!(sql.contains("source_data"), "sql: {sql}");
    assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
}

#[test]
fn orchestrate_fk_edge_sql() {
    let plans = lower_plans(
        vec![NodePlanInput {
            name: "Group".into(),
            scope: Scope::Namespaced,
            extract: project_extract(),
            node_columns: vec![NodeColumn::Identity("id".into())],
            edges: vec![FkEdgeTransformInput {
                relationship_kind: "owns".into(),
                source_id: EdgeId::Column("id".into()),
                source_kind: EdgeKind::Literal("Group".into()),
                target_id: EdgeId::Column("owner_id".into()),
                target_kind: EdgeKind::Literal("User".into()),
                filters: vec![EdgeFilter::IsNotNull("owner_id".into())],
                namespaced: true,
            }],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let sql = emit_pipeline(plans.namespaced[0].transforms[1].pipeline.clone());
    assert!(sql.contains("'Group' AS source_kind"), "sql: {sql}");
    assert!(sql.contains("'User' AS target_kind"), "sql: {sql}");
    assert!(sql.contains("'owns' AS relationship_kind"), "sql: {sql}");
}

#[test]
fn orchestrate_query_extract_with_join() {
    let plans = lower_plans(
        vec![NodePlanInput {
            name: "Project".into(),
            scope: Scope::Namespaced,
            extract: project_extract(),
            node_columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::Identity("name".into()),
            ],
            edges: vec![],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let sql = emit_pipeline(plans.namespaced[0].extract.pipeline.clone());
    assert!(sql.contains("INNER JOIN"), "sql: {sql}");
    assert!(
        sql.contains("startsWith(traversal_path, {traversal_path:String})"),
        "sql: {sql}"
    );
    assert!(sql.contains("project.id AS id"), "sql: {sql}");
}

#[test]
fn orchestrate_multi_value_fk_edge() {
    let plans = lower_plans(
        vec![NodePlanInput {
            name: "WorkItem".into(),
            scope: Scope::Namespaced,
            extract: project_extract(),
            node_columns: vec![NodeColumn::Identity("id".into())],
            edges: vec![FkEdgeTransformInput {
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
            }],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let sql = emit_pipeline(plans.namespaced[0].transforms[1].pipeline.clone());
    assert!(
        sql.contains("CAST(NULLIF(unnest(string_to_array("),
        "sql: {sql}"
    );
}

#[test]
fn orchestrate_int_enum() {
    let mut values = BTreeMap::new();
    values.insert(0, "active".into());
    values.insert(1, "blocked".into());

    let plans = lower_plans(
        vec![NodePlanInput {
            name: "User".into(),
            scope: Scope::Global,
            extract: user_extract(),
            node_columns: vec![
                NodeColumn::Identity("id".into()),
                NodeColumn::IntEnum {
                    source: "state".into(),
                    target: "state".into(),
                    values,
                },
            ],
            edges: vec![],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let sql = emit_pipeline(plans.global[0].transforms[0].pipeline.clone());
    assert!(sql.contains("CASE"), "sql: {sql}");
    assert!(sql.contains("END AS state"), "sql: {sql}");
}

#[test]
fn orchestrate_ir_pass_before_emit() {
    struct NoopPass;
    impl IrPass for NoopPass {
        type Error = std::convert::Infallible;
        fn transform(&self, plan: Plan) -> Result<Plan, Self::Error> {
            Ok(plan)
        }
    }

    let plans = lower_plans(
        vec![NodePlanInput {
            name: "User".into(),
            scope: Scope::Global,
            extract: user_extract(),
            node_columns: vec![NodeColumn::Identity("id".into())],
            edges: vec![],
            edge_table: "gl_edge".into(),
        }],
        vec![],
    )
    .unwrap();

    let pq = plans.global[0]
        .extract
        .pipeline
        .clone()
        .pass(&NoopPass)
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .finish();

    assert!(pq.sql.contains("siphon_users"), "sql: {}", pq.sql);
}

// ===========================================================================
// INSERT...SELECT
// ===========================================================================

struct SelectFrontend;

impl Frontend for SelectFrontend {
    type Input = ();
    type Error = std::convert::Infallible;

    fn lower(&self, _: ()) -> Result<Plan, Self::Error> {
        Ok(Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .filter(
                expr::func(
                    "startsWith",
                    vec![
                        expr::col("p", "traversal_path"),
                        expr::param("traversal_path", DataType::String),
                    ],
                )
                .and(expr::col("p", "_deleted").eq(expr::raw("false"))),
            )
            .project(&[
                (expr::col("p", "id"), "id"),
                (expr::col("p", "name"), "name"),
                (expr::raw("true"), "_deleted"),
                (expr::raw("now64(6)"), "_version"),
            ])
            .into_plan())
    }
}

#[test]
fn insert_select_with_columns() {
    let pass = InsertSelectPass::new("gl_project", &["id", "name", "_deleted", "_version"]);

    let pq = Pipeline::new()
        .input(SelectFrontend, ())
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .pass(&pass)
        .unwrap()
        .finish();

    let sql = &pq.sql;
    assert!(
        sql.starts_with("INSERT INTO gl_project (id, name, _deleted, _version) SELECT"),
        "sql: {sql}"
    );
    assert!(sql.contains("true AS _deleted"), "sql: {sql}");
    assert!(sql.contains("now64(6) AS _version"), "sql: {sql}");
    assert!(
        sql.contains("startsWith(p.traversal_path, {traversal_path:String})"),
        "sql: {sql}"
    );
}

#[test]
fn insert_select_without_columns() {
    let pass = InsertSelectPass::new("gl_project", &[]);

    let pq = Pipeline::new()
        .input(SelectFrontend, ())
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .pass(&pass)
        .unwrap()
        .finish();

    assert!(
        pq.sql.starts_with("INSERT INTO gl_project SELECT"),
        "sql: {}",
        pq.sql
    );
}

#[test]
fn insert_select_preserves_params() {
    let pass = InsertSelectPass::new("t", &["id"]);

    let pq = Pipeline::new()
        .input(SelectFrontend, ())
        .lower()
        .unwrap()
        .emit(&ClickHouseBackend)
        .unwrap()
        .pass(&pass)
        .unwrap()
        .finish();

    assert!(
        pq.sql.contains("{traversal_path:String}"),
        "sql: {}",
        pq.sql
    );
}
