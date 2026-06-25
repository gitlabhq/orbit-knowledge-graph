use ontology::{EtlScope, Ontology};

use super::input::{
    DerivedEntityPlan, EdgeKind, EtlInputs, NodePlan, SourceFrom, SourceQuerySpec,
    StandaloneEdgePlan,
};
use super::projection::ProjectionSqlRenderer;
use super::source_query::SourceQueryRenderer;
use super::{Plan, Plans, TransformSpec};

pub(in crate::modules::sdlc) fn assemble(
    inputs: EtlInputs,
    ontology: &Ontology,
    global_batch_size: u64,
    namespaced_batch_size: u64,
    batch_size_overrides: &std::collections::HashMap<String, u64>,
) -> Plans {
    let mut global = Vec::new();
    let mut namespaced = Vec::new();

    for node in inputs.node_plans {
        let scope_default = match node.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let batch_size = batch_size_overrides
            .get(&node.name)
            .copied()
            .unwrap_or(scope_default);
        let scope = node.scope;
        let plan = node_plan(node, batch_size, ontology);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    for edge in inputs.standalone_edge_plans {
        let scope_default = match edge.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let batch_size = batch_size_overrides
            .get(&edge.relationship_kind)
            .copied()
            .unwrap_or(scope_default);
        let scope = edge.scope;
        let plan = standalone_edge_plan(edge, batch_size, ontology);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    for derived in inputs.derived_entity_plans {
        let scope_default = match derived.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let batch_size = batch_size_overrides
            .get(&derived.name)
            .copied()
            .unwrap_or(scope_default);
        let scope = derived.scope;
        let plan = derived_entity_plan(derived, batch_size);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    // A plan name is both the handler name (`entity.{name}`) and the checkpoint
    // key (`{scope}.{name}`), so a collision silently clobbers a handler and
    // makes two plans share one cursor. `plan_name` disambiguates standalone
    // edges that share a source table only when the target kind is a literal;
    // a `Column` target yields no suffix, so two such ETLs would collide. This
    // is a build-from-ontology invariant checked once at boot (not on the data
    // path), so panicking on a duplicate is the right failure mode.
    assert_unique_plan_names(&global, &namespaced);

    Plans { global, namespaced }
}

fn assert_unique_plan_names(global: &[Plan], namespaced: &[Plan]) {
    let mut seen = std::collections::HashSet::new();
    for plan in global.iter().chain(namespaced.iter()) {
        assert!(
            seen.insert(plan.name.as_str()),
            "duplicate plan name '{}': plan names are handler names and checkpoint keys and must be unique",
            plan.name
        );
    }
}

fn derived_entity_plan(input: DerivedEntityPlan, batch_size: u64) -> Plan {
    let mut plan = runtime_plan(input.extract, batch_size);
    plan.name = input.name;
    plan.transform = TransformSpec::Rust(input.transform);
    plan
}

fn node_plan(input: NodePlan, batch_size: u64, ontology: &Ontology) -> Plan {
    let node_destination = input.extract.destination_table.clone();
    let mut plan = runtime_plan(input.extract, batch_size);

    let mut transforms = vec![ProjectionSqlRenderer::node(
        &input.name,
        &input.columns,
        node_destination,
        ontology,
    )];

    for fk_edge in &input.edges {
        transforms.push(ProjectionSqlRenderer::fk_edge(fk_edge, ontology));
    }

    plan.name = input.name;
    plan.transform = TransformSpec::DataFusion(transforms);
    plan
}

fn standalone_edge_plan(input: StandaloneEdgePlan, batch_size: u64, ontology: &Ontology) -> Plan {
    let destination_table = input.extract.destination_table.clone();
    let name = plan_name(
        &input.relationship_kind,
        &input.extract.source,
        &input.target_kind,
    );
    let mut plan = runtime_plan(input.extract.clone(), batch_size);
    let transform = ProjectionSqlRenderer::standalone_edge(&input, destination_table, ontology);
    plan.name = name;
    plan.transform = TransformSpec::DataFusion(vec![transform]);
    plan
}

fn plan_name(relationship_kind: &str, source: &SourceFrom, target_kind: &EdgeKind) -> String {
    // A relationship kind can have several ETLs over the same source table that
    // differ only by their target (REOPENED: one MR-targeted, one WorkItem-
    // targeted, both filtered on `siphon_resource_state_events`). The plan name
    // is the handler name and checkpoint key, so it must be unique per ETL; a
    // literal target kind disambiguates them.
    let target_suffix = match target_kind {
        EdgeKind::Literal(t) => format!("_{t}"),
        EdgeKind::Column { .. } => String::new(),
    };
    match source {
        SourceFrom::Table(table) => format!("{relationship_kind}_{table}{target_suffix}"),
        SourceFrom::Raw(_) => format!("{relationship_kind}{target_suffix}"),
    }
}

fn runtime_plan(input: SourceQuerySpec, batch_size: u64) -> Plan {
    SourceQueryRenderer::render_plan(input, batch_size)
}

#[cfg(test)]
mod tests {
    use super::super::input;
    use super::super::input::{EdgeFilter, EdgeId, FkEdgeTransform, NodeColumn, SourceColumn};
    use super::super::projection::ProjectionSqlRenderer;
    use super::super::{Cursor, CursorFilter, TraversalPathFilter, WatermarkFilter};
    use super::*;
    use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn test_ontology() -> ontology::Ontology {
        ontology::Ontology::load_embedded().expect("should load ontology")
    }

    fn named_plan(name: &str) -> Plan {
        Plan {
            name: name.to_string(),
            extract_template: String::new(),
            watermark_column: String::new(),
            deleted_column: String::new(),
            sort_key: vec![],
            batch_size: 1,
            transform: TransformSpec::DataFusion(vec![]),
        }
    }

    #[test]
    fn assert_unique_plan_names_accepts_distinct_names_across_scopes() {
        assert_unique_plan_names(
            &[named_plan("User")],
            &[named_plan("REOPENED_t_MergeRequest")],
        );
    }

    #[test]
    #[should_panic(expected = "duplicate plan name")]
    fn assert_unique_plan_names_rejects_collision() {
        assert_unique_plan_names(&[named_plan("dup")], &[named_plan("dup")]);
    }

    #[test]
    fn embedded_ontology_yields_unique_plan_names() {
        // Assembling plans already runs assert_unique_plan_names; this names the
        // guarantee so a future same-source/same-kind edge with a Column target
        // (empty plan-name suffix) trips here instead of silently clobbering a
        // handler and sharing a checkpoint cursor.
        let _ = assembled_plans(&test_ontology(), 1000);
    }

    fn assembled_plans(ontology: &ontology::Ontology, batch_size: u64) -> Plans {
        assemble(
            input::from_ontology(ontology),
            ontology,
            batch_size,
            batch_size,
            &std::collections::HashMap::new(),
        )
    }

    fn render_namespaced_extract(plan: &Plan, path: &str) -> String {
        plan.prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .with(TraversalPathFilter { path })
            .to_sql()
    }

    fn render_global_extract(plan: &Plan) -> String {
        plan.prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .to_sql()
    }

    fn node_projection_sql(columns: &[NodeColumn]) -> String {
        ProjectionSqlRenderer::node("User", columns, "gl_user".to_string(), &test_ontology()).sql
    }

    fn fk_edge_projection_sql(fk_edge: &FkEdgeTransform) -> String {
        ProjectionSqlRenderer::fk_edge(fk_edge, &test_ontology()).sql
    }

    #[test]
    fn assemble_partitions_by_scope() {
        let ontology = test_ontology();
        let plans = assembled_plans(&ontology, 1_000_000);

        let global_names: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        let namespaced_names: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();

        assert!(global_names.contains(&"User"));
        assert!(namespaced_names.contains(&"Group"));
        assert!(namespaced_names.contains(&"Project"));
    }

    #[test]
    fn batch_size_override_applies_to_named_pipeline() {
        let ontology = test_ontology();
        let overrides = std::collections::HashMap::from([("WorkItem".to_string(), 50_000u64)]);
        let plans = assemble(
            input::from_ontology(&ontology),
            &ontology,
            1_000_000,
            1_000_000,
            &overrides,
        );

        let work_item = plans
            .namespaced
            .iter()
            .find(|p| p.name == "WorkItem")
            .expect("WorkItem plan should exist");
        assert_eq!(work_item.batch_size, 50_000);

        let group = plans
            .namespaced
            .iter()
            .find(|p| p.name == "Group")
            .expect("Group plan should exist");
        assert_eq!(group.batch_size, 1_000_000);
    }

    #[test]
    fn node_plan_includes_fk_edge_transforms() {
        let ontology = test_ontology();
        let plans = assembled_plans(&ontology, 1_000_000);

        let note_plan = plans.namespaced.iter().find(|p| p.name == "Note").unwrap();
        assert!(note_plan.transformations().len() >= 2);
        assert_eq!(
            note_plan.transformations()[0].destination_table,
            prefixed_table_name("gl_note", *SCHEMA_VERSION),
        );
        assert_eq!(
            note_plan.transformations()[1].destination_table,
            prefixed_table_name(ontology.edge_table(), *SCHEMA_VERSION),
        );
    }

    #[test]
    fn note_has_note_edge_transform_applies_type_mapping() {
        let ontology = test_ontology();
        let plans = assembled_plans(&ontology, 1_000_000);

        let note_plan = plans.namespaced.iter().find(|p| p.name == "Note").unwrap();
        let sql = note_plan
            .transformations()
            .iter()
            .map(|t| t.sql.clone())
            .find(|sql| sql.contains("'HAS_NOTE' AS relationship_kind"))
            .expect("HAS_NOTE transform on Note plan");

        assert!(
            sql.contains("WHEN noteable_type = 'Issue' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("WHEN noteable_type = 'Epic' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(sql.contains("'MergeRequest'"), "sql: {sql}");
        assert!(sql.contains("'Vulnerability'"), "sql: {sql}");
    }

    #[test]
    fn enriched_standalone_edge_extract_sql() {
        let ontology = test_ontology();
        let plans = assembled_plans(&ontology, 1_000_000);

        let plan = plans
            .namespaced
            .iter()
            .find(|p| {
                let sql = render_namespaced_extract(p, "1/2/");
                sql.contains("siphon_issue_assignees")
            })
            .expect("siphon_issue_assignees plan");

        let sql = render_namespaced_extract(plan, "1/2/");

        assert!(sql.contains("WITH _batch AS ("), "sql: {sql}");
        assert!(sql.contains("_e0 AS ("), "sql: {sql}");
        assert!(sql.contains("FROM _batch"), "sql: {sql}");
        assert!(sql.contains("LEFT JOIN _e0"), "sql: {sql}");
        assert!(sql.contains("argMax("), "sql: {sql}");
        assert!(sql.contains("GROUP BY id"), "sql: {sql}");
        assert!(
            sql.contains("id IN (SELECT DISTINCT issue_id FROM _batch)"),
            "sql: {sql}"
        );

        let e0_body = sql
            .split("_e0 AS (")
            .nth(1)
            .and_then(|s| s.split(')').next())
            .unwrap_or("");
        assert!(
            !e0_body.contains("traversal_path"),
            "enrichment CTE body should not filter by traversal_path: {e0_body}"
        );

        assert!(
            plan.transformations()[0].sql.contains("target_tags"),
            "transform should produce target_tags"
        );
    }

    #[test]
    fn node_transform_handles_rename_and_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "blocked".to_string());

        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::Rename {
                source: "admin".to_string(),
                target: "is_admin".to_string(),
            },
            NodeColumn::IntEnum {
                source: "state".to_string(),
                target: "state".to_string(),
                values,
                nullable: false,
            },
        ];

        let sql = node_projection_sql(&columns);
        assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
        assert!(sql.contains("WHEN state = 0 THEN 'active'"), "sql: {sql}");
        assert!(sql.contains("ELSE 'unknown' END AS state"), "sql: {sql}");
    }

    #[test]
    fn node_transform_preserves_nullable_int_enum_nulls() {
        let mut values = BTreeMap::new();
        values.insert(1, "script_failure".to_string());

        let columns = vec![NodeColumn::IntEnum {
            source: "failure_reason".to_string(),
            target: "failure_reason".to_string(),
            values,
            nullable: true,
        }];

        let sql = node_projection_sql(&columns);
        assert!(sql.contains("WHEN failure_reason IS NULL THEN NULL"));
        assert!(sql.contains("WHEN failure_reason = 1 THEN 'script_failure'"));
        assert!(sql.contains("ELSE 'unknown' END AS failure_reason"));
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
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };

        let sql = fk_edge_projection_sql(&fk_edge);

        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
        assert!(sql.contains("(owner_id IS NOT NULL)"));
    }

    #[test]
    fn fk_edge_transform_type_mapping_collapses_raw_values() {
        let mut mapping = BTreeMap::new();
        mapping.insert("Issue".to_string(), "WorkItem".to_string());
        mapping.insert("Epic".to_string(), "WorkItem".to_string());

        let fk_edge = FkEdgeTransform {
            relationship_kind: "HAS_NOTE".to_string(),
            source_id: EdgeId::Column("noteable_id".to_string()),
            source_kind: EdgeKind::Column {
                column: "noteable_type".to_string(),
                mapping,
            },
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("Note".to_string()),
            filters: vec![
                EdgeFilter::IsNotNull("noteable_id".to_string()),
                EdgeFilter::TypeIn {
                    column: "noteable_type".to_string(),
                    types: vec!["MergeRequest".to_string(), "Issue".to_string()],
                },
            ],
            namespaced: true,
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };

        let sql = fk_edge_projection_sql(&fk_edge);
        assert!(
            sql.contains("WHEN noteable_type = 'Issue' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(sql.contains("ELSE noteable_type END"), "sql: {sql}");
        assert!(
            sql.contains("noteable_type IN ('MergeRequest', 'Issue')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn fk_edge_transform_exploded_id_and_array_element() {
        let fk_edge_exploded = FkEdgeTransform {
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
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };
        let sql = fk_edge_projection_sql(&fk_edge_exploded);
        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS BIGINT)"),
            "sql: {sql}"
        );
        assert!(sql.contains("(assignee_ids != '')"), "sql: {sql}");

        let fk_edge_array = FkEdgeTransform {
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
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };
        let sql = fk_edge_projection_sql(&fk_edge_array);
        assert!(sql.contains("unnest(assignees)['user_id']"), "sql: {sql}");
        assert!(sql.contains("(cardinality(assignees) > 0)"), "sql: {sql}");
    }

    #[test]
    fn source_query_produces_template_with_markers() {
        let query = SourceQuerySpec {
            destination_table: "gl_user".to_string(),
            columns: vec![SourceColumn::Bare("id".to_string())],
            source: SourceFrom::Table("siphon_user".to_string()),
            base_table: "siphon_user".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = runtime_plan(query, 1000);
        assert_eq!(plan.watermark_column, "_siphon_watermark");
        assert_eq!(plan.sort_key, vec!["id"]);
        assert_eq!(plan.batch_size, 1000);
        assert!(plan.extract_template.contains("{{filters}}"));
        assert!(plan.extract_template.contains("{{batch_size}}"));
    }

    #[test]
    fn table_source_query_emits_expected_sql() {
        let query = SourceQuerySpec {
            destination_table: "gl_user".to_string(),
            columns: vec![
                SourceColumn::Bare("id".to_string()),
                SourceColumn::Bare("name".to_string()),
            ],
            source: SourceFrom::Table("siphon_user".to_string()),
            base_table: "siphon_user".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = runtime_plan(query, 1000);
        let sql = render_global_extract(&plan);
        assert!(sql.contains("SELECT id, name,"), "sql: {sql}");
        assert!(sql.contains("_siphon_watermark AS _version"), "sql: {sql}");
        assert!(sql.contains("_siphon_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("FROM siphon_user"), "sql: {sql}");
        assert!(sql.contains("ORDER BY id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000"), "sql: {sql}");
    }

    #[test]
    fn source_query_clamps_date_columns() {
        let query = SourceQuerySpec {
            destination_table: "gl_work_item".to_string(),
            columns: vec![SourceColumn::DateClamp("due_date".to_string())],
            source: SourceFrom::Table("siphon_work_items".to_string()),
            base_table: "siphon_work_items".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = runtime_plan(query, 1000);
        let sql = render_global_extract(&plan);
        assert!(
            sql.contains("if(due_date >= toDate('1900-01-01') AND due_date <= toDate('2299-12-31'), due_date, NULL) AS due_date"),
            "sql: {sql}"
        );
    }

    #[test]
    fn raw_source_query_uses_from_and_filter() {
        let query = SourceQuerySpec {
            destination_table: "gl_project".to_string(),
            columns: vec![
                SourceColumn::Bare("project.id AS id".to_string()),
                SourceColumn::Bare(
                    "traversal_paths.traversal_path AS traversal_path".to_string(),
                ),
            ],
            source: SourceFrom::Raw(
                "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id"
                    .to_string(),
            ),
            base_table: "siphon_projects".to_string(),
            watermark: "project._siphon_watermark".to_string(),
            deleted: "project._siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
            namespaced: true,
            traversal_path_filter: Some(
                "startsWith(traversal_path, {traversal_path:String})".to_string(),
            ),
            additional_where: None,
            enrichment: None,
        };

        let plan = runtime_plan(query, 500);
        let sql = render_namespaced_extract(&plan, "1/2/");

        assert!(sql.contains("project.id AS id"), "sql: {sql}");
        assert!(sql.contains("INNER JOIN"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
        assert!(sql.contains("ORDER BY traversal_path, id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 500"), "sql: {sql}");
    }

    #[test]
    fn cursor_filter_renders_dnf_in_extract_sql() {
        let query = SourceQuerySpec {
            destination_table: "gl_user".to_string(),
            columns: vec![SourceColumn::Bare("id".to_string())],
            source: SourceFrom::Table("siphon_user".to_string()),
            base_table: "siphon_user".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = runtime_plan(query, 1000);
        let cursor = Cursor::from_checkpoint(&crate::checkpoint::Checkpoint {
            watermark: Utc::now(),
            cursor_values: Some(vec!["42".to_string()]),
            resume_floor: None,
        });

        let sql = plan
            .prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .with(CursorFilter {
                sort_key: &plan.sort_key,
                values: cursor.values(),
            })
            .to_sql();

        assert!(sql.contains("(id > '42')"), "sql: {sql}");
        assert!(
            sql.contains("_siphon_watermark > {last_watermark:String}"),
            "sql: {sql}"
        );
    }

    // Render every plan derived from the embedded ontology and check the SQL
    // has no leftover markers, no malformed splice, and the structural pieces
    // (watermark, _version/_deleted aliases, namespace-only traversal filter).
    #[test]
    fn every_plan_renders_valid_sql() {
        use ontology::EtlScope;

        let ontology = test_ontology();
        let plans = assembled_plans(&ontology, 1_000_000);

        let cases = plans
            .global
            .iter()
            .map(|p| (p, EtlScope::Global))
            .chain(plans.namespaced.iter().map(|p| (p, EtlScope::Namespaced)));

        let mut count = 0;
        for (plan, scope) in cases {
            count += 1;
            let sql = match scope {
                EtlScope::Global => render_global_extract(plan),
                EtlScope::Namespaced => render_namespaced_extract(plan, "1/2/"),
            };
            let name = &plan.name;

            assert!(
                !sql.contains("{{filters}}"),
                "{name}: unresolved {{filters}}: {sql}"
            );
            assert!(
                !sql.contains("{{batch_size}}"),
                "{name}: unresolved {{batch_size}}: {sql}"
            );
            assert!(
                !sql.contains("WHERE WHERE"),
                "{name}: malformed double-WHERE: {sql}"
            );
            assert!(
                !sql.contains("AND AND"),
                "{name}: malformed double-AND: {sql}"
            );
            assert!(
                sql.contains("_version"),
                "{name}: missing _version alias: {sql}"
            );
            assert!(
                sql.contains("_deleted"),
                "{name}: missing _deleted alias: {sql}"
            );
            assert!(
                sql.contains("> {last_watermark:String}"),
                "{name}: missing watermark lower bound: {sql}"
            );
            assert!(
                sql.contains("<= {watermark:String}"),
                "{name}: missing watermark upper bound: {sql}"
            );
            if scope == EtlScope::Namespaced {
                assert!(
                    sql.contains("startsWith"),
                    "{name}: missing traversal_path filter: {sql}"
                );
            } else {
                assert!(
                    !sql.contains("traversal_path"),
                    "{name}: global plan should not reference traversal_path: {sql}"
                );
            }
        }
        assert!(count > 0, "ontology produced no plans");
    }

    #[test]
    fn system_note_extract_bounds_metadata_join_to_page() {
        let ontology = test_ontology();
        let plans = assembled_plans(&ontology, 10_000);

        let plan = plans
            .namespaced
            .iter()
            .find(|p| p.name == "SystemNote")
            .expect("SystemNote plan");

        let sql = render_namespaced_extract(plan, "1/2/");

        // _batch CTE wraps the base table scan with LIMIT inside the CTE.
        assert!(sql.contains("WITH _batch AS ("), "sql: {sql}");
        assert!(sql.contains("LIMIT 10000"), "sql: {sql}");

        // The base scan inside _batch is the bare siphon_notes table, not the
        // INNER JOIN that previously caused FillingRightJoinSide OOM (#830).
        let batch_body = sql
            .split("WITH _batch AS (")
            .nth(1)
            .and_then(|s| s.split("), _e0 AS (").next())
            .unwrap_or("");
        assert!(
            batch_body.contains("FROM siphon_notes AS sn"),
            "batch body: {batch_body}"
        );
        assert!(
            !batch_body.contains("siphon_system_note_metadata"),
            "_batch must not join the metadata table: {batch_body}"
        );

        // Enrichment CTE scopes metadata read to the page's note IDs.
        assert!(
            sql.contains("note_id IN (SELECT DISTINCT id FROM _batch)"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("LEFT JOIN _e0 ON _batch.id = _e0.id"),
            "sql: {sql}"
        );
        assert!(sql.contains("_e0.action AS action"), "sql: {sql}");
        assert!(sql.contains("snm._siphon_deleted = false"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(snm.traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
    }
}
