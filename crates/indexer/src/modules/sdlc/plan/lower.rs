use ontology::EtlScope;

use super::codegen;
use super::input::{NodePlan, PlanInput, StandaloneEdgePlan};
use super::{ExtractQuery, PipelinePlan, Plans, Transformation};

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
    let extract_query = ExtractQuery::new(input.extract, batch_size);

    let mut transforms = vec![Transformation::new(
        codegen::emit_node_transform_sql(&input.columns),
        node_destination,
    )];

    for fk_edge in &input.edges {
        transforms.push(Transformation::new(
            codegen::emit_fk_edge_transform_sql(fk_edge),
            edge_table.to_string(),
        ));
    }

    PipelinePlan {
        name: input.name,
        extract_query,
        transforms,
    }
}

fn lower_standalone_edge_plan(input: StandaloneEdgePlan, batch_size: u64) -> PipelinePlan {
    let destination_table = input.extract.destination_table.clone();
    let extract_query = ExtractQuery::new(input.extract, batch_size);

    let transform_sql = codegen::emit_edge_transform_sql(
        &input.source_id,
        &input.source_kind,
        &input.relationship_kind,
        &input.target_id,
        &input.target_kind,
        &input.filters,
        input.namespaced,
    );

    PipelinePlan {
        name: input.relationship_kind,
        extract_query,
        transforms: vec![Transformation::new(transform_sql, destination_table)],
    }
}

#[cfg(test)]
mod tests {
    use super::super::input;
    use super::*;
    use std::collections::BTreeMap;

    use super::super::input::{
        EdgeFilter, EdgeId, EdgeKind, ExtractColumn, ExtractPlan, ExtractSource, FkEdgeTransform,
        NodeColumn,
    };

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

        let sql = codegen::emit_node_transform_sql(&columns);
        assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
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

        let sql = codegen::emit_node_transform_sql(&columns);
        assert!(sql.contains("CASE"), "sql: {sql}");
        assert!(sql.contains("WHEN state = 0 THEN 'active'"), "sql: {sql}");
        assert!(sql.contains("WHEN state = 1 THEN 'blocked'"), "sql: {sql}");
        assert!(sql.contains("ELSE 'unknown' END AS state"), "sql: {sql}");
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

        let sql = codegen::emit_fk_edge_transform_sql(&fk_edge);

        assert!(sql.contains("id AS source_id"), "sql: {sql}");
        assert!(sql.contains("'Group' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("owner_id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'User' AS target_kind"), "sql: {sql}");
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

        let sql = codegen::emit_fk_edge_transform_sql(&fk_edge);

        assert!(sql.contains("author_id AS source_id"), "sql: {sql}");
        assert!(sql.contains("'User' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'Note' AS target_kind"), "sql: {sql}");
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

        let sql = codegen::emit_fk_edge_transform_sql(&fk_edge);

        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS BIGINT)"),
            "sql: {sql}"
        );
        assert!(sql.contains("'User' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("id AS target_id"), "sql: {sql}");
        assert!(sql.contains("'WorkItem' AS target_kind"), "sql: {sql}");
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

        let sql = ExtractQuery::new(extract, 1000).to_sql();

        assert!(sql.contains("SELECT "), "sql: {sql}");
        assert!(sql.contains("id"), "sql: {sql}");
        assert!(sql.contains("name"), "sql: {sql}");
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

        let sql = ExtractQuery::new(extract, 500).to_sql();

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
        assert!(
            sql.contains("ORDER BY traversal_path ASC, id ASC"),
            "sql: {sql}"
        );
        assert!(sql.contains("LIMIT 500"), "sql: {sql}");
    }
}
