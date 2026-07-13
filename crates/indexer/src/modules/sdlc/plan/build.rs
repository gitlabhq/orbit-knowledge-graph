//! Walks the ontology and builds each pipeline as an extract plus a transform.

use std::collections::{HashMap, HashSet};

use indexmap::IndexSet;
use ontology::{
    EdgeMapping, EtlScope, Extract, ExtractQuery, NodeEntity, NodeRefKind, Ontology, Pipeline,
    Transform,
    constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN},
};

use super::extract::{EnrichmentJoin, ExtractDecl, ExtractSpec, SourceColumn, generated, sql};
use super::transform;
use super::{Plan, Plans, TransformSpec};

pub(in crate::modules::sdlc) struct Sizing<'a> {
    pub global_batch_size: u64,
    pub namespaced_batch_size: u64,
    pub overrides: &'a HashMap<String, u64>,
}

impl Sizing<'_> {
    fn resolve(&self, name: &str, scope: EtlScope) -> u64 {
        let default = match scope {
            EtlScope::Global => self.global_batch_size,
            EtlScope::Namespaced => self.namespaced_batch_size,
        };
        self.overrides.get(name).copied().unwrap_or(default)
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::modules::sdlc) enum PlanError {
    #[error(
        "duplicate plan name '{0}': plan names are handler names and checkpoint keys and must be unique"
    )]
    DuplicateName(String),
    #[error("malformed extract template: {0}")]
    MalformedTemplate(String),
    #[error("edge '{relationship_kind}' pipeline '{pipeline}' has an unexpected transform shape")]
    UnexpectedTransform {
        relationship_kind: String,
        pipeline: String,
    },
    #[error(
        "derived entity pipeline '{0}' declares a datafusion transform; the plan builder only implements Rust transforms for derived pipelines"
    )]
    UnsupportedDerivedTransform(String),
    #[error(
        "derived entity pipeline '{0}' must carry an authored .sql extract; its rows are neither node properties nor edge endpoints to generate a projection from"
    )]
    DerivedRequiresSql(String),
}

pub(in crate::modules::sdlc) fn build_plans(
    ontology: &Ontology,
    sizing: Sizing<'_>,
) -> Result<Plans, PlanError> {
    let mut plans = PlanSet::default();

    for node in ontology.nodes() {
        for pipeline in &node.pipelines {
            plans.insert(node_plan(node, pipeline, ontology, &sizing)?)?;
        }
    }

    for (relationship_kind, pipeline) in ontology.edge_etl_configs() {
        plans.insert(edge_plan(relationship_kind, pipeline, ontology, &sizing)?)?;
    }

    for derived in ontology.derived_entities() {
        for pipeline in &derived.pipelines {
            plans.insert(derived_plan(pipeline, &sizing)?)?;
        }
    }

    Ok(plans.into_plans())
}

fn node_plan(
    node: &NodeEntity,
    pipeline: &Pipeline,
    ontology: &Ontology,
    sizing: &Sizing<'_>,
) -> Result<Plan, PlanError> {
    let Extract::ClickHouse(extract) = &pipeline.extract;
    let namespaced = pipeline.scope == EtlScope::Namespaced;

    let decl = ExtractDecl::of(pipeline);
    let (spec, _) = match &extract.query {
        ExtractQuery::Generated { filter } => {
            let columns = SourceColumn::from_node(node);
            let node_refs = fk_node_ref_columns(node, pipeline.transform.edges(), namespaced);
            generated::build(
                &decl,
                generated::Shape::Node {
                    columns: &columns,
                    node_ref_columns: &node_refs,
                },
                filter.as_deref(),
            )?
        }
        ExtractQuery::Sql(raw) => sql::build(&decl, raw)?,
    };

    let transform =
        transform::node_transform(node, pipeline.transform.edges(), namespaced, ontology);

    Ok(assemble(
        pipeline.name.clone(),
        pipeline.name.clone(),
        pipeline.scope,
        spec,
        transform,
        sizing,
    ))
}

fn edge_plan(
    relationship_kind: &str,
    pipeline: &Pipeline,
    ontology: &Ontology,
    sizing: &Sizing<'_>,
) -> Result<Plan, PlanError> {
    let Transform::DataFusion { edges } = &pipeline.transform else {
        return Err(PlanError::UnexpectedTransform {
            relationship_kind: relationship_kind.to_string(),
            pipeline: pipeline.name.clone(),
        });
    };

    let [mapping] = edges.as_slice() else {
        return Err(PlanError::UnexpectedTransform {
            relationship_kind: relationship_kind.to_string(),
            pipeline: pipeline.name.clone(),
        });
    };

    let Extract::ClickHouse(extract) = &pipeline.extract;
    let decl = ExtractDecl::of(pipeline);

    let (spec, batch_schema) = match &extract.query {
        ExtractQuery::Generated { filter } => {
            let joins = EnrichmentJoin::from_mapping(mapping, pipeline.scope, ontology);
            if joins.is_empty() {
                let columns = edge_single_table_columns(mapping);
                generated::build(
                    &decl,
                    generated::Shape::SingleTable { columns: &columns },
                    filter.as_deref(),
                )?
            } else {
                let batch = SourceColumn::bare_all(&edge_batch_columns(mapping, &extract.order_by));
                generated::build(
                    &decl,
                    generated::Shape::Enriched {
                        batch_columns: &batch,
                        joins: &joins,
                    },
                    filter.as_deref(),
                )?
            }
        }
        ExtractQuery::Sql(raw) => sql::build(&decl, raw)?,
    };

    let transform = transform::edge_transform(
        relationship_kind,
        mapping,
        pipeline.scope,
        &batch_schema,
        ontology,
    );

    Ok(assemble(
        pipeline.name.clone(),
        relationship_kind.to_string(),
        pipeline.scope,
        spec,
        transform,
        sizing,
    ))
}

fn derived_plan(pipeline: &Pipeline, sizing: &Sizing<'_>) -> Result<Plan, PlanError> {
    let Transform::Rust(name) = &pipeline.transform else {
        return Err(PlanError::UnsupportedDerivedTransform(
            pipeline.name.clone(),
        ));
    };
    let Extract::ClickHouse(extract) = &pipeline.extract;
    let decl = ExtractDecl::of(pipeline);
    let ExtractQuery::Sql(raw) = &extract.query else {
        return Err(PlanError::DerivedRequiresSql(pipeline.name.clone()));
    };
    let (spec, _) = sql::build(&decl, raw)?;

    let transform = transform::rust_transform(name);

    Ok(assemble(
        pipeline.name.clone(),
        pipeline.name.clone(),
        pipeline.scope,
        spec,
        transform,
        sizing,
    ))
}

/// FK node-ref columns to append, skipping ones the node already selects.
fn fk_node_ref_columns(node: &NodeEntity, edges: &[EdgeMapping], namespaced: bool) -> Vec<String> {
    let node_columns: HashSet<&str> = node.fields.iter().filter_map(|f| f.column_name()).collect();

    let mut columns = IndexSet::new();
    for mapping in edges {
        columns.extend(node_ref_columns(mapping));
    }
    if namespaced {
        columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
    }

    columns
        .into_iter()
        .filter(|column| !node_columns.contains(column.as_str()))
        .collect()
}

/// Both node-ref fields plus, for `Derived` node refs, the column the kind is derived from.
fn node_ref_columns(mapping: &EdgeMapping) -> IndexSet<String> {
    let mut columns = IndexSet::new();
    columns.insert(mapping.source.field.clone());
    columns.insert(mapping.target.field.clone());
    for node_ref in [&mapping.source, &mapping.target] {
        if let NodeRefKind::Derived { column, .. } = &node_ref.kind {
            columns.insert(column.clone());
        }
    }
    columns
}

fn edge_single_table_columns(mapping: &EdgeMapping) -> Vec<String> {
    let mut columns = node_ref_columns(mapping);
    columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
    columns.insert(DEFAULT_PRIMARY_KEY.to_string());
    columns.into_iter().collect()
}

fn edge_batch_columns(mapping: &EdgeMapping, order_by: &[String]) -> Vec<String> {
    let mut columns = node_ref_columns(mapping);
    columns.extend(order_by.iter().cloned());
    columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
    columns.into_iter().collect()
}

fn assemble(
    name: String,
    target: String,
    scope: EtlScope,
    spec: ExtractSpec,
    transform: TransformSpec,
    sizing: &Sizing<'_>,
) -> Plan {
    let batch_size = sizing.resolve(&name, scope);
    Plan {
        name,
        target,
        scope,
        extract_template: spec.template,
        watermark_column: spec.watermark,
        deleted_column: spec.deleted,
        sort_key: spec.order_by,
        batch_size,
        transform,
    }
}

#[derive(Default)]
struct PlanSet {
    global: Vec<Plan>,
    namespaced: Vec<Plan>,
    seen: std::collections::HashSet<String>,
}

impl PlanSet {
    fn insert(&mut self, plan: Plan) -> Result<(), PlanError> {
        if !self.seen.insert(plan.name.clone()) {
            return Err(PlanError::DuplicateName(plan.name));
        }
        match plan.scope {
            EtlScope::Global => self.global.push(plan),
            EtlScope::Namespaced => self.namespaced.push(plan),
        }
        Ok(())
    }

    fn into_plans(self) -> Plans {
        Plans {
            global: self.global,
            namespaced: self.namespaced,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::Checkpoint;
    use crate::modules::sdlc::plan::{
        Cursor, CursorFilter, DeletedFilter, Plan, TransformSpec, TraversalPathFilter,
        WatermarkFilter,
    };
    use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
    use chrono::{DateTime, Utc};

    fn test_ontology() -> Ontology {
        Ontology::load_embedded().expect("should load ontology")
    }

    fn plans(ontology: &Ontology, batch_size: u64) -> Plans {
        build_plans(
            ontology,
            Sizing {
                global_batch_size: batch_size,
                namespaced_batch_size: batch_size,
                overrides: &HashMap::new(),
            },
        )
        .expect("plans should build")
    }

    fn normalize(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn timestamp(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("timestamp literal should parse")
            .with_timezone(&Utc)
    }

    fn render_namespaced(plan: &Plan, path: &str) -> String {
        plan.prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .with(TraversalPathFilter { path })
            .to_sql()
            .expect("renders extract SQL")
    }

    fn render_global(plan: &Plan) -> String {
        plan.prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .to_sql()
            .expect("renders extract SQL")
    }

    #[test]
    fn build_plans_partitions_by_scope() {
        let plans = plans(&test_ontology(), 1_000_000);
        let global: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        let namespaced: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();
        assert!(global.contains(&"User"));
        assert!(namespaced.contains(&"Group"));
        assert!(namespaced.contains(&"Project"));
    }

    #[test]
    fn batch_size_override_applies_to_named_pipeline() {
        let ontology = test_ontology();
        let overrides = HashMap::from([("WorkItem".to_string(), 50_000u64)]);
        let built = build_plans(
            &ontology,
            Sizing {
                global_batch_size: 1_000_000,
                namespaced_batch_size: 1_000_000,
                overrides: &overrides,
            },
        )
        .expect("plans should build");

        let work_item = built
            .namespaced
            .iter()
            .find(|p| p.name == "WorkItem")
            .expect("WorkItem plan should exist");
        assert_eq!(work_item.batch_size, 50_000);

        let group = built
            .namespaced
            .iter()
            .find(|p| p.name == "Group")
            .expect("Group plan should exist");
        assert_eq!(group.batch_size, 1_000_000);
    }

    #[test]
    fn embedded_ontology_yields_unique_plan_names() {
        // `build_plans` rejects duplicate names, so building without error is the assertion.
        let _ = plans(&test_ontology(), 1000);
    }

    #[test]
    fn duplicate_plan_name_is_rejected() {
        let mut set = PlanSet::default();
        let plan = |name: &str| {
            Plan {
            name: name.to_string(),
            target: name.to_string(),
            scope: EtlScope::Namespaced,
            extract_template: crate::modules::sdlc::plan::ExtractTemplate::new(
                "SELECT x AS _version, y AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}"
                    .to_string(),
            )
            .unwrap(),
            watermark_column: String::new(),
            deleted_column: String::new(),
            sort_key: vec![],
            batch_size: 1,
            transform: TransformSpec::DataFusion(vec![]),
        }
        };
        set.insert(plan("dup")).expect("first insert");
        let err = set.insert(plan("dup")).expect_err("collision should fail");
        assert!(matches!(err, PlanError::DuplicateName(name) if name == "dup"));
    }

    #[test]
    fn node_plan_includes_fk_edge_transforms() {
        let ontology = test_ontology();
        let built = plans(&ontology, 1_000_000);

        let note = built.namespaced.iter().find(|p| p.name == "Note").unwrap();
        let TransformSpec::DataFusion(transforms) = &note.transform else {
            panic!("Note should be a datafusion transform");
        };
        assert!(transforms.len() >= 2);
        assert_eq!(
            transforms[0].destination_table,
            prefixed_table_name("gl_note", *SCHEMA_VERSION),
        );
        assert_eq!(
            transforms[1].destination_table,
            prefixed_table_name(ontology.edge_table(), *SCHEMA_VERSION),
        );
    }

    #[test]
    fn note_has_note_edge_transform_applies_type_mapping() {
        let built = plans(&test_ontology(), 1_000_000);
        let note = built.namespaced.iter().find(|p| p.name == "Note").unwrap();
        let TransformSpec::DataFusion(transforms) = &note.transform else {
            panic!("Note should be a datafusion transform");
        };
        let sql = transforms
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
    fn milestone_and_work_item_date_columns_are_clamped() {
        let built = plans(&test_ontology(), 1000);
        for name in ["WorkItem", "Milestone"] {
            let plan = built.namespaced.iter().find(|p| p.name == name).unwrap();
            let sql = plan.extract_template.as_str();
            for col in ["due_date", "start_date"] {
                assert!(
                    sql.contains(&format!(
                        "if({col} >= toDate('1900-01-01') AND {col} <= toDate('2299-12-31'), {col}, NULL) AS {col}"
                    )),
                    "{name}.{col} must be date-clamped: {sql}"
                );
            }
        }
    }

    #[test]
    fn has_label_standalone_plan_keeps_raw_type_mapping_keys() {
        let built = plans(&test_ontology(), 1_000_000);
        let plan = built
            .namespaced
            .iter()
            .find(|p| p.name == "HAS_LABEL_siphon_label_links_Label")
            .expect("HAS_LABEL standalone plan");
        let TransformSpec::DataFusion(transforms) = &plan.transform else {
            panic!("HAS_LABEL should be datafusion");
        };
        let sql = &transforms[0].sql;
        assert!(
            sql.contains("target_type IN (") && sql.contains("'Issue'"),
            "filter must include the raw Rails value that maps to WorkItem: {sql}"
        );
        assert!(
            sql.contains("'MergeRequest'"),
            "filter must keep the ontology-native MergeRequest value: {sql}"
        );
    }

    #[test]
    fn reopened_standalone_plans_carry_state_filter_for_both_issuable_sides() {
        // Source of truth: app/models/resource_state_event.rb (`enum :state ... reopened: 5`).
        const REOPENED_STATE: i64 = 5;

        let built = plans(&test_ontology(), 1000);
        let reopened: Vec<&Plan> = built
            .namespaced
            .iter()
            .filter(|p| p.name.starts_with("REOPENED_siphon_resource_state_events"))
            .collect();
        assert_eq!(
            reopened.len(),
            2,
            "REOPENED has an MR-side and a WorkItem-side ETL"
        );

        let expected = format!("state = {REOPENED_STATE}");
        for plan in &reopened {
            assert!(
                plan.extract_template.as_str().contains(&expected),
                "{}: {}",
                plan.name,
                plan.extract_template.as_str()
            );
        }
        assert!(reopened.iter().any(|p| p.name.ends_with("_MergeRequest")));
        assert!(reopened.iter().any(|p| p.name.ends_with("_WorkItem")));
    }

    #[test]
    fn enriched_standalone_edge_extract_sql() {
        let built = plans(&test_ontology(), 1_000_000);
        let plan = built
            .namespaced
            .iter()
            .find(|p| render_namespaced(p, "1/2/").contains("siphon_issue_assignees"))
            .expect("siphon_issue_assignees plan");

        let sql = render_namespaced(plan, "1/2/");
        let normalized = normalize(&sql);
        assert!(normalized.contains("WITH _batch AS ("), "sql: {sql}");
        assert!(normalized.contains("_e0 AS ("), "sql: {sql}");
        assert!(normalized.contains("FROM _batch"), "sql: {sql}");
        assert!(normalized.contains("LEFT JOIN _e0"), "sql: {sql}");
        assert!(normalized.contains("argMax("), "sql: {sql}");
        assert!(normalized.contains("GROUP BY id"), "sql: {sql}");
        assert!(
            normalized.contains("id IN (SELECT DISTINCT issue_id FROM _batch)"),
            "sql: {sql}"
        );

        let e0_body = normalized
            .split("_e0 AS (")
            .nth(1)
            .and_then(|s| s.split(')').next())
            .unwrap_or("");
        assert!(
            !e0_body.contains("traversal_path"),
            "User is a global node with no traversal_path column, so _e0 must not prune: {e0_body}"
        );
        assert!(
            normalized.contains(
                "issue_id FROM _batch) AND startsWith(traversal_path, {traversal_path:String})"
            ),
            "sql: {sql}"
        );

        let TransformSpec::DataFusion(transforms) = &plan.transform else {
            panic!("edge should be datafusion");
        };
        assert!(
            transforms[0].sql.contains("target_tags"),
            "transform should produce target_tags"
        );
    }

    #[test]
    fn system_note_extract_bounds_metadata_join_to_page() {
        let built = plans(&test_ontology(), 10_000);
        let plan = built
            .namespaced
            .iter()
            .find(|p| p.name == "SystemNote")
            .expect("SystemNote plan");

        let sql = render_namespaced(plan, "1/2/");
        let normalized = normalize(&sql);

        assert!(normalized.contains("WITH _batch AS ("), "sql: {sql}");
        assert!(normalized.contains("LIMIT 10000"), "sql: {sql}");

        // Bare siphon_notes scan, not the INNER JOIN that caused FillingRightJoinSide OOM (#830).
        let batch_body = normalized
            .split("WITH _batch AS (")
            .nth(1)
            .and_then(|s| s.split("), _e0 AS (").next())
            .unwrap_or("");
        assert!(
            batch_body.contains("FROM siphon_notes"),
            "batch body: {batch_body}"
        );
        assert!(
            !batch_body.contains("siphon_system_note_metadata"),
            "_batch must not join the metadata table: {batch_body}"
        );
        assert!(
            normalized.contains("note_id IN (SELECT DISTINCT id FROM _batch)"),
            "sql: {sql}"
        );
        assert!(
            normalized.contains("LEFT JOIN _e0 ON _batch.id = _e0.id"),
            "sql: {sql}"
        );
        assert!(normalized.contains("_e0.action AS action"), "sql: {sql}");
        // The #830 invariants, now generated: metadata join page-bounded and namespace-scoped.
        assert!(
            normalized.contains("FROM siphon_system_note_metadata"),
            "sql: {sql}"
        );
        assert!(
            normalized.contains("note_id IN (SELECT DISTINCT id FROM _batch) AND startsWith(traversal_path, {traversal_path:String}) GROUP BY note_id HAVING argMax(_siphon_deleted, _siphon_watermark) = false"),
            "sql: {sql}"
        );
    }

    #[test]
    fn cursor_filter_renders_dnf_in_extract_sql() {
        let built = plans(&test_ontology(), 1000);
        let user = built.global.iter().find(|p| p.name == "User").unwrap();
        let cursor = Cursor::from_checkpoint(&Checkpoint {
            watermark: Utc::now(),
            cursor_values: Some(vec!["42".to_string()]),
            resume_floor: None,
        });
        let sql = user
            .prepare()
            .with(WatermarkFilter {
                column: &user.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .with(CursorFilter {
                sort_key: &user.sort_key,
                values: cursor.values(),
            })
            .to_sql()
            .expect("renders extract SQL");
        assert!(sql.contains("(id > '42')"), "sql: {sql}");
        assert!(
            sql.contains("_siphon_watermark > {last_watermark:String}"),
            "sql: {sql}"
        );
    }

    #[test]
    fn every_plan_renders_valid_sql() {
        let built = plans(&test_ontology(), 1_000_000);
        let cases = built
            .global
            .iter()
            .map(|p| (p, EtlScope::Global))
            .chain(built.namespaced.iter().map(|p| (p, EtlScope::Namespaced)));

        let mut count = 0;
        for (plan, scope) in cases {
            count += 1;
            let sql = match scope {
                EtlScope::Global => render_global(plan),
                EtlScope::Namespaced => render_namespaced(plan, "1/2/"),
            };
            let name = &plan.name;
            assert!(
                !sql.contains("{{filters}}"),
                "{name}: unresolved filters: {sql}"
            );
            assert!(
                !sql.contains("{{batch_size}}"),
                "{name}: unresolved batch_size: {sql}"
            );
            assert!(!sql.contains("WHERE WHERE"), "{name}: double-WHERE: {sql}");
            assert!(!sql.contains("AND AND"), "{name}: double-AND: {sql}");
            assert!(sql.contains("_version"), "{name}: missing _version: {sql}");
            assert!(sql.contains("_deleted"), "{name}: missing _deleted: {sql}");
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
                    "{name}: missing traversal filter: {sql}"
                );
            } else {
                assert!(
                    !sql.contains("traversal_path"),
                    "{name}: global plan references traversal_path: {sql}"
                );
            }
        }
        assert!(count > 0, "ontology produced no plans");
    }

    #[test]
    fn embedded_extract_sql_matches_golden_snapshot() {
        let built = plans(&test_ontology(), 1000);
        let mut cases: Vec<_> = built
            .global
            .iter()
            .map(|plan| (EtlScope::Global, plan))
            .chain(
                built
                    .namespaced
                    .iter()
                    .map(|plan| (EtlScope::Namespaced, plan)),
            )
            .collect();
        cases.sort_by(|(_, left), (_, right)| left.name.cmp(&right.name));

        let mut actual = String::new();
        for (scope, plan) in cases {
            actual.push_str(&format!("=== {} ===\n", plan.name));
            actual.push_str(&runtime_sql(scope, plan));
            actual.push_str("\n\n");
        }

        let expected = include_str!("../../../../tests/golden/extract_sql.txt");
        assert_eq!(normalize(&actual), normalize(expected));
    }

    fn runtime_sql(scope: EtlScope, plan: &Plan) -> String {
        let last = timestamp("2024-01-01T00:00:00Z");
        let current = timestamp("2024-01-02T00:00:00Z");
        plan.prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last,
                current,
            })
            .with((scope == EtlScope::Namespaced).then_some(TraversalPathFilter { path: "1/" }))
            .with(Some(DeletedFilter {
                column: &plan.deleted_column,
            }))
            .with(CursorFilter {
                sort_key: &plan.sort_key,
                values: Cursor::first_page().values(),
            })
            .to_sql()
            .expect("renders extract SQL")
    }
}
