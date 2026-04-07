//! Tests for virtual column resolution dispatch logic.
//!
//! Exercises `resolve_virtual_columns` with `MockColumnResolver` — no
//! ClickHouse or Gitaly needed.
//!
//! Also tests compiler-level virtual column handling: stripping from
//! Search/Aggregation queries, preserving for Traversal, and injecting
//! depends_on columns.

use std::collections::HashMap;
use std::sync::Arc;

use gkg_server::content::{ColumnResolverRegistry, PropertyRow};
use gkg_server::pipeline::HydrationStage;
use gkg_utils::arrow::ColumnValue;
use ontology::Ontology;
use query_engine::compiler::{SecurityContext, VirtualColumnRequest};
use query_engine::pipeline::{PipelineError, QueryPipelineContext, TypeMap};

use super::common::MockColumnResolver;

fn test_ctx() -> QueryPipelineContext {
    test_ctx_with_batch_size(100)
}

fn test_ctx_with_batch_size(max_batch_size: usize) -> QueryPipelineContext {
    let mut registry = ColumnResolverRegistry::new().with_max_batch_size(max_batch_size);
    registry.register("gitaly", Arc::new(MockColumnResolver));

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(registry);

    QueryPipelineContext {
        query_json: String::new(),
        compiled: None,
        ontology: Arc::new(Ontology::new()),
        security_context: Some(SecurityContext::new(1, vec!["1/2/".into()]).unwrap()),
        server_extensions,
        phases: TypeMap::default(),
    }
}

type PropertyMap = HashMap<(String, i64), PropertyRow>;

fn file_property_map() -> PropertyMap {
    let mut props = PropertyRow::new();
    props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));
    let mut map = PropertyMap::new();
    map.insert(("File".into(), 1), props.clone());
    map.insert(("File".into(), 2), props);
    map
}

#[tokio::test]
async fn skips_when_no_virtual_columns() {
    let ctx = test_ctx();
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &[])];
    let mut map = file_property_map();
    let original_len = map.values().next().unwrap().len();

    HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap();

    assert_eq!(map.values().next().unwrap().len(), original_len);
}

#[tokio::test]
async fn merges_results_into_property_map() {
    let ctx = test_ctx();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap();

    for props in map.values() {
        assert_eq!(
            props.get("content"),
            Some(&ColumnValue::String("mock:blob_content".into()))
        );
    }
}

#[tokio::test]
async fn errors_without_registry() {
    let ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: None,
        ontology: Arc::new(Ontology::new()),
        security_context: Some(SecurityContext::new(1, vec!["1/2/".into()]).unwrap()),
        server_extensions: TypeMap::default(),
        phases: TypeMap::default(),
    };
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    let err = HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap_err();

    assert!(matches!(err, PipelineError::ContentResolution(_)));
}

#[tokio::test]
async fn errors_for_unknown_service() {
    let ctx = test_ctx();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "unknown_service".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    let err = HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap_err();

    assert!(
        matches!(&err, PipelineError::ContentResolution(msg) if msg.contains("unknown_service"))
    );
}

#[tokio::test]
async fn skips_unmatched_entity_type() {
    let ctx = test_ctx();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("Definition", &vcrs)];
    let mut map = file_property_map();
    let original_len = map.values().next().unwrap().len();

    HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap();

    assert_eq!(map.values().next().unwrap().len(), original_len);
}

#[tokio::test]
async fn errors_when_batch_size_exceeded() {
    let ctx = test_ctx_with_batch_size(1);
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map(); // 2 File entries, limit is 1

    let err = HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap_err();

    assert!(matches!(&err, PipelineError::ContentResolution(msg) if msg.contains("batch size")));
}

// ─────────────────────────────────────────────────────────────────────────────
// Compiler-level virtual column tests
// ─────────────────────────────────────────────────────────────────────────────

mod compiler_integration {
    use ontology::Ontology;
    use query_engine::compiler::{SecurityContext, compile};

    fn compile_query(json: &str) -> query_engine::compiler::CompiledQueryContext {
        let ontology = Ontology::load_embedded().unwrap();
        let security_ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        compile(json, &ontology, &security_ctx).unwrap()
    }

    #[test]
    fn search_with_wildcard_excludes_virtual_columns_from_sql() {
        let compiled = compile_query(
            r#"{"query_type": "search", "node": {"id": "f", "entity": "File", "columns": "*"}, "limit": 5}"#,
        );
        let sql = &compiled.base.sql;
        // "content" should not appear anywhere in the SQL for a search query
        assert!(
            !sql.contains("content"),
            "virtual column 'content' should not appear in search SQL, got:\n{sql}"
        );
        // But normal columns should be present
        assert!(
            sql.contains("f_name") || sql.contains("f.name"),
            "normal columns should be in SQL"
        );
    }

    #[test]
    fn search_with_explicit_content_excludes_from_sql() {
        let compiled = compile_query(
            r#"{"query_type": "search", "node": {"id": "f", "entity": "File", "columns": ["id", "name", "content"]}, "limit": 5}"#,
        );
        let sql = &compiled.base.sql;
        assert!(
            !sql.contains("content"),
            "explicitly requested virtual column 'content' should be stripped from search SQL"
        );
        assert!(sql.contains("f_name") || sql.contains("f.name"));
    }

    #[test]
    fn search_with_content_produces_hydration_plan() {
        // Search with virtual columns should produce a Static hydration plan
        // so content resolution can happen post-query.
        let compiled = compile_query(
            r#"{"query_type": "search", "node": {"id": "f", "entity": "File", "columns": ["id", "name", "content"]}, "limit": 5}"#,
        );
        match &compiled.hydration {
            query_engine::compiler::HydrationPlan::Static(templates) => {
                assert_eq!(templates.len(), 1);
                let t = &templates[0];
                assert_eq!(t.entity_type, "File");
                assert_eq!(t.node_alias, "f");
                // Should have the content VCR
                assert!(
                    t.virtual_columns
                        .iter()
                        .any(|vc| vc.column_name == "content" && vc.service == "gitaly"),
                    "search hydration plan should include content VCR"
                );
                // DB columns list should be empty (search already has them)
                // but depends_on columns should be present for the resolver
                for dep in &["project_id", "commit_sha", "branch", "path"] {
                    assert!(
                        t.columns.contains(&dep.to_string()),
                        "depends_on column '{dep}' should be in search hydration plan"
                    );
                }
            }
            other => panic!(
                "expected Static hydration plan for search with virtual cols, got: {other:?}"
            ),
        }
    }

    #[test]
    fn search_without_content_has_no_hydration_plan() {
        // Search without virtual columns should still produce HydrationPlan::None.
        let compiled = compile_query(
            r#"{"query_type": "search", "node": {"id": "f", "entity": "File", "columns": ["id", "name", "path"]}, "limit": 5}"#,
        );
        assert!(
            matches!(
                &compiled.hydration,
                query_engine::compiler::HydrationPlan::None
            ),
            "search without virtual cols should have HydrationPlan::None, got: {:?}",
            compiled.hydration
        );
    }

    #[test]
    fn aggregation_with_content_produces_hydration_plan() {
        let compiled = compile_query(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "f", "entity": "File", "columns": ["id", "content"]}],
                "aggregations": [{"function": "count", "target": "f", "alias": "total"}],
                "limit": 5
            }"#,
        );
        match &compiled.hydration {
            query_engine::compiler::HydrationPlan::Static(templates) => {
                assert!(
                    templates.iter().any(|t| t
                        .virtual_columns
                        .iter()
                        .any(|vc| vc.column_name == "content")),
                    "aggregation hydration plan should include content VCR"
                );
            }
            other => panic!(
                "expected Static hydration plan for aggregation with virtual cols, got: {other:?}"
            ),
        }
    }

    #[test]
    fn aggregation_with_wildcard_excludes_virtual_columns_from_sql() {
        let compiled = compile_query(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "f", "entity": "File", "columns": "*"}],
                "aggregations": [{"function": "count", "target": "f", "alias": "total"}],
                "limit": 5
            }"#,
        );
        let sql = &compiled.base.sql;
        assert!(
            !sql.contains("content"),
            "virtual column 'content' should not appear in aggregation SQL"
        );
    }

    #[test]
    fn traversal_with_content_includes_virtual_in_hydration_plan() {
        let compiled = compile_query(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "f", "entity": "File", "columns": ["id", "name", "content"]},
                    {"id": "b", "entity": "Branch", "columns": ["id", "name"]}
                ],
                "relationships": [{"type": "ON_BRANCH", "from": "f", "to": "b"}],
                "limit": 5
            }"#,
        );

        // The base SQL should NOT contain "content" (it's resolved post-query)
        let sql = &compiled.base.sql;
        assert!(
            !sql.contains("f_content") && !sql.contains("f.content"),
            "virtual column should not be in traversal base SQL"
        );

        // But the hydration plan should have a virtual column request
        match &compiled.hydration {
            query_engine::compiler::HydrationPlan::Static(templates) => {
                let file_template = templates.iter().find(|t| t.entity_type == "File");
                assert!(
                    file_template.is_some(),
                    "File should have a hydration template"
                );
                let vcs = &file_template.unwrap().virtual_columns;
                assert!(
                    vcs.iter()
                        .any(|vc| vc.column_name == "content" && vc.service == "gitaly"),
                    "hydration plan should include content virtual column, got: {vcs:?}"
                );
            }
            other => panic!("expected Static hydration plan for traversal, got: {other:?}"),
        }
    }

    #[test]
    fn traversal_hydration_injects_depends_on_columns() {
        // Request only "content" (virtual) -- the hydration plan should still
        // include depends_on columns (project_id, commit_sha, branch, path)
        // so the resolver has the data it needs.
        let compiled = compile_query(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "b", "entity": "Branch", "columns": ["id", "name"]},
                    {"id": "f", "entity": "File", "columns": ["id", "content"]}
                ],
                "relationships": [{"type": "ON_BRANCH", "from": "f", "to": "b"}],
                "limit": 5
            }"#,
        );

        match &compiled.hydration {
            query_engine::compiler::HydrationPlan::Static(templates) => {
                let file_template = templates.iter().find(|t| t.entity_type == "File").unwrap();
                // depends_on for File.content: [project_id, commit_sha, branch, path]
                let cols = &file_template.columns;
                for dep in &["project_id", "commit_sha", "branch", "path"] {
                    assert!(
                        cols.iter().any(|c| c == dep),
                        "depends_on column '{dep}' should be auto-injected into hydration columns, got: {cols:?}"
                    );
                }
            }
            other => panic!("expected Static hydration plan, got: {other:?}"),
        }
    }

    #[test]
    fn definition_content_also_handled() {
        let compiled = compile_query(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "f", "entity": "File", "columns": ["id", "path"]},
                    {"id": "def", "entity": "Definition", "columns": ["id", "name", "content"]}
                ],
                "relationships": [{"type": "DEFINES", "from": "f", "to": "def"}],
                "limit": 5
            }"#,
        );

        match &compiled.hydration {
            query_engine::compiler::HydrationPlan::Static(templates) => {
                let def_template = templates
                    .iter()
                    .find(|t| t.entity_type == "Definition")
                    .unwrap();
                let vcs = &def_template.virtual_columns;
                assert!(
                    vcs.iter().any(|vc| vc.column_name == "content"),
                    "Definition should have content virtual column in hydration plan"
                );
                // Definition.content depends_on includes start_byte/end_byte for slicing
                let cols = &def_template.columns;
                for dep in &["project_id", "file_path", "start_byte", "end_byte"] {
                    assert!(
                        cols.iter().any(|c| c == dep),
                        "depends_on column '{dep}' should be injected for Definition.content, got: {cols:?}"
                    );
                }
            }
            other => panic!("expected Static hydration plan, got: {other:?}"),
        }
    }

    #[test]
    fn multiple_virtual_entities_in_same_query() {
        // Both File and Definition request "content" in the same traversal
        let compiled = compile_query(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "f", "entity": "File", "columns": ["id", "path", "content"]},
                    {"id": "def", "entity": "Definition", "columns": ["id", "name", "content"]}
                ],
                "relationships": [{"type": "DEFINES", "from": "f", "to": "def"}],
                "limit": 5
            }"#,
        );

        match &compiled.hydration {
            query_engine::compiler::HydrationPlan::Static(templates) => {
                let def_vcs = &templates
                    .iter()
                    .find(|t| t.entity_type == "Definition")
                    .unwrap()
                    .virtual_columns;
                let file_vcs = &templates
                    .iter()
                    .find(|t| t.entity_type == "File")
                    .unwrap()
                    .virtual_columns;
                assert!(
                    def_vcs.iter().any(|vc| vc.column_name == "content"),
                    "Definition missing content VC"
                );
                assert!(
                    file_vcs.iter().any(|vc| vc.column_name == "content"),
                    "File missing content VC"
                );
            }
            other => panic!("expected Static hydration plan, got: {other:?}"),
        }
    }
}
