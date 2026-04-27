//! Virtual column handling: SQL stripping, hydration plans, depends_on injection.

use ontology::Ontology;
use query_engine::compiler::{HydrationPlan, SecurityContext, compile};

fn compile_query(json: &str) -> query_engine::compiler::CompiledQueryContext {
    let ontology = Ontology::load_embedded().unwrap();
    let security_ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    compile(json, &ontology, &security_ctx).unwrap()
}

#[test]
fn search_with_wildcard_excludes_virtual_columns_from_sql() {
    let compiled = compile_query(
        r#"{"query_type": "traversal", "node": {"id": "f", "entity": "File", "node_ids": [1], "columns": "*"}, "limit": 5}"#,
    );
    let sql = &compiled.base.sql;
    assert!(
        !sql.contains("f_content") && !sql.contains("f.content"),
        "virtual column 'content' should not appear in search SQL, got:\n{sql}"
    );
    assert!(
        sql.contains("f_name") || sql.contains("f.name"),
        "normal columns should be in SQL"
    );
}

#[test]
fn search_with_explicit_content_excludes_from_sql() {
    let compiled = compile_query(
        r#"{"query_type": "traversal", "node": {"id": "f", "entity": "File", "node_ids": [1], "columns": ["id", "name", "content"]}, "limit": 5}"#,
    );
    let sql = &compiled.base.sql;
    assert!(
        !sql.contains("f_content") && !sql.contains("f.content"),
        "explicitly requested virtual column 'content' should be stripped from search SQL"
    );
    assert!(sql.contains("f_name") || sql.contains("f.name"));
}

#[test]
fn search_with_content_produces_hydration_plan() {
    let compiled = compile_query(
        r#"{"query_type": "traversal", "node": {"id": "f", "entity": "File", "node_ids": [1], "columns": ["id", "name", "content"]}, "limit": 5}"#,
    );
    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
            assert_eq!(templates.len(), 1);
            let t = &templates[0];
            assert_eq!(t.entity_type, "File");
            assert_eq!(t.node_alias, "f");
            assert!(
                t.virtual_columns
                    .iter()
                    .any(|vc| vc.column_name == "content" && vc.service == "gitaly"),
                "search hydration plan should include content VCR"
            );
            for dep in &["project_id", "commit_sha", "branch", "path"] {
                assert!(
                    t.columns.contains(&dep.to_string()),
                    "depends_on column '{dep}' should be in search hydration plan"
                );
                // User only asked for ["id", "name", "content"], so deps are injected
                assert!(
                    t.injected_columns.contains(&dep.to_string()),
                    "'{dep}' should be in injected_columns"
                );
            }
        }
        other => {
            panic!("expected Static hydration plan for search with virtual cols, got: {other:?}")
        }
    }
}

#[test]
fn search_without_content_has_no_hydration_plan() {
    let compiled = compile_query(
        r#"{"query_type": "traversal", "node": {"id": "f", "entity": "File", "node_ids": [1], "columns": ["id", "name", "path"]}, "limit": 5}"#,
    );
    assert!(
        matches!(&compiled.hydration, HydrationPlan::None),
        "search without virtual cols should have HydrationPlan::None, got: {:?}",
        compiled.hydration
    );
}

#[test]
fn aggregation_with_content_produces_hydration_plan() {
    let compiled = compile_query(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "f", "entity": "File", "node_ids": [1], "columns": ["id", "content"]}],
            "aggregations": [{"function": "count", "target": "f", "alias": "total"}],
            "limit": 5
        }"#,
    );
    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
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
            "nodes": [{"id": "f", "entity": "File", "node_ids": [1], "columns": "*"}],
            "aggregations": [{"function": "count", "target": "f", "alias": "total"}],
            "limit": 5
        }"#,
    );
    let sql = &compiled.base.sql;
    assert!(
        !sql.contains("f_content") && !sql.contains("f.content"),
        "virtual column 'content' should not appear in aggregation SQL"
    );
}

#[test]
fn traversal_with_content_includes_virtual_in_hydration_plan() {
    let compiled = compile_query(
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "f", "entity": "File", "node_ids": [1], "columns": ["id", "name", "content"]},
                {"id": "b", "entity": "Branch", "columns": ["id", "name"]}
            ],
            "relationships": [{"type": "ON_BRANCH", "from": "f", "to": "b"}],
            "limit": 5
        }"#,
    );

    let sql = &compiled.base.sql;
    assert!(
        !sql.contains("f_content") && !sql.contains("f.content"),
        "virtual column should not be in traversal base SQL"
    );

    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
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
    let compiled = compile_query(
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "b", "entity": "Branch", "node_ids": [1], "columns": ["id", "name"]},
                {"id": "f", "entity": "File", "columns": ["id", "content"]}
            ],
            "relationships": [{"type": "ON_BRANCH", "from": "f", "to": "b"}],
            "limit": 5
        }"#,
    );

    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
            let file_template = templates.iter().find(|t| t.entity_type == "File").unwrap();
            let cols = &file_template.columns;
            for dep in &["project_id", "commit_sha", "branch", "path"] {
                assert!(
                    cols.iter().any(|c| c == dep),
                    "depends_on column '{dep}' should be auto-injected into hydration columns, got: {cols:?}"
                );
                assert!(
                    file_template.injected_columns.contains(&dep.to_string()),
                    "'{dep}' should be tracked in injected_columns for stripping"
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
                {"id": "f", "entity": "File", "node_ids": [1], "columns": ["id", "path"]},
                {"id": "def", "entity": "Definition", "columns": ["id", "name", "content"]}
            ],
            "relationships": [{"type": "DEFINES", "from": "f", "to": "def"}],
            "limit": 5
        }"#,
    );

    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
            let def_template = templates
                .iter()
                .find(|t| t.entity_type == "Definition")
                .unwrap();
            let vcs = &def_template.virtual_columns;
            assert!(
                vcs.iter().any(|vc| vc.column_name == "content"),
                "Definition should have content virtual column in hydration plan"
            );
            let cols = &def_template.columns;
            for dep in &[
                "project_id",
                "commit_sha",
                "branch",
                "file_path",
                "start_byte",
                "end_byte",
            ] {
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
    let compiled = compile_query(
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "f", "entity": "File", "node_ids": [1], "columns": ["id", "path", "content"]},
                {"id": "def", "entity": "Definition", "columns": ["id", "name", "content"]}
            ],
            "relationships": [{"type": "DEFINES", "from": "f", "to": "def"}],
            "limit": 5
        }"#,
    );

    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
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
