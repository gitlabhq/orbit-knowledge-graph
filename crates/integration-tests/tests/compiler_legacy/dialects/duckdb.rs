//! DuckDB dialect end-to-end tests.

use crate::compiler_legacy::setup::test_ontology;
use compiler::{Frontend, compile_local};

fn compile(json: &str) -> compiler::passes::codegen::CompiledQueryContext {
    compile_local(json, Frontend::JsonDsl, &test_ontology()).unwrap()
}

#[test]
fn local_starts_with_folds_unicode_case() {
    let ontology = std::sync::Arc::new(ontology::Ontology::load_embedded().unwrap());
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("folding.duckdb")).unwrap();
    database
        .initialize_schema(&compiler::generate_local_ddl(&ontology, ""))
        .unwrap();
    database
        .execute(
            "INSERT INTO gl_file (id, traversal_path, project_id, branch, name, extension, language, path) VALUES (1, '1/', 1, 'main', 'f', '', '', 'ärger'), (2, '1/', 1, 'main', 'f', '', '', 'other')",
            &[],
        )
        .unwrap();
    let compiled = compile_local(
        r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["path"],"filters":{"path":{"starts_with":"ÄRG"}}}],"limit":10}"#,
        Frontend::JsonDsl,
        &ontology,
    )
    .unwrap();

    let result = database
        .query_arrow(&compiled.base.render())
        .unwrap_or_else(|error| panic!("{error}: {}", compiled.base.render()));

    assert_eq!(duckdb_client::string_column(&result, "f_path"), ["ärger"]);
}

#[test]
fn fused_neighbors_execute_both_directions_including_self_loops() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("neighbors.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE gl_project(id BIGINT, traversal_path VARCHAR); INSERT INTO gl_project VALUES (1, '1/'); CREATE TABLE gl_edge(source_id BIGINT, source_kind VARCHAR, target_id BIGINT, target_kind VARCHAR, relationship_kind VARCHAR, traversal_path VARCHAR); INSERT INTO gl_edge VALUES (1,'Project',2,'Project','CONTAINS','1/'), (3,'Project',1,'Project','CONTAINS','1/'), (1,'Project',1,'Project','CONTAINS','1/');").unwrap();
    let compiled = compile(
        r#"{"query_type":"neighbors","nodes":[{"id":"p","entity":"Project","node_ids":[1]}],"neighbors":{"direction":"both","rel_types":["CONTAINS"]}}"#,
    );
    let result = database
        .query_arrow(&compiled.base.render())
        .unwrap_or_else(|error| panic!("{error}: {}", compiled.base.render()));
    assert_eq!(
        result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        4
    );
}
