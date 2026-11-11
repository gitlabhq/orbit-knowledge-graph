//! Entities whose identity is a string property not named `id`, joined by an edge table with
//! string ID columns: plans read the declared identity column and type keys by it.

use std::sync::Arc;

use compiler::orbit_authorization::{Caller, Grants, Key, ScopeSet};
use integration_testkit::authorization_variants::platform;
use query_data_model::testkit::limits;
use query_data_model::{Catalog, ClickHouse};

fn catalog() -> Arc<Catalog<ClickHouse>> {
    let model = platform(&["acme", "acme.threads"]).load().unwrap();
    Arc::new(Catalog::<ClickHouse>::from_model(&model).unwrap())
}

fn grants() -> Grants {
    Grants::new(Caller::new("user:1", false, ()))
        .with("space", "read", ScopeSet::Subtrees(vec!["1/".into()]))
        .with(
            "team",
            "read",
            ScopeSet::Keys(vec![Key::Text("team-a".into())]),
        )
        .with("everyone", "read", ScopeSet::All)
}

fn sql(query: &str) -> String {
    compiler::compile(query, &catalog(), &grants(), limits())
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .base
        .sql
}

#[test]
fn a_node_read_pins_and_returns_its_declared_identity_column() {
    let sql = sql("MATCH (t:Thread) WHERE t.key IN ['t1', 't2'] RETURN t");
    assert!(sql.contains("t.key IN {p"), "{sql}");
    assert!(sql.contains("t.key AS _orbit_t_id"), "{sql}");
    assert!(!sql.contains("t.id"), "{sql}");
}

#[test]
fn an_edge_hop_joins_both_identity_columns() {
    let sql = sql(
        "MATCH (p:Post)-[:REPLIES_TO]->(t:Thread) WHERE p.slug = 's1' AND t.title = 'x' RETURN p, t",
    );
    assert!(sql.contains("acme_thread_edge"), "{sql}");
    assert!(sql.contains("source_id = {p"), "{sql}");
    assert!(sql.contains("t.key"), "{sql}");
    assert!(!sql.contains(".id "), "{sql}");
}

#[test]
fn path_finding_over_a_string_edge_table_keeps_string_node_ids() {
    let sql = sql(
        "MATCH path = ANY SHORTEST (p:Post)-[:REPLIES_TO*1..2]->(t:Thread) \
         WHERE p.slug IN ['s1'] AND t.key IN ['t1'] RETURN path",
    );
    assert!(sql.contains("acme_thread_edge"), "{sql}");
    assert!(sql.contains("Array(Tuple(String, String))"), "{sql}");
    assert!(!sql.contains("Tuple(Int64"), "{sql}");
}

#[test]
fn an_edge_endpoint_id_has_its_endpoint_identity_type() {
    let compile = |query: &str| compiler::compile(query, &catalog(), &grants(), limits());
    let error = compile("MATCH (p:Post)-[r:REPLIES_TO]->(t:Thread) WHERE p.slug = 's1' AND r.source_id = 5 RETURN p")
        .unwrap_err()
        .to_string();
    assert!(error.contains("not a string"), "{error}");
    compile("MATCH (p:Post)-[r:REPLIES_TO]->(t:Thread) WHERE p.slug = 's1' AND r.source_id = 's1' RETURN p")
        .unwrap();
}
