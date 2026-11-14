//! Placement over a deployment where `Snippet` has a complete ClickHouse store and a partial code
//! store on its own connection that holds a subset of its columns and searches its body.

use std::sync::Arc;

use compiler::dag::{Connections, Env, Frontend, Runs, Source, split_query};
use query_data_model::{ClickHouse, EntityId, Model, StoreId};
use query_engine::pipeline::{AuthorizedPath, SecurityContext};

use crate::dag_fakes::peer_packages;

fn model() -> Arc<Model> {
    peer_packages()
        .deployed(&query_data_model::testkit::deployments(), "snippets")
        .unwrap()
}

/// The store each stage reads `Snippet` from, or the placement error.
fn placed(query: &str) -> Result<Vec<StoreId>, String> {
    let model = model();
    let main = model.names().connection("main").unwrap();
    let frontend = Frontend::Server(Arc::new(model.physical::<ClickHouse>(main).unwrap()));
    let connections = Connections::of(&model).unwrap();
    let session =
        SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/9970/", 20)]).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    let grants =
        orbit_server::authorization::gitlab::session_grants(&config, &model, session).unwrap();
    let env = Env {
        frontend: &frontend,
        connections: &connections,
        grants: &grants,
        limits: query_data_model::testkit::limits(),
        trace: false,
    };
    let snippet: EntityId = model.names().entity("Snippet").unwrap();
    let dag = split_query(Source::Text(query), &env).map_err(|error| error.to_string())?;
    Ok(dag
        .stages
        .iter()
        .filter_map(|stage| match &stage.runs {
            Runs::On { stores, .. } => stores
                .iter()
                .find(|(entity, _)| *entity == snippet)
                .map(|(_, store)| *store),
            Runs::Combine => None,
        })
        .collect())
}

fn store(name: &str) -> StoreId {
    model().names().store(name).unwrap()
}

#[test]
fn a_read_of_the_partial_stores_columns_stays_on_the_complete_store() {
    let placed = placed(
        "MATCH (s:Snippet) WHERE s.project_id = 42 AND s.branch = 'main' RETURN s.path LIMIT 5",
    );
    assert_eq!(placed.unwrap(), [store("graph")]);
}

#[test]
fn a_structural_match_on_the_body_reads_from_the_partial_store() {
    let placed = placed(
        "MATCH (s:Snippet) WHERE s.project_id = 42 AND s.body MATCHES ast('todo!($$$MSG)', 'rust') RETURN s.path, match.line LIMIT 5",
    );
    assert_eq!(placed.unwrap(), [store("snippets")]);
}

#[test]
fn a_structural_match_that_also_reads_a_column_only_the_complete_store_holds_fails() {
    let placed = placed(
        "MATCH (s:Snippet) WHERE s.project_id = 42 AND s.body MATCHES ast('todo!($$$MSG)', 'rust') RETURN s.id LIMIT 5",
    );
    assert_eq!(
        placed.unwrap_err(),
        "allowlist rejected: no one store of Snippet serves both a structural search on Snippet.body and a read of Snippet.id"
    );
}
