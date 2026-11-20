//! Parallel requests that share one correlation ID must each get their own
//! ClickHouse `query_id`, or ClickHouse rejects all but the first with
//! Code 216 (QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING).

use std::sync::Arc;

use compiler::Frontend;
use futures::future::join_all;
use integration_testkit::load_seed;
use labkit::correlation::CorrelationCaptureLayer;
use orbit_server::auth::Claims;
use orbit_server::pipeline::{ClickHouseExecutor, SecurityStage};
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{CompilationStage, QueryExecutionLog};
use tracing::Instrument;
use tracing_subscriber::layer::SubscriberExt;

use crate::common::{
    DummyClaims, GRAPH_SCHEMA_SQL, TestContext, derive_clickhouse_data_model, load_ontology,
};

const PARALLEL_REQUESTS: usize = 20;
const SHARED_CORRELATION_ID: &str = "parallel-shared-correlation";

#[tokio::test]
async fn parallel_queries_sharing_a_correlation_id_all_execute() {
    let subscriber = tracing_subscriber::registry().with(CorrelationCaptureLayer::new());
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let db = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    load_seed(&db, "data_correctness").await;

    let requests = (0..PARALLEL_REQUESTS).map(|_| {
        let span = tracing::info_span!("request", correlation_id = SHARED_CORRELATION_ID);
        execute_project_query(&db).instrument(span)
    });
    let outcomes = join_all(requests).await;

    let failures: Vec<String> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err().map(ToString::to_string))
        .collect();
    assert!(failures.is_empty(), "{failures:#?}");

    let mut query_ids: Vec<&str> = outcomes
        .iter()
        .map(|outcome| outcome.as_deref().unwrap())
        .collect();
    assert!(
        query_ids
            .iter()
            .all(|id| id.starts_with(&format!("{SHARED_CORRELATION_ID}-"))),
        "{query_ids:#?}"
    );
    query_ids.sort_unstable();
    query_ids.dedup();
    assert_eq!(query_ids.len(), PARALLEL_REQUESTS);
}

async fn execute_project_query(db: &TestContext) -> Result<String, PipelineError> {
    let ontology = load_ontology();
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::new(db.create_client()));
    server_extensions.insert(derive_clickhouse_data_model(&ontology));
    server_extensions.insert(Claims::dummy());
    let mut ctx = QueryPipelineContext {
        frontend: Frontend::JsonDsl,
        query_json: r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","node_ids":[1000,1001],"columns":["name"]}],"limit":10}"#.to_string(),
        compiled: None,
        ontology,
        security_context: None,
        server_extensions,
        phases: TypeMap::default(),
    };
    let executor = ClickHouseExecutor {
        migration_version: *orbit_migrations::version::SCHEMA_VERSION,
    };

    PipelineRunner::start(&mut ctx, &mut NoOpObserver)
        .then(&SecurityStage)
        .await?
        .then(&CompilationStage)
        .await?
        .then(&executor)
        .await?;

    let log = ctx.phases.remove::<QueryExecutionLog>().unwrap_or_default();
    Ok(log.0[0].query_id.clone())
}
