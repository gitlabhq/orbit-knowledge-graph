use std::sync::Arc;

use chrono::Utc;
use indexer::campaign::CampaignState;
use indexer::checkpoint::{Checkpoint, CheckpointStore, ClickHouseCheckpointStore};
use indexer::durability::WriteDurability;
use indexer::indexing_status::IndexingStatusStore;
use indexer::orchestrator::dispatch::CodeBackfill;
use indexer::orchestrator::dispatch::backfill_status::InitialBackfillTracker;
use indexer::orchestrator::scheduled::ScheduledTaskMetrics;
use indexer::testkit::MockNatsServices;
use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_ontology, run_subtests_shared,
};
use nats_client::testkit::MockKvServices;
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

use super::{
    BackfillState, BackfillStatus, GraphStatusService, ResponseFormat, admin_context,
    backfill_status, get_graph_status_response,
};

const ROOT_NAMESPACE_ID: i64 = 100;
const ROOT_PATH: &str = "1/100/";

struct EnrolledNamespace {
    database: TestContext,
    status_store: Arc<IndexingStatusStore>,
    status_service: GraphStatusService,
    dispatcher: CodeBackfill,
    root: TraversalPath,
}

impl EnrolledNamespace {
    async fn new(context: &TestContext, name: &str) -> Self {
        let database = context.fork(name).await;
        database
            .execute(&format!(
                "INSERT INTO namespace_traversal_paths (id, traversal_path) VALUES ({ROOT_NAMESPACE_ID}, '{ROOT_PATH}')"
            ))
            .await;
        database
            .execute(&format!(
                "INSERT INTO siphon_knowledge_graph_enabled_namespaces
                    (id, root_namespace_id, traversal_path, created_at, updated_at)
                 VALUES (1, {ROOT_NAMESPACE_ID}, '{ROOT_PATH}', now(), now())"
            ))
            .await;

        let kv = Arc::new(MockKvServices::new());
        let status_store = Arc::new(IndexingStatusStore::new(kv.clone()));
        let status_service = GraphStatusService::new(Arc::new(database.create_client()))
            .with_indexing_status(IndexingStatusStore::new(kv));
        let dispatcher = CodeBackfill::new(
            Arc::new(MockNatsServices::new()),
            database.create_client(),
            database.create_client(),
            ScheduledTaskMetrics::new(),
            Arc::new(CampaignState::new()),
            1,
            Arc::new(InitialBackfillTracker::new(
                database.create_client(),
                status_store.clone(),
                &load_ontology(),
            )),
        );

        Self {
            database,
            status_store,
            status_service,
            dispatcher,
            root: TraversalPath::new_unchecked(ROOT_PATH),
        }
    }

    async fn add_project(&self, project_id: i64) {
        self.database
            .execute(&format!(
                "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
                 VALUES ({project_id}, '{ROOT_PATH}{project_id}/')"
            ))
            .await;
    }

    async fn checkpoint_project(&self, project_id: i64) {
        let table = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
        self.database
            .execute(&format!(
                "INSERT INTO {table} (traversal_path, project_id, branch, last_task_id, indexed_at)
                 VALUES ('{ROOT_PATH}{project_id}/', {project_id}, 'main', 1, now())"
            ))
            .await;
    }

    fn sdlc_checkpoints(&self) -> ClickHouseCheckpointStore {
        ClickHouseCheckpointStore::new(Arc::new(self.database.create_client()))
    }

    async fn checkpoint_every_sdlc_pipeline(&self) {
        for pipeline in namespaced_pipelines() {
            self.sdlc_checkpoints()
                .save_completed(
                    &format!("ns.{ROOT_NAMESPACE_ID}.{pipeline}"),
                    &Utc::now(),
                    WriteDurability::Durable,
                )
                .await
                .unwrap();
        }
    }

    async fn checkpoint_pipeline_mid_run(&self, pipeline: &str) {
        self.sdlc_checkpoints()
            .save_progress(
                &format!("ns.{ROOT_NAMESPACE_ID}.{pipeline}"),
                &Checkpoint {
                    watermark: Utc::now(),
                    cursor_values: Some(vec!["1000".into()]),
                    resume_floor: None,
                },
            )
            .await
            .unwrap();
    }

    async fn wipe_checkpoint_tables(&self) {
        for table in ["checkpoint", "code_indexing_checkpoint"] {
            self.database
                .execute(&format!(
                    "TRUNCATE TABLE {}",
                    prefixed_table_name(table, *SCHEMA_VERSION)
                ))
                .await;
        }
    }

    async fn drop_project_inventory(&self) {
        self.database
            .execute("DROP DICTIONARY project_traversal_paths_dict")
            .await;
        self.database
            .execute("DROP TABLE project_namespace_traversal_paths")
            .await;
    }

    async fn dispatch(&self) {
        self.dispatcher
            .dispatch_enabled(Uuid::new_v4())
            .await
            .unwrap();
    }

    async fn status(&self) -> BackfillStatus {
        self.status_at(ROOT_PATH).await
    }

    async fn status_at(&self, path: &str) -> BackfillStatus {
        backfill_status(&self.status_service, path).await
    }
}

fn namespaced_pipelines() -> Vec<String> {
    load_ontology()
        .pipeline_descriptors()
        .into_iter()
        .filter(|pipeline| pipeline.scope == ontology::EtlScope::Namespaced)
        .map(|pipeline| pipeline.name)
        .collect()
}

async fn status_is_unknown_until_the_dispatcher_has_looked(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_unknown_before_dispatch").await;

    assert_eq!(namespace.status().await.state(), BackfillState::Unknown);

    namespace.dispatch().await;

    assert_eq!(namespace.status().await.state(), BackfillState::Running);
}

async fn running_reports_checkpointed_projects_and_finished_pipelines(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_running_counts").await;
    for project_id in 1000..1012 {
        namespace.add_project(project_id).await;
        namespace.checkpoint_project(project_id).await;
    }
    namespace.add_project(1012).await;

    namespace.dispatch().await;
    let status = namespace.status().await;

    assert_eq!(status.state(), BackfillState::Running);
    assert_eq!(status.code.unwrap().completed, 12);
    let sdlc = status.sdlc.unwrap();
    assert_eq!(sdlc.completed, 0);
    assert_eq!(sdlc.total, Some(namespaced_pipelines().len() as u64));
    assert!(status.last_progress_at.is_some());
}

async fn completes_when_every_pipeline_and_project_is_checkpointed(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_completes").await;
    namespace.add_project(1000).await;
    namespace.checkpoint_every_sdlc_pipeline().await;

    namespace.dispatch().await;
    assert_eq!(namespace.status().await.state(), BackfillState::Running);

    namespace.checkpoint_project(1000).await;
    namespace.dispatch().await;
    let status = namespace.status().await;

    assert_eq!(status.state(), BackfillState::Completed);
    let sdlc = status.sdlc.unwrap();
    assert_eq!(sdlc.completed, sdlc.total.unwrap());
    assert_eq!(status.code.unwrap().completed, 1);
}

async fn a_pipeline_stopped_mid_run_keeps_the_namespace_running(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_mid_run_pipeline").await;
    namespace.checkpoint_every_sdlc_pipeline().await;
    namespace.checkpoint_pipeline_mid_run("Project").await;

    namespace.dispatch().await;
    let status = namespace.status().await;

    assert_eq!(status.state(), BackfillState::Running);
    let sdlc = status.sdlc.unwrap();
    assert_eq!(sdlc.total, Some(sdlc.completed + 1));
}

async fn a_project_added_after_completion_does_not_reopen_it(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_late_project").await;
    namespace.checkpoint_every_sdlc_pipeline().await;
    namespace.dispatch().await;
    let completed = namespace.status().await;
    assert_eq!(completed.state(), BackfillState::Completed);

    namespace.add_project(1001).await;
    namespace.dispatch().await;

    assert_eq!(namespace.status().await, completed);
}

async fn a_schema_rebuild_does_not_reopen_a_completed_namespace(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_rebuild").await;
    namespace.add_project(1000).await;
    namespace.checkpoint_project(1000).await;
    namespace.checkpoint_every_sdlc_pipeline().await;
    namespace.dispatch().await;
    let completed = namespace.status().await;
    assert_eq!(completed.state(), BackfillState::Completed);

    namespace.wipe_checkpoint_tables().await;
    namespace.dispatch().await;

    assert_eq!(namespace.status().await, completed);
}

async fn a_source_read_failure_keeps_the_last_recorded_status(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_source_failure").await;
    namespace.add_project(1000).await;
    namespace.dispatch().await;
    let running = namespace.status().await;
    assert_eq!(running.state(), BackfillState::Running);

    namespace.drop_project_inventory().await;
    assert!(
        namespace
            .dispatcher
            .dispatch_enabled(Uuid::new_v4())
            .await
            .is_err()
    );

    assert_eq!(namespace.status().await, running);
}

async fn subgroup_and_project_requests_share_the_root_status(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_root_scope").await;
    namespace.add_project(1000).await;
    namespace.dispatch().await;

    let root = namespace.status().await;

    assert_eq!(root.state(), BackfillState::Running);
    assert_eq!(namespace.status_at("1/100/1000/").await, root);
}

async fn deleting_the_root_namespace_forgets_its_status(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_forget").await;
    namespace.dispatch().await;
    assert_eq!(namespace.status().await.state(), BackfillState::Running);

    namespace
        .status_store
        .forget_namespace(&TraversalPath::new_unchecked("1/100/1000/"))
        .await
        .unwrap();
    assert_eq!(namespace.status().await.state(), BackfillState::Running);

    namespace
        .status_store
        .forget_namespace(&namespace.root)
        .await
        .unwrap();
    assert_eq!(namespace.status().await.state(), BackfillState::Unknown);
}

async fn llm_format_puts_backfill_first_and_hides_missing_totals(context: &TestContext) {
    let namespace = EnrolledNamespace::new(context, "backfill_llm_format").await;
    namespace.dispatch().await;

    let response = namespace
        .status_service
        .get_status(
            &load_ontology(),
            &namespace.root,
            ResponseFormat::Llm as i32,
            &admin_context(),
        )
        .await
        .unwrap();
    let Some(get_graph_status_response::Content::FormattedText(text)) = response.content else {
        panic!("expected formatted response");
    };

    assert!(text.starts_with("backfill:"));
    assert!(text.contains("state: running"));
    assert!(!text.contains("total: null"));
}

#[tokio::test]
async fn namespace_initial_backfill_status() {
    let context = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    run_subtests_shared!(
        &context,
        status_is_unknown_until_the_dispatcher_has_looked,
        running_reports_checkpointed_projects_and_finished_pipelines,
        completes_when_every_pipeline_and_project_is_checkpointed,
        a_pipeline_stopped_mid_run_keeps_the_namespace_running,
        a_project_added_after_completion_does_not_reopen_it,
        a_schema_rebuild_does_not_reopen_a_completed_namespace,
        a_source_read_failure_keeps_the_last_recorded_status,
        subgroup_and_project_requests_share_the_root_status,
        deleting_the_root_namespace_forgets_its_status,
        llm_format_puts_backfill_first_and_hides_missing_totals,
    );
}
