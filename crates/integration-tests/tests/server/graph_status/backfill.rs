use std::sync::Arc;

use chrono::Utc;
use indexer::campaign::CampaignState;
use indexer::checkpoint::{Checkpoint, CheckpointStore, ClickHouseCheckpointStore};
use indexer::durability::WriteDurability;
use indexer::indexing_status::{IndexingStatusStore, RunRows};
use indexer::orchestrator::dispatch::backfill_status::BackfillStatus as BackfillRecorder;
use indexer::orchestrator::dispatch::{CodeBackfill, DispatchOutcome};
use indexer::orchestrator::scheduled::ScheduledTaskMetrics;
use indexer::testkit::{MockLockService, MockNatsServices};
use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_ontology, run_subtests_shared,
};
use nats_client::testkit::MockKvServices;
use orbit_migrations::version::{
    SCHEMA_VERSION, ensure_version_table, mark_version_active, mark_version_migrating,
    prefixed_table_name,
};
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

use super::{
    BackfillState, BackfillStatus, GraphStatusService, ResponseFormat, admin_context,
    backfill_status, get_graph_status_response,
};

struct BackfillFixture {
    database: TestContext,
    store: Arc<IndexingStatusStore>,
    service: GraphStatusService,
    locks: Arc<MockLockService>,
    messages: Arc<MockNatsServices>,
    path: TraversalPath,
}

impl BackfillFixture {
    async fn new(context: &TestContext, name: &str) -> Self {
        let database = context.fork(name).await;
        let client = database.create_client();
        ensure_version_table(&client).await.unwrap();
        mark_version_active(&client, *SCHEMA_VERSION).await.unwrap();

        database
            .execute(
                "INSERT INTO namespace_traversal_paths (id, traversal_path) VALUES (100, '1/100/')",
            )
            .await;
        database
            .execute(
                "INSERT INTO siphon_knowledge_graph_enabled_namespaces
                    (id, root_namespace_id, traversal_path, created_at, updated_at)
                 VALUES (1, 100, '1/100/', now(), now())",
            )
            .await;

        let storage = Arc::new(MockKvServices::new());
        let service = GraphStatusService::new(
            Arc::new(database.create_client()),
            Arc::new(load_ontology()),
        )
        .with_indexing_status(IndexingStatusStore::new(storage.clone()));

        Self {
            database,
            store: Arc::new(IndexingStatusStore::new(storage)),
            service,
            locks: Arc::new(MockLockService::new()),
            messages: Arc::new(MockNatsServices::new()),
            path: TraversalPath::new_unchecked("1/100/"),
        }
    }

    fn recorder(&self, version: u32) -> Arc<BackfillRecorder> {
        Arc::new(BackfillRecorder::new(
            self.database.create_client(),
            self.store.clone(),
            self.locks.clone(),
            &load_ontology(),
            version,
        ))
    }

    fn dispatcher(&self, version: u32) -> CodeBackfill {
        CodeBackfill::new(
            self.messages.clone(),
            self.database.create_client(),
            self.database.create_client(),
            ScheduledTaskMetrics::new(),
            Arc::new(CampaignState::new()),
            1,
        )
        .with_status(self.recorder(version))
    }

    async fn add_project(&self, project_id: i64) {
        self.database
            .execute(&format!(
                "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
                 VALUES ({project_id}, '1/100/{project_id}/')"
            ))
            .await;
    }

    async fn index_project(&self, version: u32, project_id: i64) {
        let table = prefixed_table_name("code_indexing_checkpoint", version);
        self.database
            .execute(&format!(
                "INSERT INTO {table}
                    (traversal_path, project_id, branch, last_task_id, indexed_at)
                 VALUES ('1/100/{project_id}/', {project_id}, 'main', 1, now())"
            ))
            .await;
    }

    fn checkpoints(&self, version: u32) -> ClickHouseCheckpointStore {
        ClickHouseCheckpointStore::for_version(Arc::new(self.database.create_client()), version)
    }

    async fn finish_sdlc(&self, version: u32) {
        let checkpoints = self.checkpoints(version);

        for pipeline in load_ontology()
            .pipeline_descriptors()
            .into_iter()
            .filter(|pipeline| pipeline.scope == ontology::EtlScope::Namespaced)
        {
            checkpoints
                .save_completed(
                    &format!("ns.100.{}", pipeline.name),
                    &Utc::now(),
                    WriteDurability::Durable,
                )
                .await
                .unwrap();
        }
    }

    async fn rebuild(&self, version: u32, copy_checkpoints: bool) {
        for table in ["checkpoint", "code_indexing_checkpoint"] {
            let source = prefixed_table_name(table, *SCHEMA_VERSION);
            let destination = prefixed_table_name(table, version);
            self.database
                .execute(&format!("CREATE TABLE {destination} AS {source}"))
                .await;
            if copy_checkpoints {
                self.database
                    .execute(&format!("INSERT INTO {destination} SELECT * FROM {source}"))
                    .await;
            }
        }
        mark_version_migrating(&self.database.create_client(), version)
            .await
            .unwrap();
    }

    async fn status(&self) -> BackfillStatus {
        backfill_status(&self.service, self.path.as_str()).await
    }

    async fn dispatch(&self, version: u32) -> DispatchOutcome {
        self.dispatcher(version)
            .dispatch_enabled(Uuid::new_v4())
            .await
            .unwrap()
    }

    async fn remove_source_inventory(&self) {
        self.database
            .execute("DROP DICTIONARY project_traversal_paths_dict")
            .await;
        self.database
            .execute("DROP TABLE project_namespace_traversal_paths")
            .await;
    }
}

async fn checkpoints_count_before_project_metadata(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_before_metadata").await;
    for project_id in 1000..1012 {
        fixture.add_project(project_id).await;
        fixture.index_project(*SCHEMA_VERSION, project_id).await;
    }
    fixture.add_project(1012).await;

    let outcome = fixture.dispatch(*SCHEMA_VERSION).await;

    assert_eq!(outcome.dispatched, 1);

    let status = fixture.status().await;

    assert_eq!(status.state(), BackfillState::Running);
    assert_eq!(status.code.as_ref().unwrap().completed, 12);
    assert_eq!(status.code.as_ref().unwrap().total, None);
    assert_eq!(
        status,
        backfill_status(&fixture.service, "1/100/1000/").await
    );
    assert_eq!(status.last_progress_at, None);
}

async fn completion_requires_parent_checkpoints_then_another_scan(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_parent_evidence").await;
    fixture
        .checkpoints(*SCHEMA_VERSION)
        .save_completed("ns.100.Project.p0", &Utc::now(), WriteDurability::Durable)
        .await
        .unwrap();

    fixture.dispatch(*SCHEMA_VERSION).await;

    assert_eq!(fixture.status().await.sdlc.unwrap().completed, 0);

    let recorder = fixture.recorder(*SCHEMA_VERSION);
    let before_completion = recorder.begin(&fixture.path).await.unwrap().unwrap();
    fixture.finish_sdlc(*SCHEMA_VERSION).await;
    recorder
        .finish(&fixture.path, before_completion, 0, true)
        .await
        .unwrap();

    assert_eq!(fixture.status().await.state(), BackfillState::Running);

    fixture.dispatch(*SCHEMA_VERSION).await;

    assert_eq!(fixture.status().await.state(), BackfillState::Completed);
    assert_eq!(fixture.status().await.last_progress_at, None);
}

async fn interrupted_parent_does_not_complete_initial_backfill(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_partial_parent").await;
    fixture.finish_sdlc(*SCHEMA_VERSION).await;
    fixture
        .checkpoints(*SCHEMA_VERSION)
        .save_progress(
            "ns.100.Project",
            &Checkpoint {
                watermark: Utc::now(),
                cursor_values: Some(vec!["1000".into()]),
                resume_floor: None,
            },
        )
        .await
        .unwrap();

    fixture.dispatch(*SCHEMA_VERSION).await;

    let status = fixture.status().await;

    assert_eq!(status.state(), BackfillState::Running);
    let counts = status.sdlc.unwrap();
    assert_eq!(counts.total, Some(counts.completed + 1));
}

async fn restart_and_late_arrivals_preserve_first_completion(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_first_completion").await;
    fixture.add_project(1000).await;
    fixture.finish_sdlc(*SCHEMA_VERSION).await;

    fixture
        .dispatcher(*SCHEMA_VERSION)
        .dispatch_for_namespaces(&[(100, fixture.path.clone())], Uuid::new_v4())
        .await
        .unwrap();
    let started = fixture
        .store
        .namespace_backfill(&fixture.path)
        .await
        .unwrap()
        .unwrap();
    fixture.dispatch(*SCHEMA_VERSION).await;
    let resumed = fixture
        .store
        .namespace_backfill(&fixture.path)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(resumed.generation, started.generation);
    assert_eq!(fixture.status().await.state(), BackfillState::Running);

    fixture.index_project(*SCHEMA_VERSION, 1000).await;
    fixture
        .store
        .record_progress(&fixture.path, *SCHEMA_VERSION)
        .await;
    fixture.dispatch(*SCHEMA_VERSION).await;
    let completed = fixture.status().await;

    assert_eq!(completed.state(), BackfillState::Completed);
    assert!(completed.last_progress_at.is_some());

    fixture.add_project(1001).await;
    let ongoing = fixture.dispatch(*SCHEMA_VERSION).await;

    assert_eq!(ongoing.dispatched, 1);

    let replacement = *SCHEMA_VERSION + 1;
    fixture.rebuild(replacement, false).await;
    fixture.dispatch(replacement).await;
    fixture
        .store
        .record_progress(&fixture.path, replacement)
        .await;
    assert_eq!(fixture.status().await, completed);

    fixture.remove_source_inventory().await;

    assert_eq!(fixture.status().await, completed);
}

async fn rebuild_cannot_combine_completion_from_different_schemas(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_rebuild_fence").await;
    fixture.add_project(1000).await;
    fixture.finish_sdlc(*SCHEMA_VERSION).await;
    fixture.dispatch(*SCHEMA_VERSION).await;
    let old_recorder = fixture.recorder(*SCHEMA_VERSION);
    let old_observation = old_recorder.begin(&fixture.path).await.unwrap().unwrap();

    let replacement = *SCHEMA_VERSION + 1;
    fixture.rebuild(replacement, false).await;
    fixture.index_project(replacement, 1000).await;
    fixture.dispatch(replacement).await;
    let rebuilding = fixture.status().await;
    assert_eq!(rebuilding.state(), BackfillState::Running);
    assert_eq!(rebuilding.sdlc.as_ref().unwrap().completed, 0);
    assert_eq!(rebuilding.code.as_ref().unwrap().completed, 1);

    old_recorder
        .finish(&fixture.path, old_observation, 1, true)
        .await
        .unwrap();
    fixture
        .store
        .record_progress(&fixture.path, *SCHEMA_VERSION)
        .await;
    assert_eq!(fixture.status().await, rebuilding);
    fixture.finish_sdlc(replacement).await;
    fixture.dispatch(replacement).await;
    assert_eq!(fixture.status().await.state(), BackfillState::Completed);
}

async fn selective_rebuild_uses_copied_checkpoints(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_copied_checkpoints").await;
    fixture.add_project(1000).await;
    fixture.finish_sdlc(*SCHEMA_VERSION).await;
    fixture.dispatch(*SCHEMA_VERSION).await;
    let replacement = *SCHEMA_VERSION + 1;
    fixture.rebuild(replacement, true).await;
    fixture.dispatch(replacement).await;
    let status = fixture.status().await;
    assert_eq!(status.state(), BackfillState::Running);
    let counts = status.sdlc.unwrap();
    assert_eq!(counts.total, Some(counts.completed));
    fixture.index_project(replacement, 1000).await;
    fixture.dispatch(replacement).await;
    assert_eq!(fixture.status().await.state(), BackfillState::Completed);
}

async fn source_failure_is_unknown_not_empty_inventory(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_source_failure").await;
    fixture.add_project(1000).await;
    fixture.finish_sdlc(*SCHEMA_VERSION).await;
    fixture.dispatch(*SCHEMA_VERSION).await;

    fixture.remove_source_inventory().await;

    assert!(
        fixture
            .dispatcher(*SCHEMA_VERSION)
            .dispatch_enabled(Uuid::new_v4())
            .await
            .is_err()
    );
    let status = fixture.status().await;
    assert_eq!(status.state(), BackfillState::Unknown);
    assert_eq!(
        status.error.as_deref(),
        Some("Initial indexing status is temporarily unavailable.")
    );
}

async fn polling_and_dispatch_do_not_manufacture_progress(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_meaningful_progress").await;
    assert_eq!(fixture.status().await.state(), BackfillState::Unknown);
    fixture.add_project(1000).await;
    fixture.dispatch(*SCHEMA_VERSION).await;
    fixture
        .store
        .record_entity_start(&fixture.path, "Project", Utc::now())
        .await;
    assert_eq!(fixture.status().await.last_progress_at, None);
    fixture
        .store
        .record_progress(&fixture.path, *SCHEMA_VERSION)
        .await;
    let progressing = fixture.status().await;
    fixture.dispatch(*SCHEMA_VERSION).await;
    assert_eq!(fixture.status().await, progressing);
    fixture
        .store
        .record_entity_completion(
            &fixture.path,
            "Project",
            Utc::now(),
            Utc::now(),
            Some("secret backend address".into()),
            RunRows::default(),
        )
        .await;
    let retrying = fixture.status().await;
    assert_eq!(retrying.state(), BackfillState::Retrying);
    assert_eq!(
        retrying.error.as_deref(),
        Some("Some initial indexing work could not finish.")
    );
    fixture
        .store
        .record_progress(&fixture.path, *SCHEMA_VERSION)
        .await;
    assert_eq!(fixture.status().await.state(), BackfillState::Running);
    assert_eq!(fixture.status().await.error, None);
}

async fn reconciliation_adopts_existing_completion_without_reindexing(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_adopt_completion").await;
    fixture.add_project(1000).await;
    fixture.index_project(*SCHEMA_VERSION, 1000).await;
    fixture.finish_sdlc(*SCHEMA_VERSION).await;
    assert_eq!(fixture.status().await.state(), BackfillState::Unknown);
    fixture
        .dispatcher(*SCHEMA_VERSION)
        .reconcile_initial_backfills()
        .await
        .unwrap();
    assert_eq!(fixture.status().await.state(), BackfillState::Completed);
    assert!(fixture.messages.get_published().is_empty());
}

async fn deletion_fences_old_observations_without_resetting_parent_for_subgroups(
    context: &TestContext,
) {
    let fixture = BackfillFixture::new(context, "backfill_deletion_fence").await;
    fixture.finish_sdlc(*SCHEMA_VERSION).await;
    let recorder = fixture.recorder(*SCHEMA_VERSION);
    let observation = recorder.begin(&fixture.path).await.unwrap().unwrap();
    fixture
        .store
        .forget_namespace(&TraversalPath::new_unchecked("1/100/1000/"))
        .await
        .unwrap();
    assert_eq!(fixture.status().await.state(), BackfillState::Running);
    fixture.store.forget_namespace(&fixture.path).await.unwrap();
    assert_eq!(fixture.status().await.state(), BackfillState::Unknown);
    recorder
        .finish(&fixture.path, observation.clone(), 0, true)
        .await
        .unwrap();
    assert_eq!(fixture.status().await.state(), BackfillState::Unknown);
    recorder.begin(&fixture.path).await.unwrap();
    recorder
        .finish(&fixture.path, observation, 0, true)
        .await
        .unwrap();
    assert_eq!(fixture.status().await.state(), BackfillState::Running);
}

async fn formatted_response_does_not_expose_tracking_fields(context: &TestContext) {
    let fixture = BackfillFixture::new(context, "backfill_minimal_response").await;
    fixture.dispatch(*SCHEMA_VERSION).await;
    let response = fixture
        .service
        .get_status(&fixture.path, ResponseFormat::Llm as i32, &admin_context())
        .await
        .unwrap();
    let Some(get_graph_status_response::Content::FormattedText(text)) = response.content else {
        panic!("expected formatted response");
    };
    assert!(text.contains("backfill:"));
    assert!(text.contains("state: running"));
    assert!(!text.contains("total: null"));
    for private_field in ["generation", "schema_version", "dispatch_id", "scope:"] {
        assert!(!text.contains(private_field));
    }
}

#[tokio::test]
async fn namespace_backfill_lifecycle() {
    let context = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    run_subtests_shared!(
        &context,
        checkpoints_count_before_project_metadata,
        completion_requires_parent_checkpoints_then_another_scan,
        interrupted_parent_does_not_complete_initial_backfill,
        restart_and_late_arrivals_preserve_first_completion,
        rebuild_cannot_combine_completion_from_different_schemas,
        selective_rebuild_uses_copied_checkpoints,
        source_failure_is_unknown_not_empty_inventory,
        polling_and_dispatch_do_not_manufacture_progress,
        reconciliation_adopts_existing_completion_without_reindexing,
        deletion_fences_old_observations_without_resetting_parent_for_subgroups,
        formatted_response_does_not_expose_tracking_fields,
    );
}
