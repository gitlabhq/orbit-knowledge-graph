use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use indexer::handler::{Handler, HandlerContext};
use indexer::modules::code::{
    ClickHouseCodeCheckpointStore, ClickHouseStaleDataCleaner, CodeIndexer,
    CodeIndexingTaskHandler, LocalRepositoryCache, RailsRepositoryService, RepositoryCache,
    config::CodeTableNames, metrics::CodeMetrics, repository::RepositoryResolver,
};
use indexer::nats::ProgressNotifier;
use indexer::testkit::{MockLockService, MockNatsServices};
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::{Envelope, Event};
use orbit_server_config::{ClickHouseConfiguration, CodeIndexingPipelineConfig};
use orbit_utils::traversal_path::TraversalPath;

use crate::corpus::CorpusServer;

pub struct Harness {
    handler: CodeIndexingTaskHandler,
    _cache_dir: tempfile::TempDir,
}

impl Harness {
    pub fn build(
        corpus: &CorpusServer,
        clickhouse: &ClickHouseConfiguration,
        pipeline_config: CodeIndexingPipelineConfig,
        cache_dir: tempfile::TempDir,
    ) -> anyhow::Result<Self> {
        let repository_service = RailsRepositoryService::create(Arc::new(corpus.gitlab_client()?));
        let graph_client = Arc::new(clickhouse.build_client());
        let checkpoint_store = Arc::new(ClickHouseCodeCheckpointStore::new(Arc::clone(
            &graph_client,
        )));
        let ontology = ontology::Ontology::load_embedded()?;
        let table_names = Arc::new(CodeTableNames::from_ontology(&ontology)?);
        let stale_data_cleaner =
            Arc::new(ClickHouseStaleDataCleaner::new(graph_client, &table_names));
        let metrics = CodeMetrics::new();

        let max_file_size = match pipeline_config.max_file_size_bytes {
            0 => u64::MAX,
            n => n,
        };
        let cache: Arc<dyn RepositoryCache> = Arc::new(LocalRepositoryCache::new(
            cache_dir.path().to_path_buf(),
            max_file_size,
            pipeline_config.max_total_bytes,
            metrics.clone(),
        ));
        let resolver = RepositoryResolver::new(Arc::clone(&repository_service), cache);

        let writer = Arc::new(indexer::clickhouse::ClickHouseWriter::new(
            clickhouse.clone(),
            Arc::new(indexer::metrics::EngineMetrics::new()),
        )?);

        let pipeline = Arc::new(CodeIndexer::new(
            resolver,
            writer,
            Arc::clone(&checkpoint_store) as _,
            stale_data_cleaner,
            metrics.clone(),
            table_names,
            Arc::new(ontology),
            pipeline_config,
        ));

        let handler = CodeIndexingTaskHandler::new(
            pipeline,
            repository_service,
            checkpoint_store as _,
            metrics,
            std::time::Duration::from_secs(300),
            CodeIndexingTaskRequest::subscription(),
            indexer::analytics::IndexingAnalytics::disabled(),
        );

        Ok(Self {
            handler,
            _cache_dir: cache_dir,
        })
    }

    /// `task_id` must increase across rounds: the handler skips any task whose id
    /// the checkpoint has already seen, so a fixed id turns every re-index into a
    /// silent no-op and the stale sweep never runs.
    pub async fn index(
        &self,
        task_id: i64,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        traversal_path: &str,
    ) -> anyhow::Result<()> {
        let envelope = Envelope::new(&CodeIndexingTaskRequest {
            task_id,
            project_id,
            branch: Some(branch.to_string()),
            commit_sha: Some(commit_sha.to_string()),
            traversal_path: TraversalPath::new_unchecked(traversal_path),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
        })
        .map_err(|e| anyhow::anyhow!("failed to build envelope: {e}"))?;

        let mock_nats = Arc::new(MockNatsServices::new());
        let context = HandlerContext::new(
            mock_nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(indexer::indexing_status::IndexingStatusStore::new(
                mock_nats,
            )),
        );

        self.handler
            .handle(context, envelope)
            .await
            .map_err(|e| anyhow::anyhow!("code indexing handler failed: {e}"))
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.handler
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("flush failed: {e}"))
    }
}
