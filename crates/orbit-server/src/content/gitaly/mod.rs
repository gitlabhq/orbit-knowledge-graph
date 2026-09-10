pub mod file_content;
pub mod mr_diff;
mod proxy;

use std::sync::Arc;

use async_trait::async_trait;
use gitlab_client::GitlabClient;
use orbit_server_config::{GitalyProxyConfig, GitalyTransport};
use orbit_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};

pub struct GitalyService {
    file_content: file_content::GitalyContentService,
    mr_diff: mr_diff::MergeRequestDiffContentService,
}

impl GitalyService {
    pub fn new(
        client: Arc<GitlabClient>,
        transport: GitalyTransport,
        proxy_config: &GitalyProxyConfig,
    ) -> Self {
        Self {
            file_content: file_content::GitalyContentService::with_transport(
                client.clone(),
                transport,
                proxy_config,
            ),
            mr_diff: mr_diff::MergeRequestDiffContentService::new(client),
        }
    }
}

#[async_trait]
impl ColumnResolver for GitalyService {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        match lookup {
            "patch" | "raw_patch" | "mr_raw_patch" => {
                self.mr_diff.resolve_batch(lookup, rows, ctx).await
            }
            _ => self.file_content.resolve_batch(lookup, rows, ctx).await,
        }
    }
}
