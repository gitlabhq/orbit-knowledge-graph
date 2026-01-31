use crate::agent::AgentState;
use crate::api_client::{ApiClient, Discussion, PublishedDiscussion};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct CommentOnMergeRequest;

#[async_trait]
impl Action for CommentOnMergeRequest {
    fn name(&self) -> &'static str {
        "comment_on_merge_request"
    }

    fn can_execute(&self, _state: &AgentState, shared: &SharedState) -> bool {
        // Can comment on any MR in the shared pool
        shared.has_merge_requests()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        // Get a random MR from the shared pool (could be any agent's MR)
        let mr = shared
            .random_merge_request()
            .ok_or_else(|| anyhow::anyhow!("No merge requests available"))?;

        // Use discussions API for threaded comments that can be replied to
        let discussion: Discussion = client
            .post(
                &format!(
                    "/projects/{}/merge_requests/{}/discussions",
                    mr.project_id, mr.iid
                ),
                &json!({ "body": DataGenerator::comment_body() }),
            )
            .await?;

        // Publish to shared state so other agents can reply to this discussion
        shared.publish_mr_discussion(PublishedDiscussion {
            discussion_id: discussion.id,
            project_id: mr.project_id,
            mr_iid: mr.iid,
            author_id: state.user_id,
        });

        Ok(())
    }
}
