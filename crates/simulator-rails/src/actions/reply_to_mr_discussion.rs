use crate::agent::AgentState;
use crate::api_client::{ApiClient, Note};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct ReplyToMrDiscussion;

#[async_trait]
impl Action for ReplyToMrDiscussion {
    fn name(&self) -> &'static str {
        "reply_to_mr_discussion"
    }

    fn can_execute(&self, state: &AgentState, shared: &SharedState) -> bool {
        // Can reply to any MR discussion from another agent
        shared.has_mr_discussions_from_others(state.user_id)
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        // Get a random MR discussion from another agent
        let discussion = shared
            .random_mr_discussion_not_by(state.user_id)
            .ok_or_else(|| anyhow::anyhow!("No MR discussions from other agents available"))?;

        // Reply to the discussion thread using GitLab's discussions API
        let reply_body = DataGenerator::comment_body();
        let _: Note = client
            .post(
                &format!(
                    "/projects/{}/merge_requests/{}/discussions/{}/notes",
                    discussion.project_id, discussion.mr_iid, discussion.discussion_id
                ),
                &json!({ "body": reply_body }),
            )
            .await?;

        // Note: We don't publish this as a new discussion since it's a reply,
        // not a new thread. The original discussion can still receive more replies.

        Ok(())
    }
}
