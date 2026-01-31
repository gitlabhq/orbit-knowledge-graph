use crate::agent::AgentState;
use crate::api_client::ApiClient;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct ApproveMergeRequest;

#[async_trait]
impl Action for ApproveMergeRequest {
    fn name(&self) -> &'static str {
        "approve_merge_request"
    }

    fn can_execute(&self, _state: &AgentState, shared: &SharedState) -> bool {
        shared.has_open_merge_requests()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        // Get a random open MR that wasn't created by this user
        let mr = shared
            .random_open_merge_request_not_by(state.user_id)
            .ok_or_else(|| anyhow::anyhow!("No merge requests from other users available"))?;

        let (status, _) = client
            .post_with_status(
                &format!(
                    "/projects/{}/merge_requests/{}/approve",
                    mr.project_id, mr.iid
                ),
                &json!({}),
            )
            .await?;

        if !status.is_success() {
            anyhow::bail!("Failed to approve MR: {}", status);
        }

        Ok(())
    }
}
