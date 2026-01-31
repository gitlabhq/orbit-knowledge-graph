use crate::agent::AgentState;
use crate::api_client::ApiClient;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct MergeMergeRequest;

#[async_trait]
impl Action for MergeMergeRequest {
    fn name(&self) -> &'static str {
        "merge_merge_request"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        state.has_open_merge_requests()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        let mr = state.random_open_merge_request().unwrap();

        let (status, _) = client
            .put_with_status(
                &format!(
                    "/projects/{}/merge_requests/{}/merge",
                    mr.project_id, mr.iid
                ),
                &json!({ "should_remove_source_branch": true }),
            )
            .await?;

        // Accept 200 (merged), 405 (not mergeable), 406 (already merged)
        if status.is_success() {
            shared.close_merge_request(mr.project_id, mr.iid);
            state.close_merge_request(mr.project_id, mr.iid);
        } else if status.as_u16() != 405 && status.as_u16() != 406 {
            anyhow::bail!("Failed to merge MR: {}", status);
        }

        Ok(())
    }
}
