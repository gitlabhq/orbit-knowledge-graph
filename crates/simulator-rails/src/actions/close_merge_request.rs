use crate::agent::AgentState;
use crate::api_client::{ApiClient, MergeRequest};
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct CloseMergeRequest;

#[async_trait]
impl Action for CloseMergeRequest {
    fn name(&self) -> &'static str {
        "close_merge_request"
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

        let _: MergeRequest = client
            .put(
                &format!(
                    "/projects/{}/merge_requests/{}",
                    mr.project_id, mr.iid
                ),
                &json!({ "state_event": "close" }),
            )
            .await?;

        shared.close_merge_request(mr.project_id, mr.iid);
        state.close_merge_request(mr.project_id, mr.iid);
        Ok(())
    }
}
