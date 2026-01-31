use crate::agent::AgentState;
use crate::api_client::{ApiClient, Issue};
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct CloseIssue;

#[async_trait]
impl Action for CloseIssue {
    fn name(&self) -> &'static str {
        "close_issue"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        state.has_open_issues()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        let issue = state.random_open_issue().unwrap();

        let _: Issue = client
            .put(
                &format!("/projects/{}/issues/{}", issue.project_id, issue.iid),
                &json!({ "state_event": "close" }),
            )
            .await?;

        // Update both local and shared state
        shared.close_issue(issue.project_id, issue.iid);
        state.close_issue(issue.project_id, issue.iid);
        Ok(())
    }
}
