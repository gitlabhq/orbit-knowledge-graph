use crate::agent::AgentState;
use crate::api_client::{ApiClient, Issue};
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct AttachMilestone;

#[async_trait]
impl Action for AttachMilestone {
    fn name(&self) -> &'static str {
        "attach_milestone"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        !state.milestones.is_empty() && state.has_open_issues()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        _shared: &SharedState,
    ) -> Result<()> {
        let milestone = state.random_milestone().unwrap();
        let issue = state.random_open_issue().unwrap();

        let _: Issue = client
            .put(
                &format!("/projects/{}/issues/{}", issue.project_id, issue.iid),
                &json!({ "milestone_id": milestone.id }),
            )
            .await?;

        Ok(())
    }
}
