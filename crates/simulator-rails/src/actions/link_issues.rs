use crate::agent::AgentState;
use crate::api_client::ApiClient;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use rand::seq::SliceRandom;
use serde_json::json;

use super::Action;

pub struct LinkIssues;

#[async_trait]
impl Action for LinkIssues {
    fn name(&self) -> &'static str {
        "link_issues"
    }

    fn can_execute(&self, _state: &AgentState, shared: &SharedState) -> bool {
        shared.has_multiple_issues()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        _state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        let issues = shared.random_issues(2);
        if issues.len() < 2 {
            return Ok(());
        }

        let source = &issues[0];
        let target = &issues[1];

        let link_types = ["relates_to", "blocks", "is_blocked_by"];
        let link_type = link_types.choose(&mut rand::thread_rng()).unwrap();

        let (status, _) = client
            .post_with_status(
                &format!("/projects/{}/issues/{}/links", source.project_id, source.iid),
                &json!({
                    "target_project_id": target.project_id,
                    "target_issue_iid": target.iid,
                    "link_type": link_type
                }),
            )
            .await?;

        // Accept 201 (created) or 409 (already linked)
        if !status.is_success() && status.as_u16() != 409 {
            anyhow::bail!("Failed to link issues: {}", status);
        }

        Ok(())
    }
}
