use crate::agent::AgentState;
use crate::api_client::{ApiClient, Issue};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct CreateIssue;

#[async_trait]
impl Action for CreateIssue {
    fn name(&self) -> &'static str {
        "create_issue"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        !state.projects.is_empty()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        let project = state.random_project().unwrap();

        let issue: Issue = client
            .post(
                &format!("/projects/{}/issues", project.id),
                &json!({
                    "title": DataGenerator::issue_title(),
                    "description": DataGenerator::issue_description()
                }),
            )
            .await?;

        // Publish to shared state for other agents to comment on
        shared.publish_issue(issue.clone());
        state.add_issue(issue);
        Ok(())
    }
}
