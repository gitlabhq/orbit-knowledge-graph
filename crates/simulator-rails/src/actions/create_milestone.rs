use crate::agent::AgentState;
use crate::api_client::{ApiClient, Milestone};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use rand::Rng;
use serde_json::json;

use super::Action;

pub struct CreateMilestone;

#[async_trait]
impl Action for CreateMilestone {
    fn name(&self) -> &'static str {
        "create_milestone"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        !state.projects.is_empty()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        _shared: &SharedState,
    ) -> Result<()> {
        let project = state.random_project().unwrap();
        let days_offset = rand::thread_rng().gen_range(30..90);
        let due_date = chrono::Utc::now() + chrono::Duration::days(days_offset);

        let milestone: Milestone = client
            .post(
                &format!("/projects/{}/milestones", project.id),
                &json!({
                    "title": DataGenerator::milestone_title(),
                    "description": DataGenerator::milestone_description(),
                    "due_date": due_date.format("%Y-%m-%d").to_string()
                }),
            )
            .await?;

        state.add_milestone(project.id, milestone);
        Ok(())
    }
}
