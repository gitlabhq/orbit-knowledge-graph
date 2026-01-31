use crate::agent::AgentState;
use crate::api_client::{ApiClient, FileResponse, Project};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use urlencoding::encode;

use super::Action;

pub struct CreateProject;

#[async_trait]
impl Action for CreateProject {
    fn name(&self) -> &'static str {
        "create_project"
    }

    fn can_execute(&self, _state: &AgentState, _shared: &SharedState) -> bool {
        true
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        let project_name = DataGenerator::project_name();

        let project: Project = client
            .post(
                "/projects",
                &json!({
                    "name": project_name,
                    "namespace_id": state.namespace_id,
                    "visibility": "public",
                    "description": DataGenerator::project_description(),
                    "initialize_with_readme": true
                }),
            )
            .await?;

        // Create pom.xml
        let encoded_path = encode("pom.xml");
        let _: FileResponse = client
            .post(
                &format!("/projects/{}/repository/files/{}", project.id, encoded_path),
                &json!({
                    "branch": project.default_branch.as_deref().unwrap_or("main"),
                    "content": DataGenerator::pom_xml_content(&project_name),
                    "commit_message": "Add Maven pom.xml"
                }),
            )
            .await?;

        // Publish to shared state for other agents
        shared.publish_project(project.clone());
        state.add_project(project);
        Ok(())
    }
}
