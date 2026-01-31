use crate::agent::AgentState;
use crate::api_client::{ApiClient, Branch, FileResponse};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use urlencoding::encode;

use super::Action;

pub struct CreateFile;

#[async_trait]
impl Action for CreateFile {
    fn name(&self) -> &'static str {
        "create_file"
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
        let class_name = DataGenerator::java_class_name();
        let package = DataGenerator::java_package();
        let file_path = DataGenerator::java_file_path(&class_name, &package);
        let encoded_path = encode(&file_path);
        let default_branch = project.default_branch.as_deref().unwrap_or("main");

        // Create a feature branch first - never push directly to main
        let branch_name = DataGenerator::branch_name();
        let _: Branch = client
            .post(
                &format!("/projects/{}/repository/branches", project.id),
                &json!({
                    "branch": branch_name,
                    "ref": default_branch
                }),
            )
            .await?;

        // Push file to the new branch
        let _: FileResponse = client
            .post(
                &format!(
                    "/projects/{}/repository/files/{}",
                    project.id, encoded_path
                ),
                &json!({
                    "branch": branch_name,
                    "content": DataGenerator::java_class_content(&class_name, &package),
                    "commit_message": DataGenerator::commit_message_for_new_file(&class_name)
                }),
            )
            .await?;

        // Track the branch for reuse
        state.add_branch(project.id, branch_name);
        state.add_file(project.id, file_path, class_name, package);
        Ok(())
    }
}
