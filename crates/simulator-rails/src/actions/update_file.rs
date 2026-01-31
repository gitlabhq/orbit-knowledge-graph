use crate::agent::AgentState;
use crate::api_client::{ApiClient, Branch, FileResponse};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use rand::Rng;
use serde_json::json;
use urlencoding::encode;

use super::Action;

pub struct UpdateFile;

#[async_trait]
impl Action for UpdateFile {
    fn name(&self) -> &'static str {
        "update_file"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        state.has_files()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        _shared: &SharedState,
    ) -> Result<()> {
        let (project, file) = state.random_project_with_file().unwrap();
        let encoded_path = encode(&file.path);
        let default_branch = project.default_branch.as_deref().unwrap_or("main");

        // 50% chance to reuse an existing branch, otherwise create a new one
        let reuse_branch = rand::thread_rng().gen_bool(0.5) && state.has_branches(project.id);

        let branch_name = if reuse_branch {
            state.random_branch(project.id).unwrap()
        } else {
            // Create a new feature branch
            let new_branch = DataGenerator::branch_name();
            let _: Branch = client
                .post(
                    &format!("/projects/{}/repository/branches", project.id),
                    &json!({
                        "branch": new_branch,
                        "ref": default_branch
                    }),
                )
                .await?;
            // Track the new branch
            state.add_branch(project.id, new_branch.clone());
            new_branch
        };

        // Update file on the branch
        let _: FileResponse = client
            .put(
                &format!(
                    "/projects/{}/repository/files/{}",
                    project.id, encoded_path
                ),
                &json!({
                    "branch": branch_name,
                    "content": DataGenerator::java_class_content(&file.class_name, &file.package),
                    "commit_message": DataGenerator::commit_message_for_update(&file.class_name)
                }),
            )
            .await?;

        Ok(())
    }
}
