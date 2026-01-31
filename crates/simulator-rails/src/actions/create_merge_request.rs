use crate::agent::AgentState;
use crate::api_client::{ApiClient, Branch, FileResponse, MergeRequest};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use rand::Rng;
use serde_json::json;
use urlencoding::encode;

use super::Action;

pub struct CreateMergeRequest;

#[async_trait]
impl Action for CreateMergeRequest {
    fn name(&self) -> &'static str {
        "create_merge_request"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        // Need either existing branches or files to create a new branch
        state.has_files() || state.branches.values().any(|b| !b.is_empty())
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        // 70% chance to use an existing branch if available
        let use_existing_branch = rand::thread_rng().gen_bool(0.7);

        let (project, branch_name, needs_file) = if use_existing_branch {
            if let Some((project, branch)) = state.random_project_with_branches() {
                (project, branch, false)
            } else {
                // Fall back to creating a new branch
                let project = state.random_project_with_files().ok_or_else(|| {
                    anyhow::anyhow!("No projects with files available")
                })?;
                let branch = create_new_branch(client, &project, state).await?;
                (project, branch, true)
            }
        } else {
            let project = state.random_project_with_files().ok_or_else(|| {
                anyhow::anyhow!("No projects with files available")
            })?;
            let branch = create_new_branch(client, &project, state).await?;
            (project, branch, true)
        };

        let default_branch = project.default_branch.as_deref().unwrap_or("main");

        // Add a file to the branch if it's newly created
        if needs_file {
            let class_name = DataGenerator::java_class_name();
            let package = DataGenerator::java_package();
            let file_path = DataGenerator::java_file_path(&class_name, &package);
            let encoded_path = encode(&file_path);

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
        }

        // Create merge request
        let mr: MergeRequest = client
            .post(
                &format!("/projects/{}/merge_requests", project.id),
                &json!({
                    "source_branch": branch_name,
                    "target_branch": default_branch,
                    "title": DataGenerator::merge_request_title(),
                    "description": DataGenerator::merge_request_description()
                }),
            )
            .await?;

        shared.publish_merge_request(mr.clone());
        state.add_merge_request(mr);
        Ok(())
    }
}

async fn create_new_branch(
    client: &ApiClient,
    project: &crate::api_client::Project,
    state: &mut AgentState,
) -> Result<String> {
    let branch_name = DataGenerator::branch_name();
    let default_branch = project.default_branch.as_deref().unwrap_or("main");

    let _: Branch = client
        .post(
            &format!("/projects/{}/repository/branches", project.id),
            &json!({
                "branch": branch_name,
                "ref": default_branch
            }),
        )
        .await?;

    state.add_branch(project.id, branch_name.clone());
    Ok(branch_name)
}
