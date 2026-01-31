use crate::agent::AgentState;
use crate::api_client::{ApiClient, FileResponse};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::json;
use urlencoding::encode;

use super::Action;

pub struct PushToMergeRequest;

#[async_trait]
impl Action for PushToMergeRequest {
    fn name(&self) -> &'static str {
        "push_to_merge_request"
    }

    fn can_execute(&self, state: &AgentState, _shared: &SharedState) -> bool {
        state.has_open_merge_requests()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        _shared: &SharedState,
    ) -> Result<()> {
        let mr = state.random_open_merge_request().unwrap();

        // 50% chance to update an existing file, 50% to create a new one
        let update_existing = rand::thread_rng().gen_bool(0.5)
            && state.files.get(&mr.project_id).map_or(false, |f| !f.is_empty());

        if update_existing {
            // Update an existing file
            let files = state.files.get(&mr.project_id).unwrap();
            let file = files.choose(&mut rand::thread_rng()).unwrap();
            let encoded_path = encode(&file.path);

            let _: FileResponse = client
                .put(
                    &format!(
                        "/projects/{}/repository/files/{}",
                        mr.project_id, encoded_path
                    ),
                    &json!({
                        "branch": mr.source_branch,
                        "content": DataGenerator::java_class_content(&file.class_name, &file.package),
                        "commit_message": DataGenerator::commit_message_for_update(&file.class_name)
                    }),
                )
                .await?;
        } else {
            // Create a new file with unique name
            let class_name = DataGenerator::java_class_name();
            let package = DataGenerator::java_package();
            let file_path = DataGenerator::java_file_path(&class_name, &package);
            let encoded_path = encode(&file_path);

            let _: FileResponse = client
                .post(
                    &format!(
                        "/projects/{}/repository/files/{}",
                        mr.project_id, encoded_path
                    ),
                    &json!({
                        "branch": mr.source_branch,
                        "content": DataGenerator::java_class_content(&class_name, &package),
                        "commit_message": DataGenerator::commit_message_for_new_file(&class_name)
                    }),
                )
                .await?;

            // Track the new file
            state.add_file(mr.project_id, file_path, class_name, package);
        }

        Ok(())
    }
}
