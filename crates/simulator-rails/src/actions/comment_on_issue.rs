use crate::agent::AgentState;
use crate::api_client::{ApiClient, Note, PublishedNote};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct CommentOnIssue;

#[async_trait]
impl Action for CommentOnIssue {
    fn name(&self) -> &'static str {
        "comment_on_issue"
    }

    fn can_execute(&self, _state: &AgentState, shared: &SharedState) -> bool {
        shared.has_issues()
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        let issue = shared
            .random_issue()
            .ok_or_else(|| anyhow::anyhow!("No issues available"))?;

        let body = DataGenerator::comment_body();
        let note: Note = client
            .post(
                &format!(
                    "/projects/{}/issues/{}/notes",
                    issue.project_id, issue.iid
                ),
                &json!({ "body": body }),
            )
            .await?;

        shared.publish_issue_note(PublishedNote {
            note_id: note.id,
            project_id: issue.project_id,
            noteable_type: "Issue".to_string(),
            noteable_iid: issue.iid,
            author_id: state.user_id,
            body,
        });

        Ok(())
    }
}
