use crate::agent::AgentState;
use crate::api_client::{ApiClient, Note, PublishedNote};
use crate::data_generator::DataGenerator;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use super::Action;

pub struct ReplyToIssueComment;

#[async_trait]
impl Action for ReplyToIssueComment {
    fn name(&self) -> &'static str {
        "reply_to_issue_comment"
    }

    fn can_execute(&self, state: &AgentState, shared: &SharedState) -> bool {
        // Can reply to any issue note from another agent
        shared.has_issue_notes_from_others(state.user_id)
    }

    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()> {
        // Get a random issue note from another agent
        let original_note = shared
            .random_issue_note_not_by(state.user_id)
            .ok_or_else(|| anyhow::anyhow!("No issue comments from other agents available"))?;

        // Create a reply that quotes the original comment
        let reply_body = DataGenerator::reply_body(&original_note.body);
        let note: Note = client
            .post(
                &format!(
                    "/projects/{}/issues/{}/notes",
                    original_note.project_id, original_note.noteable_iid
                ),
                &json!({ "body": reply_body }),
            )
            .await?;

        // Publish this reply so others can respond to it too
        shared.publish_issue_note(PublishedNote {
            note_id: note.id,
            project_id: original_note.project_id,
            noteable_type: "Issue".to_string(),
            noteable_iid: original_note.noteable_iid,
            author_id: state.user_id,
            body: reply_body,
        });

        Ok(())
    }
}
