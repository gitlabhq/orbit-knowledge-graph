mod approve_merge_request;
mod attach_milestone;
mod close_issue;
mod close_merge_request;
mod comment_on_issue;
mod comment_on_merge_request;
mod create_file;
mod create_issue;
mod create_merge_request;
mod create_milestone;
mod create_project;
mod link_issues;
mod merge_merge_request;
mod push_to_merge_request;
mod reply_to_issue_comment;
mod reply_to_mr_discussion;
mod update_file;

use crate::agent::AgentState;
use crate::api_client::ApiClient;
use crate::shared_state::SharedState;
use anyhow::Result;
use async_trait::async_trait;

pub use approve_merge_request::ApproveMergeRequest;
pub use attach_milestone::AttachMilestone;
pub use close_issue::CloseIssue;
pub use close_merge_request::CloseMergeRequest;
pub use comment_on_issue::CommentOnIssue;
pub use comment_on_merge_request::CommentOnMergeRequest;
pub use create_file::CreateFile;
pub use create_issue::CreateIssue;
pub use create_merge_request::CreateMergeRequest;
pub use create_milestone::CreateMilestone;
pub use create_project::CreateProject;
pub use link_issues::LinkIssues;
pub use merge_merge_request::MergeMergeRequest;
pub use push_to_merge_request::PushToMergeRequest;
pub use reply_to_issue_comment::ReplyToIssueComment;
pub use reply_to_mr_discussion::ReplyToMrDiscussion;
pub use update_file::UpdateFile;

#[async_trait]
pub trait Action: Send + Sync {
    fn name(&self) -> &'static str;
    fn can_execute(&self, state: &AgentState, shared: &SharedState) -> bool;
    async fn execute(
        &self,
        client: &ApiClient,
        state: &mut AgentState,
        shared: &SharedState,
    ) -> Result<()>;
}

pub fn all_actions() -> Vec<Box<dyn Action>> {
    vec![
        Box::new(CreateProject),
        Box::new(CreateFile),
        Box::new(UpdateFile),
        Box::new(CreateIssue),
        Box::new(CloseIssue),
        Box::new(LinkIssues),
        Box::new(CreateMilestone),
        Box::new(AttachMilestone),
        Box::new(CreateMergeRequest),
        Box::new(PushToMergeRequest),
        Box::new(CommentOnIssue),
        Box::new(CommentOnMergeRequest),
        Box::new(ApproveMergeRequest),
        Box::new(MergeMergeRequest),
        Box::new(CloseMergeRequest),
        Box::new(ReplyToIssueComment),
        Box::new(ReplyToMrDiscussion),
    ]
}
