#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Rejected {
    #[error("the handle belongs to another plan")]
    ForeignPlan,
    #[error("the declaration or plan belongs to another catalog snapshot")]
    ForeignSnapshot,
    #[error("the rows already feed another operation")]
    Consumed,
    #[error("the rows belong to another scope")]
    OutOfScope,
    #[error("output {0} is not visible to this operation")]
    NotVisible(String),
    #[error("shared body {0} is not visible in this scope")]
    BodyNotVisible(String),
    #[error("{0} operations feed nothing")]
    Unused(usize),
    #[error("an earlier construction step failed; the session is closed")]
    Failed,
    #[error("the replacement changes outputs {0} without reconnecting their consumers")]
    ChangedOutputs(String),
    #[error("reconnection needs a rewrite and an output of the replaced operation")]
    BadReconnection,
    #[error(
        "only the operation a rewrite is visiting can be detached, before anything consumes it"
    )]
    NotDetachable,
}
