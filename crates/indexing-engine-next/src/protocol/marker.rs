use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publish {
    New(TaskId),
    Pending(TaskId),
}

pub fn decide_submit(marker: Option<TaskId>, new_task: TaskId) -> Publish {
    match marker {
        None => Publish::New(new_task),
        Some(pending) => Publish::Pending(pending),
    }
}
