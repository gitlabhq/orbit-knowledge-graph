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
pub enum Publish<T = TaskId> {
    New(T),
    Pending(T),
}

pub fn decide_submit<T>(marker: Option<T>, new_task: T) -> Publish<T> {
    match marker {
        None => Publish::New(new_task),
        Some(pending) => Publish::Pending(pending),
    }
}

pub fn start_clears_marker(marker: Option<TaskId>, started: TaskId) -> bool {
    marker == Some(started)
}
