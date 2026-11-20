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
