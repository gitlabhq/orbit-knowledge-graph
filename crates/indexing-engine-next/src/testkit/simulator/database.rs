use crate::protocol::Fence;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseWrite {
    Kept,
    Rejected,
}

#[derive(Clone, Copy, Debug)]
pub struct SimulatedDatabase {
    highest_fence: Fence,
}

impl Default for SimulatedDatabase {
    fn default() -> Self {
        Self {
            highest_fence: Fence(0),
        }
    }
}

impl SimulatedDatabase {
    pub fn write(&mut self, fence: Fence) -> DatabaseWrite {
        if fence < self.highest_fence {
            return DatabaseWrite::Rejected;
        }
        self.highest_fence = fence;
        DatabaseWrite::Kept
    }

    pub fn highest_fence(&self) -> Fence {
        self.highest_fence
    }
}
