use crate::protocol::Fence;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreWrite {
    Kept,
    Rejected,
}

#[derive(Clone, Copy, Debug)]
pub struct SimulatedStore {
    highest_fence: Fence,
}

impl Default for SimulatedStore {
    fn default() -> Self {
        Self {
            highest_fence: Fence(0),
        }
    }
}

impl SimulatedStore {
    pub fn write(&mut self, fence: Fence) -> StoreWrite {
        if fence < self.highest_fence {
            return StoreWrite::Rejected;
        }
        self.highest_fence = fence;
        StoreWrite::Kept
    }

    pub fn highest_fence(&self) -> Fence {
        self.highest_fence
    }
}
