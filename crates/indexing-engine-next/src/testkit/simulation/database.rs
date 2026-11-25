use crate::lease::{Fence, Generation, Revision};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseWrite {
    Kept,
    Rejected,
}

#[derive(Clone, Copy, Debug)]
pub struct SimulatedDatabase {
    highest_fence: Fence,
    generation: Generation,
}

impl Default for SimulatedDatabase {
    fn default() -> Self {
        Self {
            highest_fence: Fence {
                generation: Generation(0),
                revision: Revision(0),
            },
            generation: Generation(1),
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

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn start_next_generation(&mut self) -> Generation {
        self.generation = Generation(self.generation.0 + 1);
        self.generation
    }
}
