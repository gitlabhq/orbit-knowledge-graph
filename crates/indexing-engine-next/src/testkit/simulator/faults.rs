#[derive(Clone, Copy, Debug)]
pub struct FaultBudget {
    stalls_left: u32,
    crashes_left: u32,
}

impl FaultBudget {
    pub fn new(stalls: u32, crashes: u32) -> Self {
        Self {
            stalls_left: stalls,
            crashes_left: crashes,
        }
    }

    pub fn can_stall(&self) -> bool {
        self.stalls_left > 0
    }

    pub fn can_crash(&self) -> bool {
        self.crashes_left > 0
    }

    pub fn take_stall(&mut self) {
        self.stalls_left = self.stalls_left.saturating_sub(1);
    }

    pub fn take_crash(&mut self) {
        self.crashes_left = self.crashes_left.saturating_sub(1);
    }

    pub fn stalls_left(&self) -> u32 {
        self.stalls_left
    }

    pub fn crashes_left(&self) -> u32 {
        self.crashes_left
    }
}
