#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FaultBudget {
    pub stalls: u32,
    pub crashes: u32,
    pub wipes: u32,
    pub duplicates: u32,
    pub losses: u32,
}
