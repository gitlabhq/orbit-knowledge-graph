use std::time::Duration;

use crate::sentinel::Killed;

#[derive(Default)]
pub struct Outcome {
    pub files: usize,
    pub source_bytes: u64,
    pub candidates: usize,
    pub filtered: usize,
    pub directories: usize,
    pub definitions: usize,
    pub imports: usize,
    pub edges: usize,
    pub skipped: Vec<Killed>,
    pub faults: Vec<ReadFailure>,
    pub families: Vec<FamilyTiming>,
    pub elapsed: Duration,
}

pub struct ReadFailure {
    pub path: String,
    pub detail: String,
}

pub struct FamilyTiming {
    pub language: String,
    pub files: usize,
    pub bytes: u64,
    pub elapsed: Duration,
}
