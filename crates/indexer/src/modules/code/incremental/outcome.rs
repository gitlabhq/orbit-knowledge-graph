use std::time::Duration;

use code_graph_incremental::sentinel::Killed;

#[derive(Default)]
pub(in crate::modules::code) struct Outcome {
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

pub(in crate::modules::code) struct ReadFailure {
    pub path: String,
    pub detail: String,
}

pub(in crate::modules::code) struct FamilyTiming {
    pub language: String,
    pub files: usize,
    pub bytes: u64,
    pub elapsed: Duration,
}
