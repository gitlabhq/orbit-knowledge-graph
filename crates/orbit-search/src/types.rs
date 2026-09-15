#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Definition {
    pub id: i64,
    pub fqn: String,
    pub kind: String,
    pub file: String,
    pub start: usize,
    pub end: usize,
}

#[derive(Clone)]
pub struct CorpusRow {
    pub definition: Definition,
    pub degree: u64,
    pub grams: u64,
}
