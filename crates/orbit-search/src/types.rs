#[derive(Clone)]
pub struct CorpusRow {
    pub id: i64,
    pub fqn: String,
    pub kind: String,
    pub loc: String,
    pub end_line: i64,
    pub degree: u64,
    pub grams: u64,
}
