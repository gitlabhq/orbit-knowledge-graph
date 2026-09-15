#[derive(Clone)]
pub struct CorpusRow {
    pub id: i64,
    pub fqn: String,
    pub file: String,
    pub degree: u64,
    pub grams: u64,
}
