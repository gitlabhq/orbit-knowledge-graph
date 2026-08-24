#[derive(Clone)]
pub struct CorpusRow {
    pub id: String,
    pub fqn: String,
    pub kind: String,
    pub loc: String,
    pub end_line: String,
    pub degree: String,
}

pub struct Edge {
    pub kind: String,
    pub source: String,
    pub source_loc: String,
    pub target: String,
    pub target_loc: String,
}
