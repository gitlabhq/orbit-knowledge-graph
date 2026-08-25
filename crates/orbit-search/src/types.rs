pub struct GraphEdge {
    pub kind: u16,
    pub source: i64,
    pub target: i64,
}

pub struct Graph {
    pub kinds: Vec<String>,
    pub edges: Vec<GraphEdge>,
}

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

pub struct TermSeeds {
    pub seeds: Vec<(i64, f64)>,
    pub weight: f64,
}

pub struct Edge {
    pub kind: String,
    pub source: String,
    pub source_loc: String,
    pub target: String,
    pub target_loc: String,
}
