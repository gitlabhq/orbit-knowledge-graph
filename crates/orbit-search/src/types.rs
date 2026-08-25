use std::collections::HashMap;

pub struct GraphEdge {
    pub kind: u16,
    pub source: i64,
    pub target: i64,
}

pub struct Graph {
    pub kinds: Vec<String>,
    pub edges: Vec<GraphEdge>,
    pub degrees: Option<HashMap<i64, u64>>,
}

impl Graph {
    pub fn degrees_or_derived(&self) -> HashMap<i64, u64> {
        match &self.degrees {
            Some(degrees) => degrees.clone(),
            None => {
                let mut derived: HashMap<i64, u64> = HashMap::new();
                for e in &self.edges {
                    *derived.entry(e.source).or_insert(0) += 1;
                    *derived.entry(e.target).or_insert(0) += 1;
                }
                derived
            }
        }
    }
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

pub struct Edge {
    pub kind: String,
    pub source: String,
    pub source_loc: String,
    pub target: String,
    pub target_loc: String,
}
