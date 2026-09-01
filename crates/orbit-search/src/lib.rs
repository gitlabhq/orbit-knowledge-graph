pub mod anchor;
pub mod ask;
pub mod corpus;
pub mod expand;
pub mod ppr;
pub mod rank;
pub mod text;
pub mod types;
pub mod vocab;

pub use anchor::{
    BASE_SET_PER_TERM, MAX_SEEDS, MIN_SEEDS_PER_TERM, term_base_sets, unmatched_terms,
};
pub use ask::{AskMatch, AskOutcome, Caller, CallerEdge, Reranker, TermRecall};
pub use ppr::KindRates;
pub use rank::{ANCHOR_SIM, CONFIDENT_COVERAGE, Hit, rank_and_trim};
pub use text::content_words;
pub use types::{CorpusRow, Edge, Graph, GraphEdge};
pub use vocab::SearchVocab;

#[cfg(test)]
pub(crate) mod testutil {
    use crate::types::CorpusRow;
    use crate::vocab::SearchVocab;

    pub fn test_vocab() -> SearchVocab {
        SearchVocab::new([("call", "Calls")])
    }

    pub fn test_stem(word: &str) -> String {
        match word {
            "calls" | "calling" | "called" => "call",
            other => other,
        }
        .to_string()
    }

    pub fn row(id: i64, fqn: &str) -> CorpusRow {
        CorpusRow {
            id,
            fqn: fqn.to_string(),
            kind: "Definition".to_string(),
            loc: String::new(),
            end_line: 0,
            degree: 0,
            grams: 0,
        }
    }
}
