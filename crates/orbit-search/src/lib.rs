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
pub use ask::{AskMatch, AskOutcome};
pub use ppr::KindRates;
pub use rank::{BM25_B, BM25_K1, CONFIDENT_COVERAGE, Hit, rank_and_trim};
pub use text::{candidate_splits, content_words, query_tokens, search_document, split_words, stem};
pub use types::{CorpusRow, Edge};
pub use vocab::SearchVocab;

#[cfg(test)]
pub(crate) mod testutil {
    use crate::types::CorpusRow;
    use crate::vocab::SearchVocab;

    pub fn test_vocab() -> SearchVocab {
        SearchVocab::new(["Contains", "Defines", "Imports", "Calls", "Extends"])
    }

    pub fn row(fqn: &str) -> CorpusRow {
        CorpusRow {
            id: fqn.to_string(),
            fqn: fqn.to_string(),
            kind: "Definition".to_string(),
            loc: String::new(),
            end_line: "0".to_string(),
            degree: "0".to_string(),
        }
    }
}
