pub mod corpus;
pub mod grep;
pub mod rank;
pub mod text;
pub mod types;
pub mod vocab;

pub use grep::{GrepMatch, GrepOutcome, RecallFilter, TermRecall, unmatched_terms};
pub use rank::{ANCHOR_SIM, EXACT_NAME_SIM, Hit, rank_and_trim};
pub use text::content_words;
pub use types::SearchCandidate;
pub use vocab::SearchVocab;

#[cfg(test)]
pub(crate) mod testutil {
    use crate::types::SearchCandidate;
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

    pub fn row(id: i64, label: &str) -> SearchCandidate {
        let parent_group = label
            .rfind("::")
            .or_else(|| label.rfind('.'))
            .map_or(label, |index| &label[..index]);
        SearchCandidate {
            id,
            label: label.to_string(),
            parent_group: parent_group.to_string(),
            diversity_group: String::new(),
            degree: 0,
            document_length: 0,
        }
    }
}
