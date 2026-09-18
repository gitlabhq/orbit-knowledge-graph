use std::collections::HashSet;

pub struct GrepOutcome {
    pub alternatives: Vec<String>,
    pub exact_alternatives: Vec<String>,
    pub matches: Vec<GrepMatch>,
    pub total: usize,
}

pub struct GrepMatch {
    pub id: i64,
    pub score: f64,
    pub exact_name: bool,
    pub name_match: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecallFilter {
    pub kinds: Vec<String>,
}

impl RecallFilter {
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }
}

pub fn query_alternatives(query: &str) -> Result<Vec<String>, String> {
    let mut seen = HashSet::new();
    let mut alternatives = Vec::new();
    for alternative in query.split('|').map(str::trim) {
        if !alternative.chars().any(char::is_alphanumeric) {
            return Err(format!("no usable search terms in query: {query:?}"));
        }
        if seen.insert(alternative.to_lowercase()) {
            alternatives.push(alternative.to_string());
        }
    }
    Ok(alternatives)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alternatives_preserve_full_queries_and_first_spelling() {
        assert_eq!(
            query_alternatives(" markInSync | MARKINSYNC | prepare_multipart | who calls clone ")
                .unwrap(),
            ["markInSync", "prepare_multipart", "who calls clone"]
        );
    }

    #[test]
    fn empty_and_punctuation_alternatives_are_rejected() {
        for query in ["", " ", "!?", "|", "clone|", "clone||prepare", "clone|?!"] {
            assert!(query_alternatives(query).is_err(), "{query:?}");
        }
    }
}
