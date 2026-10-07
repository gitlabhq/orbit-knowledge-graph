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
    pub body_offset: Option<usize>,
    pub body_text: String,
    pub mentions: usize,
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
    for alternative in split_unescaped(query) {
        let alternative = alternative.trim();
        if !alternative.chars().any(char::is_alphanumeric) {
            return Err(format!("no usable search terms in query: {query:?}"));
        }
        if seen.insert(alternative.to_lowercase()) {
            alternatives.push(alternative.to_string());
        }
    }
    Ok(alternatives)
}

fn split_unescaped(query: &str) -> Vec<String> {
    let (mut parts, mut current) = (Vec::new(), String::new());
    let mut chars = query.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => current.extend(chars.next()),
            '|' => parts.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    parts.push(current);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backslashes_escape_literally() {
        assert_eq!(
            query_alternatives(r"Router::new|route\(|^\[dependencies\]|a\|b").unwrap(),
            ["Router::new", "route(", "^[dependencies]", "a|b"]
        );
    }

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
