use std::collections::{HashMap, HashSet};

use crate::text::{split_words, stem};

const EDGE_KIND_SYNONYMS: &[(&str, &str)] = &[
    ("use", "CALLS"),
    ("invoke", "CALLS"),
    ("caller", "CALLS"),
    ("callee", "CALLS"),
    ("depend", "IMPORTS"),
    ("implement", "EXTENDS"),
    ("inherit", "EXTENDS"),
];

const RELATIONAL_SYNONYMS: &[&str] = &[
    "caller",
    "callee",
    "depend",
    "export",
    "implement",
    "invoke",
    "mention",
    "reference",
    "render",
    "use",
    "used",
    "uses",
    "using",
];

pub struct SearchVocab {
    by_stem: HashMap<String, String>,
    relational: HashSet<String>,
}

impl SearchVocab {
    pub fn new<I, S>(edge_kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut by_stem: HashMap<String, String> = HashMap::new();
        let mut relational: HashSet<String> = HashSet::new();
        for kind in edge_kinds {
            let name = kind.as_ref();
            by_stem.insert(stem(&name.to_lowercase()), name.to_uppercase());
            relational.extend(split_words(name).iter().map(|word| stem(word)));
        }
        for (word, kind) in EDGE_KIND_SYNONYMS {
            by_stem.insert(stem(word), (*kind).to_string());
        }
        relational.extend(RELATIONAL_SYNONYMS.iter().map(|word| stem(word)));
        Self {
            by_stem,
            relational,
        }
    }

    pub fn focus_edge_kind(&self, terms: &[String]) -> Option<String> {
        terms
            .iter()
            .find_map(|t| self.by_stem.get(&stem(t)).cloned())
    }

    pub fn is_relational(&self, term: &str) -> bool {
        self.relational.contains(&stem(term))
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::test_vocab;

    #[test]
    fn maps_edge_kind_synonyms_and_relational_terms() {
        let vocab = test_vocab();
        assert_eq!(
            vocab.focus_edge_kind(&["calls".to_string()]),
            Some("CALLS".to_string())
        );
        assert_eq!(
            vocab.focus_edge_kind(&["depend".to_string()]),
            Some("IMPORTS".to_string())
        );
        assert_eq!(vocab.focus_edge_kind(&["dlq".to_string()]), None);
        assert!(vocab.is_relational("uses"));
        assert!(!vocab.is_relational("dlq"));
    }
}
