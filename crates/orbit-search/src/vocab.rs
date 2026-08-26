use std::collections::HashMap;

pub struct SearchVocab {
    by_stem: HashMap<String, String>,
}

impl SearchVocab {
    pub fn kind_name_parts(name: &str) -> impl Iterator<Item = &str> {
        name.split(|c: char| !c.is_alphanumeric())
            .filter(|part| !part.is_empty())
    }

    pub fn new<I, A, B>(stemmed_parts: I) -> Self
    where
        I: IntoIterator<Item = (A, B)>,
        A: AsRef<str>,
        B: AsRef<str>,
    {
        Self {
            by_stem: stemmed_parts
                .into_iter()
                .map(|(stem, kind)| (stem.as_ref().to_string(), kind.as_ref().to_uppercase()))
                .collect(),
        }
    }

    pub fn focus_edge_kind(&self, stemmed_terms: &[String]) -> Option<String> {
        stemmed_terms
            .iter()
            .find_map(|s| self.by_stem.get(s).cloned())
    }

    pub fn is_relational(&self, stemmed_term: &str) -> bool {
        self.by_stem.contains_key(stemmed_term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookups_use_stemmed_terms_and_kinds_are_uppercased() {
        let vocab = SearchVocab::new([("call", "Calls"), ("import", "Imports")]);
        assert!(vocab.is_relational("call"));
        assert!(!vocab.is_relational("calls"));
        assert!(!vocab.is_relational("dlq"));
        assert_eq!(
            vocab.focus_edge_kind(&["widget".to_string(), "import".to_string()]),
            Some("IMPORTS".to_string())
        );
        assert_eq!(vocab.focus_edge_kind(&["widget".to_string()]), None);
    }

    #[test]
    fn kind_name_parts_splits_on_non_alphanumerics() {
        let parts: Vec<&str> = SearchVocab::kind_name_parts("HAS_TAG").collect();
        assert_eq!(parts, vec!["HAS", "TAG"]);
    }
}
