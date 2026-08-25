use std::collections::HashMap;

fn stem(word: &str) -> String {
    thread_local! {
        static STEMMER: rust_stemmers::Stemmer =
            rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);
    }
    STEMMER.with(|s| s.stem(&word.to_lowercase()).into_owned())
}

pub struct SearchVocab {
    by_stem: HashMap<String, String>,
}

impl SearchVocab {
    pub fn new<I, S>(edge_kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut by_stem: HashMap<String, String> = HashMap::new();
        for kind in edge_kinds {
            let name = kind.as_ref();
            for part in name.split(|c: char| !c.is_alphanumeric()) {
                if !part.is_empty() {
                    by_stem.insert(stem(part), name.to_uppercase());
                }
            }
        }
        Self { by_stem }
    }

    pub fn focus_edge_kind(&self, terms: &[String]) -> Option<String> {
        terms
            .iter()
            .find_map(|t| self.by_stem.get(&stem(t)).cloned())
    }

    pub fn is_relational(&self, term: &str) -> bool {
        self.by_stem.contains_key(&stem(term))
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::test_vocab;

    #[test]
    fn edge_kind_names_are_the_whole_vocabulary() {
        let vocab = test_vocab();
        assert_eq!(
            vocab.focus_edge_kind(&["calls".to_string()]),
            Some("CALLS".to_string())
        );
        assert_eq!(
            vocab.focus_edge_kind(&["importing".to_string()]),
            Some("IMPORTS".to_string())
        );
        assert_eq!(vocab.focus_edge_kind(&["dlq".to_string()]), None);
        for word in [
            "calls", "called", "calling", "imports", "extends", "defines",
        ] {
            assert!(vocab.is_relational(word), "{word} should be relational");
        }
        for word in ["dlq", "widget", "userland", "usefulness"] {
            assert!(
                !vocab.is_relational(word),
                "{word} should not be relational"
            );
        }
    }
}
