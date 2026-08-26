use std::collections::HashSet;

const KEEP_ANCHOR_WORDS: &[&str] = &[
    "after", "around", "before", "down", "off", "on", "out", "over", "under", "up", "with",
];

const CODE_STOPWORDS: &[&str] = &[
    "get",
    "set",
    "use",
    "used",
    "using",
    "work",
    "working",
    "works",
    "actually",
    "anybody",
    "anyone",
    "anything",
    "basically",
    "everybody",
    "everyone",
    "everything",
    "nobody",
    "really",
    "somebody",
    "someone",
    "something",
];

fn query_stopwords() -> &'static HashSet<String> {
    static STOPWORDS: std::sync::OnceLock<HashSet<String>> = std::sync::OnceLock::new();
    STOPWORDS.get_or_init(|| {
        let mut words: HashSet<String> = stop_words::get(stop_words::LANGUAGE::English)
            .iter()
            .map(|w| (*w).to_string())
            .collect();
        for keep in KEEP_ANCHOR_WORDS {
            words.remove(*keep);
        }
        words.extend(CODE_STOPWORDS.iter().map(|w| (*w).to_string()));
        words
    })
}

pub fn content_words(input: &str) -> Vec<String> {
    let words: Vec<String> = input
        .split_whitespace()
        .filter_map(|w| {
            let trimmed = w.trim_matches(|c: char| !c.is_alphanumeric());
            (!trimmed.is_empty()).then(|| trimmed.to_lowercase())
        })
        .collect();
    let content: Vec<String> = words
        .iter()
        .filter(|w| !query_stopwords().contains(w.as_str()))
        .cloned()
        .collect();
    if content.is_empty() { words } else { content }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_words_keep_identifiers_whole_and_drop_stopwords() {
        assert_eq!(
            content_words("which issues mention the ontology"),
            vec!["issues", "mention", "ontology"]
        );
        assert_eq!(content_words("what is this"), vec!["what", "is", "this"]);
        assert_eq!(
            content_words("who calls Dlq::publish?"),
            vec!["calls", "dlq::publish"]
        );
        assert_eq!(
            content_words("where does MergeRequestWidget render"),
            vec!["mergerequestwidget", "render"]
        );

        let sw = query_stopwords();
        for keep in ["after", "before", "up", "on", "with"] {
            assert!(
                !sw.contains(keep),
                "{keep} is identifier vocabulary and must stay anchor-able"
            );
        }
        for drop in ["get", "set", "using", "someone", "the", "should"] {
            assert!(sw.contains(drop), "{drop} must be a stopword");
        }
    }
}
