use std::collections::{HashMap, HashSet};

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

pub fn split_words(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for word in input.split(|c: char| !c.is_ascii_alphanumeric()) {
        let chars: Vec<char> = word.chars().collect();
        let mut parts: Vec<(usize, usize)> = Vec::new();
        let mut start = 0;
        for i in 1..=chars.len() {
            let boundary = i == chars.len()
                || (chars[i].is_ascii_uppercase()
                    && (chars[i - 1].is_ascii_lowercase()
                        || chars[i - 1].is_ascii_digit()
                        || (i + 1 < chars.len() && chars[i + 1].is_ascii_lowercase())));
            if boundary {
                parts.push((start, i));
                start = i;
            }
        }
        for (idx, &(s, e)) in parts.iter().enumerate() {
            if e - s >= 2 {
                tokens.push(chars[s..e].iter().collect::<String>().to_lowercase());
            } else if let Some(&(_, next_end)) = parts.get(idx + 1) {
                tokens.push(chars[s..next_end].iter().collect::<String>().to_lowercase());
            }
        }
    }
    tokens
}

const STEM_CACHE_CAP: usize = 65_536;

pub fn stem(word: &str) -> String {
    thread_local! {
        static STEMMER: rust_stemmers::Stemmer =
            rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);
        static CACHE: std::cell::RefCell<HashMap<String, String>> =
            std::cell::RefCell::new(HashMap::new());
    }
    CACHE.with(|cache| {
        if let Some(stemmed) = cache.borrow().get(word) {
            return stemmed.clone();
        }
        let stemmed = STEMMER.with(|s| s.stem(word).into_owned());
        let mut cache = cache.borrow_mut();
        if cache.len() >= STEM_CACHE_CAP {
            cache.clear();
        }
        cache.insert(word.to_string(), stemmed.clone());
        stemmed
    })
}

pub fn search_document(fqn: &str, file_path: &str) -> (String, i64) {
    let mut tokens = split_words(fqn);
    tokens.extend(split_words(file_path));
    let stemmed: Vec<String> = tokens.iter().map(|t| stem(t)).collect();
    let count = stemmed.len() as i64;
    (stemmed.join(" "), count)
}

pub fn query_tokens(terms: &[String]) -> Vec<String> {
    let mut tokens: Vec<String> = terms
        .iter()
        .flat_map(|t| split_words(t))
        .map(|t| stem(&t))
        .collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

pub fn content_words(input: &str) -> Vec<String> {
    let words = split_words(input);
    let content: Vec<String> = words
        .iter()
        .filter(|w| !query_stopwords().contains(w.as_str()))
        .cloned()
        .collect();
    if content.is_empty() { words } else { content }
}

const MIN_COMPOUND_PART: usize = 3;

pub fn candidate_splits(term: &str) -> Vec<(String, String)> {
    let lower = term.to_lowercase();
    let chars: Vec<char> = lower.chars().collect();
    if chars.len() < MIN_COMPOUND_PART * 2 || !chars.iter().all(|c| c.is_ascii_alphabetic()) {
        return Vec::new();
    }
    let mut splits: Vec<(String, String)> = (MIN_COMPOUND_PART..=chars.len() - MIN_COMPOUND_PART)
        .map(|i| {
            (
                chars[..i].iter().collect::<String>(),
                chars[i..].iter().collect::<String>(),
            )
        })
        .filter(|(a, b)| {
            !query_stopwords().contains(a.as_str()) && !query_stopwords().contains(b.as_str())
        })
        .collect();
    splits.sort_by_key(|(a, b)| std::cmp::Reverse(a.len().min(b.len())));
    splits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizer_splits_stems_and_filters_the_same_way_for_docs_and_queries() {
        assert_eq!(
            split_words("MergeRequestWidget"),
            ["merge", "request", "widget"]
        );
        assert_eq!(split_words("HTTPServer"), ["http", "server"]);
        assert_eq!(split_words("getUserByID"), ["get", "user", "by", "id"]);
        assert_eq!(
            split_words("app/models/merge_request.rb"),
            ["app", "models", "merge", "request", "rb"]
        );
        assert_eq!(
            split_words("OAuth2Provider"),
            ["oauth2", "auth2", "provider"]
        );
        assert_eq!(split_words("IUserService"), ["iuser", "user", "service"]);
        assert_eq!(split_words("UserT"), ["user"]);

        let (text, count) = search_document(
            "indexer::nats::message::NatsMessage::to_dlq",
            "crates/indexer/src/nats/message.rs",
        );
        assert_eq!(count, 13);
        assert!(text.split(' ').any(|t| t == "dlq"), "text was {text}");
        let tokens = query_tokens(&["validated".to_string(), "Validate".to_string()]);
        assert_eq!(tokens, vec!["valid".to_string()]);

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
        assert_eq!(
            content_words("which issues mention the ontology"),
            vec!["issues", "mention", "ontology"]
        );
        assert_eq!(content_words("what is this"), vec!["what", "is", "this"]);

        assert!(candidate_splits("webhooks").contains(&("web".to_string(), "hooks".to_string())));
        assert!(!candidate_splits("someone").iter().any(|(a, _)| a == "some"));
        assert!(candidate_splits("up").is_empty());
        assert!(candidate_splits("oauth2").is_empty());
    }
}
