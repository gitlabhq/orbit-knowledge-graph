pub mod ask;
pub mod corpus;
pub mod expand;
pub mod ppr;

use std::collections::{HashMap, HashSet};

pub const BM25_K1: f64 = 1.2;
pub const BM25_B: f64 = 0.75;

const CANDIDATE_FACTOR: usize = 5;

const MAX_PER_PARENT: usize = 2;
const MAX_PER_FILE: usize = 3;

const EXACT_BONUS: f64 = 1000.0;
const PREFIX_BONUS: f64 = 100.0;
const SUBSTRING_BONUS: f64 = 1.0;
const SOURCE_BONUS: f64 = 0.5;

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
        cache.borrow_mut().insert(word.to_string(), stemmed.clone());
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

pub struct AskOutcome {
    pub terms: Vec<String>,
    pub splits: Vec<(String, String)>,
    pub matches: Vec<AskMatch>,
    pub surfaced: Vec<AskMatch>,
    pub seed_count: usize,
    pub focus: Option<String>,
    pub edges: Vec<Edge>,
    pub hidden_by_kind: Vec<(String, usize)>,
    pub weak: bool,
    pub unmatched_terms: Vec<String>,
}

pub struct AskMatch {
    pub row: CorpusRow,
    pub score: f64,
}

#[derive(Clone)]
pub struct CorpusRow {
    pub id: String,
    pub fqn: String,
    pub kind: String,
    pub loc: String,
    pub end_line: String,
    pub degree: String,
}

pub struct Edge {
    pub kind: String,
    pub source: String,
    pub source_loc: String,
    pub target: String,
    pub target_loc: String,
}

pub struct Hit {
    pub index: usize,
    pub score: f64,
    tiered: bool,
    coverage: f64,
}

pub const CONFIDENT_COVERAGE: f64 = 0.5;

impl Hit {
    pub fn tiered(&self) -> bool {
        self.tiered
    }

    pub fn confident(&self) -> bool {
        self.tiered && self.coverage >= CONFIDENT_COVERAGE
    }
}

pub const BASE_SET_PER_TERM: usize = 10;
pub const MAX_SEEDS: usize = 50;

pub fn term_base_sets(
    terms: &[String],
    corpus: &[CorpusRow],
    weights: Option<&[f64]>,
    vocab: &SearchVocab,
) -> Vec<Vec<(usize, f64)>> {
    let rows: Vec<RowTokens> = corpus.iter().map(RowTokens::of).collect();
    let mut sets: Vec<Vec<(usize, f64)>> = Vec::new();
    let mut seeded: HashSet<usize> = HashSet::new();
    for (t, term) in terms.iter().enumerate() {
        if vocab.is_relational(term) {
            continue;
        }
        let idf = weights.and_then(|w| w.get(t)).copied().unwrap_or(1.0);
        let term_stem = stem(term);
        let mut matched: Vec<(usize, f64)> = rows
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                let tier_factor = match row.name_tier(term, &term_stem) {
                    Tier::Exact => 1.0,
                    Tier::Prefix => 0.7,
                    Tier::Inner => 0.2,
                    Tier::None => match tier_of(term, &row.path) {
                        Tier::Exact | Tier::Prefix => 0.3,
                        _ => return None,
                    },
                };
                Some((i, idf * tier_factor))
            })
            .collect();
        matched.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        matched.truncate(BASE_SET_PER_TERM);
        if !matched.is_empty() {
            seeded.extend(matched.iter().map(|&(i, _)| i));
            sets.push(matched);
            if seeded.len() >= MAX_SEEDS {
                break;
            }
        }
    }
    sets
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

pub fn unmatched_terms(terms: &[String], corpus: &[CorpusRow], vocab: &SearchVocab) -> Vec<String> {
    terms
        .iter()
        .filter(|term| !vocab.is_relational(term))
        .filter(|term| {
            let term_stem = stem(term);
            !corpus
                .iter()
                .any(|row| RowTokens::of(row).matches(term, &term_stem))
        })
        .cloned()
        .collect()
}

enum Tier {
    Exact,
    Prefix,
    Inner,
    None,
}

fn tier_of(term: &str, tokens: &[String]) -> Tier {
    if tokens.iter().any(|t| t == term) {
        Tier::Exact
    } else if tokens.iter().any(|t| t.starts_with(term)) {
        Tier::Prefix
    } else if tokens.iter().any(|t| t.contains(term)) {
        Tier::Inner
    } else {
        Tier::None
    }
}

struct RowTokens {
    name: Vec<String>,
    name_stems: Vec<String>,
    path: Vec<String>,
}

impl RowTokens {
    fn of(row: &CorpusRow) -> Self {
        let name = split_words(&row.fqn);
        let name_stems = name.iter().map(|t| stem(t)).collect();
        Self {
            name,
            name_stems,
            path: split_words(&row.loc),
        }
    }

    fn name_tier(&self, term: &str, term_stem: &str) -> Tier {
        match tier_of(term, &self.name) {
            Tier::None if self.name_stems.iter().any(|s| s == term_stem) => Tier::Prefix,
            tier => tier,
        }
    }

    fn matches(&self, term: &str, term_stem: &str) -> bool {
        !matches!(self.name_tier(term, term_stem), Tier::None)
            || matches!(tier_of(term, &self.path), Tier::Exact | Tier::Prefix)
    }
}

pub fn rank_and_trim(
    terms: &[String],
    corpus: &[CorpusRow],
    limit: usize,
    weights: Option<&[f64]>,
    vocab: &SearchVocab,
) -> Vec<Hit> {
    dedupe_by_parent(
        rank(terms, corpus, limit * CANDIDATE_FACTOR, weights, vocab),
        corpus,
        limit,
    )
}

fn rank(
    terms: &[String],
    corpus: &[CorpusRow],
    cap: usize,
    weights: Option<&[f64]>,
    vocab: &SearchVocab,
) -> Vec<Hit> {
    let joined = terms.join(" ");
    let term_stems: Vec<String> = terms.iter().map(|t| stem(t)).collect();
    let rows: Vec<RowTokens> = corpus.iter().map(RowTokens::of).collect();
    let weights: Vec<f64> = match weights {
        Some(w) if w.len() == terms.len() => terms
            .iter()
            .zip(w)
            .map(|(term, weight)| {
                if vocab.is_relational(term) {
                    weight.min(1.0)
                } else {
                    *weight
                }
            })
            .collect(),
        _ => vec![1.0; terms.len()],
    };

    let mut hits: Vec<Hit> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let name_joined = row.name.join(" ");
        let mut score = 0.0;
        if !joined.is_empty() {
            if name_joined == joined {
                score += EXACT_BONUS * 10.0;
            } else if name_joined.starts_with(&joined) {
                score += PREFIX_BONUS * 10.0;
            }
        }
        let mut tiered = 0.0;
        let mut matched = 0usize;
        let mut anchored = 0usize;
        for ((term, term_stem), weight) in terms.iter().zip(&term_stems).zip(&weights) {
            match row.name_tier(term, term_stem) {
                Tier::Exact => {
                    tiered += EXACT_BONUS * weight;
                    matched += 1;
                    anchored += 1;
                }
                Tier::Prefix => {
                    tiered += PREFIX_BONUS * weight;
                    matched += 1;
                    anchored += 1;
                }
                Tier::Inner => {
                    score += SUBSTRING_BONUS * weight;
                    matched += 1;
                }
                Tier::None => {
                    if matches!(tier_of(term, &row.path), Tier::Exact | Tier::Prefix) {
                        score += SOURCE_BONUS * weight;
                    }
                }
            }
        }
        if tiered > 0.0 {
            let coverage = matched as f64 / terms.len() as f64;
            score += tiered * coverage * coverage;
        }
        if score > 0.0 {
            hits.push(Hit {
                index,
                score,
                tiered: tiered > 0.0,
                coverage: anchored as f64 / terms.len().max(1) as f64,
            });
        }
    }
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                if a.tiered && b.tiered {
                    corpus[a.index].fqn.len().cmp(&corpus[b.index].fqn.len())
                } else {
                    std::cmp::Ordering::Equal
                }
            })
    });
    hits.truncate(cap);
    hits
}

fn dedupe_by_parent(results: Vec<Hit>, corpus: &[CorpusRow], limit: usize) -> Vec<Hit> {
    let mut per_parent: HashMap<String, usize> = HashMap::new();
    let mut per_file: HashMap<String, usize> = HashMap::new();
    let mut kept: Vec<Hit> = Vec::with_capacity(limit);
    for r in results {
        if kept.len() >= limit {
            break;
        }
        let row = &corpus[r.index];
        let file = row
            .loc
            .rsplit_once(':')
            .map_or(row.loc.clone(), |(f, _)| f.to_string());
        if !file.is_empty() {
            let seen = per_file.entry(file).or_insert(0);
            if *seen >= MAX_PER_FILE {
                continue;
            }
            *seen += 1;
        }
        let count = per_parent.entry(parent_key(&row.fqn)).or_insert(0);
        if *count >= MAX_PER_PARENT {
            continue;
        }
        *count += 1;
        kept.push(r);
    }
    kept
}

fn parent_key(fqn: &str) -> String {
    match fqn.rfind("::") {
        Some(i) => fqn[..i].to_string(),
        None => match fqn.rfind('.') {
            Some(i) => fqn[..i].to_string(),
            None => fqn.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_vocab() -> SearchVocab {
        SearchVocab::new(["Contains", "Defines", "Imports", "Calls", "Extends"])
    }

    fn row(fqn: &str) -> CorpusRow {
        CorpusRow {
            id: fqn.to_string(),
            fqn: fqn.to_string(),
            kind: "Definition".to_string(),
            loc: String::new(),
            end_line: "0".to_string(),
            degree: "0".to_string(),
        }
    }

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

    #[test]
    fn anchor_helpers_respect_caps_relational_terms_and_parent_keys() {
        let corpus = vec![row("Repo::commit_hook"), row("Project::setup")];
        let terms = vec![
            "commit".to_string(),
            "setup".to_string(),
            "uses".to_string(),
        ];
        assert_eq!(
            term_base_sets(&terms, &corpus, None, &test_vocab()).len(),
            2
        );
        let big: Vec<CorpusRow> = (0..40).map(|i| row(&format!("m{i}::commit"))).collect();
        let capped = term_base_sets(&["commit".to_string()], &big, None, &test_vocab());
        assert_eq!(capped[0].len(), BASE_SET_PER_TERM);

        assert_eq!(
            unmatched_terms(&terms, &corpus, &test_vocab()),
            Vec::<String>::new()
        );
        assert_eq!(
            unmatched_terms(
                &["zzzz".to_string(), "uses".to_string()],
                &corpus,
                &test_vocab()
            ),
            vec!["zzzz".to_string()]
        );

        let hits = rank(&["commit".to_string()], &corpus, 10, None, &test_vocab());
        assert_eq!(hits.len(), 1);
        let limited = dedupe_by_parent(hits, &corpus, 0);
        assert!(limited.is_empty());

        assert_eq!(parent_key("a::B::field"), "a::B");
        assert_eq!(parent_key("pkg.Func"), "pkg");
        assert_eq!(parent_key("bare"), "bare");

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
