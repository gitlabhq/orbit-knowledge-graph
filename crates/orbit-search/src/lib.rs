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

#[rustfmt::skip]
const RELATIONAL_SYNONYMS: &[&str] = &[
    "caller", "callee", "depend", "export", "implement", "invoke", "mention",
    "reference", "render", "use", "used", "uses", "using",
];

const QUERY_STOPWORDS: &[&str] = &[
    "a", "about", "all", "an", "and", "any", "are", "be", "been", "being", "but", "can", "could",
    "did", "do", "does", "for", "from", "had", "has", "have", "here", "how", "in", "into", "is",
    "get", "it", "its", "may", "might", "must", "not", "of", "off", "on", "onto", "or", "our",
    "set", "shall", "should", "some", "that", "the", "their", "them", "there", "these", "they",
    "this", "those", "to", "was", "we", "were", "what", "when", "where", "which", "while", "who",
    "whom", "whose", "why", "will", "with", "without", "work", "working", "works", "would", "you",
    "your",
];

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
                // A lone capital ("OAuth", "IUser") is noise on its own but
                // meaningful fused with the next part; emitting the fused form
                // alongside the parts lets both `oauth` and `user` match.
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

pub fn token_counts(words: Vec<String>) -> HashMap<String, i32> {
    let mut counts: HashMap<String, i32> = HashMap::new();
    for word in words {
        *counts.entry(stem(&word)).or_insert(0) += 1;
    }
    counts
}

pub fn doc_token_counts(fqn: &str, file_path: &str) -> HashMap<String, i32> {
    let mut counts = token_counts(split_words(fqn));
    for (token, tf) in token_counts(split_words(file_path)) {
        *counts.entry(token).or_insert(0) += tf;
    }
    counts
}

/// Stemmed token stream persisted on `gl_definition.search_text`, plus its
/// length for BM25 document-length normalization. Stemming at write time keeps
/// stored tokens aligned with [`query_tokens`] so query-side matching is exact
/// token equality.
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
        .filter(|w| !QUERY_STOPWORDS.contains(&w.as_str()))
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
    pub matches: Vec<AskMatch>,
    pub seed_count: usize,
    pub focus: Option<String>,
    pub edges: Vec<Edge>,
    pub hidden_by_kind: Vec<(String, usize)>,
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
    guaranteed: bool,
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
    let rows: Vec<RowTokens> = corpus
        .iter()
        .map(|r| {
            let name = split_words(&r.fqn);
            let name_stems = name.iter().map(|t| stem(t)).collect();
            RowTokens {
                name,
                name_stems,
                path: split_words(&r.loc),
            }
        })
        .collect();
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

    let mut per_term_best: Vec<Option<(f64, usize)>> = vec![None; terms.len()];
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
        for ((term, term_stem), weight) in terms.iter().zip(&term_stems).zip(&weights) {
            match row.name_tier(term, term_stem) {
                Tier::Exact => {
                    tiered += EXACT_BONUS * weight;
                    matched += 1;
                }
                Tier::Prefix => {
                    tiered += PREFIX_BONUS * weight;
                    matched += 1;
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
                guaranteed: false,
            });
            for ((slot, term), term_stem) in per_term_best.iter_mut().zip(terms).zip(&term_stems) {
                if row.matches(term, term_stem) && slot.is_none_or(|(best, _)| score > best) {
                    *slot = Some((score, index));
                }
            }
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

    let mut guaranteed = false;
    for (slot, term) in per_term_best.iter().zip(terms) {
        let Some((score, index)) = slot else { continue };
        if vocab.is_relational(term) {
            continue;
        }
        if let Some(hit) = hits.iter_mut().find(|h| h.index == *index) {
            hit.guaranteed = true;
            continue;
        }
        hits.push(Hit {
            index: *index,
            score: *score,
            tiered: false,
            guaranteed: true,
        });
        guaranteed = true;
    }
    if guaranteed {
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    hits
}

fn dedupe_by_parent(results: Vec<Hit>, corpus: &[CorpusRow], limit: usize) -> Vec<Hit> {
    let mut per_parent: HashMap<String, usize> = HashMap::new();
    let mut per_file: HashMap<String, usize> = HashMap::new();
    let mut kept: Vec<Hit> = Vec::with_capacity(limit);
    let mut overflow_guaranteed: Vec<Hit> = Vec::new();
    for r in results {
        if kept.len() >= limit && !r.guaranteed {
            continue;
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
        if kept.len() < limit {
            kept.push(r);
        } else {
            overflow_guaranteed.push(r);
        }
    }
    for g in overflow_guaranteed {
        let Some(pos) = kept.iter().rposition(|h| !h.guaranteed) else {
            break;
        };
        kept.remove(pos);
        kept.push(g);
    }
    kept.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
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

    #[test]
    fn split_words_handles_camel_snake_and_acronym_boundaries() {
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
    }

    #[test]
    fn split_words_emits_fused_and_bare_forms_for_a_lone_capital() {
        assert_eq!(
            split_words("OAuth2Provider"),
            ["oauth2", "auth2", "provider"]
        );
        assert_eq!(split_words("IUserService"), ["iuser", "user", "service"]);
        assert_eq!(split_words("UserT"), ["user"]);
    }

    #[test]
    fn doc_token_counts_stems_and_counts_identifier_fragments() {
        let counts = doc_token_counts(
            "indexer::nats::message::NatsMessage::to_dlq",
            "crates/indexer/src/nats/message.rs",
        );
        assert_eq!(counts.get("dlq"), Some(&1));
        assert_eq!(counts.get("nat"), Some(&3));
        assert_eq!(counts.get("messag"), Some(&3));
    }

    #[test]
    fn query_tokens_stem_and_dedupe() {
        let tokens = query_tokens(&["validated".to_string(), "Validate".to_string()]);
        assert_eq!(tokens, vec!["valid".to_string()]);
    }

    #[test]
    fn content_words_drops_question_words_but_keeps_the_subject() {
        assert_eq!(
            content_words("which issues mention the ontology"),
            vec!["issues", "mention", "ontology"]
        );
    }

    #[test]
    fn content_words_falls_back_when_every_word_is_filler() {
        assert_eq!(content_words("what is this"), vec!["what", "is", "this"]);
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
    fn rank_prefers_rows_matching_more_query_terms() {
        let corpus = vec![row("issues found during testing"), row("ontology issues")];
        let terms = vec!["issues".to_string(), "ontology".to_string()];
        let hits = rank(&terms, &corpus, 10, None, &test_vocab());
        assert_eq!(corpus[hits[0].index].fqn, "ontology issues");
    }

    #[test]
    fn rank_scores_every_substring_match_above_zero() {
        let corpus = vec![row("feat(ontology): add plan ontology")];
        let hits = rank(&["ontology".to_string()], &corpus, 10, None, &test_vocab());
        assert_eq!(hits.len(), 1);
        assert!(hits[0].score > 0.0, "score was {}", hits[0].score);
    }

    #[test]
    fn idf_lets_a_rare_term_outrank_common_filler_when_the_corpus_is_complete() {
        let mut corpus: Vec<CorpusRow> = (0..40)
            .map(|i| row(&format!("pkg::send_thing_{i}")))
            .collect();
        corpus.push(row("indexer::nats::message::NatsMessage::to_dlq"));
        let terms = vec!["send".to_string(), "dlq".to_string()];

        let weighted = rank(&terms, &corpus, 5, Some(&[0.69, 3.07]), &test_vocab());
        assert!(
            corpus[weighted[0].index].fqn.ends_with("to_dlq"),
            "weighted top was {}",
            corpus[weighted[0].index].fqn
        );

        let flat = rank(&terms, &corpus, 5, None, &test_vocab());
        let top = flat[0].score;
        assert!(
            flat.iter().all(|h| (h.score - top).abs() < f64::EPSILON),
            "without weights every one-term match should tie"
        );
    }

    #[test]
    fn rank_prefers_the_short_exact_symbol_over_a_longer_tie() {
        let corpus = vec![
            row("Ci::ExecuteBuildHooksWorker::execute_hooks_for_created_build"),
            row("Group::execute_hooks"),
        ];
        let terms = vec!["execute".to_string(), "hooks".to_string()];
        let hits = rank(&terms, &corpus, 10, None, &test_vocab());
        assert_eq!(corpus[hits[0].index].fqn, "Group::execute_hooks");
    }

    #[test]
    fn guaranteed_rare_term_row_survives_the_display_cap() {
        let mut corpus: Vec<CorpusRow> = (0..20)
            .map(|i| row(&format!("pkg{i}::parse_file_entry")))
            .collect();
        corpus.push(row("code_graph::langs::js::frameworks::vue"));
        let terms = vec!["parse".to_string(), "vue".to_string(), "file".to_string()];
        let hits = dedupe_by_parent(rank(&terms, &corpus, 15, None, &test_vocab()), &corpus, 3);
        assert_eq!(hits.len(), 3);
        assert!(
            hits.iter().any(|h| corpus[h.index].fqn.ends_with("::vue")),
            "the only row matching 'vue' must survive the cap"
        );
    }

    #[test]
    fn dedupe_returns_hits_in_score_order_after_guaranteed_reinsertion() {
        let corpus = vec![row("a::one"), row("b::two"), row("c::three")];
        let results = vec![
            Hit {
                index: 0,
                score: 5.0,
                tiered: false,
                guaranteed: false,
            },
            Hit {
                index: 1,
                score: 3.0,
                tiered: false,
                guaranteed: true,
            },
            Hit {
                index: 2,
                score: 4.0,
                tiered: false,
                guaranteed: true,
            },
        ];
        let hits = dedupe_by_parent(results, &corpus, 2);
        let scores: Vec<f64> = hits.iter().map(|h| h.score).collect();
        assert_eq!(scores, vec![4.0, 3.0]);
    }

    #[test]
    fn stemmed_query_matches_inflected_symbols() {
        let corpus = vec![
            row("ontology::validation::validate"),
            row("indexer::unrelated::thing"),
        ];
        let hits = rank(&["validated".to_string()], &corpus, 10, None, &test_vocab());
        assert_eq!(hits.len(), 1);
        assert_eq!(corpus[hits[0].index].fqn, "ontology::validation::validate");
    }

    #[test]
    fn parent_key_strips_the_last_segment_for_rust_and_go_fqns() {
        assert_eq!(parent_key("a::B::field"), "a::B");
        assert_eq!(parent_key("pkg.Func"), "pkg");
        assert_eq!(parent_key("bare"), "bare");
    }

    #[test]
    fn vocab_maps_synonyms_and_kind_names_to_edge_kinds() {
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
