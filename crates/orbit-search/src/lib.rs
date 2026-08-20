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
    guaranteed: bool,
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
            !QUERY_STOPWORDS.contains(&a.as_str()) && !QUERY_STOPWORDS.contains(&b.as_str())
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
                guaranteed: false,
                coverage: anchored as f64 / terms.len().max(1) as f64,
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
            coverage: 0.0,
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
    fn split_words_handles_camel_snake_acronym_and_lone_capital_boundaries() {
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
    }

    #[test]
    fn document_and_query_tokens_stem_to_the_same_space() {
        let (text, count) = search_document(
            "indexer::nats::message::NatsMessage::to_dlq",
            "crates/indexer/src/nats/message.rs",
        );
        assert_eq!(count, 13);
        assert!(text.split(' ').any(|t| t == "dlq"), "text was {text}");
        assert_eq!(text.matches("messag").count(), 3, "text was {text}");

        let tokens = query_tokens(&["validated".to_string(), "Validate".to_string()]);
        assert_eq!(tokens, vec!["valid".to_string()]);
    }

    #[test]
    fn content_words_drops_fillers_unless_nothing_would_remain() {
        assert_eq!(
            content_words("which issues mention the ontology"),
            vec!["issues", "mention", "ontology"]
        );
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
    fn rank_orders_by_coverage_stems_inflections_and_breaks_ties_short() {
        let corpus = vec![row("issues found during testing"), row("ontology issues")];
        let terms = vec!["issues".to_string(), "ontology".to_string()];
        let hits = rank(&terms, &corpus, 10, None, &test_vocab());
        assert_eq!(corpus[hits[0].index].fqn, "ontology issues");

        let corpus = vec![row("feat(ontology): add plan ontology")];
        let hits = rank(&["ontology".to_string()], &corpus, 10, None, &test_vocab());
        assert_eq!(hits.len(), 1);
        assert!(hits[0].score > 0.0, "score was {}", hits[0].score);

        let corpus = vec![
            row("Ci::ExecuteBuildHooksWorker::execute_hooks_for_created_build"),
            row("Group::execute_hooks"),
        ];
        let terms = vec!["execute".to_string(), "hooks".to_string()];
        let hits = rank(&terms, &corpus, 10, None, &test_vocab());
        assert_eq!(corpus[hits[0].index].fqn, "Group::execute_hooks");

        let corpus = vec![
            row("ontology::validation::validate"),
            row("indexer::unrelated::thing"),
        ];
        let hits = rank(&["validated".to_string()], &corpus, 10, None, &test_vocab());
        assert_eq!(hits.len(), 1);
        assert_eq!(corpus[hits[0].index].fqn, "ontology::validation::validate");
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
    fn dedupe_keeps_guaranteed_rows_under_the_cap_in_score_order() {
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

        let corpus = vec![row("a::one"), row("b::two"), row("c::three")];
        let hit = |index: usize, score: f64, guaranteed: bool| Hit {
            index,
            score,
            tiered: false,
            guaranteed,
            coverage: 0.0,
        };
        let results = vec![hit(0, 5.0, false), hit(1, 3.0, true), hit(2, 4.0, true)];
        let hits = dedupe_by_parent(results, &corpus, 2);
        let scores: Vec<f64> = hits.iter().map(|h| h.score).collect();
        assert_eq!(scores, vec![4.0, 3.0]);
    }

    #[test]
    fn term_base_sets_seed_every_term_and_accumulate_overlap() {
        let corpus = vec![
            row("Repo::commit_hook"),
            row("Project::setup"),
            row("Project::commit_and_setup"),
            row("Other::thing"),
        ];
        let terms = vec![
            "commit".to_string(),
            "setup".to_string(),
            "uses".to_string(),
        ];
        let sets = term_base_sets(&terms, &corpus, None, &test_vocab());
        assert_eq!(sets.len(), 2, "relational 'uses' must not produce a set");
        let commit_ids: Vec<usize> = sets[0].iter().map(|&(i, _)| i).collect();
        let setup_ids: Vec<usize> = sets[1].iter().map(|&(i, _)| i).collect();
        assert!(commit_ids.contains(&0) && commit_ids.contains(&2));
        assert!(setup_ids.contains(&1) && setup_ids.contains(&2));
        assert!(!commit_ids.contains(&3) && !setup_ids.contains(&3));

        let big: Vec<CorpusRow> = (0..40).map(|i| row(&format!("m{i}::commit"))).collect();
        let capped = term_base_sets(&["commit".to_string()], &big, None, &test_vocab());
        assert_eq!(capped[0].len(), BASE_SET_PER_TERM);
    }

    #[test]
    fn candidate_splits_offer_balanced_compound_parts_and_reject_stopword_halves() {
        let splits = candidate_splits("webhooks");
        assert!(
            splits.contains(&("web".to_string(), "hooks".to_string())),
            "splits were {splits:?}"
        );
        assert!(
            !candidate_splits("someone").iter().any(|(a, _)| a == "some"),
            "stopword halves must be rejected"
        );
        assert!(candidate_splits("up").is_empty());
        assert!(
            candidate_splits("oauth2").is_empty(),
            "non-alphabetic terms are not split"
        );
    }

    #[test]
    fn unmatched_terms_reports_dead_words_but_not_relational_ones() {
        let corpus = vec![row("Project::execute_hooks")];
        let terms: Vec<String> = ["fires", "hooks", "push", "uses"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            unmatched_terms(&terms, &corpus, &test_vocab()),
            vec!["fires".to_string(), "push".to_string()]
        );
    }

    #[test]
    fn top_hit_confidence_requires_a_name_tier_hit_and_half_coverage() {
        let corpus = vec![row("Dlq::publish")];
        let strong = rank(
            &["dlq".to_string(), "publish".to_string()],
            &corpus,
            10,
            None,
            &test_vocab(),
        );
        assert!(strong[0].confident());

        let corpus = vec![row("prehooksetup")];
        let substring_only = rank(&["hooks".to_string()], &corpus, 10, None, &test_vocab());
        assert!(!substring_only.is_empty());
        assert!(!substring_only[0].confident());

        let corpus = vec![row("getTestRunsForProject")];
        let terms = content_words(
            "when a repository gets its first commit, what runs to set the project up",
        );
        let low_coverage = rank(&terms, &corpus, 10, None, &test_vocab());
        assert!(!low_coverage.is_empty());
        assert!(
            !low_coverage[0].confident(),
            "generic-verb name hits covering under half the question must not present as confident"
        );
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
