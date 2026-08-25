use std::collections::HashMap;

use crate::types::CorpusRow;

const CANDIDATE_FACTOR: usize = 5;

const MAX_PER_PARENT: usize = 2;
const MAX_PER_FILE: usize = 3;

pub const ANCHOR_SIM: f64 = 0.999;
pub const CONFIDENT_COVERAGE: f64 = 0.5;
pub const LENGTH_NORM_B: f64 = 0.75;

pub struct Hit {
    pub index: usize,
    pub score: f64,
    anchored: bool,
    coverage: f64,
}

impl Hit {
    pub fn anchored(&self) -> bool {
        self.anchored
    }

    pub fn confident(&self) -> bool {
        self.anchored && self.coverage >= CONFIDENT_COVERAGE
    }
}

pub fn rank_and_trim(
    corpus: &[CorpusRow],
    sims: &[Vec<f64>],
    idfs: &[f64],
    limit: usize,
) -> Vec<Hit> {
    dedupe_by_parent(
        rank(corpus, sims, idfs, limit * CANDIDATE_FACTOR),
        corpus,
        limit,
    )
}

fn rank(corpus: &[CorpusRow], sims: &[Vec<f64>], idfs: &[f64], cap: usize) -> Vec<Hit> {
    let measured: Vec<f64> = corpus
        .iter()
        .filter(|r| r.grams > 0)
        .map(|r| r.grams as f64)
        .collect();
    let avgdl = if measured.is_empty() {
        1.0
    } else {
        measured.iter().sum::<f64>() / measured.len() as f64
    };
    let mut hits: Vec<Hit> = Vec::new();
    for (index, row_sims) in sims.iter().enumerate() {
        let total: f64 = row_sims.iter().zip(idfs).map(|(sim, idf)| sim * idf).sum();
        if total <= 0.0 {
            continue;
        }
        let len = corpus[index].grams.max(1) as f64;
        let length_norm = 1.0 - LENGTH_NORM_B + LENGTH_NORM_B * len / avgdl;
        let idf_total: f64 = idfs.iter().sum::<f64>().max(f64::MIN_POSITIVE);
        let matched_idf: f64 = row_sims
            .iter()
            .zip(idfs)
            .filter(|&(&s, _)| s > 0.0)
            .map(|(_, idf)| idf)
            .sum();
        let anchored_idf: f64 = row_sims
            .iter()
            .zip(idfs)
            .filter(|&(&s, _)| s >= ANCHOR_SIM)
            .map(|(_, idf)| idf)
            .sum();
        let coverage = matched_idf / idf_total;
        hits.push(Hit {
            index,
            score: total * coverage * coverage / length_norm,
            anchored: anchored_idf > 0.0,
            coverage: anchored_idf / idf_total,
        });
    }
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| corpus[a.index].fqn.len().cmp(&corpus[b.index].fqn.len()))
            .then_with(|| corpus[a.index].id.cmp(&corpus[b.index].id))
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
        if !file.is_empty() && per_file.get(&file).is_some_and(|&n| n >= MAX_PER_FILE) {
            continue;
        }
        let parent = parent_key(&row.fqn);
        if per_parent
            .get(&parent)
            .is_some_and(|&n| n >= MAX_PER_PARENT)
        {
            continue;
        }
        if !file.is_empty() {
            *per_file.entry(file).or_insert(0) += 1;
        }
        *per_parent.entry(parent).or_insert(0) += 1;
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
    use crate::testutil::row;

    #[test]
    fn flood_terms_do_not_dilute_confidence() {
        let corpus = vec![row(1, "Repo::commit_hook")];
        let sims = vec![vec![1.0, 1.0, 0.0, 0.0, 0.0, 0.0]];
        let high_idf_anchors = rank(&corpus, &sims, &[5.0, 5.0, 0.2, 0.2, 0.2, 0.2], 10);
        assert!(
            high_idf_anchors[0].confident(),
            "anchoring the informative mass must clear the bar despite four flood terms"
        );
        let low_idf_anchors = rank(&corpus, &sims, &[0.2, 0.2, 5.0, 5.0, 5.0, 5.0], 10);
        assert!(
            !low_idf_anchors[0].confident(),
            "anchoring only flood terms must stay weak"
        );
    }

    #[test]
    fn full_sim_outranks_fuzzy_and_coverage_squares_partial_matches() {
        let corpus = vec![
            row(1, "Repo::commit"),
            row(2, "Repo::komit"),
            row(3, "Repo::other"),
        ];
        let sims = vec![
            vec![1.0, 1.0, 1.0],
            vec![0.8, 0.8, 0.8],
            vec![1.0, 0.0, 0.0],
        ];
        let hits = rank(&corpus, &sims, &[1.0, 1.0, 1.0], 10);
        let order: Vec<&str> = hits.iter().map(|h| corpus[h.index].fqn.as_str()).collect();
        assert_eq!(order, vec!["Repo::commit", "Repo::komit", "Repo::other"]);
        assert!(hits[0].confident());
        assert!(!hits[1].anchored());
        assert!(
            !hits[2].confident(),
            "one anchored term of three must stay below the confidence floor"
        );
        assert!(hits[0].score > 4.0 * hits[2].score);
    }

    #[test]
    fn zero_sim_rows_are_dropped_and_ties_prefer_shorter_fqns() {
        let corpus = vec![
            row(4, "Repo::commit_hook"),
            row(5, "Repo::commit"),
            row(6, "X::y"),
        ];
        let sims = vec![vec![1.0], vec![1.0], vec![0.0]];
        let hits = rank(&corpus, &sims, &[1.0], 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(corpus[hits[0].index].fqn, "Repo::commit");
    }

    #[test]
    fn rank_dedupe_and_parent_keys_respect_limits() {
        let corpus = vec![row(7, "Repo::commit_hook"), row(8, "Project::setup")];
        let sims = vec![vec![1.0], vec![0.0]];
        let hits = rank(&corpus, &sims, &[1.0], 10);
        assert_eq!(hits.len(), 1);
        let limited = dedupe_by_parent(hits, &corpus, 0);
        assert!(limited.is_empty());

        assert_eq!(parent_key("a::B::field"), "a::B");
        assert_eq!(parent_key("pkg.Func"), "pkg");
        assert_eq!(parent_key("bare"), "bare");
    }

    #[test]
    fn idf_weights_rare_terms_above_flood_terms() {
        let corpus = vec![
            row(9, "Ci::AutoCancel"),
            row(10, "Project::Repository::List"),
        ];
        let sims = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let hits = rank(&corpus, &sims, &[9.0, 1.1], 10);
        assert_eq!(corpus[hits[0].index].fqn, "Ci::AutoCancel");
        assert!(hits[0].score > 5.0 * hits[1].score);
    }

    #[test]
    fn parent_rejection_does_not_burn_file_quota() {
        let row_at = |id: i64, fqn: &str, loc: &str| {
            let mut r = row(id, fqn);
            r.loc = loc.to_string();
            r
        };
        let corpus = vec![
            row_at(1, "A::x1", "f.rb:1"),
            row_at(2, "A::x2", "f.rb:2"),
            row_at(3, "A::x3", "f.rb:3"),
            row_at(4, "B::y", "f.rb:4"),
        ];
        let hits = (0..4)
            .map(|index| Hit {
                index,
                score: 1.0,
                anchored: false,
                coverage: 0.0,
            })
            .collect();
        let kept = dedupe_by_parent(hits, &corpus, 10);
        let indices: Vec<usize> = kept.iter().map(|h| h.index).collect();
        assert_eq!(indices, vec![0, 1, 3]);
    }
}
