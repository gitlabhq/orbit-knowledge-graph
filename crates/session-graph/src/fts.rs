use std::collections::HashMap;

use duckdb::params;

use crate::db::SessionDb;
use crate::error::{Result, SessionGraphError};
use crate::types::Node;

struct ScoredId {
    id: String,
}

impl SessionDb {
    /// Rebuild the full-text search index on gl_node.
    pub fn rebuild_fts_index(&self) -> Result<()> {
        let _ = self
            .conn()
            .execute_batch("PRAGMA drop_fts_index('gl_node');");

        self.conn()
            .execute_batch(
                "PRAGMA create_fts_index(
                'gl_node',
                'id',
                'kind',
                'properties',
                stemmer = 'porter',
                stopwords = 'english',
                overwrite = 1
            );",
            )
            .map_err(|e| SessionGraphError::Other(format!("FTS index creation failed: {e}")))?;

        Ok(())
    }

    /// Multi-signal search: BM25 + fuzzy title matching + exact property matching,
    /// fused via Reciprocal Rank Fusion (RRF). Inspired by qmd's search architecture.
    pub fn search_fts(&self, query: &str, kind: Option<&str>, limit: usize) -> Result<Vec<Node>> {
        let fetch_limit = (limit * 3).max(20) as i64;

        let bm25_ids = self
            .search_bm25(query, kind, fetch_limit)
            .unwrap_or_default();
        let fuzzy_ids = self
            .search_fuzzy(query, kind, fetch_limit)
            .unwrap_or_default();
        let exact_ids = self
            .search_exact_properties(query, kind, fetch_limit)
            .unwrap_or_default();

        let fused = reciprocal_rank_fusion(
            &[(&bm25_ids, 2.0), (&fuzzy_ids, 1.0), (&exact_ids, 1.5)],
            60.0,
        );

        let top_ids: Vec<&str> = fused
            .iter()
            .take(limit)
            .map(|(id, _)| id.as_str())
            .collect();
        if top_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut result_map: HashMap<String, Node> = HashMap::new();
        for id in &top_ids {
            if let Some(node) = self.get_node(id)? {
                result_map.insert(node.id.clone(), node);
            }
        }

        Ok(fused
            .iter()
            .take(limit)
            .filter_map(|(id, _)| result_map.remove(id))
            .collect())
    }

    fn search_bm25(&self, query: &str, kind: Option<&str>, limit: i64) -> Result<Vec<ScoredId>> {
        // BM25 requires the FTS index to exist; returns error if missing
        match kind {
            Some(k) => {
                let mut stmt = self.conn().prepare(
                    "SELECT n.id
                     FROM (
                         SELECT id, fts_main_gl_node.match_bm25(id, ?) AS score
                         FROM gl_node
                     ) n
                     WHERE score IS NOT NULL
                       AND (SELECT kind FROM gl_node WHERE gl_node.id = n.id) = ?
                     ORDER BY score DESC
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![query, k, limit], |row| {
                    Ok(ScoredId { id: row.get(0)? })
                })?;
                rows.map(|r| Ok(r?)).collect()
            }
            None => {
                let mut stmt = self.conn().prepare(
                    "SELECT n.id
                     FROM (
                         SELECT id, fts_main_gl_node.match_bm25(id, ?) AS score
                         FROM gl_node
                     ) n
                     WHERE score IS NOT NULL
                     ORDER BY score DESC
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![query, limit], |row| {
                    Ok(ScoredId { id: row.get(0)? })
                })?;
                rows.map(|r| Ok(r?)).collect()
            }
        }
    }

    fn search_fuzzy(&self, query: &str, kind: Option<&str>, limit: i64) -> Result<Vec<ScoredId>> {
        match kind {
            Some(k) => {
                let mut stmt = self.conn().prepare(
                    "SELECT id FROM (
                        SELECT id,
                            GREATEST(
                                jaro_winkler_similarity(
                                    lower(COALESCE(properties->>'$.title', properties->>'$.name', '')),
                                    lower(?)
                                ),
                                jaro_winkler_similarity(
                                    lower(COALESCE(properties->>'$.summary', properties->>'$.description', '')),
                                    lower(?)
                                )
                            ) AS sim
                         FROM gl_node
                         WHERE kind = ?
                     ) sub
                     WHERE sim > 0.55
                     ORDER BY sim DESC
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![query, query, k, limit], |row| {
                    Ok(ScoredId { id: row.get(0)? })
                })?;
                rows.map(|r| Ok(r?)).collect()
            }
            None => {
                let mut stmt = self.conn().prepare(
                    "SELECT id FROM (
                        SELECT id,
                            GREATEST(
                                jaro_winkler_similarity(
                                    lower(COALESCE(properties->>'$.title', properties->>'$.name', '')),
                                    lower(?)
                                ),
                                jaro_winkler_similarity(
                                    lower(COALESCE(properties->>'$.summary', properties->>'$.description', '')),
                                    lower(?)
                                )
                            ) AS sim
                         FROM gl_node
                     ) sub
                     WHERE sim > 0.55
                     ORDER BY sim DESC
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![query, query, limit], |row| {
                    Ok(ScoredId { id: row.get(0)? })
                })?;
                rows.map(|r| Ok(r?)).collect()
            }
        }
    }

    fn search_exact_properties(
        &self,
        query: &str,
        kind: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ScoredId>> {
        let like_pattern = format!("%{query}%");
        match kind {
            Some(k) => {
                let mut stmt = self.conn().prepare(
                    "SELECT id
                     FROM gl_node
                     WHERE kind = ?
                       AND (
                           (properties->>'$.title') ILIKE ?
                           OR (properties->>'$.summary') ILIKE ?
                           OR (properties->>'$.name') ILIKE ?
                           OR (properties->>'$.description') ILIKE ?
                           OR (properties->>'$.project') ILIKE ?
                       )
                     ORDER BY created_at DESC
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(
                    params![
                        k,
                        like_pattern,
                        like_pattern,
                        like_pattern,
                        like_pattern,
                        like_pattern,
                        limit
                    ],
                    |row| Ok(ScoredId { id: row.get(0)? }),
                )?;
                rows.map(|r| Ok(r?)).collect()
            }
            None => {
                let mut stmt = self.conn().prepare(
                    "SELECT id
                     FROM gl_node
                     WHERE (properties->>'$.title') ILIKE ?
                        OR (properties->>'$.summary') ILIKE ?
                        OR (properties->>'$.name') ILIKE ?
                        OR (properties->>'$.description') ILIKE ?
                        OR (properties->>'$.project') ILIKE ?
                     ORDER BY created_at DESC
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(
                    params![
                        like_pattern,
                        like_pattern,
                        like_pattern,
                        like_pattern,
                        like_pattern,
                        limit
                    ],
                    |row| Ok(ScoredId { id: row.get(0)? }),
                )?;
                rows.map(|r| Ok(r?)).collect()
            }
        }
    }
}

/// Reciprocal Rank Fusion: combines multiple ranked ID lists into a single ranking.
/// Score for doc d = sum over lists of (weight / (k + rank + 1)).
fn reciprocal_rank_fusion(result_lists: &[(&[ScoredId], f64)], k: f64) -> Vec<(String, f64)> {
    let mut scores: HashMap<String, f64> = HashMap::new();

    for (list, weight) in result_lists {
        for (rank, item) in list.iter().enumerate() {
            *scores.entry(item.id.clone()).or_default() += weight / (k + rank as f64 + 1.0);
        }
    }

    let mut results: Vec<_> = scores.into_iter().collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::db::SessionDb;

    #[test]
    fn search_finds_by_title_substring() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node(
            "s1",
            "Session",
            &json!({"title": "Working on DuckDB graph"}),
        )
        .unwrap();
        db.create_node("s2", "Session", &json!({"title": "Fixing auth bugs"}))
            .unwrap();

        let results = db.search_fts("DuckDB", None, 10).unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().any(|n| n.id == "s1"));
    }

    #[test]
    fn search_finds_by_project_property() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node(
            "s1",
            "Session",
            &json!({"title": "Work session", "project": "/path/to/knowledge-graph"}),
        )
        .unwrap();

        let results = db.search_fts("knowledge-graph", None, 10).unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().any(|n| n.id == "s1"));
    }

    #[test]
    fn search_filters_by_kind() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({"title": "Test session"}))
            .unwrap();
        db.create_node("t1", "Topic", &json!({"name": "Test topic"}))
            .unwrap();

        let sessions = db.search_fts("Test", Some("Session"), 10).unwrap();
        assert!(sessions.iter().all(|n| n.kind == "Session"));
    }

    #[test]
    fn rrf_fusion_combines_signals() {
        use super::{ScoredId, reciprocal_rank_fusion};

        let list1 = vec![ScoredId { id: "a".into() }, ScoredId { id: "b".into() }];
        let list2 = vec![ScoredId { id: "b".into() }, ScoredId { id: "c".into() }];

        let fused = reciprocal_rank_fusion(&[(&list1, 2.0), (&list2, 1.0)], 60.0);
        // "b" appears in both lists — should rank highest
        assert_eq!(fused[0].0, "b");
    }
}
