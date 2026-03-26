use duckdb::params;

use crate::db::SessionDb;
use crate::error::Result;
use crate::types::{Node, TraversalResult};

impl SessionDb {
    /// Traverse related nodes from a starting point using recursive CTE.
    ///
    /// Deduplicates by keeping only the shortest path to each node.
    /// Surfaces edge properties (reason, link_type) on each result.
    pub fn traverse(
        &self,
        start_id: &str,
        max_depth: u32,
        rel_filter: Option<&str>,
    ) -> Result<Vec<TraversalResult>> {
        let max_depth = max_depth.min(5);

        let rel_clause = if rel_filter.is_some() {
            "AND relationship_kind = ?3"
        } else {
            ""
        };
        let rel_clause_aliased = if rel_filter.is_some() {
            "AND e.relationship_kind = ?3"
        } else {
            ""
        };

        // The CTE traverses bidirectionally and tracks:
        //   - visited list for cycle prevention
        //   - edge properties from the edge that discovered each node
        // The outer query deduplicates by keeping only the shallowest path to each node.
        let sql = format!(
            "WITH RECURSIVE related(id, kind, depth, via_rel, edge_props, visited) AS (
                SELECT * FROM (
                    SELECT target_id, target_kind, 1, relationship_kind, properties,
                           list_value(?1, target_id)
                    FROM gl_edge
                    WHERE source_id = ?1 {rel_clause}
                    UNION ALL
                    SELECT source_id, source_kind, 1, relationship_kind, properties,
                           list_value(?1, source_id)
                    FROM gl_edge
                    WHERE target_id = ?1 {rel_clause}
                ) anchors
                UNION ALL
                SELECT * FROM (
                    SELECT e.target_id, e.target_kind, r.depth + 1, e.relationship_kind,
                           e.properties,
                           list_append(r.visited, e.target_id)
                    FROM gl_edge e
                    JOIN related r ON e.source_id = r.id
                    WHERE r.depth < ?2
                      AND NOT list_contains(r.visited, e.target_id)
                      {rel_clause_aliased}
                    UNION ALL
                    SELECT e.source_id, e.source_kind, r.depth + 1, e.relationship_kind,
                           e.properties,
                           list_append(r.visited, e.source_id)
                    FROM gl_edge e
                    JOIN related r ON e.target_id = r.id
                    WHERE r.depth < ?2
                      AND NOT list_contains(r.visited, e.source_id)
                      {rel_clause_aliased}
                ) recurse
            ),
            -- Deduplicate: keep only the shallowest path to each node
            best AS (
                SELECT id, kind, MIN(depth) AS depth,
                       FIRST(via_rel ORDER BY depth) AS via_rel,
                       FIRST(edge_props ORDER BY depth) AS edge_props
                FROM related
                GROUP BY id, kind
            )
            SELECT n.id, n.kind, n.properties, n.created_at::VARCHAR,
                   n.updated_at::VARCHAR, b.depth, b.via_rel, b.edge_props
            FROM best b
            JOIN gl_node n ON n.id = b.id
            ORDER BY b.depth, n.created_at DESC",
        );

        let mut stmt = self.conn().prepare(&sql)?;

        let map_row = |row: &duckdb::Row| -> duckdb::Result<TraversalResult> {
            let edge_props_str: String = row.get::<_, Option<String>>(7)?.unwrap_or_default();
            Ok(TraversalResult {
                node: Node {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    properties: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                },
                depth: row.get(5)?,
                via_relationship: row.get(6)?,
                edge_properties: serde_json::from_str(&edge_props_str).unwrap_or_default(),
            })
        };

        let rows = if let Some(rel) = rel_filter {
            stmt.query_map(params![start_id, max_depth as i64, rel], map_row)?
        } else {
            stmt.query_map(params![start_id, max_depth as i64], map_row)?
        };

        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    /// Build context for agent working memory.
    /// Combines project filter + topic search + FTS into ranked results.
    pub fn build_context(
        &self,
        project: Option<&str>,
        topic: Option<&str>,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Node>> {
        if let Some(q) = query {
            return self.search_fts(q, Some("Session"), limit);
        }

        if let Some(topic_name) = topic {
            let mut stmt = self.conn().prepare(
                "SELECT n.id, n.kind, n.properties, n.created_at::VARCHAR, n.updated_at::VARCHAR
                 FROM gl_node n
                 JOIN gl_edge e ON e.source_id = n.id AND e.relationship_kind = 'HAS_TOPIC'
                 JOIN gl_node t ON t.id = e.target_id AND t.kind = 'Topic'
                 WHERE n.kind = 'Session'
                   AND (t.properties->>'$.name') = ?
                 ORDER BY n.created_at DESC
                 LIMIT ?",
            )?;
            let rows = stmt.query_map(params![topic_name, limit as i64], |row| {
                Ok(Node {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    properties: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                })
            })?;
            return rows.map(|r| r.map_err(|e| e.into())).collect();
        }

        if let Some(proj) = project {
            let like_pattern = format!("%{proj}%");
            let mut stmt = self.conn().prepare(
                "SELECT id, kind, properties, created_at::VARCHAR, updated_at::VARCHAR
                 FROM gl_node
                 WHERE kind = 'Session'
                   AND (properties->>'$.project') LIKE ?
                 ORDER BY created_at DESC
                 LIMIT ?",
            )?;
            let rows = stmt.query_map(params![like_pattern, limit as i64], |row| {
                Ok(Node {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    properties: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                })
            })?;
            return rows.map(|r| r.map_err(|e| e.into())).collect();
        }

        self.list_nodes(Some("Session"), limit)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::SessionDb;
    use serde_json::json;

    #[test]
    fn traverse_finds_linked_sessions() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({"title": "Session 1"}))
            .unwrap();
        db.create_node("s2", "Session", &json!({"title": "Session 2"}))
            .unwrap();
        db.create_node("s3", "Session", &json!({"title": "Session 3"}))
            .unwrap();

        db.create_edge("s1", "s2", "LINKED_TO", &json!({"reason": "continuation"}))
            .unwrap();
        db.create_edge("s2", "s3", "LINKED_TO", &json!({"reason": "follow up"}))
            .unwrap();

        let results = db.traverse("s1", 2, None).unwrap();
        assert!(results.len() >= 2);
        assert!(results.iter().any(|r| r.node.id == "s2"));
        assert!(results.iter().any(|r| r.node.id == "s3"));

        // Edge properties should be surfaced
        let s2 = results.iter().find(|r| r.node.id == "s2").unwrap();
        assert_eq!(s2.edge_properties["reason"], "continuation");
    }

    #[test]
    fn traverse_deduplicates_by_shortest_path() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({})).unwrap();
        db.create_node("s2", "Session", &json!({})).unwrap();
        db.create_node("s3", "Session", &json!({})).unwrap();

        // Two paths to s3: s1→s2→s3 and s1→s3
        db.create_edge("s1", "s2", "LINKED_TO", &json!({})).unwrap();
        db.create_edge("s2", "s3", "LINKED_TO", &json!({})).unwrap();
        db.create_edge("s1", "s3", "LINKED_TO", &json!({})).unwrap();

        let results = db.traverse("s1", 3, None).unwrap();
        // s3 should appear exactly once, at depth 1 (direct link)
        let s3_results: Vec<_> = results.iter().filter(|r| r.node.id == "s3").collect();
        assert_eq!(s3_results.len(), 1);
        assert_eq!(s3_results[0].depth, 1);
    }

    #[test]
    fn traverse_respects_depth_limit() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({})).unwrap();
        db.create_node("s2", "Session", &json!({})).unwrap();
        db.create_node("s3", "Session", &json!({})).unwrap();

        db.create_edge("s1", "s2", "LINKED_TO", &json!({})).unwrap();
        db.create_edge("s2", "s3", "LINKED_TO", &json!({})).unwrap();

        let results = db.traverse("s1", 1, None).unwrap();
        assert!(results.iter().all(|r| r.depth <= 1));
    }

    #[test]
    fn traverse_with_rel_filter() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({})).unwrap();
        db.create_node("s2", "Session", &json!({})).unwrap();
        db.create_node("t1", "Topic", &json!({"name": "test"}))
            .unwrap();

        db.create_edge("s1", "s2", "LINKED_TO", &json!({})).unwrap();
        db.create_edge("s1", "t1", "HAS_TOPIC", &json!({})).unwrap();

        let linked = db.traverse("s1", 2, Some("LINKED_TO")).unwrap();
        assert!(linked.iter().all(|r| r.via_relationship == "LINKED_TO"));
    }

    #[test]
    fn context_by_project() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node(
            "s1",
            "Session",
            &json!({"project": "/path/to/knowledge-graph"}),
        )
        .unwrap();
        db.create_node("s2", "Session", &json!({"project": "/path/to/other"}))
            .unwrap();

        let results = db
            .build_context(Some("knowledge-graph"), None, None, 10)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "s1");
    }

    #[test]
    fn context_by_topic() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({"title": "Session 1"}))
            .unwrap();
        db.create_node("t1", "Topic", &json!({"name": "duckdb"}))
            .unwrap();
        db.create_edge("s1", "t1", "HAS_TOPIC", &json!({})).unwrap();

        let results = db.build_context(None, Some("duckdb"), None, 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "s1");
    }
}
