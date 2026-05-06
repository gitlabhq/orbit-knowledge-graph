//! Post-backfill DDL: projections that are deferred until the migrating
//! version's backfill has completed.
//!
//! The same statement plan is consumed in two places:
//!
//! - The runtime migration phase ([`indexer::schema::completion`]) executes
//!   each statement against ClickHouse with `mutations_sync = 2`.
//! - The dev-time file emitter (`orbit debug ddl-projections`) writes the
//!   statements to `config/graph_projections.sql`.
//!
//! Generating from a single function guarantees that the file under review
//! and the SQL the indexer issues at promotion time cannot drift.
//!
//! # Why deferred?
//!
//! `ReplacingMergeTree` rebuilds projections during merges
//! (`deduplicate_merge_projection_mode = 'rebuild'`). For a 16 M-row backfill
//! that fan-outs into multiple projections per table, the projection
//! maintenance cost dominates the write path. Deferring projection creation
//! until after the bulk INSERTs settle moves the work into a single
//! `MATERIALIZE PROJECTION` mutation per projection, which is typically
//! cheaper because it operates on already-merged parts.

use ontology::Ontology;

use crate::ast::ddl::{CreateTable, ProjectionDef};
use crate::passes::codegen::ddl::clickhouse::emit_projection;
use crate::passes::codegen::ddl::generate_graph_tables_with_prefix;

/// The `deduplicate_merge_projection_mode` value applied via
/// `ALTER TABLE … MODIFY SETTING` before any projection is added back.
/// Mirrors the value the inline DDL path bakes into the table at create time.
const DEDUPLICATE_MERGE_PROJECTION_MODE: &str = "'rebuild'";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostBackfillKind {
    /// `ALTER TABLE … MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'`.
    /// Emitted once per table with at least one projection, before any ADD.
    ModifyDeduplicateSetting,
    /// `ALTER TABLE … ADD PROJECTION …`.
    AddProjection,
    /// `ALTER TABLE … MATERIALIZE PROJECTION … SETTINGS mutations_sync = 2`.
    MaterializeProjection,
}

impl PostBackfillKind {
    /// Stable label suitable for metrics and structured logs.
    pub fn as_label(&self) -> &'static str {
        match self {
            PostBackfillKind::ModifyDeduplicateSetting => "modify_setting",
            PostBackfillKind::AddProjection => "add_projection",
            PostBackfillKind::MaterializeProjection => "materialize_projection",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostBackfillStatement {
    /// Prefixed table this statement targets. Used by the runtime path for
    /// metrics labels and the `system.projections` idempotency lookup.
    pub table: String,
    /// `Some(name)` for ADD/MATERIALIZE; `None` for the per-table MODIFY.
    pub projection: Option<String>,
    pub kind: PostBackfillKind,
    pub sql: String,
}

/// Generates the post-backfill DDL for every table in `ontology` that
/// declares projections, prefixed with `prefix` (use `""` for the
/// `config/graph_projections.sql` snapshot, `"vN_"` for runtime migration).
///
/// Order matters: for each table we emit MODIFY first, then for each
/// projection we emit ADD immediately followed by MATERIALIZE. The runtime
/// executes them as-is; the file emitter writes them with `;\n` separators.
pub fn generate_post_backfill_statements(
    ontology: &Ontology,
    prefix: &str,
) -> Vec<PostBackfillStatement> {
    let mut out = Vec::new();
    for table in generate_graph_tables_with_prefix(ontology, prefix) {
        if table.projections.is_empty() {
            continue;
        }
        out.push(PostBackfillStatement {
            table: table.name.clone(),
            projection: None,
            kind: PostBackfillKind::ModifyDeduplicateSetting,
            sql: format!(
                "ALTER TABLE {} MODIFY SETTING deduplicate_merge_projection_mode = {DEDUPLICATE_MERGE_PROJECTION_MODE}",
                table.name
            ),
        });
        for proj in &table.projections {
            let name = projection_name(proj).to_string();
            let body = emit_projection(proj);
            let add_clause = strip_projection_prefix(&body);
            // `IF NOT EXISTS` / `IF EXISTS` make both statements
            // idempotent at the SQL level. The completion task retries on
            // its next tick if it crashed mid-phase; without these flags a
            // partially-applied retry would fail on the duplicate ADD or
            // skip a needed MATERIALIZE.
            out.push(PostBackfillStatement {
                table: table.name.clone(),
                projection: Some(name.clone()),
                kind: PostBackfillKind::AddProjection,
                sql: format!(
                    "ALTER TABLE {} ADD PROJECTION IF NOT EXISTS {add_clause}",
                    table.name
                ),
            });
            out.push(PostBackfillStatement {
                table: table.name.clone(),
                projection: Some(name.clone()),
                kind: PostBackfillKind::MaterializeProjection,
                sql: format!(
                    "ALTER TABLE {} MATERIALIZE PROJECTION IF EXISTS {name} \
                     SETTINGS mutations_sync = 2",
                    table.name
                ),
            });
        }
    }
    out
}

/// Companion to [`generate_post_backfill_statements`] for the create-time
/// side. Removes projections and the now-irrelevant
/// `deduplicate_merge_projection_mode` setting from each table, so
/// `emit_create_table` produces tables-only DDL.
///
/// Called by both the runtime migration (`create_prefixed_tables`) and the
/// `orbit debug ddl` file emitter so the two paths stay in lockstep with
/// `generate_post_backfill_statements`.
pub fn strip_projections_for_create(tables: &mut [CreateTable]) {
    for table in tables {
        if table.projections.is_empty() {
            continue;
        }
        table.projections.clear();
        table
            .settings
            .retain(|s| s.key != "deduplicate_merge_projection_mode");
    }
}

fn projection_name(proj: &ProjectionDef) -> &str {
    match proj {
        ProjectionDef::Reorder { name, .. }
        | ProjectionDef::Lightweight { name, .. }
        | ProjectionDef::Aggregate { name, .. } => name,
    }
}

/// `emit_projection` returns `"    PROJECTION <name> (SELECT …)"` for inline
/// `CREATE TABLE` use. `ALTER TABLE … ADD PROJECTION` wants `<name> (SELECT …)`
/// — same body, no leading indentation or keyword.
fn strip_projection_prefix(line: &str) -> &str {
    line.trim_start().trim_start_matches("PROJECTION ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ontology() -> Ontology {
        Ontology::load_embedded().expect("embedded ontology must load")
    }

    #[test]
    fn statements_pair_add_with_materialize_per_projection() {
        let stmts = generate_post_backfill_statements(&ontology(), "v9_");

        let adds = stmts
            .iter()
            .filter(|s| s.kind == PostBackfillKind::AddProjection)
            .count();
        let mats = stmts
            .iter()
            .filter(|s| s.kind == PostBackfillKind::MaterializeProjection)
            .count();
        assert_eq!(
            adds, mats,
            "every ADD must be paired with a MATERIALIZE: adds={adds}, mats={mats}"
        );
        assert!(adds > 0, "ontology must declare at least one projection");
    }

    #[test]
    fn modify_setting_emitted_before_first_add_per_table() {
        let stmts = generate_post_backfill_statements(&ontology(), "");
        let mut seen_modify_for: std::collections::HashSet<&str> = Default::default();
        for s in &stmts {
            match s.kind {
                PostBackfillKind::ModifyDeduplicateSetting => {
                    seen_modify_for.insert(&s.table);
                }
                PostBackfillKind::AddProjection => {
                    assert!(
                        seen_modify_for.contains(s.table.as_str()),
                        "ADD on {} preceded its MODIFY SETTING",
                        s.table
                    );
                }
                PostBackfillKind::MaterializeProjection => {}
            }
        }
    }

    #[test]
    fn add_immediately_followed_by_materialize_for_same_projection() {
        let stmts = generate_post_backfill_statements(&ontology(), "");
        for window in stmts.windows(2) {
            if window[0].kind == PostBackfillKind::AddProjection {
                assert_eq!(window[1].kind, PostBackfillKind::MaterializeProjection);
                assert_eq!(window[0].table, window[1].table);
                assert_eq!(window[0].projection, window[1].projection);
            }
        }
    }

    #[test]
    fn prefix_applies_to_every_target() {
        for s in generate_post_backfill_statements(&ontology(), "v9_") {
            assert!(
                s.sql.contains(" v9_"),
                "every statement must reference a prefixed table: {}",
                s.sql
            );
            assert!(s.table.starts_with("v9_"));
        }
    }

    #[test]
    fn materialize_uses_mutations_sync_two() {
        for s in generate_post_backfill_statements(&ontology(), "") {
            if s.kind == PostBackfillKind::MaterializeProjection {
                assert!(
                    s.sql.contains("SETTINGS mutations_sync = 2"),
                    "materialize must block via mutations_sync = 2: {}",
                    s.sql
                );
            }
        }
    }

    #[test]
    fn idempotency_flags_are_present() {
        for s in generate_post_backfill_statements(&ontology(), "") {
            match s.kind {
                PostBackfillKind::AddProjection => assert!(
                    s.sql.contains("ADD PROJECTION IF NOT EXISTS"),
                    "ADD must be idempotent across retries: {}",
                    s.sql
                ),
                PostBackfillKind::MaterializeProjection => assert!(
                    s.sql.contains("MATERIALIZE PROJECTION IF EXISTS"),
                    "MATERIALIZE must guard against missing-projection on retry: {}",
                    s.sql
                ),
                PostBackfillKind::ModifyDeduplicateSetting => {}
            }
        }
    }

    #[test]
    fn modify_setting_target_value_is_rebuild() {
        for s in generate_post_backfill_statements(&ontology(), "") {
            if s.kind == PostBackfillKind::ModifyDeduplicateSetting {
                assert!(s.sql.contains("= 'rebuild'"), "{}", s.sql);
                assert!(s.projection.is_none());
            }
        }
    }

    #[test]
    fn strip_projections_drops_dedup_setting() {
        let mut tables = generate_graph_tables_with_prefix(&ontology(), "");
        strip_projections_for_create(&mut tables);
        for table in &tables {
            assert!(
                table.projections.is_empty(),
                "{}: projections must be empty after strip",
                table.name
            );
            assert!(
                !table
                    .settings
                    .iter()
                    .any(|s| s.key == "deduplicate_merge_projection_mode"),
                "{}: dedup setting must be stripped when projections are stripped",
                table.name
            );
        }
    }

    #[test]
    fn strip_then_generate_round_trip_covers_every_projection() {
        let mut stripped = generate_graph_tables_with_prefix(&ontology(), "v9_");
        let original_projection_count: usize = stripped.iter().map(|t| t.projections.len()).sum();

        strip_projections_for_create(&mut stripped);
        let stmts = generate_post_backfill_statements(&ontology(), "v9_");
        let regenerated = stmts
            .iter()
            .filter(|s| s.kind == PostBackfillKind::AddProjection)
            .count();

        assert_eq!(
            original_projection_count, regenerated,
            "post-backfill plan must reintroduce every projection that was stripped"
        );
    }

    #[test]
    fn label_strings_are_stable() {
        assert_eq!(
            PostBackfillKind::ModifyDeduplicateSetting.as_label(),
            "modify_setting"
        );
        assert_eq!(PostBackfillKind::AddProjection.as_label(), "add_projection");
        assert_eq!(
            PostBackfillKind::MaterializeProjection.as_label(),
            "materialize_projection"
        );
    }
}
