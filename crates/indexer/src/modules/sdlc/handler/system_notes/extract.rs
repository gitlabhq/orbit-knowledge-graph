//! Datalake extraction SQL templates for the system-notes handler.
//!
//! The handler reads `siphon_notes` joined to `siphon_system_note_metadata`
//! on `note_id` (Mode A). Mode B (body-text fallback) is documented in ADR
//! 013 but not implemented in this draft — Mode A is the production target
//! and the table is replicated via the parallel Siphon-side MR
//! (`gitlab-org/analytics-section/siphon`).
//!
//! All SQL is exposed as `pub(super) const` so the unit tests in `mod.rs`
//! can cross-check shape (named parameters, scoping filters, deleted
//! tombstones) without a live ClickHouse.

/// Mode A extraction: join `siphon_notes` to `siphon_system_note_metadata`
/// on `note_id`, restricted to the system-note half of the table.
///
/// Parameters:
///   `{traversal_path:String}` — namespace scope prefix (e.g. `1/100/`).
///   `{last_watermark:DateTime64(6,'UTC')}` — exclusive lower bound on the
///       note's `created_at`. Drives incremental ingestion via the
///       checkpoint store. **System notes are immutable post-creation**
///       (Rails writes them once, never edits), so `created_at` is the
///       semantically correct watermark column — using
///       `_siphon_replicated_at` (the default for mutable entities) would
///       reprocess the same note on every Siphon-side compaction without
///       any new edges to materialise.
///   `{watermark:DateTime64(6,'UTC')}` — inclusive upper bound on
///       `created_at`. Stamped from the dispatcher's wall clock at message
///       publish time.
///   `{batch_limit:UInt64}` — per-page row cap; the production handler
///       paginates via `(created_at, id)` cursor (see `siphon_notes`
///       primary key).
///
/// Filter rationale:
///   * `system = true` — explicit even though the metadata-table join
///     implies it; cheap and makes the query plan unambiguous.
///   * `_siphon_deleted = false` on both sides — never resolve against
///     tombstoned rows.
///   * `startsWith(traversal_path, …)` — exploits the leading column of
///     `siphon_notes`' primary key.
///   * `action IN (…)` — pre-filters in-CH to the parser's handled subset
///     before the body crosses the wire. Mirrors the
///     `HANDLED_CROSS_REFERENCE_ACTIONS` ∪ `HANDLED_LIFECYCLE_ACTIONS`
///     vendored list.
pub(super) const SYSTEM_NOTES_EXTRACT_SQL: &str = "\
SELECT \
    sn.id AS id, \
    sn.note AS note, \
    sn.noteable_id AS noteable_id, \
    sn.noteable_type AS noteable_type, \
    sn.author_id AS author_id, \
    sn.project_id AS project_id, \
    sn.created_at AS created_at, \
    sn.traversal_path AS traversal_path, \
    snm.action AS action, \
    snm.commit_count AS commit_count \
FROM siphon_notes AS sn \
INNER JOIN siphon_system_note_metadata AS snm ON sn.id = snm.note_id \
WHERE sn.system = true \
  AND sn._siphon_deleted = false \
  AND snm._siphon_deleted = false \
  AND startsWith(sn.traversal_path, {traversal_path:String}) \
  AND sn.created_at > {last_watermark:DateTime64(6,'UTC')} \
  AND sn.created_at <= {watermark:DateTime64(6,'UTC')} \
  AND snm.action IN {actions:Array(String)} \
ORDER BY sn.created_at, sn.id \
LIMIT {batch_limit:UInt64}";

/// SQL parameter name for the action IN-list, exposed so the production
/// handler can bind it from the vendored `HANDLED_*` constants without
/// hard-coding the name in two places.
pub(super) const ACTIONS_PARAM_NAME: &str = "actions";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_sql_uses_named_parameters() {
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("{traversal_path:String}"));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("{last_watermark:DateTime64(6,'UTC')}"));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("{watermark:DateTime64(6,'UTC')}"));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("{actions:Array(String)}"));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("{batch_limit:UInt64}"));
    }

    #[test]
    fn extract_sql_pre_filters_to_system_rows_and_excludes_tombstones() {
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("sn.system = true"));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("sn._siphon_deleted = false"));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("snm._siphon_deleted = false"));
    }

    #[test]
    fn extract_sql_uses_metadata_join_on_note_id() {
        // The !1109 fixture-bug was `USING(note_id)` which doesn't work
        // because `note_id` is not present in `siphon_notes`. Make the join
        // shape explicit so a future copy-paste doesn't regress to it.
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("ON sn.id = snm.note_id"));
    }

    #[test]
    fn extract_sql_uses_traversal_path_prefix_filter() {
        // Exploits the leading column of siphon_notes.PRIMARY KEY for index
        // skipping rather than a per-row equality.
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("startsWith(sn.traversal_path"));
    }

    #[test]
    fn extract_sql_pulls_commit_count_for_drift_assertion() {
        // The production handler compares `commit_count` against the
        // parser's SHA count for `action='commit'` rows; a Rails template
        // change that breaks `extract_commit_shas_from_list` shows up here
        // before it shows up in edge density. Keep the column in the SELECT
        // even before the metric is wired so the SQL doesn't need a churn
        // when it lands.
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("snm.commit_count"));
    }
}
