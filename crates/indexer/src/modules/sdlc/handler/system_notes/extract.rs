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
///       checkpoint store. Bound from `TIMESTAMP_FORMAT` (space-separated),
///       which the typed `DateTime64` param parses. **System notes are
///       immutable post-creation** (Rails writes them once, never edits),
///       so `created_at` is the semantically correct watermark column —
///       using `_siphon_replicated_at` (the default for mutable entities)
///       would reprocess the same note on every Siphon-side compaction
///       without any new edges to materialise.
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
///
/// Keyset pagination: the handler pages within a `(last_watermark,
/// watermark]` window by carrying the last seen `(created_at, id)` forward
/// through the shared [`crate::modules::sdlc::plan::Cursor`]. The
/// `{{cursor}}` placeholder is filled by
/// [`crate::modules::sdlc::plan::CursorFilter`] with the sort key
/// `["sn.created_at", "sn.id"]`, which emits the DNF keyset predicate
/// matching `ORDER BY sn.created_at, sn.id`, so each page resumes exactly
/// after the previous one with no row skipped or repeated. The first page
/// supplies an empty cursor (the filter degenerates to a no-op). Using the
/// shared cursor + filter avoids reimplementing the keyset machinery the
/// SDLC entity pipeline already provides.
///
/// `{{cursor}}` expands to either an empty string (first page) or an
/// `AND (...)` clause; it is substituted at query-build time, not by the
/// ClickHouse parameter channel.
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
    snm.action AS action \
FROM siphon_notes AS sn \
INNER JOIN siphon_system_note_metadata AS snm ON sn.id = snm.note_id \
WHERE sn.system = true \
  AND sn._siphon_deleted = false \
  AND snm._siphon_deleted = false \
  AND startsWith(sn.traversal_path, {traversal_path:String}) \
  AND sn.created_at > {last_watermark:DateTime64(6,'UTC')} \
  AND sn.created_at <= {watermark:DateTime64(6,'UTC')} \
  {{cursor}} \
  AND snm.action IN {actions:Array(String)} \
ORDER BY sn.created_at, sn.id \
LIMIT {batch_limit:UInt64}";

/// Placeholder in [`SYSTEM_NOTES_EXTRACT_SQL`] replaced by the keyset
/// predicate from `CursorFilter` (or an empty string on the first page).
pub(super) const CURSOR_PLACEHOLDER: &str = "{{cursor}}";

/// Sort key for the keyset cursor `WHERE` predicate. `CursorFilter` inlines
/// the cursor values as **string literals**, and the shared `Cursor`
/// formats the `created_at` keyset value as ISO `…T…Z` (the
/// `ArrowUtils::array_value_to_string` form, chrono `%Y-%m-%dT%H:%M:%SZ`).
/// A `DateTime64` column cannot be compared to that ISO literal directly
/// (ClickHouse rejects it), so the timestamp leg projects the column to the
/// same ISO string with `formatDateTime(...)` — then both sides are ISO
/// strings and the comparison is lexicographic (ISO `YYYY-MM-DDTHH:MM:SSZ`
/// sorts correctly).
///
/// The format string uses ClickHouse's `%i` for minutes, **not** chrono's
/// `%M`: in ClickHouse `formatDateTime`, `%M` is the full month name
/// (`January`) and `%i` is minutes, while chrono's `%M` is minutes. Using
/// `%M` here would emit `2024-01-15T09:January:00Z` and never match the
/// arrow-produced `2024-01-15T09:00:00Z` cursor literal, silently breaking
/// keyset paging past the first page.
///
/// The `id` leg stays numeric (its literal parses as the column type).
/// Both columns are table-qualified because the joined tables each carry
/// `created_at` / `id`; the order matches the `ORDER BY` above.
pub(super) const CURSOR_SORT_KEY: &[&str] = &[
    "formatDateTime(sn.created_at, '%Y-%m-%dT%H:%i:%SZ')",
    "sn.id",
];

/// Sort key for *advancing* the cursor off a result batch. The SELECT
/// aliases the qualified sort key to bare `created_at` / `id`, so
/// `Cursor::advance` reads those names. Same columns, same order, same
/// values as [`CURSOR_SORT_KEY`] — only the qualification differs.
///
/// `Cursor::advance` formats the timestamp without sub-second precision
/// (shared `ArrowUtils` behaviour); a resumed cursor can therefore re-admit
/// rows sharing the cursor's whole-second `created_at`, but `id` breaks the
/// tie and `gl_edge`'s ReplacingMergeTree collapses any re-emitted edge, so
/// this is bounded rework, never a gap. Matches the SDLC entity pipeline.
pub(super) const CURSOR_ADVANCE_KEY: &[&str] = &["created_at", "id"];

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
    fn extract_sql_uses_keyset_cursor_placeholder() {
        // The keyset predicate is injected via the shared `CursorFilter`
        // through the `{{cursor}}` placeholder rather than hard-coded params,
        // so the handler reuses the SDLC pipeline's cursor machinery. The
        // ORDER BY must still match the cursor sort key.
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains(CURSOR_PLACEHOLDER));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("ORDER BY sn.created_at, sn.id"));
        // The timestamp leg is projected to the ISO `…T…Z` string the shared
        // `Cursor` emits, so the keyset literal comparison is string-vs-string
        // (a raw `DateTime64` column rejects the ISO literal).
        // `%i` (minutes) not `%M` (month name) — ClickHouse `formatDateTime`
        // diverges from chrono here; see the `CURSOR_SORT_KEY` doc comment.
        assert_eq!(
            CURSOR_SORT_KEY,
            [
                "formatDateTime(sn.created_at, '%Y-%m-%dT%H:%i:%SZ')",
                "sn.id"
            ]
        );
    }

    #[test]
    fn extract_sql_uses_traversal_path_prefix_filter() {
        // Exploits the leading column of siphon_notes.PRIMARY KEY for index
        // skipping rather than a per-row equality.
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("startsWith(sn.traversal_path"));
    }
}
