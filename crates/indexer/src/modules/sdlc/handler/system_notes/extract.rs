//! Datalake extraction SQL template for the system-notes handler.
//!
//! The handler reads `siphon_notes` joined to `siphon_system_note_metadata`
//! on `note_id` (Mode A). Mode B (body-text fallback) is documented in ADR
//! 013 but not implemented in this draft — Mode A is the production target
//! and the table is replicated via the parallel Siphon-side MR
//! (`gitlab-org/analytics-section/siphon`).
//!
//! The template plugs into the shared SDLC [`crate::modules::sdlc::plan::Plan`]
//! extract mechanism: the `{{filters}}` marker is filled by the chained
//! [`crate::modules::sdlc::plan::Filter`]s the handler applies (watermark,
//! traversal-path, the action IN-list, and the keyset cursor), and
//! `{{batch_size}}` by the plan's page size. The handler therefore reuses the
//! same `Producer`/`Extractor`/`Loader` paging the entity handlers use rather
//! than driving its own loop.
//!
//! The SQL is exposed as `pub(super) const` so the unit tests in `mod.rs` can
//! cross-check shape (named parameters, scoping filters, deleted tombstones)
//! without a live ClickHouse.

/// Mode A extraction: join `siphon_notes` to `siphon_system_note_metadata`
/// on `note_id`, restricted to the system-note half of the table.
///
/// Filters supplied by the handler through the shared `Plan`/`PreparedQuery`
/// `.with(...)` chain, all landing in the `{{filters}}` marker:
///   * watermark (`WatermarkFilter` on `sn.created_at`) — exclusive lower /
///     inclusive upper bound. **System notes are immutable post-creation**
///     (Rails writes them once, never edits), so `created_at` is the
///     semantically correct watermark column — using `_siphon_replicated_at`
///     (the default for mutable entities) would reprocess the same note on
///     every Siphon-side compaction without any new edges to materialise.
///   * traversal-path prefix (`TraversalPathFilter` on `sn.traversal_path`)
///     — exploits the leading column of `siphon_notes`' primary key.
///   * action IN-list (`ActionsFilter`) — pre-filters in-CH to the parser's
///     handled subset (`HANDLED_CROSS_REFERENCE_ACTIONS` ∪
///     `HANDLED_LIFECYCLE_ACTIONS`) before bodies cross the wire.
///   * keyset cursor (`CursorFilter` over [`CURSOR_SORT_KEY`]) — the DNF
///     predicate matching `ORDER BY sn.created_at, sn.id`, so each page
///     resumes exactly after the previous one. Empty on the first page.
///
/// `{{batch_size}}` is the plan's page size.
///
/// Static filter rationale:
///   * `system = true` — explicit even though the metadata-table join
///     implies it; cheap and makes the query plan unambiguous.
///   * `_siphon_deleted = false` on both sides — never resolve against
///     tombstoned rows.
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
  {{filters}} \
ORDER BY sn.created_at, sn.id \
LIMIT {{batch_size}}";

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
    fn extract_sql_uses_shared_plan_markers() {
        // The dynamic predicates (watermark, traversal-path, action IN-list,
        // keyset cursor) and the page size are supplied by the shared
        // `Plan`/`PreparedQuery` mechanism, so the template carries only the
        // `{{filters}}` and `{{batch_size}}` markers it substitutes.
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("{{filters}}"));
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("LIMIT {{batch_size}}"));
        // No hard-coded params remain in the template — they would double up
        // with the filter-supplied ones.
        assert!(!SYSTEM_NOTES_EXTRACT_SQL.contains("{traversal_path:"));
        assert!(!SYSTEM_NOTES_EXTRACT_SQL.contains("{actions:"));
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
    fn extract_sql_orders_by_keyset_columns() {
        // The `ORDER BY` must match the keyset cursor sort key the shared
        // `CursorFilter` fills into `{{filters}}`.
        assert!(SYSTEM_NOTES_EXTRACT_SQL.contains("ORDER BY sn.created_at, sn.id"));
    }

    #[test]
    fn cursor_sort_key_projects_timestamp_to_iso_string() {
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
        // The advance key reads the SELECT-aliased bare columns off the result
        // batch (the pipeline advances the cursor on these names).
        assert_eq!(CURSOR_ADVANCE_KEY, ["created_at", "id"]);
    }
}
