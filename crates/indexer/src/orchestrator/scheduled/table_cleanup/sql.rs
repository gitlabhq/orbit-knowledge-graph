use chrono::{DateTime, Utc};

use crate::clickhouse::{PATCH_PART_PREFIX, TIMESTAMP_FORMAT};
use crate::modules::code::config::CodeTableNames;

pub(super) const PATH_COLUMN: &str = "traversal_path";
const PATCH_DELETE_MODE: &str = "lightweight_update_force";
const CODE_SCOPE: &str = "traversal_path, project_id, branch";
/// Part names are `<partition>_<min block>_<max block>_<level>[_<mutation>]`.
const PART_MAX_BLOCK: &str = "toUInt64OrZero(splitByChar('_', _part)[3])";

/// Subquery predicates were a silent no-op before ClickHouse PR #87285 (25.10.1, backports 25.7.8, 25.8.8, 25.9.3).
pub(super) fn supports_patch_deletes(version: &str) -> bool {
    let mut parts = version
        .split('.')
        .map(|part| part.parse::<u32>().unwrap_or(0));
    let (major, minor, patch) = (
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
    );
    match (major, minor) {
        (26.., _) => true,
        (25, 10..) => true,
        (25, 9) => patch >= 3,
        (25, 8) => patch >= 8,
        (25, 7) => patch >= 8,
        _ => false,
    }
}

fn escape(literal: &str) -> String {
    literal.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Sequential consistency is off: it waits behind every pending replication-queue entry, and the cursor overlap covers late parts.
fn settings(timeout_secs: u64) -> String {
    format!(
        "SETTINGS lightweight_delete_mode = '{PATCH_DELETE_MODE}', \
         update_sequential_consistency = 0, max_execution_time = {timeout_secs}"
    )
}

pub(super) fn version_filter(operator: &str, cutoff: DateTime<Utc>) -> String {
    format!(
        " AND _version {operator} toDateTime64('{}', 6, 'UTC')",
        cutoff.format(TIMESTAMP_FORMAT)
    )
}

/// Only parts whose name ends above the cursor are read at all.
pub(super) fn new_rows_filter(after_block: u64) -> String {
    format!(" AND {PART_MAX_BLOCK} > {after_block} AND _block_number > {after_block}")
}

pub(super) fn block_settings_missing_sql(table: &str) -> String {
    format!(
        "SELECT 1 FROM system.tables WHERE database = currentDatabase() AND name = '{table}' \
         AND (engine_full NOT LIKE '%enable_block_number_column = 1%' \
              OR engine_full NOT LIKE '%enable_block_offset_column = 1%')"
    )
}

/// ClickHouse Cloud persists `_block_offset` alone on merged parts, so the patch join identity repeats inside a part.
pub(super) fn offset_only_parts_sql(table: &str) -> String {
    format!(
        "SELECT 1 FROM system.parts_columns \
         WHERE database = currentDatabase() AND table = '{table}' AND active AND column = '_block_offset' \
           AND name NOT IN (SELECT name FROM system.parts_columns \
                            WHERE database = currentDatabase() AND table = '{table}' AND active AND column = '_block_number')"
    )
}

/// Parts attached from another table keep its block numbers, which collide with the new table's own.
pub(super) fn foreign_block_numbers_sql(table: &str) -> String {
    format!(
        "SELECT 1 FROM (SELECT _part, max(_block_number) AS persisted FROM {table} GROUP BY _part) AS rows_by_part \
         INNER JOIN (SELECT name, max_block_number FROM system.parts \
                     WHERE database = currentDatabase() AND table = '{table}' AND active) AS parts \
           ON parts.name = rows_by_part._part \
         WHERE rows_by_part.persisted > parts.max_block_number"
    )
}

pub(super) fn high_block_sql(table: &str) -> String {
    format!(
        "SELECT toString(max(max_block_number)) FROM system.parts \
         WHERE database = currentDatabase() AND table = '{table}' AND active \
           AND NOT startsWith(name, '{PATCH_PART_PREFIX}')"
    )
}

pub(super) fn patch_bytes_sql(tables: &[String]) -> String {
    format!(
        "SELECT table, toString(sum(data_uncompressed_bytes)) FROM system.parts \
         WHERE database = currentDatabase() AND active AND startsWith(name, '{PATCH_PART_PREFIX}') \
           AND table IN ({}) GROUP BY table",
        list_sql(tables)
    )
}

pub(super) fn apply_patches_statement(table: &str) -> String {
    format!("ALTER TABLE {table} APPLY PATCHES SETTINGS mutations_sync = 0")
}

/// A large join-mode patch can take hours to apply; a second `APPLY PATCHES` only queues a duplicate.
pub(super) fn pending_apply_patches_sql() -> &'static str {
    "SELECT DISTINCT table FROM system.mutations \
     WHERE database = currentDatabase() AND NOT is_done AND command LIKE '%APPLY PATCHES%'"
}

pub(super) fn tombstone_rows_sql(table: &str, filter: &str) -> String {
    format!("SELECT 1 FROM {table} WHERE _deleted{filter}")
}

pub(super) fn tombstones_per_path_sql(table: &str, filter: &str) -> String {
    format!(
        "SELECT {PATH_COLUMN}, toString(count()) FROM {table} WHERE _deleted{filter} \
         GROUP BY {PATH_COLUMN} ORDER BY {PATH_COLUMN}"
    )
}

fn list_sql(literals: &[String]) -> String {
    literals
        .iter()
        .map(|literal| format!("'{}'", escape(literal)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Lightweight deletes prune parts only by literal predicates, never by `IN (subquery)` sets.
pub(super) fn path_prune_sql(paths: &[String]) -> String {
    format!("{PATH_COLUMN} IN ({})", list_sql(paths))
}

pub(super) fn candidates_sql(
    table: &str,
    key: &str,
    filter: &str,
    chunk: Option<(usize, usize)>,
) -> String {
    let chunk = chunk
        .map(|(chunks, index)| format!(" AND cityHash64({key}) % {chunks} = {index}"))
        .unwrap_or_default();
    format!("SELECT {key} FROM {table} WHERE {filter}{chunk}")
}

/// Which row of a candidate key survives a collapse.
#[derive(Clone, Copy)]
pub(super) enum Keep {
    Newest,
    NewestUnlessExpiredTombstone(DateTime<Utc>),
}

pub(super) fn collapse_statement(
    table: &str,
    key: &str,
    candidates: &str,
    prune: Option<&str>,
    keep: Keep,
    timeout_secs: u64,
) -> String {
    // A live row tied with a tombstone at the same `_version` counts as live.
    let having = match keep {
        Keep::Newest => String::new(),
        Keep::NewestUnlessExpiredTombstone(cutoff) => format!(
            " HAVING maxIf(_version, NOT _deleted) = max(_version) OR max(_version) >= toDateTime64('{}', 6, 'UTC')",
            cutoff.format(TIMESTAMP_FORMAT)
        ),
    };
    let prune = prune
        .map(|prune| format!("{prune} AND "))
        .unwrap_or_default();
    // Positive list: a row that lands between the subquery snapshot and the outer read is never matched.
    format!(
        "DELETE FROM {table} WHERE {prune}({key}) IN ({candidates}) \
         AND ({key}, _version) IN (\
           SELECT {key}, _version FROM {table} WHERE {prune}({key}) IN ({candidates}) \
           AND ({key}, _version) NOT IN (\
             SELECT {key}, max(_version) FROM {table} WHERE {prune}({key}) IN ({candidates}) \
             GROUP BY {key}{having})) {}",
        settings(timeout_secs)
    )
}

/// An "indexed empty" checkpoint has no branch row at or after its bound, so its scope is left alone.
pub(super) fn code_scopes_sql(
    checkpoint_table: &str,
    branch_table: &str,
    after_block: Option<u64>,
    chunk: Option<(usize, usize)>,
) -> String {
    let block = after_block
        .map(|block| format!("_block_number > {block}"))
        .unwrap_or_else(|| "1".to_string());
    let chunk = chunk
        .map(|(chunks, index)| format!(" AND cityHash64({CODE_SCOPE}) % {chunks} = {index}"))
        .unwrap_or_default();
    let changed = format!("SELECT {CODE_SCOPE} FROM {checkpoint_table} WHERE {block}{chunk}");
    format!(
        "SELECT s.traversal_path AS traversal_path, s.project_id AS project_id, s.branch AS branch, s.bound AS bound FROM (\
           SELECT {CODE_SCOPE}, max(indexed_at) AS bound FROM {checkpoint_table} \
           WHERE NOT _deleted AND ({CODE_SCOPE}) IN ({changed}) GROUP BY {CODE_SCOPE}) AS s \
         INNER JOIN (\
           SELECT traversal_path, project_id, name AS branch, max(_version) AS branch_version FROM {branch_table} \
           WHERE (traversal_path, project_id, name) IN ({changed}) AND NOT _deleted \
           GROUP BY traversal_path, project_id, name) AS b \
           ON s.traversal_path = b.traversal_path AND s.project_id = b.project_id AND s.branch = b.branch \
         WHERE b.branch_version >= s.bound"
    )
}

pub(super) fn scope_paths_sql(scopes: &str) -> String {
    format!("SELECT DISTINCT {PATH_COLUMN} FROM ({scopes}) ORDER BY {PATH_COLUMN}")
}

pub(super) fn code_snapshot_statement(
    table: &str,
    scopes: &str,
    prune: &str,
    timeout_secs: u64,
) -> String {
    format!(
        "DELETE FROM {table} WHERE {prune} AND ({CODE_SCOPE}) IN (SELECT {CODE_SCOPE} FROM ({scopes})) \
         AND ({CODE_SCOPE}, _version) IN (\
           SELECT e.traversal_path, e.project_id, e.branch, e._version FROM {table} AS e \
           INNER JOIN ({scopes}) AS c \
             ON e.traversal_path = c.traversal_path AND e.project_id = c.project_id AND e.branch = c.branch \
           WHERE e.{prune} AND (e.traversal_path, e.project_id, e.branch) IN (SELECT {CODE_SCOPE} FROM ({scopes})) \
             AND e._version < c.bound) {}",
        settings(timeout_secs)
    )
}

/// Shared edge tables carry no branch, so only paths with a single indexed branch have a checkpoint to bound them.
pub(super) fn shared_edge_snapshot_statement(
    table: &str,
    checkpoint_table: &str,
    scopes: &str,
    prune: &str,
    timeout_secs: u64,
) -> String {
    let kinds = CodeTableNames::node_kinds_sql_list();
    let paths = format!(
        "SELECT traversal_path, min(scope_bound) AS bound FROM (\
           SELECT {CODE_SCOPE}, max(indexed_at) AS scope_bound FROM {checkpoint_table} \
           WHERE NOT _deleted GROUP BY {CODE_SCOPE}) \
         WHERE traversal_path IN (SELECT DISTINCT traversal_path FROM ({scopes})) \
         GROUP BY traversal_path HAVING count() = 1"
    );
    format!(
        "DELETE FROM {table} WHERE {prune} AND traversal_path IN (SELECT traversal_path FROM ({paths})) \
         AND source_kind IN ({kinds}) \
         AND (traversal_path, _version) IN (\
           SELECT e.traversal_path, e._version FROM {table} AS e \
           INNER JOIN ({paths}) AS c ON e.traversal_path = c.traversal_path \
           WHERE e.{prune} AND e.traversal_path IN (SELECT traversal_path FROM ({paths})) \
             AND e.source_kind IN ({kinds}) AND e._version < c.bound) {}",
        settings(timeout_secs)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_gate_requires_the_subquery_fix() {
        for ok in [
            "26.4.1.2212",
            "25.10.1.3832",
            "25.9.3.48",
            "25.8.8.26",
            "25.7.8.71",
        ] {
            assert!(supports_patch_deletes(ok), "{ok}");
        }
        for bad in ["25.8.3.1", "25.7.1.1", "25.9.2.5", "24.12.1.1", "garbage"] {
            assert!(!supports_patch_deletes(bad), "{bad}");
        }
    }

    #[test]
    fn candidates_are_limited_to_parts_and_rows_above_the_cursor() {
        let filter = format!(
            "{} AND _deleted{}",
            path_prune_sql(&["1/2/".to_string()]),
            new_rows_filter(42)
        );
        assert_eq!(
            candidates_sql("v1_gl_edge", "traversal_path, id", &filter, None),
            "SELECT traversal_path, id FROM v1_gl_edge WHERE traversal_path IN ('1/2/') AND _deleted AND toUInt64OrZero(splitByChar('_', _part)[3]) > 42 AND _block_number > 42"
        );
    }

    #[test]
    fn candidate_chunks_partition_by_key_hash() {
        let sql = candidates_sql(
            "t",
            "k",
            &format!("_deleted{}", new_rows_filter(0)),
            Some((4, 3)),
        );
        assert!(sql.ends_with("AND _block_number > 0 AND cityHash64(k) % 4 = 3"));
    }

    #[test]
    fn path_literals_are_escaped() {
        assert_eq!(
            path_prune_sql(&["1/2/".to_string(), "it's".to_string()]),
            "traversal_path IN ('1/2/', 'it\\'s')"
        );
    }

    #[test]
    fn incremental_collapse_keeps_the_newest_row_of_each_key() {
        let sql = collapse_statement(
            "t",
            "a, b",
            "SELECT a, b FROM t WHERE _deleted",
            Some("traversal_path IN ('1/2/')"),
            Keep::Newest,
            30,
        );
        assert!(sql.starts_with(
            "DELETE FROM t WHERE traversal_path IN ('1/2/') AND (a, b) IN (SELECT a, b FROM t WHERE _deleted) AND (a, b, _version) IN (\
             SELECT a, b, _version FROM t WHERE traversal_path IN ('1/2/') AND (a, b) IN (SELECT a, b FROM t WHERE _deleted) AND (a, b, _version) NOT IN (\
             SELECT a, b, max(_version) FROM t WHERE traversal_path IN ('1/2/') AND (a, b) IN ("
        ));
        assert!(sql.contains("GROUP BY a, b)) SETTINGS"));
        assert!(sql.ends_with(
            "SETTINGS lightweight_delete_mode = 'lightweight_update_force', update_sequential_consistency = 0, max_execution_time = 30"
        ));
    }

    #[test]
    fn purge_collapse_drops_expired_dead_keys_and_keeps_ties_and_young_tombstones() {
        let cutoff = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        let sql = collapse_statement(
            "t",
            "a, b",
            "SELECT a, b FROM t WHERE _deleted",
            None,
            Keep::NewestUnlessExpiredTombstone(cutoff),
            30,
        );
        assert!(sql.starts_with("DELETE FROM t WHERE (a, b) IN ("));
        assert!(sql.contains(
            "GROUP BY a, b HAVING maxIf(_version, NOT _deleted) = max(_version) OR max(_version) >= toDateTime64('2026-01-01 00:00:00.000000', 6, 'UTC')))"
        ));
    }

    #[test]
    fn code_scopes_require_a_branch_row_at_or_after_the_checkpoint_bound() {
        let sql = code_scopes_sql("cp", "br", Some(17), None);
        assert_eq!(
            sql,
            "SELECT s.traversal_path AS traversal_path, s.project_id AS project_id, s.branch AS branch, s.bound AS bound FROM (\
               SELECT traversal_path, project_id, branch, max(indexed_at) AS bound FROM cp \
               WHERE NOT _deleted AND (traversal_path, project_id, branch) IN (\
                 SELECT traversal_path, project_id, branch FROM cp WHERE _block_number > 17) \
               GROUP BY traversal_path, project_id, branch) AS s \
             INNER JOIN (\
               SELECT traversal_path, project_id, name AS branch, max(_version) AS branch_version FROM br \
               WHERE (traversal_path, project_id, name) IN (\
                 SELECT traversal_path, project_id, branch FROM cp WHERE _block_number > 17) AND NOT _deleted \
               GROUP BY traversal_path, project_id, name) AS b \
               ON s.traversal_path = b.traversal_path AND s.project_id = b.project_id AND s.branch = b.branch \
             WHERE b.branch_version >= s.bound"
        );
    }

    #[test]
    fn code_history_covers_every_checkpointed_scope() {
        let sql = code_scopes_sql("cp", "br", None, None);
        assert!(sql.contains("SELECT traversal_path, project_id, branch FROM cp WHERE 1)"));
    }

    #[test]
    fn code_scope_chunks_partition_by_scope_hash() {
        let sql = code_scopes_sql("cp", "br", Some(0), Some((3, 1)));
        assert!(sql.contains(
            "_block_number > 0 AND cityHash64(traversal_path, project_id, branch) % 3 = 1)"
        ));
    }

    #[test]
    fn code_snapshot_delete_is_bounded_by_each_scope_checkpoint() {
        let scopes = "SELECT traversal_path, project_id, branch, bound FROM cp";
        let sql = code_snapshot_statement(
            "v1_gl_definition",
            scopes,
            "traversal_path IN ('1/2/', '1/3/')",
            60,
        );
        assert!(sql.contains("AND e._version < c.bound"));
        assert!(sql.contains(
            "WHERE traversal_path IN ('1/2/', '1/3/') AND (traversal_path, project_id, branch) IN (SELECT traversal_path, project_id, branch FROM (SELECT traversal_path, project_id, branch, bound FROM cp))"
        ));
        assert!(sql.contains("WHERE e.traversal_path IN ('1/2/', '1/3/') AND (e.traversal_path"));
    }

    #[test]
    fn shared_edge_delete_only_covers_single_branch_paths() {
        let sql = shared_edge_snapshot_statement(
            "v1_gl_edge",
            "v1_code_indexing_checkpoint",
            "SELECT 1",
            "traversal_path IN ('1/2/')",
            60,
        );
        assert!(sql.starts_with(
            "DELETE FROM v1_gl_edge WHERE traversal_path IN ('1/2/') AND traversal_path IN (SELECT"
        ));
        assert!(sql.contains("WHERE e.traversal_path IN ('1/2/') AND e.traversal_path IN (SELECT"));
        assert!(sql.contains("GROUP BY traversal_path HAVING count() = 1"));
        assert!(
            sql.contains("source_kind IN ('Directory', 'File', 'Definition', 'ImportedSymbol')")
        );
        assert!(sql.contains("AND e._version < c.bound"));
    }

    #[test]
    fn guards_read_the_system_tables_they_need() {
        assert!(block_settings_missing_sql("v1_gl_edge").contains(
            "engine_full NOT LIKE '%enable_block_number_column = 1%' OR engine_full NOT LIKE '%enable_block_offset_column = 1%'"
        ));
        let offset_only = offset_only_parts_sql("v1_gl_edge");
        assert!(
            offset_only.contains("column = '_block_offset'"),
            "{offset_only}"
        );
        assert!(
            offset_only.contains("NOT IN (SELECT name FROM system.parts_columns"),
            "{offset_only}"
        );
        let foreign = foreign_block_numbers_sql("v1_gl_edge");
        assert!(foreign.contains("max(_block_number) AS persisted FROM v1_gl_edge GROUP BY _part"));
        assert!(foreign.ends_with("WHERE rows_by_part.persisted > parts.max_block_number"));
        assert!(
            pending_apply_patches_sql().contains("NOT is_done AND command LIKE '%APPLY PATCHES%'")
        );
    }
}
