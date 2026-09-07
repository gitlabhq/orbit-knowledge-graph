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

pub(super) fn cluster_exists_sql(cluster: &str) -> String {
    format!(
        "SELECT 1 FROM system.clusters WHERE cluster = '{}'",
        escape(cluster)
    )
}

/// A patch written while one of its parts is merging or mutating is applied to the result in join mode.
/// Merges are local to the replica that runs them, so the cluster view is needed wherever one exists.
pub(super) fn busy_parts_sql(database: &str, table: &str, cluster: Option<&str>) -> String {
    let merges = match cluster {
        Some(cluster) => format!("clusterAllReplicas('{}', system.merges)", escape(cluster)),
        None => "system.merges".to_string(),
    };
    format!(
        "SELECT DISTINCT arrayJoin(source_part_names) FROM {merges} \
         WHERE database = '{}' AND table = '{table}'",
        escape(database)
    )
}

/// A part still queued for a mutation is renamed when its turn comes, so every patch written before that turns into join mode.
pub(super) fn pending_mutations_sql(database: &str, table: &str) -> String {
    format!(
        "SELECT 1 FROM system.mutations WHERE database = '{}' AND table = '{table}' AND NOT is_done",
        escape(database)
    )
}

pub(super) fn exclude_parts_sql(parts: &[String]) -> String {
    if parts.is_empty() {
        return String::new();
    }
    format!(" AND _part NOT IN ({})", list_sql(parts))
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
/// Bytes one literal adds to a `list_sql` rendering, quotes and separator included.
pub(super) fn list_item_len(literal: &str) -> usize {
    escape(literal).len() + "'', ".len()
}

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
    exclude: &str,
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
             GROUP BY {key}{having})){exclude} {}",
        settings(timeout_secs)
    )
}

pub(super) fn changed_scopes_sql(checkpoint_table: &str, after_block: u64) -> String {
    format!("SELECT {CODE_SCOPE} FROM {checkpoint_table} WHERE _block_number > {after_block}")
}

/// Every snapshot of a scope carries its own `_version`, so a second version in the file table marks a scope with history to remove.
pub(super) fn multi_snapshot_scopes_sql(file_table: &str) -> String {
    format!(
        "SELECT {CODE_SCOPE} FROM {file_table} GROUP BY {CODE_SCOPE} HAVING uniqExact(_version) > 1"
    )
}

/// An "indexed empty" checkpoint has no branch row at or after its bound, so its scope is left alone.
pub(super) fn code_scopes_sql(
    checkpoint_table: &str,
    branch_table: &str,
    changed: &str,
    chunk: Option<(usize, usize)>,
) -> String {
    let chunk = chunk
        .map(|(chunks, index)| format!(" AND cityHash64({CODE_SCOPE}) % {chunks} = {index}"))
        .unwrap_or_default();
    let changed = format!("SELECT {CODE_SCOPE} FROM ({changed}) WHERE 1{chunk}");
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

/// The target is joined through a pre-filtered derived table: joining it directly made the lightweight update read the whole table once more without a key condition.
pub(super) fn code_snapshot_statement(
    table: &str,
    scopes: &str,
    prune: &str,
    exclude: &str,
    timeout_secs: u64,
) -> String {
    let in_scope = format!("({CODE_SCOPE}) IN (SELECT {CODE_SCOPE} FROM ({scopes}))");
    format!(
        "DELETE FROM {table} WHERE {prune} AND {in_scope} \
         AND ({CODE_SCOPE}, _version) IN (\
           SELECT v.traversal_path, v.project_id, v.branch, v._version FROM (\
             SELECT DISTINCT {CODE_SCOPE}, _version FROM {table} WHERE {prune} AND {in_scope}) AS v \
           INNER JOIN ({scopes}) AS c \
             ON v.traversal_path = c.traversal_path AND v.project_id = c.project_id AND v.branch = c.branch \
           WHERE v._version < c.bound){exclude} {}",
        settings(timeout_secs)
    )
}

/// Shared edge tables carry no branch, so only paths with a single indexed branch have a checkpoint to bound them.
pub(super) fn shared_edge_snapshot_statement(
    table: &str,
    checkpoint_table: &str,
    scopes: &str,
    prune: &str,
    exclude: &str,
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
    let in_paths = format!(
        "traversal_path IN (SELECT traversal_path FROM ({paths})) AND source_kind IN ({kinds})"
    );
    format!(
        "DELETE FROM {table} WHERE {prune} AND {in_paths} \
         AND (traversal_path, _version) IN (\
           SELECT v.traversal_path, v._version FROM (\
             SELECT DISTINCT traversal_path, _version FROM {table} WHERE {prune} AND {in_paths}) AS v \
           INNER JOIN ({paths}) AS c ON v.traversal_path = c.traversal_path \
           WHERE v._version < c.bound){exclude} {}",
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
            "",
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
            "",
            30,
        );
        assert!(sql.starts_with("DELETE FROM t WHERE (a, b) IN ("));
        assert!(sql.contains(
            "GROUP BY a, b HAVING maxIf(_version, NOT _deleted) = max(_version) OR max(_version) >= toDateTime64('2026-01-01 00:00:00.000000', 6, 'UTC')))"
        ));
    }

    #[test]
    fn busy_parts_are_excluded_from_the_outer_predicate_only() {
        let exclude = exclude_parts_sql(&["all_1_5_1".to_string(), "all_6_6_0".to_string()]);
        assert_eq!(exclude, " AND _part NOT IN ('all_1_5_1', 'all_6_6_0')");
        let sql = collapse_statement(
            "t",
            "a",
            "SELECT a FROM t WHERE _deleted",
            None,
            Keep::Newest,
            &exclude,
            30,
        );
        assert!(sql.contains("GROUP BY a)) AND _part NOT IN ('all_1_5_1', 'all_6_6_0') SETTINGS"));
        assert_eq!(sql.matches("_part NOT IN").count(), 1);
        assert_eq!(exclude_parts_sql(&[]), "");
    }

    #[test]
    fn busy_parts_come_from_every_replica_when_a_cluster_is_known() {
        assert_eq!(
            busy_parts_sql("gkg", "v1_gl_edge", Some("default")),
            "SELECT DISTINCT arrayJoin(source_part_names) FROM clusterAllReplicas('default', system.merges) WHERE database = 'gkg' AND table = 'v1_gl_edge'"
        );
        assert_eq!(
            busy_parts_sql("gkg", "v1_gl_edge", None),
            "SELECT DISTINCT arrayJoin(source_part_names) FROM system.merges WHERE database = 'gkg' AND table = 'v1_gl_edge'"
        );
        assert_eq!(
            pending_mutations_sql("gkg", "v1_gl_edge"),
            "SELECT 1 FROM system.mutations WHERE database = 'gkg' AND table = 'v1_gl_edge' AND NOT is_done"
        );
        assert_eq!(
            cluster_exists_sql("default"),
            "SELECT 1 FROM system.clusters WHERE cluster = 'default'"
        );
    }

    #[test]
    fn code_scopes_require_a_branch_row_at_or_after_the_checkpoint_bound() {
        let sql = code_scopes_sql("cp", "br", &changed_scopes_sql("cp", 17), None);
        assert_eq!(
            sql,
            "SELECT s.traversal_path AS traversal_path, s.project_id AS project_id, s.branch AS branch, s.bound AS bound FROM (\
               SELECT traversal_path, project_id, branch, max(indexed_at) AS bound FROM cp \
               WHERE NOT _deleted AND (traversal_path, project_id, branch) IN (\
                 SELECT traversal_path, project_id, branch FROM (SELECT traversal_path, project_id, branch FROM cp WHERE _block_number > 17) WHERE 1) \
               GROUP BY traversal_path, project_id, branch) AS s \
             INNER JOIN (\
               SELECT traversal_path, project_id, name AS branch, max(_version) AS branch_version FROM br \
               WHERE (traversal_path, project_id, name) IN (\
                 SELECT traversal_path, project_id, branch FROM (SELECT traversal_path, project_id, branch FROM cp WHERE _block_number > 17) WHERE 1) AND NOT _deleted \
               GROUP BY traversal_path, project_id, name) AS b \
               ON s.traversal_path = b.traversal_path AND s.project_id = b.project_id AND s.branch = b.branch \
             WHERE b.branch_version >= s.bound"
        );
    }

    #[test]
    fn code_history_covers_only_scopes_with_more_than_one_snapshot() {
        let sql = code_scopes_sql("cp", "br", &multi_snapshot_scopes_sql("v1_gl_file"), None);
        assert!(sql.contains(
            "(SELECT traversal_path, project_id, branch FROM v1_gl_file GROUP BY traversal_path, project_id, branch HAVING uniqExact(_version) > 1) WHERE 1)"
        ));
    }

    #[test]
    fn code_scope_chunks_partition_by_scope_hash() {
        let sql = code_scopes_sql("cp", "br", &changed_scopes_sql("cp", 0), Some((3, 1)));
        assert!(sql.contains(
            "WHERE _block_number > 0) WHERE 1 AND cityHash64(traversal_path, project_id, branch) % 3 = 1)"
        ));
    }

    #[test]
    fn code_snapshot_delete_joins_a_prefiltered_view_of_the_target() {
        let scopes = "SELECT traversal_path, project_id, branch, bound FROM cp";
        let sql = code_snapshot_statement(
            "v1_gl_definition",
            scopes,
            "traversal_path IN ('1/2/', '1/3/')",
            " AND _part NOT IN ('all_1_1_0')",
            60,
        );
        assert_eq!(
            sql,
            "DELETE FROM v1_gl_definition WHERE traversal_path IN ('1/2/', '1/3/') AND (traversal_path, project_id, branch) IN (SELECT traversal_path, project_id, branch FROM (SELECT traversal_path, project_id, branch, bound FROM cp)) \
             AND (traversal_path, project_id, branch, _version) IN (\
               SELECT v.traversal_path, v.project_id, v.branch, v._version FROM (\
                 SELECT DISTINCT traversal_path, project_id, branch, _version FROM v1_gl_definition WHERE traversal_path IN ('1/2/', '1/3/') AND (traversal_path, project_id, branch) IN (SELECT traversal_path, project_id, branch FROM (SELECT traversal_path, project_id, branch, bound FROM cp))) AS v \
               INNER JOIN (SELECT traversal_path, project_id, branch, bound FROM cp) AS c \
                 ON v.traversal_path = c.traversal_path AND v.project_id = c.project_id AND v.branch = c.branch \
               WHERE v._version < c.bound) AND _part NOT IN ('all_1_1_0') \
             SETTINGS lightweight_delete_mode = 'lightweight_update_force', update_sequential_consistency = 0, max_execution_time = 60"
        );
        assert!(!sql.contains("FROM v1_gl_definition AS e"));
    }

    #[test]
    fn shared_edge_delete_only_covers_single_branch_paths() {
        let sql = shared_edge_snapshot_statement(
            "v1_gl_edge",
            "v1_code_indexing_checkpoint",
            "SELECT 1",
            "traversal_path IN ('1/2/')",
            "",
            60,
        );
        assert!(sql.starts_with(
            "DELETE FROM v1_gl_edge WHERE traversal_path IN ('1/2/') AND traversal_path IN (SELECT"
        ));
        assert!(sql.contains(
            "SELECT DISTINCT traversal_path, _version FROM v1_gl_edge WHERE traversal_path IN ('1/2/') AND traversal_path IN (SELECT"
        ));
        assert!(sql.contains("GROUP BY traversal_path HAVING count() = 1"));
        assert!(
            sql.contains("source_kind IN ('Directory', 'File', 'Definition', 'ImportedSymbol')")
        );
        assert!(
            sql.contains(") AS v INNER JOIN (")
                && sql.contains("WHERE v._version < c.bound) SETTINGS")
        );
        assert!(!sql.contains("FROM v1_gl_edge AS e"));
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
