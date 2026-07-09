SELECT
    today() AS snapshot_date,
    __ACTIVE_VERSION__ AS schema_version,
    '__LOGICAL__' AS logical_table,
    top_level_namespace,
    toUInt64(sum(attributed)) AS compressed_bytes
FROM (
  SELECT top_level_namespace,
         data_compressed_bytes * (w_eff / sum(w_eff) OVER (PARTITION BY part_name)) AS attributed
  FROM (
    SELECT part_name, top_level_namespace, data_compressed_bytes,
           -- Single-granule parts carry no mark-offset deltas, so their weights
           -- collapse to 0 and fall back to a granule-count share (avoids NULL rows).
           if(max(w_raw) OVER (PARTITION BY part_name) = 0, n_granules, w_raw) AS w_eff
    FROM (
      SELECT pp.part_name AS part_name, pp.top_level_namespace AS top_level_namespace, pp.n_granules AS n_granules,
             p.data_compressed_bytes AS data_compressed_bytes,
             if(p.part_type = 'Wide', pp.w_sum, pp.w_min) AS w_raw
      FROM (
        SELECT part_name, top_level_namespace,
               sum(d_sum) AS w_sum, sum(d_min) AS w_min, toInt64(count()) AS n_granules
        FROM (
          SELECT
            part_name,
            arrayStringConcat(arraySlice(splitByChar('/', traversal_path), 1, 2), '/') AS top_level_namespace,
            greatest(toInt64(leadInFrame(sum_off, 1, sum_off) OVER w) - toInt64(sum_off), 0) AS d_sum,
            greatest(toInt64(leadInFrame(min_off, 1, min_off) OVER w) - toInt64(min_off), 0) AS d_min
          FROM (
            SELECT part_name, mark_number, traversal_path,
                   arraySum(x -> ifNull(x.1, 0), array(COLUMNS('\\.mark$'))) AS sum_off,
                   arrayMin(x -> ifNull(x.1, 0), array(COLUMNS('\\.mark$'))) AS min_off
            FROM mergeTreeIndex(currentDatabase(), '__ACTIVE_PREFIX____LOGICAL__', with_marks = true)
          )
          WINDOW w AS (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        )
        GROUP BY part_name, top_level_namespace
      ) pp
      JOIN (
        SELECT name, part_type, data_compressed_bytes
        FROM system.parts
        WHERE database = currentDatabase() AND active
          AND table = '__ACTIVE_PREFIX____LOGICAL__'
      ) p ON p.name = pp.part_name
    )
  )
)
GROUP BY top_level_namespace