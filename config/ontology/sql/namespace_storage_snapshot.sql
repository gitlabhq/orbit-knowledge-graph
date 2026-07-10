{% for table in graph.tables if table.has_traversal_path and table.kind != "auxiliary" %}
SELECT
    today() AS snapshot_date,
    {{ schema.version }} AS schema_version,
    '{{ table.logical_name }}' AS logical_table,
    top_level_namespace,
    toUInt64(sum(attributed)) AS compressed_bytes
FROM (
  SELECT top_level_namespace,
         data_compressed_bytes * (w_eff / sum(w_eff) OVER (PARTITION BY part_name)) AS attributed
  FROM (
    SELECT part_name, top_level_namespace, data_compressed_bytes,
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
            FROM mergeTreeIndex(currentDatabase(), '{{ table.physical_name }}', with_marks = true)
          )
          WINDOW w AS (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        )
        GROUP BY part_name, top_level_namespace
      ) pp
      JOIN (
        SELECT name, part_type, data_compressed_bytes
        FROM system.parts
        WHERE database = currentDatabase() AND active
          AND table = '{{ table.physical_name }}'
      ) p ON p.name = pp.part_name
    )
  )
)
GROUP BY top_level_namespace
UNION ALL
{% endfor %}
SELECT
    today() AS snapshot_date,
    {{ schema.version }} AS schema_version,
    replaceRegexpOne(table, '^v\\d+_', '') AS logical_table,
    '__global' AS top_level_namespace,
    sum(data_compressed_bytes) AS compressed_bytes
FROM system.parts
WHERE database = currentDatabase() AND active
  AND table IN (
{% for table in graph.tables if table.global %}
    '{{ table.physical_name }}'{% if not loop.last %},{% endif %}
{% endfor %}
  )
GROUP BY table
