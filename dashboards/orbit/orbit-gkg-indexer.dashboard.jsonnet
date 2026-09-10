// Orbit — GKG indexer.
//
// Layout is story-shaped, not catalog-shaped:
//   1. Health — four headline ratios for at-a-glance status.
//   2. Volume — "how many X in the dashboard window?" stat tiles,
//               code first then SDLC.
//   3. Throughput — SDLC rows-per-window as a smoothed line on the
//               left, code projects-per-window as stacked bars on the
//               right.
//   4. Latency — heatmaps for the histograms, plus a top-N entity table.
//   5. Reliability — error ratios and stage/kind breakdowns.
//   6. Freshness and saturation — watermark lag per entity, ETL permits.
//   7. Fleet — replica counts per indexer pool kind.
//   8. Resources — CPU, memory, FS I/O, pressure, OOM, restarts, reduced to
//               avg and max per pool kind. Per-pod detail is a collapsed row.
//   9. Schema migration — coverage and phase activity.
//  10. Reference — every metric in every indexer domain, collapsed by
//                  default. Kept as the deep-debug fallback.

local o = import 'lib/orbit.libsonnet';

local DS = 'ORBIT_DS';
local SEL = o.GKG_IDX_SEL;

// Floor for the per-bucket `increase()` windows on event-count bars. Pods are
// scraped every 15s, so 2m buckets are wide enough that extrapolation stays
// close to whole events, and narrow enough that a burst still reads as a spike.
local BUCKET = '2m';

// Catalog lookups. Build aborts if any prom name drifts away from the
// catalog so panel queries stay aligned with what the service emits.
local codeCompleted = o.metric('gkg_indexer_code_repository_indexing_completed_total');
local codeEmpty = o.metric('gkg_indexer_code_repository_empty_total');
local codeErrors = o.metric('gkg_indexer_code_errors_total');
local codeFileFaults = o.metric('gkg_indexer_code_file_faults_total');
local codeIndexDur = o.metric('gkg_indexer_code_indexing_duration_seconds');
local codeFetchDur = o.metric('gkg_indexer_code_repository_fetch_duration_seconds');

local sdlcRows = o.metric('gkg_indexer_sdlc_pipeline_rows_processed_total');
local sdlcErrors = o.metric('gkg_indexer_sdlc_pipeline_errors_total');
local sdlcPipelineDur = o.metric('gkg_indexer_sdlc_pipeline_duration_seconds');

// 1. Health ---------------------------------------------------------------
local health = [
  o.row('Health'),
  o.gaugeStat(
    'Code: success rate (1h)',
    'Share of repository indexing runs that ended in outcome=indexed over the last hour. Renders 0 when no indexing runs happened in the window.',
    '(sum(rate(gkg_indexer_code_repository_indexing_completed_total{%s, outcome="indexed"}[1h]))) / (sum(rate(gkg_indexer_code_repository_indexing_completed_total{%s}[1h])) > 0)' % [SEL, SEL],
    DS, 'percentunit', 6,
  ),
  o.gaugeStat(
    'SDLC: error rate (1h)',
    'SDLC pipeline errors divided by rows processed, last hour. Lower is better.',
    '(sum(rate(gkg_indexer_sdlc_pipeline_errors_total{%s}[1h]))) / (sum(rate(gkg_indexer_sdlc_pipeline_rows_processed_total{%s}[1h])) > 0)' % [SEL, SEL],
    DS, 'percentunit', 6,
  ),
  o.gaugeStat(
    'SDLC: max watermark lag',
    'Highest current watermark-to-now lag across all SDLC entities. Rising means SDLC is falling behind.',
    'max(gkg_indexer_sdlc_watermark_lag_seconds{%s})' % [SEL],
    DS, 's', 6,
  ),
  o.gaugeStat(
    'Indexer: message p95 (5m)',
    'p95 of end-to-end NATS message dispatch duration over the last 5 minutes.',
    'histogram_quantile(0.95, sum by (le) (rate(gkg_etl_message_duration_seconds_bucket{%s}[5m])))' % [SEL],
    DS, 's', 6,
  ),
];

// 2. Volume in window -----------------------------------------------------
// Each tile is a stacked pair: a stat header (h=3) showing the count
// over $__range and a thin timeseries strip below (h=2) showing the
// rate over time. Hover the strip for exact values, click the arrow in
// the header to drill the metric into Grafana Explore.
local volume = [
  o.row('Volume in window — code indexing'),
] + o.volumeTiles([
  {
    prom: 'gkg_indexer_code_repository_indexing_completed_total',
    title: 'Projects indexed',
    desc: 'Successful repository indexing runs in the dashboard window. Click the arrow to open the per-outcome rate in Explore; hover the strip below for the rate at a point in time.',
    filter: 'outcome="indexed"',
  },
  {
    prom: 'gkg_indexer_code_events_processed_total',
    title: 'Push events',
    desc: 'Push events processed by the code indexing handler in the dashboard window.',
    filter: 'outcome="indexed"',
  },
  {
    prom: 'gkg_indexer_code_files_processed_total',
    title: 'Files parsed',
    desc: 'Source files seen by the code-graph indexer in the dashboard window.',
    filter: 'outcome="parsed"',
  },
  {
    prom: 'gkg_indexer_code_nodes_indexed_total',
    title: 'Nodes and edges',
    desc: 'Graph nodes and edges indexed by the code handler in the dashboard window.',
  },
], DS, SEL, w=6) + [
  o.row('Volume in window — SDLC indexing'),
] + o.volumeTiles([
  {
    prom: 'gkg_indexer_sdlc_pipeline_rows_processed_total',
    title: 'Rows ingested',
    desc: 'Rows extracted and written by SDLC pipelines in the dashboard window.',
  },
  {
    prom: 'gkg_indexer_sdlc_datalake_query_bytes_total',
    title: 'Bytes from datalake',
    desc: 'Bytes returned by ClickHouse datalake extraction queries in the dashboard window.',
    unit: 'bytes',
  },
  {
    prom: 'gkg_indexer_sdlc_pipeline_duration_seconds_count',
    title: 'Pipeline runs',
    desc: 'Total SDLC pipeline runs across all entities in the dashboard window.',
  },
  {
    prom: 'gkg_indexer_sdlc_pipeline_errors_total',
    title: 'Pipeline errors',
    desc: 'Total SDLC pipeline failures in the dashboard window.',
  },
], DS, SEL, w=6);

// 3. Throughput over time -------------------------------------------------
// SDLC sits on the left because it carries most of the volume in prod.
// Each data point is a count over one Grafana auto-window
// (`$__rate_interval`, ~2 to 4 minutes for a 3h time picker). SDLC
// renders as a smoothed line per entity; code renders as stacked bars
// per outcome since the volume there is sparse and bars read better.
local throughput = [
  o.row('Throughput over time'),
  o.counterIncreaseBars(
    sdlcRows,
    'SDLC: rows ingested over time',
    'SDLC pipeline rows processed per bucket, drawn as a smoothed trend line per entity.',
    DS, SEL, by=['entity'], unit='short', w=12, draw='line', stack=false, range='$__interval', minInterval=BUCKET,
  ),
  o.counterIncreaseBars(
    codeCompleted,
    'Code: projects indexed over time',
    'Repository indexing runs per bucket, stacked by outcome.',
    DS, SEL, by=['outcome'], unit='short', w=12, range='$__interval', minInterval=BUCKET, round=true,
  ),
];

// 4. Latency --------------------------------------------------------------
// Three p50/p95/p99 line panels (one per pipeline stage that matters)
// plus a top-10 entity table for SDLC. Heatmap variants are still
// available via o.histogramHeatmap if anyone wants to opt back in.
local latency = [
  o.row('Latency'),
  o.histogramPercentiles(
    codeIndexDur,
    'Code: time to index a project (p50/p95/p99)',
    'Code-graph parse and analysis duration percentiles. Watch p95 climbing without p50 moving for a long-tail bottleneck.',
    DS, SEL, w=12,
  ),
  o.histogramPercentiles(
    sdlcPipelineDur,
    'SDLC: pipeline duration by entity (p50/p95/p99)',
    'SDLC pipeline duration percentiles, broken down by entity. The histogram does not carry a GitLab-namespace label today, so entity is the closest available dimension. Use the legend filter to isolate one entity if the panel gets busy.',
    DS, SEL, by=['entity'], w=12,
  ),
  o.histogramPercentiles(
    codeFetchDur,
    'Code: Gitaly fetch duration (p50/p95/p99)',
    'Time downloading a repository archive from Gitaly. A slow tail here often explains slow code indexing.',
    DS, SEL, w=12,
  ),
  o.histogramTopN(
    sdlcPipelineDur,
    'SDLC: top 10 slowest entities by p95',
    'Per-entity p50, p95, and p99 over the dashboard time range. Sorted by p95 descending.',
    DS, SEL, byLabel='entity', n=10, w=12,
  ),
];

// 5. Reliability ----------------------------------------------------------
local reliability = [
  o.row('Reliability'),
  o.counterIncreaseBars(
    codeErrors,
    'Code: task-level errors by pipeline stage',
    'Task-level code indexing failures, stacked by stage. Each bar counts the failures inside its own bucket, so the shape is where errors happened rather than a trailing total. Increments only when a code indexing task ends with a fatal pipeline error (sink write, thread pool, sentinel, internal panic). Per-file failures are charted in `Code: per-file faults by kind` below.',
    DS, SEL, by=['stage'], unit='short', w=12, range='$__interval', minInterval=BUCKET, or_zero=true, round=true,
  ),
  o.counterIncreaseBars(
    codeFileFaults,
    'Code: per-file faults by kind',
    'Per-file failures during code indexing, stacked by kind. The task itself completes; individual files were excluded from the graph. Compare against the task-level errors panel above.',
    DS, SEL, by=['kind'], unit='short', w=12, range='$__interval', minInterval=BUCKET, or_zero=true, round=true,
  ),
  o.ratioPanel(
    'SDLC: error rate by entity (1h window)',
    'SDLC pipeline errors over rows processed, per entity. The 1h rate window is wide enough that sporadic errors still register; a tighter window goes to "No data" between bursts.',
    'gkg_indexer_sdlc_pipeline_errors_total',
    'gkg_indexer_sdlc_pipeline_rows_processed_total',
    DS, SEL, by=['entity'], range='1h', w=12,
  ),
  o.counterIncreaseBars(
    codeEmpty,
    'Code: empty-repo short-circuits',
    'Projects skipped at fetch time because the repository was terminal-empty. Often the explanation for "missing" projects.',
    DS, SEL, by=['reason'], unit='short', w=12, range='$__interval', minInterval=BUCKET, round=true,
  ),
  o.counterIncreaseBars(
    sdlcErrors,
    'SDLC: errors by kind',
    'SDLC pipeline errors, stacked by error_kind. Falls back to a flat zero line during error-free buckets so the panel never goes to "No data".',
    DS, SEL, by=['error_kind'], unit='short', w=12, range='$__interval', minInterval=BUCKET, or_zero=true, round=true,
  ),
];

// 6. Freshness and saturation --------------------------------------------
local freshness = [
  o.row('Freshness and saturation'),
  o.timeseries(
    'SDLC: watermark lag per entity',
    'Seconds between the per-entity SDLC watermark and now, worst replica per entity. Rising lag means SDLC is falling behind on that entity.',
    [o.target('max by (entity) (gkg_indexer_sdlc_watermark_lag_seconds{%s})' % [SEL], '{{entity}}', DS)],
    's', 12, 8,
  ),
  o.timeseries(
    'ETL: worker permits in flight',
    'Active worker permits by kind. Watch the global pool flatlining at the configured ceiling, that is the bottleneck signal.',
    [o.target('sum by (permit_kind) (gkg_etl_permits_active{%s})' % [SEL], '{{permit_kind}}', DS)],
    'short', 12, 8,
  ),
];

// 7. Fleet ---------------------------------------------------------------
// The chart can deploy the indexer as split `code` and `sdlc` pools or as a
// single `universal` pool. Both topologies get a tile; the one that is not
// deployed renders the stat panel's no-value dash rather than a misleading
// zero.
local fleet = [
  o.row('Fleet'),
  o.idxReplicaStat(
    'code',
    'Code indexer replicas',
    'Indexer pods registering only the code module. Dash means no code-only pool is deployed — check the universal tile.',
    DS, 8,
  ),
  o.idxReplicaStat(
    'sdlc',
    'SDLC indexer replicas',
    'Indexer pods registering the SDLC (and namespace-deletion) modules. Dash means no SDLC-only pool is deployed — check the universal tile.',
    DS, 8,
  ),
  o.idxReplicaStat(
    'universal',
    'Universal indexer replicas',
    'Indexer pods registering every engine module in one deployment. Dash is the expected reading on GitLab.com, which runs split code and SDLC pools.',
    DS, 8,
  ),
  o.timeseries(
    'Replicas over time by pool kind',
    'Scraped indexer pods per pool kind. Steps here are KEDA scaling events; a flat ceiling during a queue backlog means the pool is at its maxReplicaCount.',
    [o.target(
      'count by (%s) (up{%s})' % [o.IDX_KIND, SEL],
      '{{%s}}' % o.IDX_KIND,
      DS,
    )],
    'short', 24, 7,
  ),
];

// 8. Resources -----------------------------------------------------------
// Code and SDLC pools have different shapes (code is CPU- and scratch-bound,
// SDLC is memory- and datalake-bound) and different limits, so a single
// series per pod across both reads as noise. Every panel here reduces to avg
// and max per pool kind instead: avg is the pool's steady state, max is the
// hot replica that a per-pod breakdown used to surface. Per-pod detail is
// still available in the collapsed row underneath.
//
// cAdvisor and kube-state-metrics carry no pool label, so `o.idxKind*`
// helpers join each series onto the app scrape target by pod identity.
local cpuUsage = 'rate(container_cpu_usage_seconds_total{%s, cpu="total"}[5m])' % [SEL];
local cpuLimit = 'kube_pod_container_resource_limits{%s, resource="cpu"}' % [SEL];
local memLimit = 'kube_pod_container_resource_limits{%s, resource="memory"}' % [SEL];
local joinOn = 'on (cluster, namespace, pod)';
local cpuRatio = '(%s) / %s (%s)' % [cpuUsage, joinOn, cpuLimit];
local memRatio = '(container_memory_working_set_bytes{%s}) / %s (%s)' % [SEL, joinOn, memLimit];

local resources = [
  o.row('Resources'),
  o.idxKindStat(
    'CPU: peak usage / limit (5m)',
    'Worst replica in each pool, as a fraction of its own CPU limit. Reads per kind because code and SDLC pools are sized differently; 1.0 means that replica is consuming its whole quota.',
    cpuRatio, 'max', DS, 'percentunit', 6,
  ),
  o.idxKindStat(
    'Memory: peak working set / limit',
    'Worst replica in each pool, as a fraction of its own memory limit. Sustained values above ~0.85 are OOM-kill territory.',
    memRatio, 'max', DS, 'percentunit', 6,
  ),
  o.idxKindStat(
    'OOM events (1h)',
    'OOM-killer events in the last hour, summed per pool kind. Any non-zero value warrants a look at the memory panels below.',
    'increase(container_oom_events_total{%s}[1h])' % [SEL], 'sum', DS, 'short', 6,
  ),
  o.idxKindStat(
    'Restarts (1h)',
    'Container restarts in the last hour, summed per pool kind. Crash-loops show up here before they show up in the app metrics.',
    'increase(kube_pod_container_status_restarts_total{%s}[1h])' % [SEL], 'sum', DS, 'short', 6,
  ),
  o.idxKindAvgMax(
    'CPU: cores used',
    'CPU seconds consumed per second. avg is the pool steady state, max is its busiest replica, and the dashed line is that pool own CPU limit. A replica riding its limit is throttled, not evicted — check the throttling panel next to this one.',
    cpuUsage, DS, 'short', 12, 8, limit_expr=cpuLimit,
  ),
  o.idxKindAvgMax(
    'CPU: throttled time fraction',
    'Share of CFS periods where the cgroup was throttled. Anything sustained above ~5% on max means at least one replica is hitting its CPU quota and pipelines will queue behind it.',
    '(rate(container_cpu_cfs_throttled_periods_total{%s}[5m])) / (rate(container_cpu_cfs_periods_total{%s}[5m]) > 0)' % [SEL, SEL],
    DS, 'percentunit', 12, 8,
  ),
  o.idxKindAvgMax(
    'Memory: working set',
    'container_memory_working_set_bytes per replica, reduced per pool kind. Working set is what the OOM killer reads, not RSS, and the dashed line is that pool own memory limit. Code and SDLC pools are sized differently, so each kind gets its own ceiling; max approaching its dashed line is the eviction warning.',
    'container_memory_working_set_bytes{%s}' % [SEL], DS, 'bytes', 12, 8, limit_expr=memLimit,
  ),
  o.idxKindPanel(
    'Memory: RSS and page cache',
    'Resident set and page cache per pool kind. RSS dominating with a tiny cache usually means heap growth; cache dominating on the code pool typically means Gitaly archive I/O.',
    [
      { expr: 'container_memory_rss{%s}' % [SEL], calc: 'avg', legend: 'rss avg' },
      { expr: 'container_memory_rss{%s}' % [SEL], calc: 'max', legend: 'rss max' },
      { expr: 'container_memory_cache{%s}' % [SEL], calc: 'avg', legend: 'cache avg' },
      { expr: 'container_memory_cache{%s}' % [SEL], calc: 'max', legend: 'cache max' },
    ],
    DS, 'bytes', 12, 8,
  ),
  o.idxKindPanel(
    'Filesystem I/O bytes',
    'Read and write throughput from cAdvisor, per pool kind. Sustained writes on the code pool are archive extraction under /tmp; the SDLC pool writes nothing to disk, so a signal there is worth investigating.',
    [
      { expr: 'rate(container_fs_reads_bytes_total{%s}[5m])' % [SEL], calc: 'avg', legend: 'read avg' },
      { expr: 'rate(container_fs_reads_bytes_total{%s}[5m])' % [SEL], calc: 'max', legend: 'read max' },
      { expr: 'rate(container_fs_writes_bytes_total{%s}[5m])' % [SEL], calc: 'avg', legend: 'write avg' },
      { expr: 'rate(container_fs_writes_bytes_total{%s}[5m])' % [SEL], calc: 'max', legend: 'write max' },
    ],
    DS, 'Bps', 12, 8,
  ),
  o.idxKindPanel(
    'Pressure stall: IO and memory',
    'PSI seconds per second, per pool kind. IO pressure rising means processes are waiting on disk; memory pressure rising means the kernel is reclaiming pages, often the leading edge of an OOM.',
    [
      { expr: 'rate(container_pressure_io_waiting_seconds_total{%s}[5m])' % [SEL], calc: 'avg', legend: 'io avg' },
      { expr: 'rate(container_pressure_io_waiting_seconds_total{%s}[5m])' % [SEL], calc: 'max', legend: 'io max' },
      { expr: 'rate(container_pressure_memory_waiting_seconds_total{%s}[5m])' % [SEL], calc: 'avg', legend: 'mem avg' },
      { expr: 'rate(container_pressure_memory_waiting_seconds_total{%s}[5m])' % [SEL], calc: 'max', legend: 'mem max' },
    ],
    DS, 's', 12, 8,
  ),
  o.idxKindPanel(
    'Threads and sockets',
    'OS-level concurrency counters per pool kind. Threads climbing without a matching workload increase usually points at tokio blocking-pool growth; sockets climbing flags a NATS or ClickHouse connection leak.',
    [
      { expr: 'container_threads{%s}' % [SEL], calc: 'avg', legend: 'threads avg' },
      { expr: 'container_threads{%s}' % [SEL], calc: 'max', legend: 'threads max' },
      { expr: 'container_sockets{%s}' % [SEL], calc: 'avg', legend: 'sockets avg' },
      { expr: 'container_sockets{%s}' % [SEL], calc: 'max', legend: 'sockets max' },
    ],
    DS, 'short', 12, 8,
  ),
];

// Per-pod detail, collapsed. The avg/max panels above answer "is this pool
// healthy"; these answer "which replica is the outlier".
local resourcesPerPod = [
  o.rowCollapsed('Resources per pod (reference)'),
  o.timeseries(
    'CPU: cores used per pod',
    'Per-pod CPU seconds consumed per second.',
    [o.target('sum by (pod) (%s)' % [cpuUsage], '{{pod}}', DS)],
    'short', 12, 8,
  ),
  o.timeseries(
    'CPU: throttled time fraction per pod',
    'Share of CFS periods where the cgroup was throttled, per pod.',
    [o.target(
      '(sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{%s}[5m]))) / (sum by (pod) (rate(container_cpu_cfs_periods_total{%s}[5m])) > 0)' % [SEL, SEL],
      '{{pod}}', DS,
    )],
    'percentunit', 12, 8,
  ),
  o.timeseries(
    'Memory: working set per pod',
    'container_memory_working_set_bytes per replica, against that replica own limit.',
    [
      o.target('sum by (pod) (container_memory_working_set_bytes{%s})' % [SEL], '{{pod}}', DS, 'A'),
      o.target('sum by (pod) (%s)' % [memLimit], 'limit / {{pod}}', DS, 'B'),
    ],
    'bytes', 12, 8,
  ),
  o.timeseries(
    'Memory: RSS and cache per pod',
    'Resident set and page cache, per pod.',
    [
      o.target('sum by (pod) (container_memory_rss{%s})' % [SEL], 'rss / {{pod}}', DS, 'A'),
      o.target('sum by (pod) (container_memory_cache{%s})' % [SEL], 'cache / {{pod}}', DS, 'B'),
    ],
    'bytes', 12, 8,
  ),
  o.timeseries(
    'Filesystem I/O bytes per pod',
    'Read and write throughput from cAdvisor, per pod.',
    [
      o.target('sum by (pod) (rate(container_fs_reads_bytes_total{%s}[5m]))' % [SEL], 'read / {{pod}}', DS, 'A'),
      o.target('sum by (pod) (rate(container_fs_writes_bytes_total{%s}[5m]))' % [SEL], 'write / {{pod}}', DS, 'B'),
    ],
    'Bps', 12, 8,
  ),
  o.timeseries(
    'Pressure stall per pod: IO and memory',
    'PSI seconds per second, per pod.',
    [
      o.target('sum by (pod) (rate(container_pressure_io_waiting_seconds_total{%s}[5m]))' % [SEL], 'io / {{pod}}', DS, 'A'),
      o.target('sum by (pod) (rate(container_pressure_memory_waiting_seconds_total{%s}[5m]))' % [SEL], 'mem / {{pod}}', DS, 'B'),
    ],
    's', 12, 8,
  ),
  o.timeseries(
    'Threads and sockets per pod',
    'OS-level concurrency counters, per pod.',
    [
      o.target('sum by (pod) (container_threads{%s})' % [SEL], 'threads / {{pod}}', DS, 'A'),
      o.target('sum by (pod) (container_sockets{%s})' % [SEL], 'sockets / {{pod}}', DS, 'B'),
    ],
    'short', 12, 8,
  ),
];

// 9. Schema migration ----------------------------------------------------
// Schema migration is orchestrated by the dispatcher (DispatchIndexing
// mode), so every series here is filtered to the gkg-dispatcher container,
// not the indexer.
local MIG_SEL = o.GKG_DSP_SEL;
local migration = [
  o.row('Schema migration'),
  o.timeseries(
    'Migration: indexed / eligible coverage',
    'Per-scope coverage of the migrating schema version. SDLC reaching 100% triggers promotion. Code coverage is informational.',
    [o.target(
      '(sum by (scope) (gkg_schema_indexed_units{%s})) / (sum by (scope) (gkg_schema_eligible_units{%s}) > 0)' % [MIG_SEL, MIG_SEL],
      '{{scope}}',
      DS,
    )],
    'percentunit', 12, 8,
  ),
  o.timeseries(
    'Migration: migrating-version age',
    'Wall-clock seconds since the current migrating version was marked. Flat zero when no migration is active.',
    [o.target('gkg_schema_migrating_age_seconds{%s}' % [MIG_SEL], 'age', DS)],
    's', 12, 8,
  ),
]
+ o.counterPanels(o.metric('gkg_schema_migration_phase_total'), DS, MIG_SEL)
+ o.counterPanels(o.metric('gkg_schema_migration_completed_total'), DS, MIG_SEL)
+ o.counterPanels(o.metric('gkg_schema_cleanup_total'), DS, MIG_SEL);

// 10. Reference (collapsed by default) ------------------------------------
// Scheduler metrics used by the data deletion panels below.
local schedulerDur = o.metric('gkg_scheduler_task_duration_seconds');
local schedulerRuns = o.metric('gkg_scheduler_task_runs_total');
local schedulerErrors = o.metric('gkg_scheduler_task_errors_total');

local deletionTableDur = o.metric('gkg_indexer_namespace_deletion_table_duration_seconds');
local deletionTableErrors = o.metric('gkg_indexer_namespace_deletion_table_errors_total');

local DSP_SEL = o.GKG_DSP_SEL;
local NS_DEL_SEL = DSP_SEL + ', task="dispatch.namespace_deletion"';
local CLEANUP_SEL = DSP_SEL + ', task="maintenance.table_cleanup"';

local dataDeletionPanels = [
  // Namespace deletion
  o.histogramPercentiles(
    deletionTableDur,
    'Namespace deletion: per-table soft-delete duration (p50/p95/p99)',
    'Duration of the INSERT-SELECT that soft-deletes rows for a single table during namespace deletion.',
    DS, SEL, w=12,
  ),
  o.counterIncreaseBars(
    deletionTableErrors,
    'Namespace deletion: errors',
    'Table deletion failures during namespace deletion. Per-table detail is in the logs.',
    DS, SEL, unit='short', w=12, range='$__interval', minInterval=BUCKET, or_zero=true, round=true,
  ),
  o.counterIncreaseBars(
    schedulerRuns,
    'Namespace deletion: scheduler runs by outcome',
    'Namespace deletion scheduler runs, stacked by outcome.',
    DS, NS_DEL_SEL, by=['outcome'], unit='short', w=12, range='$__interval', minInterval=BUCKET, or_zero=true, round=true,
  ),
  // Table cleanup
  o.histogramPercentiles(
    schedulerDur,
    'Table cleanup: run duration (p50/p95/p99)',
    'End-to-end duration of the table cleanup task. Covers all tables in a single run.',
    DS, CLEANUP_SEL, w=12,
  ),
  o.counterIncreaseBars(
    schedulerRuns,
    'Table cleanup: runs by outcome',
    'Table cleanup task runs, stacked by outcome (success/error).',
    DS, CLEANUP_SEL, by=['outcome'], unit='short', w=12, range='$__interval', minInterval=BUCKET, or_zero=true, round=true,
  ),
  o.counterIncreaseBars(
    schedulerErrors,
    'Table cleanup: errors by stage',
    'Table cleanup errors, stacked by stage. Non-zero means at least one table failed OPTIMIZE.',
    DS, CLEANUP_SEL, by=['stage'], unit='short', w=12, range='$__interval', minInterval=BUCKET, or_zero=true, round=true,
  ),
] + std.flattenArrays([o.panelsFor(m, DS, SEL) for m in o.metricsInDomain('indexer.namespace_deletion')]);

local reference =
  o.sectionCollapsed('ETL engine (reference)', o.metricsInDomain('indexer.etl'), DS, SEL)
  + o.sectionCollapsed('Code pipeline (reference)', o.metricsInDomain('indexer.code'), DS, SEL)
  + o.sectionCollapsed('SDLC pipeline (reference)', o.metricsInDomain('indexer.sdlc'), DS, SEL)
  + [o.rowCollapsed('Data deletion')] + dataDeletionPanels
  + o.sectionCollapsed('Scheduler (reference)', o.metricsInDomain('indexer.scheduler'), DS, DSP_SEL);

local items =
  health
  + volume
  + throughput
  + latency
  + reliability
  + freshness
  + fleet
  + resources
  + resourcesPerPod
  + migration
  + reference;

local annotations = [
  o.deployAnnotation(DS, SEL),
];

o.dashboard(
  'orbit-gkg-indexer',
  'Orbit — GKG indexer',
  ['gkg', 'indexer'],
  'GKG indexer dashboard. Top-of-page rows tell the story (health, volume, throughput, latency, reliability, freshness). Bottom rows are the per-domain catalog reference and are collapsed by default.',
  items,
  annotations,
)
