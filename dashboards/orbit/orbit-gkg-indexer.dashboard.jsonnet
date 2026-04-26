// Orbit — GKG indexer.
//
// Layout is story-shaped, not catalog-shaped:
//   1. Health — four headline ratios for at-a-glance status.
//   2. Volume — "how many X in the dashboard window?" stat tiles,
//               Code on the left, SDLC on the right.
//   3. Throughput — stacked bars of count-per-bucket via increase().
//   4. Latency — heatmaps for the histograms, plus a top-N entity table.
//   5. Reliability — error ratios and stage/kind breakdowns.
//   6. Freshness and saturation — watermark lag per entity, ETL permits.
//   7. Schema migration — coverage and phase activity.
//   8. Reference — every metric in every indexer domain, collapsed by
//                  default. Kept as the deep-debug fallback.

local o = import 'lib/orbit.libsonnet';

local DS = 'ORBIT_DS';
local SEL = o.GKG_IDX_SEL;

// Catalog lookups. Build aborts if any prom name drifts away from the
// catalog so panel queries stay aligned with what the service emits.
local codeCompleted = o.metric('gkg_indexer_code_repository_indexing_completed_total');
local codeEmpty = o.metric('gkg_indexer_code_repository_empty_total');
local codeErrors = o.metric('gkg_indexer_code_errors_total');
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
    'Share of repository indexing runs that ended in outcome=success over the last hour.',
    '(sum(rate(gkg_indexer_code_repository_indexing_completed_total{%s, outcome="success"}[1h]))) / (sum(rate(gkg_indexer_code_repository_indexing_completed_total{%s}[1h])) > 0)' % [SEL, SEL],
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
// 8 stat tiles. Code on the left (4 tiles), SDLC on the right (4 tiles).
// Each tile uses $__range so the headline number always answers the
// current time picker.
local volume = [
  o.row('Volume in window'),
  // Code column
  o.counterRangeStat(
    'gkg_indexer_code_repository_indexing_completed_total',
    'Code: projects indexed',
    'Successful repository indexing runs in the dashboard window.',
    DS, SEL, 'outcome="indexed"', 'short', 3,
  ),
  o.counterRangeStat(
    'gkg_indexer_code_events_processed_total',
    'Code: push events',
    'Push events processed by the code indexing handler in the dashboard window.',
    DS, SEL, 'outcome="indexed"', 'short', 3,
  ),
  o.counterRangeStat(
    'gkg_indexer_code_files_processed_total',
    'Code: files parsed',
    'Source files seen by the code-graph indexer in the dashboard window.',
    DS, SEL, 'outcome="parsed"', 'short', 3,
  ),
  o.counterRangeStat(
    'gkg_indexer_code_nodes_indexed_total',
    'Code: nodes + edges',
    'Graph nodes and edges indexed by the code handler in the dashboard window.',
    DS, SEL, '', 'short', 3,
  ),
  // SDLC column
  o.counterRangeStat(
    'gkg_indexer_sdlc_pipeline_rows_processed_total',
    'SDLC: rows ingested',
    'Rows extracted and written by SDLC pipelines in the dashboard window.',
    DS, SEL, '', 'short', 3,
  ),
  o.counterRangeStat(
    'gkg_indexer_sdlc_datalake_query_bytes_total',
    'SDLC: bytes from datalake',
    'Bytes returned by ClickHouse datalake extraction queries in the dashboard window.',
    DS, SEL, '', 'bytes', 3,
  ),
  o.counterRangeStat(
    'gkg_indexer_sdlc_pipeline_duration_seconds_count',
    'SDLC: pipeline runs',
    'Total SDLC pipeline runs across all entities in the dashboard window.',
    DS, SEL, '', 'short', 3,
  ),
  o.counterRangeStat(
    'gkg_indexer_sdlc_pipeline_errors_total',
    'SDLC: pipeline errors',
    'Total SDLC pipeline failures in the dashboard window.',
    DS, SEL, '', 'short', 3,
  ),
];

// 3. Throughput over time -------------------------------------------------
local throughput = [
  o.row('Throughput over time'),
  o.counterIncreaseBars(
    codeCompleted,
    'Code: projects indexed per bucket',
    'Repository indexing runs per bucket, stacked by outcome.',
    DS, SEL, by=['outcome'], unit='short', w=12,
  ),
  o.counterIncreaseBars(
    sdlcRows,
    'SDLC: rows ingested per bucket',
    'SDLC pipeline rows processed per bucket, stacked by entity.',
    DS, SEL, by=['entity'], unit='short', w=12,
  ),
];

// 4. Latency --------------------------------------------------------------
local latency = [
  o.row('Latency'),
  o.histogramHeatmap(
    codeIndexDur,
    'Code: time to index a project',
    'Bucket density of code-graph parse and analysis duration. Watch for slow tails forming.',
    DS, SEL, w=12,
  ),
  o.histogramHeatmap(
    sdlcPipelineDur,
    'SDLC: pipeline duration (all entities)',
    'Bucket density of SDLC entity pipeline duration. Heatmap surfaces multimodality the p95 line hides.',
    DS, SEL, w=12,
  ),
  o.histogramHeatmap(
    codeFetchDur,
    'Code: Gitaly fetch duration',
    'Time downloading a repository archive from Gitaly. Slow tail here often explains slow code indexing.',
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
    'Code: errors by pipeline stage',
    'Code indexing error counts per bucket, stacked by stage.',
    DS, SEL, by=['stage'], unit='short', w=12,
  ),
  o.ratioPanel(
    'SDLC: error rate by entity',
    'SDLC pipeline errors over rows processed, per entity.',
    'gkg_indexer_sdlc_pipeline_errors_total',
    'gkg_indexer_sdlc_pipeline_rows_processed_total',
    DS, SEL, by=['entity'], range='5m', w=12,
  ),
  o.counterIncreaseBars(
    codeEmpty,
    'Code: empty-repo short-circuits',
    'Projects skipped at fetch time because the repository was terminal-empty. Often the explanation for "missing" projects.',
    DS, SEL, by=['reason'], unit='short', w=12,
  ),
  o.counterIncreaseBars(
    sdlcErrors,
    'SDLC: errors by kind',
    'SDLC pipeline error counts per bucket, stacked by error_kind.',
    DS, SEL, by=['error_kind'], unit='short', w=12,
  ),
];

// 6. Freshness and saturation --------------------------------------------
local freshness = [
  o.row('Freshness and saturation'),
  o.timeseries(
    'SDLC: watermark lag per entity',
    'Seconds between the per-entity SDLC watermark and now. Rising lag means SDLC is falling behind on that entity.',
    [o.target('sum by (entity) (gkg_indexer_sdlc_watermark_lag_seconds{%s})' % [SEL], '{{entity}}', DS)],
    's', 12, 8,
  ),
  o.timeseries(
    'ETL: worker permits in flight',
    'Active worker permits by kind. Watch the global pool flatlining at the configured ceiling, that is the bottleneck signal.',
    [o.target('sum by (permit_kind) (gkg_etl_permits_active{%s})' % [SEL], '{{permit_kind}}', DS)],
    'short', 12, 8,
  ),
];

// 7. Schema migration ----------------------------------------------------
// Coverage relies on dispatcher-emitted gauges, currently dark in prod
// (issue #524). The phase counter is emitted from indexer-mode startup
// and populates today.
local migration = [
  o.row('Schema migration'),
  o.timeseries(
    'Migration: indexed / eligible coverage',
    'Per-scope coverage of the migrating schema version. SDLC reaching 100% triggers promotion. Code coverage is informational. Currently dark in prod, see #524.',
    [o.target(
      '(sum by (scope) (gkg_schema_indexed_units{%s})) / (sum by (scope) (gkg_schema_eligible_units{%s}) > 0)' % [SEL, SEL],
      '{{scope}}',
      DS,
    )],
    'percentunit', 12, 8,
  ),
  o.timeseries(
    'Migration: migrating-version age',
    'Wall-clock seconds since the current migrating version was marked. Flat zero when no migration is active. Currently dark in prod, see #524.',
    [o.target('gkg_schema_migrating_age_seconds{%s}' % [SEL], 'age', DS)],
    's', 12, 8,
  ),
] + o.counterPanels(o.metric('gkg_schema_migration_phase_total'), DS, SEL);

// 8. Reference (collapsed by default) ------------------------------------
local reference =
  o.sectionCollapsed('ETL engine (reference)', o.metricsInDomain('indexer.etl'), DS, SEL)
  + o.sectionCollapsed('Code pipeline (reference)', o.metricsInDomain('indexer.code'), DS, SEL)
  + o.sectionCollapsed('SDLC pipeline (reference)', o.metricsInDomain('indexer.sdlc'), DS, SEL)
  + o.sectionCollapsed('Namespace deletion (reference)', o.metricsInDomain('indexer.namespace_deletion'), DS, SEL)
  + o.sectionCollapsed('Scheduler (reference)', o.metricsInDomain('indexer.scheduler'), DS, SEL);

local items =
  health
  + volume
  + throughput
  + latency
  + reliability
  + freshness
  + migration
  + reference;

o.dashboard(
  'orbit-gkg-indexer',
  'Orbit — GKG indexer',
  ['gkg', 'indexer'],
  'GKG indexer dashboard. Top-of-page rows tell the story (health, volume, throughput, latency, reliability, freshness). Bottom rows are the per-domain catalog reference and are collapsed by default.',
  items,
)
