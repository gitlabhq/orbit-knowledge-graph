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
// Two sub-rows: Code volume on top, SDLC volume below. Four wide stat
// tiles per row (w=6, h=5). Each tile is clickable and opens the
// underlying rate(...) query in Grafana Explore over the same time
// range, so a stat that catches your eye drills into a per-label trend
// in one click.
local volume = [
  o.row('Volume in window — code indexing'),
  o.counterRangeStat(
    'gkg_indexer_code_repository_indexing_completed_total',
    'Projects indexed',
    'Successful repository indexing runs in the dashboard window. Click to drill into the per-outcome rate in Explore.',
    DS, SEL, 'outcome="indexed"', 'short', 6, 5,
  ),
  o.counterRangeStat(
    'gkg_indexer_code_events_processed_total',
    'Push events',
    'Push events processed by the code indexing handler in the dashboard window. Click to drill into Explore.',
    DS, SEL, 'outcome="indexed"', 'short', 6, 5,
  ),
  o.counterRangeStat(
    'gkg_indexer_code_files_processed_total',
    'Files parsed',
    'Source files seen by the code-graph indexer in the dashboard window. Click to drill into Explore.',
    DS, SEL, 'outcome="parsed"', 'short', 6, 5,
  ),
  o.counterRangeStat(
    'gkg_indexer_code_nodes_indexed_total',
    'Nodes and edges',
    'Graph nodes and edges indexed by the code handler in the dashboard window. Click to drill into the per-kind rate in Explore.',
    DS, SEL, '', 'short', 6, 5,
  ),
  o.row('Volume in window — SDLC indexing'),
  o.counterRangeStat(
    'gkg_indexer_sdlc_pipeline_rows_processed_total',
    'Rows ingested',
    'Rows extracted and written by SDLC pipelines in the dashboard window. Click to drill into the per-entity rate in Explore.',
    DS, SEL, '', 'short', 6, 5,
  ),
  o.counterRangeStat(
    'gkg_indexer_sdlc_datalake_query_bytes_total',
    'Bytes from datalake',
    'Bytes returned by ClickHouse datalake extraction queries in the dashboard window. Click to drill into Explore.',
    DS, SEL, '', 'bytes', 6, 5,
  ),
  o.counterRangeStat(
    'gkg_indexer_sdlc_pipeline_duration_seconds_count',
    'Pipeline runs',
    'Total SDLC pipeline runs across all entities in the dashboard window. Click to drill into Explore.',
    DS, SEL, '', 'short', 6, 5,
  ),
  o.counterRangeStat(
    'gkg_indexer_sdlc_pipeline_errors_total',
    'Pipeline errors',
    'Total SDLC pipeline failures in the dashboard window. Click to drill into the per-entity error rate in Explore.',
    DS, SEL, '', 'short', 6, 5,
  ),
];

// 3. Throughput over time -------------------------------------------------
// Each bar represents the count over one auto-sized window (Grafana's
// `$__rate_interval`, ~2 to 4 minutes for a 3h time picker). The bar
// envelope is total throughput, the colors are the per-label mix.
local throughput = [
  o.row('Throughput over time'),
  o.counterIncreaseBars(
    codeCompleted,
    'Code: projects indexed over time',
    'Repository indexing runs, stacked by outcome.',
    DS, SEL, by=['outcome'], unit='short', w=12,
  ),
  o.counterIncreaseBars(
    sdlcRows,
    'SDLC: rows ingested over time',
    'SDLC pipeline rows processed, stacked by entity.',
    DS, SEL, by=['entity'], unit='short', w=12,
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
    'SDLC: pipeline duration (p50/p95/p99)',
    'SDLC pipeline duration percentiles aggregated across entities. Drop into the top-N table below to find the entity driving p95.',
    DS, SEL, w=12,
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
    'Code: errors by pipeline stage (1h windows)',
    'Code indexing error counts in rolling 1h windows, stacked by stage. Falls back to a flat zero line during error-free windows so the panel never goes to "No data".',
    DS, SEL, by=['stage'], unit='short', w=12, range='1h', or_zero=true,
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
    DS, SEL, by=['reason'], unit='short', w=12,
  ),
  o.counterIncreaseBars(
    sdlcErrors,
    'SDLC: errors by kind (1h windows)',
    'SDLC pipeline errors counted in rolling 1h windows, stacked by error_kind. Falls back to a flat zero line during error-free windows so the panel never goes to "No data".',
    DS, SEL, by=['error_kind'], unit='short', w=12, range='1h', or_zero=true,
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
