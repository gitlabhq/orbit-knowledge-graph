// Orbit — Overview. Golden signals across the four subsystems plus the
// Orbit folder dropdown for jumping into component dashboards.
local o = import 'lib/orbit.libsonnet';

local pipelineQueries = o.metric('gkg.query.pipeline.queries').prom_name;
local pipelineDuration = o.metric('gkg.query.pipeline.duration').prom_name;
local pipelineSecRej = o.metric('gkg.query.pipeline.error.security_rejected').prom_name;
local etlRows = o.metric('gkg.etl.destination.rows.written').prom_name;
local etlBytes = o.metric('gkg.etl.destination.written').prom_name;
local etlHandlerDur = o.metric('gkg.etl.handler.duration').prom_name;
local etlHandlerErr = o.metric('gkg.etl.handler.errors').prom_name;

local items = [
  o.row('GKG webserver'),
  o.stat('Queries / min', 'Query pipeline calls per minute.',
    o.target('sum(rate(%s{%s}[5m]) * 60)' % [pipelineQueries, o.GKG_WEB_SEL], 'qpm', 'ORBIT_DS'),
    'short', 6),
  o.stat('Success rate', 'Share of query pipeline calls that returned status="ok".',
    o.target(
      'sum(rate(%s{%s, status="ok"}[5m])) / clamp_min(sum(rate(%s{%s}[5m])), 1)' %
        [pipelineQueries, o.GKG_WEB_SEL, pipelineQueries, o.GKG_WEB_SEL],
      'ok', 'ORBIT_DS'),
    'percentunit', 6),
  o.stat('Pipeline p95 (s)', '95th percentile of overall query pipeline latency.',
    o.target(o.histogramQuantileExpr(pipelineDuration, 0.95, o.GKG_WEB_SEL, '5m', []), 'p95', 'ORBIT_DS'),
    's', 6),
  o.stat('Security rejects / min', 'Pipeline calls rejected for security policy violations.',
    o.target('sum(rate(%s{%s}[5m]) * 60)' % [pipelineSecRej, o.GKG_WEB_SEL], 'rejects/min', 'ORBIT_DS'),
    'short', 6),

  o.row('GKG indexer'),
  o.stat('Rows indexed / min', 'Total rows written to ClickHouse per minute.',
    o.target('sum(rate(%s{%s}[5m]) * 60)' % [etlRows, o.GKG_IDX_SEL], 'rows/min', 'ORBIT_DS'),
    'short', 6),
  o.stat('Bytes indexed / s', 'ETL destination write throughput.',
    o.target('sum(rate(%s{%s}[5m]))' % [etlBytes, o.GKG_IDX_SEL], 'B/s', 'ORBIT_DS'),
    'Bps', 6),
  o.stat('Handler p95 (s)', '95th percentile of per-handler ETL execution time.',
    o.target(o.histogramQuantileExpr(etlHandlerDur, 0.95, o.GKG_IDX_SEL, '5m', []), 'p95', 'ORBIT_DS'),
    's', 6),
  o.stat('ETL errors / min', 'ETL handler errors per minute.',
    o.target('sum(rate(%s{%s}[5m]) * 60)' % [etlHandlerErr, o.GKG_IDX_SEL], 'errors/min', 'ORBIT_DS'),
    'short', 6),

  o.row('Siphon'),
  o.stat('Producer ops / s', 'Siphon CDC operations emitted per second.',
    o.target('sum(rate(siphon_operations_total{%s}[5m]))' % o.SIPHON_SEL, 'ops/s', 'ORBIT_DS'),
    'short', 8),
  o.stat('Consumer events / s', 'Events written into ClickHouse per second.',
    o.target('sum(rate(siphon_clickhouse_consumer_number_of_events{%s}[5m]))' % o.SIPHON_SEL, 'events/s', 'ORBIT_DS'),
    'short', 8),
  o.stat('Producer apps (distinct)', 'Distinct apps producing into Siphon.',
    o.target('count(count by (app_id) (siphon_operations_total{%s}))' % o.SIPHON_SEL, 'apps', 'ORBIT_DS'),
    'short', 8),

  o.row('Rails → KG'),
  o.stat('gRPC calls / min', 'Rails → GKG gRPC requests per minute.',
    o.target('sum(rate(gitlab_knowledge_graph_grpc_duration_seconds_count{%s}[5m]) * 60)' % o.RAILS_SEL, 'calls/min', 'RAILS_DS'),
    'short', 8),
  o.stat('gRPC errors / min', 'Rails-side gRPC errors per minute.',
    o.target('sum(rate(gitlab_knowledge_graph_grpc_errors_total{%s}[5m]) * 60)' % o.RAILS_SEL, 'errors/min', 'RAILS_DS'),
    'short', 8),
  o.stat('gRPC p95 (ms)', '95th percentile of Rails-side gRPC latency.',
    o.target('1000 * histogram_quantile(0.95, sum by (le) (rate(gitlab_knowledge_graph_grpc_duration_seconds_bucket{%s}[5m])))' % o.RAILS_SEL, 'p95 ms', 'RAILS_DS'),
    'ms', 8),
];

o.dashboard(
  'orbit-overview',
  'Orbit — Overview',
  ['overview'],
  'Golden signals for the Orbit stack. Use the sub-dashboards for component-level metrics.',
  items,
)
