// Orbit — GKG indexer. ETL engine, code pipeline, SDLC pipeline,
// namespace deletion, scheduler, schema migration.
local o = import 'lib/orbit.libsonnet';

local etlRows = o.metric('gkg.etl.destination.rows.written').prom_name;
local etlBytes = o.metric('gkg.etl.destination.written').prom_name;
local etlHandlerDur = o.metric('gkg.etl.handler.duration').prom_name;
local etlHandlerErr = o.metric('gkg.etl.handler.errors').prom_name;

local items =
  [
    o.row('Overview'),
    o.stat('Rows indexed / min', 'Total rows written to ClickHouse per minute.',
      o.target('sum(rate(%s{%s}[5m]) * 60)' % [etlRows, o.GKG_IDX_SEL], 'rows/min', 'ORBIT_DS'),
      'short', 6),
    o.stat('Bytes indexed / s', 'ETL destination write throughput.',
      o.target('sum(rate(%s{%s}[5m]))' % [etlBytes, o.GKG_IDX_SEL], 'B/s', 'ORBIT_DS'),
      'Bps', 6),
    o.stat('Handler p95 (s)', '95th percentile of per-handler ETL execution time.',
      o.target(o.histogramQuantileExpr(etlHandlerDur, 0.95, o.GKG_IDX_SEL, '5m', []), 'p95', 'ORBIT_DS'),
      's', 6),
    o.stat('Handler errors / min', 'ETL handler errors per minute.',
      o.target('sum(rate(%s{%s}[5m]) * 60)' % [etlHandlerErr, o.GKG_IDX_SEL], 'errors/min', 'ORBIT_DS'),
      'short', 6),
  ]
  + o.section('ETL engine', o.metricsInDomain('indexer.etl'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Code pipeline', o.metricsInDomain('indexer.code'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('SDLC pipeline', o.metricsInDomain('indexer.sdlc'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Namespace deletion', o.metricsInDomain('indexer.namespace_deletion'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Scheduler', o.metricsInDomain('indexer.scheduler'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Schema migration', o.metricsInDomain('indexer.migration'), 'ORBIT_DS', o.GKG_IDX_SEL);

o.dashboard(
  'orbit-gkg-indexer',
  'Orbit — GKG indexer',
  ['gkg', 'indexer'],
  'Indexer metrics: ETL engine, code pipeline, SDLC pipeline, namespace deletion, scheduler, schema migration.',
  items,
)
