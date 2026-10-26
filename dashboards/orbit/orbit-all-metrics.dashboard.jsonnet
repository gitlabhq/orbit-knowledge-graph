// Orbit — All metrics (kitchen sink). Every catalog metric plus the
// external transport / datalake / Rails metrics, grouped by domain.
local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

// Emitted by the dispatcher freshness task, not the indexer.
local DISPATCHER_SDLC = ['gkg_indexer_sdlc_freshness_lag_seconds', 'gkg_indexer_sdlc_freshness_oldest_sync_timestamp_seconds'];

local items =
  o.externalSection('gRPC transport (webserver)', ext.GKG_GRPC, 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('ETL engine (indexer)', o.metricsInDomain('indexer.etl'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Code pipeline (indexer)', o.metricsInDomain('indexer.code'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('SDLC pipeline (indexer)', [m for m in o.metricsInDomain('indexer.sdlc') if !std.member(DISPATCHER_SDLC, m.prom_name)], 'ORBIT_DS', o.GKG_IDX_SEL)
  + std.flattenArrays([o.panelsFor(o.metric(name), 'ORBIT_DS', o.GKG_DSP_SEL) for name in DISPATCHER_SDLC])
  + o.section('Namespace deletion (indexer)', o.metricsInDomain('indexer.namespace_deletion'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Scheduler (dispatcher)', o.metricsInDomain('indexer.scheduler'), 'ORBIT_DS', o.GKG_DSP_SEL)
  + o.section('Schema migration (dispatcher)', o.metricsInDomain('indexer.migration'), 'ORBIT_DS', o.GKG_DSP_SEL)
  + o.section('Query pipeline (webserver)', o.metricsInDomain('query.pipeline'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Query engine — threat counters (webserver)', o.metricsInDomain('query.engine'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Content resolution (webserver)', o.metricsInDomain('server.content'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Schema watcher (webserver)', o.metricsInDomain('server.schema_watcher'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Billing events (webserver)', o.metricsInDomain('billing.events'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Billing quota (webserver)', o.metricsInDomain('billing.quota'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Circuit breaker (CH / NATS / GitLab clients)', o.metricsInDomain('resilience.circuit_breaker'), 'ORBIT_DS', o.GKG_ANY_SEL)
  + o.externalSection('Siphon producers', ext.SIPHON_PRODUCERS, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('Siphon ClickHouse consumers', ext.SIPHON_CONSUMERS, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('NATS JetStream + varz', ext.NATS_METRICS, 'ORBIT_DS', o.NATS_SEL)
  + o.externalSection('Rails Orbit request path', ext.RAILS_KG_REQUEST, 'RAILS_DS', o.RAILS_SEL)
  + o.externalSection('Rails Orbit traversal compaction', ext.RAILS_KG_TRAVERSAL, 'RAILS_DS', o.RAILS_SEL);

local annotations = [
  o.deployAnnotation('ORBIT_DS', o.GKG_ANY_SEL),
];

o.dashboard(
  'orbit-all-metrics',
  'Orbit — All metrics (kitchen sink)',
  ['all'],
  'Every metric the Orbit stack emits, grouped by domain. Use the component dashboards for focused views.',
  items,
  annotations,
)
