// Orbit — All metrics (kitchen sink). Every catalog metric plus the
// external transport / datalake / Rails metrics, grouped by domain.
local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

local items =
  o.externalSection('HTTP transport (webserver)', ext.GKG_HTTP, 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.externalSection('gRPC transport (webserver)', ext.GKG_GRPC, 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('ETL engine (indexer)', o.metricsInDomain('indexer.etl'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Code pipeline (indexer)', o.metricsInDomain('indexer.code'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('SDLC pipeline (indexer)', o.metricsInDomain('indexer.sdlc'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Namespace deletion (indexer)', o.metricsInDomain('indexer.namespace_deletion'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Scheduler (indexer)', o.metricsInDomain('indexer.scheduler'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Schema migration (indexer)', o.metricsInDomain('indexer.migration'), 'ORBIT_DS', o.GKG_IDX_SEL)
  + o.section('Query pipeline (webserver)', o.metricsInDomain('query.pipeline'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Query engine — threat counters (webserver)', o.metricsInDomain('query.engine'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Content resolution (webserver)', o.metricsInDomain('server.content'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Schema watcher (webserver)', o.metricsInDomain('server.schema_watcher'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.externalSection('Siphon producers', ext.SIPHON_PRODUCERS, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('Siphon ClickHouse consumers', ext.SIPHON_CONSUMERS, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('NATS JetStream + varz', ext.NATS_METRICS, 'ORBIT_DS', o.NATS_SEL)
  + o.externalSection('Rails KG request path', ext.RAILS_KG_REQUEST, 'RAILS_DS', o.RAILS_SEL)
  + o.externalSection('Rails KG traversal compaction', ext.RAILS_KG_TRAVERSAL, 'RAILS_DS', o.RAILS_SEL);

o.dashboard(
  'orbit-all-metrics',
  'Orbit — All metrics (kitchen sink)',
  ['all'],
  'Every metric the Orbit stack emits, grouped by domain. Use the component dashboards for focused views.',
  items,
)
