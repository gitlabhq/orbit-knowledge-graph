// Orbit — Siphon. Producer + ClickHouse consumer metrics (analytics-eventsdot tenant).
local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

local items =
  [
    o.row('Overview'),
    o.stat('Producer ops / s', 'Siphon CDC operations emitted per second.',
      o.target('sum(rate(siphon_operations_total{%s}[5m]))' % o.SIPHON_SEL, 'ops/s', 'ORBIT_DS'),
      'short', 8),
    o.stat('Consumer events / s', 'Events consumed off NATS and written to ClickHouse.',
      o.target('sum(rate(siphon_clickhouse_consumer_number_of_events{%s}[5m]))' % o.SIPHON_SEL, 'events/s', 'ORBIT_DS'),
      'short', 8),
    o.stat('Producer apps (distinct)', 'Distinct apps producing into Siphon.',
      o.target('count(count by (app_id) (siphon_operations_total{%s}))' % o.SIPHON_SEL, 'apps', 'ORBIT_DS'),
      'short', 8),
    o.stat('Data lag (max, e2e)', 'Worst end-to-end lag from Postgres commit to ClickHouse (source=data; heartbeat rows measure freshness only).',
      o.target('max(siphon_data_lag_ms{%s, source="data"})' % o.SIPHON_SEL, 'lag', 'ORBIT_DS'),
      'ms', 8),
    o.stat('Failover mode', 'Non-zero while any producer runs in emergency failover mode.',
      o.target('max(siphon_failover_mode_active{%s})' % o.SIPHON_SEL, 'active', 'ORBIT_DS'),
      'short', 8),
    o.stat('Consumer batch retries / s', 'ClickHouse batch retries; sustained non-zero means insert pressure.',
      o.target('sum(rate(siphon_clickhouse_consumer_batch_retry_total{%s}[5m]))' % o.SIPHON_SEL, 'retries/s', 'ORBIT_DS'),
      'short', 8),
  ]
  + o.externalSection('Producers', ext.SIPHON_PRODUCERS, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('ClickHouse consumers', ext.SIPHON_CONSUMERS, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('Reconciler', ext.SIPHON_RECONCILER, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('Retention manager', ext.SIPHON_RETENTION, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('Failover', ext.SIPHON_FAILOVER, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('Object storage (oversize spill)', ext.SIPHON_OBJECT_STORAGE, 'ORBIT_DS', o.SIPHON_SEL);

o.dashboard(
  'orbit-siphon',
  'Orbit — Siphon',
  ['siphon'],
  'Siphon producer and ClickHouse consumer metrics'
  + (if o.IS_DEDICATED then '.' else ' (analytics-eventsdot tenant).'),
  items,
)
