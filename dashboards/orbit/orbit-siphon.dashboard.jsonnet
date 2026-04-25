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
  ]
  + o.externalSection('Producers', ext.SIPHON_PRODUCERS, 'ORBIT_DS', o.SIPHON_SEL)
  + o.externalSection('ClickHouse consumers', ext.SIPHON_CONSUMERS, 'ORBIT_DS', o.SIPHON_SEL);

o.dashboard(
  'orbit-siphon',
  'Orbit — Siphon',
  ['siphon'],
  'Siphon producer and ClickHouse consumer metrics (analytics-eventsdot tenant).',
  items,
)
