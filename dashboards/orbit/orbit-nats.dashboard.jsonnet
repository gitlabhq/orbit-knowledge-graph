// Orbit — NATS. Server-level (varz) and JetStream stream/consumer metrics.
local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

local items =
  [
    o.row('Overview'),
    o.stat('In msgs / s', 'Inbound NATS messages per second.',
      o.target('sum(rate(nats_varz_in_msgs{%s}[5m]))' % o.NATS_SEL, 'in/s', 'ORBIT_DS'),
      'short', 8),
    o.stat('Out msgs / s', 'Outbound NATS messages per second.',
      o.target('sum(rate(nats_varz_out_msgs{%s}[5m]))' % o.NATS_SEL, 'out/s', 'ORBIT_DS'),
      'short', 8),
    o.stat('Slow consumers', 'Count of slow consumers reported by varz.',
      o.target('sum(nats_varz_slow_consumers{%s})' % o.NATS_SEL, 'count', 'ORBIT_DS'),
      'short', 8),
  ]
  + o.externalSection('JetStream + varz', ext.NATS_METRICS, 'ORBIT_DS', o.NATS_SEL);

o.dashboard(
  'orbit-nats',
  'Orbit — NATS',
  ['nats'],
  'NATS server-level (varz) and JetStream stream/consumer metrics in the Orbit clusters.',
  items,
)
