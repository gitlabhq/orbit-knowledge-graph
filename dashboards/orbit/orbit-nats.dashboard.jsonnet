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
    o.stat('JetStream storage used', 'File-storage saturation; GitLab.com pages at 80% of configured max.',
      o.target('max(nats_varz_jetstream_stats_storage{%s} / nats_varz_jetstream_config_max_storage{%s})' % [o.NATS_SEL, o.NATS_SEL], 'used', 'ORBIT_DS'),
      'percentunit', 8),
  ]
  + o.externalSection('Server (varz)', ext.NATS_METRICS, 'ORBIT_DS', o.NATS_SEL)
  + o.externalSection('JetStream capacity', ext.NATS_JETSTREAM_CAPACITY, 'ORBIT_DS', o.NATS_SEL)
  + o.externalSection('JetStream streams + consumers (leader replica)', ext.NATS_JETSTREAM_STREAMS, 'ORBIT_DS', 'is_stream_leader="true"' + (if o.NATS_SEL == '' then '' else ', ' + o.NATS_SEL));

o.dashboard(
  'orbit-nats',
  'Orbit — NATS',
  ['nats'],
  'NATS server-level (varz) and JetStream stream/consumer metrics in the Orbit clusters.',
  items,
)
