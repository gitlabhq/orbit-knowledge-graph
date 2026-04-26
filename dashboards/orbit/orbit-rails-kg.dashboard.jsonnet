// Orbit — Rails KG integration. Rails-side gitlab_knowledge_graph_*
// metrics: gRPC calls, redaction, JWT, traversal compaction.
local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

local items =
  [
    o.row('Overview'),
    o.stat('gRPC calls / min', 'Rails → GKG gRPC call rate.',
      o.target('sum(rate(gitlab_knowledge_graph_grpc_duration_seconds_count{%s}[5m]) * 60)' % o.RAILS_SEL, 'calls/min', 'RAILS_DS'),
      'short', 8),
    o.stat('gRPC errors / min', 'Rails-side gRPC errors.',
      o.target('sum(rate(gitlab_knowledge_graph_grpc_errors_total{%s}[5m]) * 60)' % o.RAILS_SEL, 'errors/min', 'RAILS_DS'),
      'short', 8),
    o.stat('gRPC p95 (ms)', '95th percentile of Rails-side gRPC latency.',
      o.target('1000 * histogram_quantile(0.95, sum by (le) (rate(gitlab_knowledge_graph_grpc_duration_seconds_bucket{%s}[5m])))' % o.RAILS_SEL, 'p95 ms', 'RAILS_DS'),
      'ms', 8),
  ]
  + o.externalSection('Request path', ext.RAILS_KG_REQUEST, 'RAILS_DS', o.RAILS_SEL)
  + o.externalSection('Traversal compaction', ext.RAILS_KG_TRAVERSAL, 'RAILS_DS', o.RAILS_SEL);

o.dashboard(
  'orbit-rails-kg',
  'Orbit — Rails KG integration',
  ['gkg', 'rails'],
  'Rails-side gitlab_knowledge_graph_* metrics: gRPC calls, redaction, JWT, traversal compaction.',
  items,
)
