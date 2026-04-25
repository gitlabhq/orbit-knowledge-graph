// Orbit — GKG webserver. HTTP + gRPC transport, query pipeline, content
// resolution, query-engine threat counters, schema watcher.
local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

local pipelineQueries = o.metric('gkg.query.pipeline.queries').prom_name;
local pipelineDuration = o.metric('gkg.query.pipeline.duration').prom_name;
local pipelineSecRej = o.metric('gkg.query.pipeline.error.security_rejected').prom_name;

local items =
  [
    o.row('Overview'),
    o.stat('Queries / min', 'Query pipeline calls per minute.',
      o.target('sum(rate(%s{%s}[5m]) * 60)' % [pipelineQueries, o.GKG_WEB_SEL], 'qpm', 'ORBIT_DS'),
      'short', 6),
    o.stat('Success rate', 'status=ok share of pipeline calls.',
      o.target(
        'sum(rate(%s{%s, status="ok"}[5m])) / clamp_min(sum(rate(%s{%s}[5m])), 1)' %
          [pipelineQueries, o.GKG_WEB_SEL, pipelineQueries, o.GKG_WEB_SEL],
        'ok', 'ORBIT_DS'),
      'percentunit', 6),
    o.stat('Pipeline p95 (s)', '95th percentile of overall pipeline latency.',
      o.target(o.histogramQuantileExpr(pipelineDuration, 0.95, o.GKG_WEB_SEL, '5m', []), 'p95', 'ORBIT_DS'),
      's', 6),
    o.stat('Security rejects / min', 'Pipeline calls rejected for security policy violations.',
      o.target('sum(rate(%s{%s}[5m]) * 60)' % [pipelineSecRej, o.GKG_WEB_SEL], 'rejects/min', 'ORBIT_DS'),
      'short', 6),
  ]
  + o.externalSection('HTTP transport', ext.GKG_HTTP, 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.externalSection('gRPC transport', ext.GKG_GRPC, 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Query pipeline', o.metricsInDomain('query.pipeline'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Content resolution (Gitaly)', o.metricsInDomain('server.content'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Schema watcher', o.metricsInDomain('server.schema_watcher'), 'ORBIT_DS', o.GKG_WEB_SEL)
  + o.section('Query engine — threat counters', o.metricsInDomain('query.engine'), 'ORBIT_DS', o.GKG_WEB_SEL);

o.dashboard(
  'orbit-gkg-webserver',
  'Orbit — GKG webserver',
  ['gkg', 'webserver'],
  'Webserver metrics: HTTP + gRPC transport, query pipeline, content resolution, query-engine threat counters, schema watcher.',
  items,
)
