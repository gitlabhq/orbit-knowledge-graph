// Panel helpers that consume the generated `gkg-metrics.json` catalog.
//
// Dashboards built on these helpers reference typed metric handles from the
// catalog instead of hardcoded PromQL strings, so drift between emitted
// metric names and dashboard queries becomes a compile-time error in the
// jsonnet evaluator.
//
// Usage:
//   local gkg   = import 'orbit-dashboards/gkg-metrics.json';
//   local orbit = import 'orbit-dashboards/orbit-panels.libsonnet';
//   local etl   = orbit.metric(gkg, 'gkg.etl.messages.processed');
//
//   orbit.counterRate(etl, by=['outcome'])

local promMatcher(selector) = std.join(',', [
  '%s="%s"' % [k, selector[k]]
  for k in std.objectFields(selector)
]);

// Looks up a single entry from the generated catalog by its OTel name.
local metric(catalog, otelName) = (
  local found = std.filter(function(m) m.otel_name == otelName, catalog.metrics);
  if std.length(found) == 0 then
    error 'metric not found in orbit-dashboards/gkg-metrics.json: ' + otelName
  else found[0]
);

// Builds a PromQL `sum by (labels) (rate(name{selector}[interval]))` query
// that pulls the metric's `prom_name` and `labels` straight from the catalog.
local counterRateQuery(m, selector={ type: 'orbit' }, interval='5m', by=null) = (
  local labels = if by == null then m.labels else by;
  local bySuffix = if std.length(labels) == 0 then '' else
    'sum by (%s) ' % std.join(', ', labels);
  '%s(rate(%s{%s}[%s]))' % [
    bySuffix,
    m.prom_name,
    promMatcher(selector),
    interval,
  ]
);

local histogramQuantileQuery(m, q, selector={ type: 'orbit' }, interval='5m', by=null) = (
  local labels = if by == null then m.labels else by;
  local extraBy = if std.length(labels) == 0 then '' else ', ' + std.join(', ', labels);
  'histogram_quantile(%g, sum by (le%s) (rate(%s_bucket{%s}[%s])))' % [
    q,
    extraBy,
    m.prom_name,
    promMatcher(selector),
    interval,
  ]
);

local gaugeQuery(m, selector={ type: 'orbit' }) = (
  '%s{%s}' % [m.prom_name, promMatcher(selector)]
);

{
  // Exposed API
  metric: metric,
  counterRateQuery: counterRateQuery,
  histogramQuantileQuery: histogramQuantileQuery,
  gaugeQuery: gaugeQuery,

  // Convenience wrappers that return `{title, description, expr}` dicts a
  // grafonnet panel factory can consume. Dashboards can either use these
  // directly or compose their own panel shapes on top of the query helpers
  // above.
  counterRate(m, selector={ type: 'orbit' }, interval='5m', by=null, title=null): {
    title: if title != null then title else m.prom_name,
    description: m.description,
    expr: counterRateQuery(m, selector, interval, by),
  },

  histogramP99(m, selector={ type: 'orbit' }, interval='5m', by=null, title=null): {
    title: if title != null then title else m.prom_name + ' p99',
    description: m.description,
    expr: histogramQuantileQuery(m, 0.99, selector, interval, by),
  },

  histogramP50(m, selector={ type: 'orbit' }, interval='5m', by=null, title=null): {
    title: if title != null then title else m.prom_name + ' p50',
    description: m.description,
    expr: histogramQuantileQuery(m, 0.50, selector, interval, by),
  },

  gauge(m, selector={ type: 'orbit' }, title=null): {
    title: if title != null then title else m.prom_name,
    description: m.description,
    expr: gaugeQuery(m, selector),
  },

  // Selects every metric in a domain. Useful for kitchen-sink dashboards
  // that want to auto-generate panels for a whole subsystem.
  metricsInDomain(catalog, domain)::
    std.filter(function(m) m.domain == domain, catalog.metrics),
}
