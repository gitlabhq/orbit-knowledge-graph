// Panel + dashboard helpers for the Orbit Playground dashboards.
//
// Consumes the generated metric catalog at
// `crates/gkg-observability/orbit-dashboards/gkg-metrics.json`. Every
// catalog-driven panel constructor (`counterPanel`, `histogramPanels`,
// `gaugePanel`) takes a metric name and looks up its kind, labels, unit,
// and description from the catalog. An unknown name aborts evaluation,
// which is the build-time check that keeps dashboards from drifting away
// from the names the service actually emits.

local catalog = import '../../../crates/gkg-observability/orbit-dashboards/gkg-metrics.json';

local GRID_WIDTH = 24;
local PANEL_W = 8;
local PANEL_H = 8;
local STAT_H = 4;

// ---------- Catalog access ----------

local metric(name) = (
  local hits = std.filter(function(m) m.otel_name == name || m.prom_name == name, catalog.metrics);
  if std.length(hits) == 0 then
    error 'metric `' + name + '` not in catalog (gkg-metrics.json)'
  else hits[0]
);

local metricsInDomain(domain) =
  std.sort(
    std.filter(function(m) m.domain == domain, catalog.metrics),
    function(m) m.otel_name,
  );

// ---------- PromQL builders ----------

local sumByLabels(labels) =
  if std.length(labels) == 0 then '' else 'sum by (' + std.join(', ', labels) + ') ';

local counterRateExpr(prom_name, selector, interval, by) = (
  if std.length(by) == 0 then
    'sum(rate(%s{%s}[%s]))' % [prom_name, selector, interval]
  else
    'sum by (%s) (rate(%s{%s}[%s]))' % [std.join(', ', by), prom_name, selector, interval]
);

local histogramQuantileExpr(prom_name, q, selector, interval, by) =
  'histogram_quantile(%g, sum by (%s) (rate(%s_bucket{%s}[%s])))' % [
    q,
    std.join(', ', ['le'] + by),
    prom_name,
    selector,
    interval,
  ];

local histogramCountRateExpr(prom_name, selector, interval, by) = (
  if std.length(by) == 0 then
    'sum(rate(%s_count{%s}[%s]))' % [prom_name, selector, interval]
  else
    'sum by (%s) (rate(%s_count{%s}[%s]))' % [std.join(', ', by), prom_name, selector, interval]
);

local gaugeExpr(prom_name, selector, by) = (
  if std.length(by) == 0 then
    'sum(%s{%s})' % [prom_name, selector]
  else
    'sum by (%s) (%s{%s})' % [std.join(', ', by), prom_name, selector]
);

// Format a Grafana legend like `{{label1}} / {{label2}}`. Falls back to
// the instance label when no grouping labels are supplied.
local legendFor(labels) = (
  if std.length(labels) == 0 then '{{instance}}'
  else std.join(' / ', ['{{' + l + '}}' for l in labels])
);

// Map the catalog's UCUM unit to a Grafana unit string. Rate panels on
// byte counters get Bps, byte gauges get bytes, etc.
local unitFor(spec, rate=false) = (
  if std.objectHas(spec, 'unit') then
    if spec.unit == 's' then 's'
    else if spec.unit == 'ms' then 'ms'
    else if spec.unit == 'By' && rate then 'Bps'
    else if spec.unit == 'By' then 'bytes'
    else if spec.unit == 'KiBy' then 'kbytes'
    else if spec.unit == 'MiBy' then 'mbytes'
    else if spec.unit == '%' then 'percent'
    else 'short'
  else 'short'
);

// ---------- Panel + datasource primitives ----------

local datasource(uid_var) = { type: 'prometheus', uid: '$' + uid_var };

local target(expr, legend, ds_var, refId='A') = {
  datasource: datasource(ds_var),
  expr: expr,
  legendFormat: legend,
  refId: refId,
};

local timeseries(title, description, targets, unit='short', w=PANEL_W, h=PANEL_H) = {
  kind: 'panel',
  type: 'timeseries',
  title: title,
  description: description,
  datasource: if std.length(targets) > 0 then targets[0].datasource else null,
  targets: targets,
  fieldConfig: {
    defaults: {
      custom: {
        drawStyle: 'line',
        lineInterpolation: 'smooth',
        fillOpacity: 10,
        showPoints: 'auto',
      },
      unit: unit,
    },
    overrides: [],
  },
  options: {
    legend: { displayMode: 'table', placement: 'bottom', calcs: ['lastNotNull', 'max'] },
    tooltip: { mode: 'multi' },
  },
  gridPos: { x: 0, y: 0, w: w, h: h },
};

local stat(title, description, t, unit='short', w=PANEL_W) = {
  kind: 'panel',
  type: 'stat',
  title: title,
  description: description,
  datasource: t.datasource,
  targets: [t],
  fieldConfig: {
    defaults: { unit: unit, noValue: '—' },
    overrides: [],
  },
  options: {
    reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
    colorMode: 'value',
    graphMode: 'area',
    textMode: 'value_and_name',
  },
  gridPos: { x: 0, y: 0, w: w, h: STAT_H },
};

local row(title) = { kind: 'row', title: title };

// ---------- Catalog-driven panel constructors ----------

local counterPanels(spec, ds_var, selector, by=null) = (
  local labels = if by == null then spec.labels else by;
  local prom = spec.prom_name;
  [
    timeseries(
      prom + ' — rate/s',
      spec.description,
      [target(counterRateExpr(prom, selector, '5m', labels), legendFor(labels), ds_var)],
      unitFor(spec, rate=true),
    ),
  ]
);

local histogramPanels(spec, ds_var, selector, by=null) = (
  local labels = if by == null then spec.labels else by;
  local prom = spec.prom_name;
  local lg = if std.length(labels) == 0 then 'overall' else legendFor(labels);
  [
    timeseries(
      prom + ' — p50/p95/p99',
      spec.description,
      [
        target(histogramQuantileExpr(prom, 0.50, selector, '5m', labels), 'p50 ' + lg, ds_var, 'A'),
        target(histogramQuantileExpr(prom, 0.95, selector, '5m', labels), 'p95 ' + lg, ds_var, 'B'),
        target(histogramQuantileExpr(prom, 0.99, selector, '5m', labels), 'p99 ' + lg, ds_var, 'C'),
      ],
      unitFor(spec),
    ),
    timeseries(
      prom + ' — observation rate',
      spec.description,
      [target(
        histogramCountRateExpr(prom, selector, '5m', labels),
        if std.length(labels) == 0 then 'count' else legendFor(labels),
        ds_var,
      )],
      'short',
    ),
  ]
);

local gaugePanels(spec, ds_var, selector, by=null) = (
  local labels = if by == null then spec.labels else by;
  local prom = spec.prom_name;
  [
    timeseries(
      prom,
      spec.description,
      [target(gaugeExpr(prom, selector, labels), legendFor(labels), ds_var)],
      unitFor(spec),
    ),
  ]
);

local panelsFor(spec, ds_var, selector) = (
  if spec.kind == 'counter' then counterPanels(spec, ds_var, selector)
  else if spec.kind == 'histogram_f64' || spec.kind == 'histogram_u64' then histogramPanels(spec, ds_var, selector)
  else gaugePanels(spec, ds_var, selector)
);

// Returns [row(title), ...panels for every metric in `metrics`].
local section(title, metrics, ds_var, selector) =
  [row(title)] + std.flattenArrays([panelsFor(m, ds_var, selector) for m in metrics]);

// ---------- External (non-catalog) metrics ----------

// External metrics live outside the Rust catalog (HTTP autoinstrumentation,
// Siphon, NATS, Rails). Same panel shape as the catalog-driven helpers but
// we have to spell out the kind, labels, and description.
local externalCounterPanels(spec, ds_var, selector) = (
  local prom = spec.name;
  local labels = if std.objectHas(spec, 'labels') then spec.labels else [];
  local unit = if std.endsWith(prom, '_bytes_total') then 'Bps' else 'short';
  [
    timeseries(
      prom + ' — rate/s',
      spec.description,
      [target(counterRateExpr(prom, selector, '5m', labels), legendFor(labels), ds_var)],
      unit,
    ),
  ]
);

local externalHistogramPanels(spec, ds_var, selector) = (
  local prom = spec.name;
  local labels = if std.objectHas(spec, 'labels') then spec.labels else [];
  local unit = if std.endsWith(prom, '_seconds') then 's'
               else if std.endsWith(prom, '_bytes') then 'bytes'
               else 'short';
  local lg = if std.length(labels) == 0 then 'overall' else legendFor(labels);
  [
    timeseries(
      prom + ' — p50/p95/p99',
      spec.description,
      [
        target(histogramQuantileExpr(prom, 0.50, selector, '5m', labels), 'p50 ' + lg, ds_var, 'A'),
        target(histogramQuantileExpr(prom, 0.95, selector, '5m', labels), 'p95 ' + lg, ds_var, 'B'),
        target(histogramQuantileExpr(prom, 0.99, selector, '5m', labels), 'p99 ' + lg, ds_var, 'C'),
      ],
      unit,
    ),
    timeseries(
      prom + ' — observation rate',
      spec.description,
      [target(
        histogramCountRateExpr(prom, selector, '5m', labels),
        if std.length(labels) == 0 then 'count' else legendFor(labels),
        ds_var,
      )],
      'short',
    ),
  ]
);

local externalGaugePanels(spec, ds_var, selector) = (
  local prom = spec.name;
  local labels = if std.objectHas(spec, 'labels') then spec.labels else [];
  local unit = if std.endsWith(prom, '_bytes') then 'bytes' else 'short';
  [
    timeseries(
      prom,
      spec.description,
      [target(gaugeExpr(prom, selector, labels), legendFor(labels), ds_var)],
      unit,
    ),
  ]
);

local externalPanelsFor(spec, ds_var, selector) = (
  if spec.kind == 'counter' then externalCounterPanels(spec, ds_var, selector)
  else if spec.kind == 'histogram' then externalHistogramPanels(spec, ds_var, selector)
  else externalGaugePanels(spec, ds_var, selector)
);

local externalSection(title, metrics, ds_var, selector) =
  [row(title)] + std.flattenArrays([externalPanelsFor(m, ds_var, selector) for m in metrics]);

// ---------- Layout (assigns gridPos + ids) ----------

local layoutItems(items) = (
  local step(acc, item) = (
    if item.kind == 'row' then
      // Flush current row of panels, then drop the row header on its own line.
      local y = if acc.row_max_h > 0 then acc.row_y + acc.row_max_h else acc.row_y;
      acc {
        result: acc.result + [{
          id: acc.next_id,
          type: 'row',
          title: item.title,
          collapsed: false,
          gridPos: { h: 1, w: GRID_WIDTH, x: 0, y: y },
          panels: [],
        }],
        next_id: acc.next_id + 1,
        y: y + 1,
        row_y: y + 1,
        cursor_x: 0,
        row_max_h: 0,
      }
    else
      // Panel: wrap to a new row when the panel would overflow GRID_WIDTH.
      local w = item.gridPos.w;
      local h = item.gridPos.h;
      local overflow = acc.cursor_x + w > GRID_WIDTH;
      local row_y = if overflow then acc.row_y + acc.row_max_h else acc.row_y;
      local cursor_x = if overflow then 0 else acc.cursor_x;
      local row_max_h = if overflow then 0 else acc.row_max_h;
      local placed = item {
        id: acc.next_id,
        gridPos: { x: cursor_x, y: row_y, w: w, h: h },
      };
      // Strip our internal `kind` marker so the JSON is pure Grafana shape.
      local cleaned = std.prune(placed { kind: null });
      acc {
        result: acc.result + [cleaned],
        next_id: acc.next_id + 1,
        cursor_x: cursor_x + w,
        row_y: row_y,
        row_max_h: std.max(row_max_h, h),
      }
  );
  local final = std.foldl(step, items, {
    result: [],
    next_id: 1,
    y: 0,
    row_y: 0,
    cursor_x: 0,
    row_max_h: 0,
  });
  final.result
);

// ---------- Templating + dashboard shell ----------

local TEMPLATING = {
  list: [
    {
      name: 'ORBIT_DS',
      label: 'Orbit datasource (analytics-eventsdot tenant)',
      type: 'datasource',
      query: 'prometheus',
      current: { text: 'Mimir - Analytics Eventsdot', value: 'mimir-analytics-eventsdot' },
      regex: '/Analytics Eventsdot/',
      hide: 0,
      refresh: 1,
    },
    {
      name: 'RAILS_DS',
      label: 'Rails datasource (gitlab tenant)',
      type: 'datasource',
      query: 'prometheus',
      current: { text: 'Mimir - Gitlab Gprd', value: 'mimir-gitlab-gprd' },
      regex: '/Gitlab Gstg|Gitlab Gprd/',
      hide: 0,
      refresh: 1,
    },
    {
      name: 'cluster',
      label: 'Orbit cluster',
      type: 'custom',
      query: 'orbit-prd,orbit-stg,orbit-.*',
      current: { text: 'orbit-prd', value: 'orbit-prd' },
      options: [
        { text: 'orbit-prd', value: 'orbit-prd', selected: true },
        { text: 'orbit-stg', value: 'orbit-stg', selected: false },
        { text: 'all', value: 'orbit-.*', selected: false },
      ],
      includeAll: false,
      multi: false,
      hide: 0,
    },
    {
      name: 'rails_env',
      label: 'Rails env',
      type: 'custom',
      query: 'gprd,gstg,gstg|gprd',
      current: { text: 'gprd', value: 'gprd' },
      options: [
        { text: 'gprd', value: 'gprd', selected: true },
        { text: 'gstg', value: 'gstg', selected: false },
        { text: 'both', value: 'gstg|gprd', selected: false },
      ],
      includeAll: false,
      multi: false,
      hide: 0,
    },
  ],
};

local dashboard(uid, title, tags, description, items) = {
  annotations: { list: [] },
  editable: true,
  fiscalYearStartMonth: 0,
  graphTooltip: 1,
  id: null,
  uid: uid,
  title: title,
  description: description,
  tags: ['orbit', 'protected'] + tags,
  schemaVersion: 39,
  version: 1,
  time: { from: 'now-3h', to: 'now' },
  timezone: 'utc',
  refresh: '1m',
  templating: TEMPLATING,
  links: [{
    type: 'dashboards',
    asDropdown: true,
    includeVars: true,
    keepTime: true,
    tags: ['orbit'],
    title: 'Orbit',
  }],
  panels: layoutItems(items),
};

// ---------- Selectors ----------

local GKG_WEB_SEL = 'container="gkg-webserver", cluster=~"$cluster"';
local GKG_IDX_SEL = 'container="gkg-indexer", cluster=~"$cluster"';
local SIPHON_SEL = 'namespace="siphon", cluster=~"$cluster"';
local NATS_SEL = 'cluster=~"$cluster"';
local RAILS_SEL = 'env=~"$rails_env"';

// ---------- Public surface ----------

{
  // Catalog access
  metric: metric,
  metricsInDomain: metricsInDomain,
  // PromQL string builders
  counterRateExpr: counterRateExpr,
  histogramQuantileExpr: histogramQuantileExpr,
  histogramCountRateExpr: histogramCountRateExpr,
  gaugeExpr: gaugeExpr,
  // Panel primitives
  target: target,
  timeseries: timeseries,
  stat: stat,
  row: row,
  // Catalog-driven
  counterPanels: counterPanels,
  histogramPanels: histogramPanels,
  gaugePanels: gaugePanels,
  panelsFor: panelsFor,
  section: section,
  // External-metric versions
  externalSection: externalSection,
  // Dashboard shell
  dashboard: dashboard,
  // Selectors
  GKG_WEB_SEL: GKG_WEB_SEL,
  GKG_IDX_SEL: GKG_IDX_SEL,
  SIPHON_SEL: SIPHON_SEL,
  NATS_SEL: NATS_SEL,
  RAILS_SEL: RAILS_SEL,
  // Constants
  GRID_WIDTH: GRID_WIDTH,
}
