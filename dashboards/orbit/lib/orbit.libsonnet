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

local counterIncreaseExpr(prom_name, selector, range, by) = (
  if std.length(by) == 0 then
    'sum(increase(%s{%s}[%s]))' % [prom_name, selector, range]
  else
    'sum by (%s) (increase(%s{%s}[%s]))' % [std.join(', ', by), prom_name, selector, range]
);

local mergedSelector(selector, filter) = (
  if filter == '' then selector else selector + ', ' + filter
);

// Minimal URL encoder for the characters that show up in PromQL inside
// Grafana data-link URLs. PromQL queries do not contain `%`, so we skip
// that one and dodge the double-encoding pitfall.
local urlEncode(s) = (
  local pairs = [
    [' ', '%20'], ['"', '%22'], ['#', '%23'], ['&', '%26'],
    ['+', '%2B'], [',', '%2C'], ['/', '%2F'], [':', '%3A'],
    ['<', '%3C'], ['=', '%3D'], ['>', '%3E'], ['?', '%3F'],
    ['@', '%40'], ['[', '%5B'], [']', '%5D'], ['{', '%7B'],
    ['|', '%7C'], ['}', '%7D'], ['(', '%28'], [')', '%29'],
    ['*', '%2A'], ['\n', '%0A'],
  ];
  std.foldl(function(acc, p) std.strReplace(acc, p[0], p[1]), pairs, s)
);

// Build a Grafana data link that opens the given PromQL expression in
// Explore over the dashboard's current time range. Grafana interpolates
// $cluster, $__from, and $__to before the redirect.
//
// Uses std.manifestJsonEx so quotes inside the expression are escaped
// correctly. Hand-rolled JSON concatenation here breaks for any expr
// containing `"`, which is most of them.
local exploreLink(expr, ds_uid='mimir-analytics-eventsdot', title='Open in Explore') = {
  local payload = {
    datasource: ds_uid,
    queries: [{
      refId: 'A',
      datasource: { type: 'prometheus', uid: ds_uid },
      expr: expr,
      range: true,
    }],
    range: { from: '$__from', to: '$__to' },
  },
  title: title,
  url: '/explore?orgId=1&left=' + urlEncode(std.manifestJsonMinified(payload)),
  targetBlank: false,
};

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

local stat(title, description, t, unit='short', w=PANEL_W, h=STAT_H, links=[]) = {
  kind: 'panel',
  type: 'stat',
  title: title,
  description: description,
  datasource: t.datasource,
  targets: [t],
  links: links,
  fieldConfig: {
    defaults: { unit: unit, noValue: '—' },
    overrides: [],
  },
  options: {
    reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
    colorMode: 'value',
    graphMode: 'area',
    textMode: 'value_and_name',
    justifyMode: 'auto',
  },
  gridPos: { x: 0, y: 0, w: w, h: h },
};

local row(title) = { kind: 'row', title: title, collapsed: false };

local rowCollapsed(title) = { kind: 'row', title: title, collapsed: true };

// Bar / line timeseries used for `increase()` per-bucket counts. Each
// data point still represents a count over one Grafana auto-window;
// `draw='line'` swaps the visual to a smoothed area-line for callers
// who prefer a trend shape over discrete bars.
local barTimeseries(title, description, targets, unit='short', w=PANEL_W, h=PANEL_H, stack=true, draw='bars') = {
  kind: 'panel',
  type: 'timeseries',
  title: title,
  description: description,
  datasource: if std.length(targets) > 0 then targets[0].datasource else null,
  targets: targets,
  fieldConfig: {
    defaults: {
      custom: {
        drawStyle: draw,
        lineInterpolation: if draw == 'line' then 'smooth' else 'linear',
        fillOpacity: if draw == 'line' then 25 else 70,
        lineWidth: if draw == 'line' then 2 else 1,
        showPoints: 'never',
        stacking: if stack then { mode: 'normal', group: 'A' } else { mode: 'none' },
        barAlignment: 0,
      },
      unit: unit,
    },
    overrides: [],
  },
  options: {
    legend: { displayMode: 'table', placement: 'bottom', calcs: ['lastNotNull', 'max', 'sum'] },
    tooltip: { mode: 'multi' },
  },
  gridPos: { x: 0, y: 0, w: w, h: h },
};

// Heatmap panel for a Prometheus histogram. Reads `<name>_bucket` and
// renders bucket density over time.
local heatmap(title, description, expr, ds_var, unit='s', w=PANEL_W, h=PANEL_H) = {
  kind: 'panel',
  type: 'heatmap',
  title: title,
  description: description,
  datasource: datasource(ds_var),
  targets: [{
    datasource: datasource(ds_var),
    expr: expr,
    format: 'heatmap',
    legendFormat: '{{le}}',
    refId: 'A',
  }],
  options: {
    calculate: false,
    yAxis: { axisPlacement: 'left', reverse: false, unit: unit },
    rowsFrame: { layout: 'auto' },
    color: { mode: 'scheme', scheme: 'Spectral', exponent: 0.5, steps: 64, fill: 'dark-orange', reverse: false },
    cellGap: 1,
    cellValues: { unit: 'short' },
    legend: { show: true },
    tooltip: { show: true, yHistogram: true },
    filterValues: { le: 1e-9 },
  },
  gridPos: { x: 0, y: 0, w: w, h: h },
};

// Table panel. Each target should set `format: 'table', instant: true`.
local tablePanel(title, description, targets, w=PANEL_W, h=PANEL_H, sortBy=null) = {
  kind: 'panel',
  type: 'table',
  title: title,
  description: description,
  datasource: targets[0].datasource,
  targets: targets,
  fieldConfig: {
    defaults: {
      unit: 'short',
      custom: { align: 'auto', cellOptions: { type: 'auto' }, filterable: true },
    },
    overrides: [],
  },
  options: {
    showHeader: true,
    sortBy: if sortBy != null then [{ displayName: sortBy, desc: true }] else [],
    cellHeight: 'sm',
    footer: { show: false },
  },
  gridPos: { x: 0, y: 0, w: w, h: h },
};

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

// Returns the same as `section` but the row is collapsed by default.
local sectionCollapsed(title, metrics, ds_var, selector) =
  [rowCollapsed(title)] + std.flattenArrays([panelsFor(m, ds_var, selector) for m in metrics]);

// ---------- Story-shaped panel constructors ----------

// Single stat tile reading "X in dashboard window". The query uses
// $__range so it always answers the user's current time picker.
local counterRangeStat(prom_name, title, description, ds_var, selector, filter='', unit='short', w=PANEL_W, h=STAT_H, or_zero=true) = (
  local sel = mergedSelector(selector, filter);
  local base = 'sum(increase(%s{%s}[$__range]))' % [prom_name, sel];
  local expr = if or_zero then '(' + base + ') or vector(0)' else base;
  // Drill-through opens the underlying rate(metric{...}[5m]) in Explore
  // over the same time range, broken out by the same labels.
  local rate_expr = 'sum(rate(%s{%s}[5m]))' % [prom_name, sel];
  stat(title, description, target(expr, title, ds_var), unit, w, h, [exploreLink(rate_expr)])
);

// Stacked bar timeseries showing count-per-bucket via increase(). The
// envelope is total throughput, color is mix.
local counterIncreaseBars(spec, title, description, ds_var, selector, by=null, filter='', unit=null, w=PANEL_W, h=PANEL_H, range='$__rate_interval', stack=true, or_zero=false, draw='bars') = (
  local labels = if by == null then spec.labels else by;
  local sel = mergedSelector(selector, filter);
  local prom = spec.prom_name;
  local expr = counterIncreaseExpr(prom, sel, range, labels);
  // Optional fallback: synthesise a zero-valued series stamped with
  // `<label>="(none)"` for each `by` label. Keeps the panel from going
  // to "No data" during quiet windows for sparse counters.
  local zero_fallback = std.foldl(
    function(inner, l) 'label_replace(%s, "%s", "(none)", "", "")' % [inner, l],
    labels,
    'vector(0)',
  );
  local final_expr = if or_zero then '(%s) or %s' % [expr, zero_fallback] else expr;
  local u = if unit != null then unit else unitFor(spec, rate=false);
  barTimeseries(
    title,
    description,
    [target(final_expr, legendFor(labels), ds_var)],
    u,
    w,
    h,
    stack,
    draw,
  )
);

// Heatmap of a histogram metric's buckets over time.
local histogramHeatmap(spec, title, description, ds_var, selector, filter='', range='$__rate_interval', w=PANEL_W, h=PANEL_H) = (
  local sel = mergedSelector(selector, filter);
  local prom = spec.prom_name;
  local expr = 'sum by (le) (rate(%s_bucket{%s}[%s]))' % [prom, sel, range];
  heatmap(title, description, expr, ds_var, unitFor(spec), w, h)
);

// Top-N table of slowest series by p95, with p50/p95/p99 columns. Uses
// the dashboard `$__range` so the table reflects the time picker.
local histogramTopN(spec, title, description, ds_var, selector, byLabel, n=10, filter='', w=PANEL_W, h=PANEL_H) = (
  local sel = mergedSelector(selector, filter);
  local prom = spec.prom_name;
  local mkTarget(q, ref, alias) = {
    datasource: datasource(ds_var),
    expr: 'topk(%d, histogram_quantile(%g, sum by (%s, le) (rate(%s_bucket{%s}[$__range]))))' % [n, q, byLabel, prom, sel],
    format: 'table',
    instant: true,
    legendFormat: alias,
    refId: ref,
  };
  tablePanel(
    title,
    description,
    [
      mkTarget(0.50, 'A', 'p50'),
      mkTarget(0.95, 'B', 'p95'),
      mkTarget(0.99, 'C', 'p99'),
    ],
    w,
    h,
    sortBy='Value #B',
  )
);

// Ratio panel: numerator counter rate divided by denominator counter
// rate, presented as a percent-unit time series. The numerator is OR'd
// with `denom*0` so every label combination in the denominator gets a
// zero-valued numerator series, which keeps the panel from flipping to
// "No data" during quiet windows when no errors have happened.
local ratioPanel(title, description, num_prom, denom_prom, ds_var, selector, by=[], num_filter='', denom_filter='', range='5m', w=PANEL_W, h=PANEL_H, unit='percentunit') = (
  local nsel = mergedSelector(selector, num_filter);
  local dsel = mergedSelector(selector, denom_filter);
  local num =
    if std.length(by) == 0 then 'sum(rate(%s{%s}[%s]))' % [num_prom, nsel, range]
    else 'sum by (%s) (rate(%s{%s}[%s]))' % [std.join(', ', by), num_prom, nsel, range];
  local denom =
    if std.length(by) == 0 then 'sum(rate(%s{%s}[%s]))' % [denom_prom, dsel, range]
    else 'sum by (%s) (rate(%s{%s}[%s]))' % [std.join(', ', by), denom_prom, dsel, range];
  // `or (denom * 0)` adds a zero-valued series for every label
  // combination in the denominator that has no matching numerator.
  local num_safe = '((%s) or (%s) * 0)' % [num, denom];
  local expr = '(%s) / (%s)' % [num_safe, denom];
  timeseries(title, description, [target(expr, if std.length(by) == 0 then 'ratio' else legendFor(by), ds_var)], unit, w, h)
);

// Stat tile showing the latest value of a gauge-style PromQL expression.
// When `or_zero=true` (the default), the expression is wrapped with
// `or vector(0)` so an empty result renders as 0 instead of "—".
local gaugeStat(title, description, expr, ds_var, unit='short', w=PANEL_W, h=STAT_H, or_zero=true) =
  local final_expr = if or_zero then '(' + expr + ') or vector(0)' else expr;
  stat(title, description, target(final_expr, title, ds_var), unit, w, h, [exploreLink(expr)]);

// Stat panel for the headline number on top of a tile pair. No
// embedded sparkline (Grafana does not support hover tooltips on stat
// sparklines), so the chart is rendered as a separate timeseries below
// via `tileSpark`. Total height per tile: 3 + 2 = 5.
local tileHeader(prom_name, title, description, ds_var, selector, filter='', unit='short', w=PANEL_W, h=3) = (
  local sel = mergedSelector(selector, filter);
  local expr = '(sum(increase(%s{%s}[$__range]))) or vector(0)' % [prom_name, sel];
  local rate_expr = 'sum(rate(%s{%s}[5m]))' % [prom_name, sel];
  {
    kind: 'panel',
    type: 'stat',
    title: title,
    description: description,
    datasource: datasource(ds_var),
    targets: [target(expr, title, ds_var)],
    links: [exploreLink(rate_expr)],
    fieldConfig: {
      defaults: { unit: unit, noValue: '—' },
      overrides: [],
    },
    options: {
      reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
      colorMode: 'value',
      graphMode: 'none',
      textMode: 'value',
      justifyMode: 'center',
    },
    gridPos: { x: 0, y: 0, w: w, h: h },
  }
);

// Compact timeseries strip rendered below a tile header. Hover
// tooltips are enabled so a viewer can read the exact rate at a point
// in time.
local tileSpark(prom_name, ds_var, selector, filter='', unit='short', w=PANEL_W, h=2) = (
  local sel = mergedSelector(selector, filter);
  local expr = 'sum(rate(%s{%s}[$__rate_interval]))' % [prom_name, sel];
  {
    kind: 'panel',
    type: 'timeseries',
    title: '',
    description: 'Rate of change over time. Hover for exact values; click the link in the header for the full Explore view.',
    datasource: datasource(ds_var),
    targets: [target(expr, 'rate', ds_var)],
    fieldConfig: {
      defaults: {
        custom: {
          drawStyle: 'line',
          lineInterpolation: 'smooth',
          fillOpacity: 25,
          showPoints: 'never',
          lineWidth: 2,
        },
        unit: unit,
      },
      overrides: [],
    },
    options: {
      legend: { showLegend: false },
      tooltip: { mode: 'single' },
    },
    gridPos: { x: 0, y: 0, w: w, h: h },
  }
);

// Helper: emit a row of tile pairs. Returns headers in order followed
// by sparks in order so the layout packer renders them as a strip of
// big-number cards with chart strips immediately below.
local volumeTiles(specs, ds_var, selector, w=PANEL_W) = (
  std.map(function(s) tileHeader(
    s.prom, s.title, s.desc, ds_var, selector,
    std.get(s, 'filter', ''), std.get(s, 'unit', 'short'), w,
  ), specs)
  + std.map(function(s) tileSpark(
    s.prom, ds_var, selector,
    std.get(s, 'filter', ''), std.get(s, 'unit', 'short'), w,
  ), specs)
);

// Single-panel histogram percentiles (p50, p95, p99). Same shape as the
// existing `histogramPanels` two-panel pair but without the observation
// rate, so the Latency row stays compact and easy to read.
local histogramPercentiles(spec, title, description, ds_var, selector, by=[], filter='', range='5m', w=PANEL_W, h=PANEL_H) = (
  local sel = mergedSelector(selector, filter);
  local prom = spec.prom_name;
  local lg = if std.length(by) == 0 then 'overall' else legendFor(by);
  timeseries(
    title,
    description,
    [
      target(histogramQuantileExpr(prom, 0.50, sel, range, by), 'p50 ' + lg, ds_var, 'A'),
      target(histogramQuantileExpr(prom, 0.95, sel, range, by), 'p95 ' + lg, ds_var, 'B'),
      target(histogramQuantileExpr(prom, 0.99, sel, range, by), 'p99 ' + lg, ds_var, 'C'),
    ],
    unitFor(spec),
    w,
    h,
  )
);

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
  // Panels under a `collapsed: true` row must live inside that row's
  // `panels` array, not at the top level. We track that row in the
  // accumulator and flush it on the next row boundary or at the end.
  local flushCollapsed(acc) =
    if acc.collapsed_open then
      acc.result + [acc.collapsed_row { panels: acc.collapsed_panels }]
    else acc.result;
  local step(acc, item) = (
    if item.kind == 'row' then
      local result0 = flushCollapsed(acc);
      local y = if acc.row_max_h > 0 then acc.row_y + acc.row_max_h else acc.row_y;
      local collapsed = std.objectHas(item, 'collapsed') && item.collapsed;
      local row_obj = {
        id: acc.next_id,
        type: 'row',
        title: item.title,
        collapsed: collapsed,
        gridPos: { h: 1, w: GRID_WIDTH, x: 0, y: y },
        panels: [],
      };
      if collapsed then
        acc {
          result: result0,
          next_id: acc.next_id + 1,
          y: y + 1,
          row_y: y + 1,
          cursor_x: 0,
          row_max_h: 0,
          collapsed_open: true,
          collapsed_row: row_obj,
          collapsed_panels: [],
        }
      else
        acc {
          result: result0 + [row_obj],
          next_id: acc.next_id + 1,
          y: y + 1,
          row_y: y + 1,
          cursor_x: 0,
          row_max_h: 0,
          collapsed_open: false,
          collapsed_panels: [],
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
      local cleaned = std.prune(placed { kind: null });
      if acc.collapsed_open then
        acc {
          collapsed_panels: acc.collapsed_panels + [cleaned],
          next_id: acc.next_id + 1,
          cursor_x: cursor_x + w,
          row_y: row_y,
          row_max_h: std.max(row_max_h, h),
        }
      else
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
    collapsed_open: false,
    collapsed_row: {},
    collapsed_panels: [],
  });
  flushCollapsed(final)
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
  counterIncreaseExpr: counterIncreaseExpr,
  histogramQuantileExpr: histogramQuantileExpr,
  histogramCountRateExpr: histogramCountRateExpr,
  gaugeExpr: gaugeExpr,
  // Panel primitives
  target: target,
  timeseries: timeseries,
  barTimeseries: barTimeseries,
  heatmap: heatmap,
  tablePanel: tablePanel,
  stat: stat,
  row: row,
  rowCollapsed: rowCollapsed,
  // Catalog-driven
  counterPanels: counterPanels,
  histogramPanels: histogramPanels,
  gaugePanels: gaugePanels,
  panelsFor: panelsFor,
  section: section,
  sectionCollapsed: sectionCollapsed,
  // Story-shaped helpers
  counterRangeStat: counterRangeStat,
  counterIncreaseBars: counterIncreaseBars,
  histogramHeatmap: histogramHeatmap,
  histogramPercentiles: histogramPercentiles,
  histogramTopN: histogramTopN,
  ratioPanel: ratioPanel,
  gaugeStat: gaugeStat,
  tileHeader: tileHeader,
  tileSpark: tileSpark,
  volumeTiles: volumeTiles,
  // URL helpers
  exploreLink: exploreLink,
  urlEncode: urlEncode,
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
