// Orbit — Webserver.
//
// Opens on the answer to "is the webserver healthy?" without scrolling,
// following the RED method (rate, errors, duration):
//
//   1. At a glance — eight small tiles for the selected time range,
//      neutral until a threshold is crossed; query totals with their
//      volume curve (all, by outcome, by query type); then latency of
//      successful queries, server errors by type, and rejected client
//      queries by reason.
//   2. Latency breakdown — per query type, per pipeline stage, and
//      ClickHouse work, to find where time goes.
//   3. Reliability — failure and rejection details.
//   4. Content resolution, schema watcher, and resources.
//   5. Reference — every metric in every server and query domain,
//      collapsed by default.

local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

local DS = 'ORBIT_DS';
local SEL = o.GKG_WEB_SEL;

// Catalog lookups. Build aborts if any prom name drifts.
local pipelineQueries = o.metric('gkg.query.pipeline.queries');
local pipelineDuration = o.metric('gkg.query.pipeline.duration');
local pipelineRedactions = o.metric('gkg.query.pipeline.redactions');
local pipelineRows = o.metric('gkg.query.pipeline.result_set.rows');
local pipelineCompile = o.metric('gkg.query.pipeline.compile.duration');
local pipelineAuthz = o.metric('gkg.query.pipeline.authorization.duration');
local pipelineExecute = o.metric('gkg.query.pipeline.execute.duration');
local pipelineHydrate = o.metric('gkg.query.pipeline.hydration.duration');
local pipelineChMem = o.metric('gkg.query.pipeline.ch.memory_usage');
local pipelineChRead = o.metric('gkg.query.pipeline.ch.read');
local pipelineChRows = o.metric('gkg.query.pipeline.ch.read_rows');

local compilerRejected = o.metric('gkg.query.engine.compiler.rejected').prom_name;
local pipelineFailed = o.metric('gkg.query.pipeline.failed').prom_name;

local contentResolveDur = o.metric('gkg.content.resolve.duration');
local contentResolveBatch = o.metric('gkg.content.resolve.batch_size');
local contentGitalyCalls = o.metric('gkg.content.gitaly.calls');


// 1. At a glance ----------------------------------------------------------
// Tiles summarize the selected time range and stay in text color until a threshold is crossed, so only real
// trouble is coloured. Thresholds follow the latency (5 s, 25 s) and error
// (99.5%) objectives.
local Q = pipelineQueries.prom_name;
local DUR = pipelineDuration.prom_name;
local VALID = 'status!~"compile_error|security_error", query_type!="unknown"';
local SERVER_STATUS = 'execution_error|timeout|streaming_error|content_resolution_error|authorization_error|custom_error';
local COMPILER_FAULT = 'lowering|enforcement|codegen|pipeline|ontology_internal|security';
local OK = 'status="ok", query_type!="unknown"';
local SCHEMA = 'status="ok", query_type="unknown"';
local REJECTED = 'status=~"compile_error|security_error"';

local serverErrors(range) =
  '(sum(increase(%s{%s, status=~"%s"}[%s])) or vector(0)) + (sum(increase(%s{%s, failure_reason=~"%s"}[%s])) or vector(0))'
  % [Q, SEL, SERVER_STATUS, range, compilerRejected, SEL, COMPILER_FAULT, range];

local NEUTRAL = [{ color: 'text', value: null }];
local steps(pairs) = NEUTRAL + [{ color: p[1], value: p[0] } for p in pairs];
local stepsDown(red, amber) = [
  { color: 'red', value: null },
  { color: '#EAB839', value: red },
  { color: 'green', value: amber },
];

local tile(title, description, expr, unit, thresholds, decimals=null) =
  o.stat(title, description, o.target(expr, title, DS) + { instant: true, range: false }, unit, 3, 4) + {
    fieldConfig+: {
      defaults+: {
        color: { mode: 'thresholds' },
        thresholds: { mode: 'absolute', steps: thresholds },
        [if decimals != null then 'decimals']: decimals,
      },
    },
    options+: {
      textMode: 'value',
      graphMode: 'none',
      colorMode: 'value',
      justifyMode: 'center',
      text: { titleSize: 13, valueSize: 32 },
    },
  };

local percentiles(title, description) = o.timeseries(
  title,
  description,
  [
    o.target('histogram_quantile(%s, sum by (le) (rate(%s_bucket{%s, %s}[30m])))' % [q[0], DUR, SEL, OK], q[1], DS, q[2])
    for q in [['0.50', 'p50', 'A'], ['0.95', 'p95', 'B'], ['0.99', 'p99', 'C']]
  ],
  's', 12, 9,
  overrides=[
    { matcher: { id: 'byName', options: n[0] }, properties: [{ id: 'color', value: { mode: 'fixed', fixedColor: n[1] } }] }
    for n in [['p50', '#3987e5'], ['p95', '#d95926'], ['p99', '#9085e9']]
  ],
) + {
  fieldConfig+: {
    defaults+: {
      custom+: { thresholdsStyle: { mode: 'dashed' }, fillOpacity: 0, lineWidth: 2, showPoints: 'never' },
      color: { mode: 'palette-classic' },
      thresholds: { mode: 'absolute', steps: [{ color: 'transparent', value: null }, { color: '#EAB839', value: 5 }, { color: 'red', value: 25 }] },
    },
  },
  options+: { legend: { displayMode: 'table', placement: 'bottom', calcs: ['mean', 'max', 'lastNotNull'] } },
};

local colored(names) = [
  { matcher: { id: 'byName', options: n[0] }, properties: [{ id: 'color', value: { mode: 'fixed', fixedColor: n[1] } }] }
  for n in names
];

local errorBars(title, description, targets, colors) =
  o.barTimeseries(title, description, targets, 'short', 6, 9, minInterval='10m') + {
    fieldConfig+: { defaults+: { decimals: 0 }, overrides: colored(colors) },
    options+: { legend: { displayMode: 'list', placement: 'bottom', calcs: ['sum'] } },
  };

// Number and trend in one panel: each series is a per-interval count, the
// panel shows its sum over the selected range with the counts as a sparkline.
local volume(title, description, targets, colors, w) =
  o.stat(title, description, targets[0], 'locale', w, 6) + {
    targets: targets,
    interval: '5m',
    fieldConfig+: {
      defaults+: { decimals: 0, color: { mode: 'fixed', fixedColor: '#3987e5' } },
      overrides: colored(colors),
    },
    options+: {
      reduceOptions: { calcs: ['sum'], fields: '', values: false },
      textMode: if std.length(targets) == 1 && std.length(colors) == 0 then 'value' else 'value_and_name',
      graphMode: 'area',
      colorMode: 'none',
      justifyMode: 'center',
      orientation: 'vertical',
      text: { titleSize: 13, valueSize: 30 },
    },
  };

local glance = [
  o.row('At a glance'),
  tile(
    'Queries/min',
    'Valid queries per minute, averaged over the selected time range: successful queries and server failures. Excludes rejected client queries and schema calls.',
    'sum(rate(%s{%s, %s}[$__range])) * 60' % [Q, SEL, VALID],
    'short', NEUTRAL, 1,
  ),
  tile(
    'Success',
    'Share of valid queries in the selected time range that did not fail on the server side. Objective: 99.5%. Amber below the objective, red below 95%. Rejected client queries are not counted.',
    '1 - (%s) / sum(increase(%s{%s, %s}[$__range]))' % [serverErrors('$__range'), Q, SEL, VALID],
    'percentunit', stepsDown(0.95, 0.995), 2,
  ),
  tile(
    'Server errors',
    'Queries that failed because of the service in the selected time range: execution, timeout, streaming, content resolution, redaction, and compiler faults. The count grows with the time range, so it stays neutral; the Success tile judges it. See the chart below for the split by type.',
    serverErrors('$__range'),
    'short', NEUTRAL, 0,
  ),
  tile(
    'Under 5 s',
    'Share of successful queries in the selected time range that finished in 5 seconds or less.',
    'sum(increase(%s_bucket{%s, %s, le="5.0"}[$__range])) / sum(increase(%s_count{%s, %s}[$__range]))' % [DUR, SEL, OK, DUR, SEL, OK],
    'percentunit', stepsDown(0.9, 0.95), 1,
  ),
  tile(
    'p50',
    'Median duration of successful queries in the selected time range.',
    'histogram_quantile(0.50, sum by (le) (rate(%s_bucket{%s, %s}[$__range])))' % [DUR, SEL, OK],
    's', NEUTRAL,
  ),
  tile(
    'p95',
    'p95 duration of successful queries in the selected time range. Amber from 5 s, red from 25 s.',
    'histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s, %s}[$__range])))' % [DUR, SEL, OK],
    's', steps([[5, '#EAB839'], [25, 'red']]), 1,
  ),
  tile(
    'p99',
    'p99 duration of successful queries in the selected time range. Amber from 25 s, the long-tail objective (99% of queries within 25 s).',
    'histogram_quantile(0.99, sum by (le) (rate(%s_bucket{%s, %s}[$__range])))' % [DUR, SEL, OK],
    's', steps([[25, '#EAB839']]), 1,
  ),
  tile(
    'Rejected',
    'Share of queries in the selected time range (schema calls excluded) that were rejected before running: invalid client input (bad syntax, unknown schema, and so on) or a user with no enabled namespace. Not a service fault, so it stays neutral.',
    '(sum(increase(%s{%s, %s}[$__range])) or vector(0)) / (sum(increase(%s{%s}[$__range])) - (sum(increase(%s{%s, %s}[$__range])) or vector(0)))' % [Q, SEL, REJECTED, Q, SEL, Q, SEL, SCHEMA],
    'percentunit', NEUTRAL, 1,
  ),
  volume(
    'Queries',
    'All queries that reached the pipeline in the selected time range, including rejected client queries and excluding schema calls. The curve shows volume per 5 minutes or more.',
    [o.target('sum(increase(%s{%s}[$__interval])) - (sum(increase(%s{%s, %s}[$__interval])) or vector(0))' % [Q, SEL, Q, SEL, SCHEMA], 'Queries', DS, 'A')],
    [], 6,
  ),
  volume(
    'By outcome',
    'Queries in the selected time range by outcome: successful, failed because of the service, and rejected as invalid client input. Totals sum 5-minute or longer buckets, so they can differ by a few from the tiles above.',
    [
      o.target('sum(increase(%s{%s, %s}[$__interval]))' % [Q, SEL, OK], 'Successful', DS, 'A'),
      o.target('(%s)' % serverErrors('$__interval'), 'Server errors', DS, 'B'),
      o.target('sum(increase(%s{%s, %s}[$__interval])) or vector(0)' % [Q, SEL, REJECTED], 'Rejected', DS, 'C'),
    ],
    [['Successful', '#3987e5'], ['Server errors', '#e66767'], ['Rejected', '#8e8e9a']], 9,
  ),
  volume(
    'By query type',
    'Successful queries in the selected time range by query type.',
    [
      o.target('sum(increase(%s{%s, %s, query_type="%s"}[$__interval]))' % [Q, SEL, OK, t[0]], t[0], DS, t[1])
      for t in [['traversal', 'A'], ['aggregation', 'B'], ['neighbors', 'C'], ['path_finding', 'D']]
    ],
    [['traversal', '#3987e5'], ['aggregation', '#d95926'], ['neighbors', '#199e70'], ['path_finding', '#c98500']], 9,
  ),
  percentiles(
    'Latency of successful queries',
    'p50, p95 and p99 of successful queries over 30-minute windows. Dashed lines mark the 5 s and 25 s objectives. Failed queries are excluded because they end early or at a timeout.',
  ),
  errorBars(
    'Server errors by type',
    'Queries that failed because of the service, by type, per 10 minutes. compiler_fault counts compiler bugs, not invalid client input.',
    [
      o.target('sum by (status) (increase(%s{%s, status=~"%s"}[$__interval])) > 0' % [Q, SEL, SERVER_STATUS], '{{status}}', DS, 'A'),
      o.target('sum(increase(%s{%s, failure_reason=~"%s"}[$__interval])) > 0' % [compilerRejected, SEL, COMPILER_FAULT], 'compiler_fault', DS, 'B'),
    ],
    [
      ['execution_error', '#3987e5'], ['streaming_error', '#d95926'], ['timeout', '#c98500'],
      ['content_resolution_error', '#199e70'], ['authorization_error', '#d55181'],
      ['custom_error', '#9085e9'], ['compiler_fault', '#e66767'],
    ],
  ),
  errorBars(
    'Rejected client queries by reason',
    'Queries the compiler rejected as invalid client input, by reason, per 10 minutes. A jump usually means a client or agent changed what it sends.',
    [o.target('sum by (failure_reason) (increase(%s{%s, failure_reason!~"%s"}[$__interval])) > 0' % [compilerRejected, SEL, COMPILER_FAULT], '{{failure_reason}}', DS, 'A')],
    [
      ['schema', '#3987e5'], ['parse', '#d95926'], ['ontology', '#199e70'], ['reference', '#c98500'],
      ['authorization', '#9085e9'], ['restrict', '#008300'],
    ],
  ),
];

// 2. Latency breakdown ----------------------------------------------------
// Where time goes: per query type, per pipeline stage, and ClickHouse work.
local latency = [
  o.row('Latency breakdown'),
  o.timeseries(
    'p95 of successful queries by query type',
    'p95 duration of successful queries per query type over 10-minute windows. One slow query type can move the overall latency on its own.',
    [o.target(
      'histogram_quantile(0.95, sum by (le, query_type) (rate(%s_bucket{%s, status="ok", query_type!="unknown"}[10m])))' % [pipelineDuration.prom_name, SEL],
      '{{query_type}}', DS,
    )],
    's', 12, 8,
  ),
  // Per-stage stacked p95: tells the budget at a glance.
  o.timeseries(
    'Server: pipeline stages p95 (stacked)',
    'p95 per pipeline stage stacked together. The visual height tells you which stage is eating the budget on a slow request — compile, authorization, execute, or hydration.',
    [
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [pipelineCompile.prom_name, SEL], 'compile', DS, 'A'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [pipelineAuthz.prom_name, SEL], 'authorization', DS, 'B'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [pipelineExecute.prom_name, SEL], 'execute', DS, 'C'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [pipelineHydrate.prom_name, SEL], 'hydration', DS, 'D'),
    ],
    's', 12, 8,
  ),
  // Per-stage detail: when a stage spikes, this is where you confirm.
  o.histogramPercentiles(
    pipelineExecute,
    'Server: ClickHouse execute (p50/p95/p99)',
    'p50/p95/p99 of the pipeline execute stage — the time spent waiting on ClickHouse. Execute spike + flat compile/hydrate means a slow query plan, not server overhead.',
    DS, SEL, w=12,
  ),
  o.histogramPercentiles(
    pipelineHydrate,
    'Server: hydration (p50/p95/p99)',
    'p50/p95/p99 of the hydration stage — graph response shaping after ClickHouse returns. Hydration spike + flat execute means the row count is huge; cross-check the result-set rows panel below.',
    DS, SEL, w=12,
  ),
  // ClickHouse work indicators.
  o.timeseries(
    'ClickHouse: bytes / rows read per second',
    'Bytes and rows pulled from ClickHouse per second. Lets you cross-correlate execute-stage latency with the volume of work the query actually did.',
    [
      o.target('sum(rate(%s{%s}[5m]))' % [pipelineChRead.prom_name, SEL], 'bytes/s', DS, 'A'),
      o.target('sum(rate(%s{%s}[5m]))' % [pipelineChRows.prom_name, SEL], 'rows/s', DS, 'B'),
    ],
    'short', 12, 8,
  ),
  o.histogramPercentiles(
    pipelineChMem,
    'ClickHouse: memory_usage_bytes per query (p50/p95/p99)',
    'p50/p95/p99 of ClickHouse memory_usage reported per query. A jump here often correlates with execute-stage latency and is the first signal of a query plan regression.',
    DS, SEL, w=12,
  ),
  o.histogramPercentiles(
    pipelineRows,
    'Server: result-set rows per query (p50/p95/p99)',
    'p50/p95/p99 of how many rows each query returned. A latency climb that tracks a row-count climb is just more work; a climb that does not is a regression.',
    DS, SEL, w=12,
  ),
];

// 3. Reliability ----------------------------------------------------------
local reliability = [
  o.row('Reliability'),
  o.timeseries(
    'Server: pipeline failures by reason (1h windows)',
    'Pipeline failure counts in rolling 1h windows, broken down by failure_reason from `gkg.query.pipeline.failed`. Renders a flat zero baseline when no failures occur in the window.',
    [
      o.target(
        'sum by (failure_reason) (increase(%s{%s}[1h]))' % [pipelineFailed, SEL],
        '{{failure_reason}}', DS, 'A',
      ),
      o.target('vector(0)', 'no failures', DS, 'B'),
    ],
    'short', 12, 8,
  ),
  o.timeseries(
    'Server: compiler rejections by reason (1h windows)',
    'Compiler rejections grouped by failure_reason from `gkg.query.engine.compiler.rejected`: parse/schema/reference/ontology/ontology_internal/depth/limit/security/lowering/enforcement/codegen/pipeline. Renders a flat zero baseline when no rejections occur in the window.',
    [
      o.target(
        'sum by (failure_reason) (increase(%s{%s}[1h]))' % [compilerRejected, SEL],
        '{{failure_reason}}', DS, 'A',
      ),
      o.target('vector(0)', 'no rejections', DS, 'B'),
    ],
    'short', 12, 8,
  ),
  o.gaugeStat(
    'Server: in-flight gRPC requests',
    'Current count of active gRPC requests on the Orbit webserver, summed across pods. A flat ceiling here is the saturation signal for the gRPC server.',
    'sum(rpc_server_active_requests{%s})' % SEL,
    DS, 'short', 12,
  ),
];

// 4. Content resolution (Gitaly) ------------------------------------------
local content = [
  o.row('Content resolution (Gitaly)'),
  o.histogramPercentiles(
    contentResolveDur,
    'Resolve duration (p50/p95/p99)',
    'Time to resolve content for a request via Gitaly. Spikes here usually correspond with execute-stage latency on queries that touch blob content.',
    DS, SEL, w=12,
  ),
  o.histogramPercentiles(
    contentResolveBatch,
    'Resolve batch size (p50/p95/p99)',
    'Number of items per content-resolve batch. Useful for telling apart "many small calls" from "one fat call" when interpreting the duration panel.',
    DS, SEL, w=12,
  ),
] + o.counterPanels(contentGitalyCalls, DS, SEL);

// 5. Schema watcher -------------------------------------------------------
local schema = [
  o.row('Schema watcher'),
] + o.gaugePanels(o.metric('gkg.webserver.schema.state'), DS, SEL);

// 6. Resources -----------------------------------------------------------
// Mirrors the indexer Resources row exactly. Selector flips to
// the webserver container. cAdvisor + kube-state-metrics, no app
// instrumentation needed.
local KUBE_SEL = SEL + ', namespace="gkg"';
local resources = [
  o.row('Resources'),
  o.gaugeStat(
    'CPU: usage / limit (5m)',
    'Aggregate CPU seconds used per second divided by configured CPU limit, across all webserver replicas. 1.0 means the pool is fully consuming its quota.',
    '(sum(rate(container_cpu_usage_seconds_total{%s, cpu="total"}[5m]))) / (sum(kube_pod_container_resource_limits{%s, resource="cpu"}) > 0)' % [SEL, KUBE_SEL],
    DS, 'percentunit', 6,
  ),
  o.gaugeStat(
    'Memory: working set / limit',
    'Working-set memory across all replicas as a fraction of the aggregate memory limit. Watch for sustained values above ~0.85 — that is OOM-kill territory.',
    '(sum(container_memory_working_set_bytes{%s})) / (sum(kube_pod_container_resource_limits{%s, resource="memory"}) > 0)' % [SEL, KUBE_SEL],
    DS, 'percentunit', 6,
  ),
  o.gaugeStat(
    'OOM events (1h)',
    'Total OOM-killer events across webserver replicas in the selected time range. Any non-zero value warrants a look at the memory panel below.',
    'sum(increase(container_oom_events_total{%s}[1h]))' % [SEL],
    DS, 'short', 6,
  ),
  o.gaugeStat(
    'Restarts (1h)',
    'Container restarts across webserver replicas in the selected time range. Crash-loops show up here before they show up in the app metrics.',
    'sum(increase(kube_pod_container_status_restarts_total{%s}[1h]))' % [KUBE_SEL],
    DS, 'short', 6,
  ),
  o.timeseries(
    'CPU: cores used per pod',
    'Per-pod CPU seconds consumed per second. The dashed reference line is the per-pod CPU limit from kube-state-metrics; pods bumping against it should be cross-checked with the throttling panel.',
    [
      o.target(
        'sum by (pod) (rate(container_cpu_usage_seconds_total{%s, cpu="total"}[5m]))' % [SEL],
        '{{pod}}', DS, 'A',
      ),
      o.target(
        'avg(kube_pod_container_resource_limits{%s, resource="cpu"})' % [KUBE_SEL],
        'limit', DS, 'B',
      ),
    ],
    'short', 12, 8,
  ),
  o.timeseries(
    'CPU: throttled time fraction per pod',
    'Share of CFS periods where the cgroup was throttled. Anything sustained above ~5% means the pod is hitting its CPU quota and requests will queue.',
    [o.target(
      '(sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{%s}[5m]))) / (sum by (pod) (rate(container_cpu_cfs_periods_total{%s}[5m])) > 0)' % [SEL, SEL],
      '{{pod}}', DS,
    )],
    'percentunit', 12, 8,
  ),
  o.timeseries(
    'Memory: working set per pod',
    'container_memory_working_set_bytes per replica. The dashed reference line is the per-pod memory limit. Working set is what the OOM killer reads, not RSS.',
    [
      o.target(
        'sum by (pod) (container_memory_working_set_bytes{%s})' % [SEL],
        '{{pod}}', DS, 'A',
      ),
      o.target(
        'avg(kube_pod_container_resource_limits{%s, resource="memory"})' % [KUBE_SEL],
        'limit', DS, 'B',
      ),
    ],
    'bytes', 12, 8,
  ),
  o.timeseries(
    'Memory: RSS and cache per pod',
    'Resident set and page cache, per pod. RSS dominating with a tiny cache often means heap growth; cache dominating typically means heavy I/O.',
    [
      o.target(
        'sum by (pod) (container_memory_rss{%s})' % [SEL],
        'rss / {{pod}}', DS, 'A',
      ),
      o.target(
        'sum by (pod) (container_memory_cache{%s})' % [SEL],
        'cache / {{pod}}', DS, 'B',
      ),
    ],
    'bytes', 12, 8,
  ),
  o.timeseries(
    'Filesystem I/O bytes per pod',
    'Read and write throughput from cAdvisor. Sustained climbs here on the webserver usually point at log volume or temp-file churn.',
    [
      o.target(
        'sum by (pod) (rate(container_fs_reads_bytes_total{%s}[5m]))' % [SEL],
        'read / {{pod}}', DS, 'A',
      ),
      o.target(
        'sum by (pod) (rate(container_fs_writes_bytes_total{%s}[5m]))' % [SEL],
        'write / {{pod}}', DS, 'B',
      ),
    ],
    'Bps', 12, 8,
  ),
  o.timeseries(
    'Pressure stall: IO and memory',
    'PSI seconds per second. IO pressure rising means processes are waiting on disk; memory pressure rising means the kernel is reclaiming pages, often the leading edge of an OOM.',
    [
      o.target(
        'sum by (pod) (rate(container_pressure_io_waiting_seconds_total{%s}[5m]))' % [SEL],
        'io / {{pod}}', DS, 'A',
      ),
      o.target(
        'sum by (pod) (rate(container_pressure_memory_waiting_seconds_total{%s}[5m]))' % [SEL],
        'mem / {{pod}}', DS, 'B',
      ),
    ],
    's', 12, 8,
  ),
  o.timeseries(
    'Threads and sockets per pod',
    'OS-level concurrency counters. Threads climbing without a corresponding workload increase usually points at a tokio blocking-pool growth event; sockets climbing flags a connection leak.',
    [
      o.target(
        'sum by (pod) (container_threads{%s})' % [SEL],
        'threads / {{pod}}', DS, 'A',
      ),
      o.target(
        'sum by (pod) (container_sockets{%s})' % [SEL],
        'sockets / {{pod}}', DS, 'B',
      ),
    ],
    'short', 12, 8,
  ),
];

// 7. Reference (collapsed by default) ------------------------------------
local reference =
  o.externalSection('gRPC transport', ext.GKG_GRPC, DS, SEL)
  + o.sectionCollapsed('Query pipeline (reference)', o.metricsInDomain('query.pipeline'), DS, SEL)
  + o.sectionCollapsed('Query engine — threats (reference)', o.metricsInDomain('query.engine'), DS, SEL)
  + o.sectionCollapsed('Content resolution (reference)', o.metricsInDomain('server.content'), DS, SEL)
  + o.sectionCollapsed('Schema watcher (reference)', o.metricsInDomain('server.schema_watcher'), DS, SEL)
  + o.sectionCollapsed('Billing events (reference)', o.metricsInDomain('billing.events'), DS, SEL)
  + o.sectionCollapsed('Billing quota (reference)', o.metricsInDomain('billing.quota'), DS, SEL)
  + o.sectionCollapsed('Circuit breaker (reference)', o.metricsInDomain('resilience.circuit_breaker'), DS, o.GKG_ANY_SEL);

local items =
  glance
  + latency
  + reliability
  + content
  + schema
  + resources
  + reference;

local annotations = [
  o.deployAnnotation(DS, SEL),
];

o.dashboard(
  'orbit-webserver',
  'Orbit — Webserver',
  ['webserver'],
  'Orbit webserver dashboard. The top section answers whether the service is healthy: throughput, success rate, latency of successful queries, and errors by type. Lower rows break down latency, reliability, and resources. The per-domain catalog reference is collapsed at the bottom.',
  items,
  annotations,
)
