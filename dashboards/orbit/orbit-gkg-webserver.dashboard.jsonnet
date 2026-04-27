// Orbit — GKG webserver.
//
// Layout is story-shaped, not catalog-shaped. The narrative is the
// path of a single query: a Rails request lands on the GKG webserver
// over gRPC, runs the query pipeline (authorize → compile → execute
// → hydrate), the server returns rows, and Rails redacts before
// handing the response to the user. Each row of the dashboard is a
// stop along that path.
//
//   1. Health — four headline numbers for at-a-glance status.
//   2. Volume — "how many queries / redactions / CH rows in the
//               dashboard window?" stat tiles.
//   3. Latency story — Rails-observed gRPC, server pipeline, per
//               stage breakdown, ClickHouse work, and Rails redaction.
//               Lets you see where wall-clock time is going.
//   4. Reliability — server pipeline errors, Rails gRPC errors,
//               threat counters.
//   5. Traversal compaction — Rails traversal-ID compaction signals.
//   6. Content resolution — Gitaly blob fetches.
//   7. Schema watcher — webserver schema-readiness gate.
//   8. Resources — pod-level CPU, memory, FS I/O, pressure, OOM,
//               restarts (mirrors the indexer dashboard).
//   9. Reference — every metric in every server / query domain,
//               collapsed by default. Kept as the deep-debug fallback.

local o = import 'lib/orbit.libsonnet';
local ext = import 'lib/external.libsonnet';

local DS = 'ORBIT_DS';
local RDS = 'RAILS_DS';
local SEL = o.GKG_WEB_SEL;
local RAIL = o.RAILS_SEL;

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

local pipelineErrAuthZ = 'gkg_query_pipeline_error_authorization_failed_total';
local pipelineErrContent = 'gkg_query_pipeline_error_content_resolution_failed_total';
local pipelineErrExec = 'gkg_query_pipeline_error_execution_failed_total';
local pipelineErrSec = 'gkg_query_pipeline_error_security_rejected_total';
local pipelineErrStream = 'gkg_query_pipeline_error_streaming_failed_total';

local contentResolveDur = o.metric('gkg.content.resolve.duration');
local contentResolveBatch = o.metric('gkg.content.resolve.batch_size');
local contentGitalyCalls = o.metric('gkg.content.gitaly.calls');

local railsGrpcDur = 'gitlab_knowledge_graph_grpc_duration_seconds';
local railsGrpcErr = 'gitlab_knowledge_graph_grpc_errors_total';
local railsRedactDur = 'gitlab_knowledge_graph_redaction_duration_seconds';
local railsRedactBatch = 'gitlab_knowledge_graph_redaction_batch_size';
local railsRedactFiltered = 'gitlab_knowledge_graph_redaction_filtered_count';
local railsJwtDur = 'gitlab_knowledge_graph_jwt_build_duration_seconds';
local railsAuthCtxDur = 'gitlab_knowledge_graph_auth_context_duration_seconds';
local railsTraversalIds = 'gitlab_knowledge_graph_traversal_ids_count';
local railsCompactRatio = 'gitlab_knowledge_graph_compaction_ratio';
local railsCompactFallback = 'gitlab_knowledge_graph_compaction_fallback_total';
local railsTraversalThresh = 'gitlab_knowledge_graph_traversal_ids_threshold_exceeded_total';

// 1. Health ---------------------------------------------------------------
local health = [
  o.row('Health'),
  o.gaugeStat(
    'Rails: gRPC p95 (5m)',
    'p95 of the user-facing Rails-side gRPC call to GKG over the last 5 minutes. This is the latency the GitLab.com web request actually experiences.',
    'histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsGrpcDur, RAIL],
    RDS, 's', 6,
  ),
  o.gaugeStat(
    'Pipeline: success rate (1h)',
    'Share of query pipeline calls with status="ok" over the last hour. Renders 0 when no calls happened in the window.',
    '(sum(rate(%s{%s, status="ok"}[1h]))) / (sum(rate(%s{%s}[1h])) > 0)' % [pipelineQueries.prom_name, SEL, pipelineQueries.prom_name, SEL],
    DS, 'percentunit', 6,
  ),
  o.gaugeStat(
    'Pipeline: p95 (5m)',
    'p95 of overall pipeline duration on the GKG webserver side. Compared with the Rails p95 above, the difference is network + JWT/auth + redaction.',
    'histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [pipelineDuration.prom_name, SEL],
    DS, 's', 6,
  ),
  o.gaugeStat(
    'Threat rejects / min (5m)',
    'Pipeline calls rejected for security policy violations. Includes rate-limit, depth, validation, allowlist, auth-filter, and timeout rejects.',
    'sum(rate(gkg_query_engine_threat_allowlist_rejected_total{%s}[5m]) + rate(gkg_query_engine_threat_auth_filter_missing_total{%s}[5m]) + rate(gkg_query_engine_threat_depth_exceeded_total{%s}[5m]) + rate(gkg_query_engine_threat_limit_exceeded_total{%s}[5m]) + rate(gkg_query_engine_threat_rate_limited_total{%s}[5m]) + rate(gkg_query_engine_threat_timeout_total{%s}[5m]) + rate(gkg_query_engine_threat_validation_failed_total{%s}[5m])) * 60' % [SEL, SEL, SEL, SEL, SEL, SEL, SEL],
    DS, 'short', 6,
  ),
];

// 2. Volume in window -----------------------------------------------------
local volume = [
  o.row('Volume in window'),
] + o.volumeTiles([
  {
    prom: pipelineQueries.prom_name,
    title: 'Queries (server)',
    desc: 'Query pipeline calls on the GKG webserver in the dashboard window. Click the arrow to open the per-status rate in Explore.',
  },
  {
    prom: railsGrpcDur + '_count',
    title: 'gRPC calls (Rails)',
    desc: 'Rails-side gRPC calls into GKG in the dashboard window. Should track closely with server-side queries; a divergence flags either a Rails-side retry storm or server-side rejected requests not counted on Rails.',
  },
  {
    prom: railsRedactDur + '_count',
    title: 'Redactions (Rails)',
    desc: 'Rails-side redaction passes in the dashboard window. Roughly one per successful query.',
  },
  {
    prom: pipelineChRows.prom_name + '_count',
    title: 'CH row reads',
    desc: 'ClickHouse row-read events in the dashboard window. Bytes read tile sits next to it.',
  },
], DS, SEL, w=6);

// 3. Latency story --------------------------------------------------------
// The wall-clock journey of a single query, top-to-bottom:
//   Rails gRPC (user-facing) > server pipeline > pipeline stages >
//   ClickHouse work > Rails redaction. The order matches request flow.
local latency = [
  o.row('Latency story (Rails → Server → ClickHouse → Rails redaction)'),
  // Rails gRPC, by method — what users actually feel.
  o.timeseries(
    'Rails: gRPC duration p95 by method (5m)',
    'p95 of the Rails-side gRPC call to GKG, broken down by RPC method. This is the user-visible latency. Use it as the ceiling — every other panel in this row should sit underneath it.',
    [o.target(
      'histogram_quantile(0.95, sum by (method, le) (rate(%s_bucket{%s}[5m])))' % [railsGrpcDur, RAIL],
      '{{method}}',
      RDS,
    )],
    's', 12, 8,
  ),
  // Server pipeline, all-up.
  o.histogramPercentiles(
    pipelineDuration,
    'Server: pipeline duration (p50/p95/p99)',
    'GKG-side query pipeline duration percentiles. The gap between this and the Rails p95 above is wire time + JWT/auth-context build + redaction.',
    DS, SEL, w=12,
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
  // Rails redaction — the last leg of the user-facing latency.
  o.timeseries(
    'Rails: redaction duration p50/p95/p99 (5m)',
    'p50/p95/p99 of redaction time on the Rails side, after GKG returns. Adds onto the server pipeline duration to make the total the user sees.',
    [
      o.target('histogram_quantile(0.50, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsRedactDur, RAIL], 'p50', RDS, 'A'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsRedactDur, RAIL], 'p95', RDS, 'B'),
      o.target('histogram_quantile(0.99, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsRedactDur, RAIL], 'p99', RDS, 'C'),
    ],
    's', 12, 8,
  ),
  o.timeseries(
    'Rails: redaction batch size and filtered rows (p95)',
    'p95 of how many rows redaction sees per request, and how many it filters out. Big batch + low filtered count is fine; small batch + everything filtered is the worst case for the user (lots of work, nothing returned).',
    [
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsRedactBatch, RAIL], 'batch p95', RDS, 'A'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsRedactFiltered, RAIL], 'filtered p95', RDS, 'B'),
    ],
    'short', 12, 8,
  ),
  // Wall-clock split: Rails total minus server pipeline.
  o.timeseries(
    'Rails total p95 vs Server pipeline p95',
    'Rails-observed gRPC p95 plotted next to the server pipeline p95. The gap is the round-trip wire time plus JWT, auth-context, and redaction. A growing gap with stable server p95 means the slowdown is on the Rails side.',
    [
      o.target(
        'histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsGrpcDur, RAIL],
        'rails total p95', RDS, 'A',
      ),
      o.target(
        'histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [pipelineDuration.prom_name, SEL],
        'server pipeline p95', DS, 'B',
      ),
    ],
    's', 12, 8,
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
  // Rails JWT and auth context — the part of "server-side delay" that
  // happens on Rails before the gRPC call even fires.
  o.timeseries(
    'Rails: JWT build and auth-context p95 (5m)',
    'Time spent on Rails preparing the JWT and the auth-context to attach to the gRPC call. Climbs here mean the Rails-side request setup is the bottleneck, not GKG.',
    [
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsJwtDur, RAIL], 'jwt p95', RDS, 'A'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsAuthCtxDur, RAIL], 'auth_ctx p95', RDS, 'B'),
    ],
    's', 12, 8,
  ),
  // Result-set size: useful denominator for the latency panels above.
  o.histogramPercentiles(
    pipelineRows,
    'Server: result-set rows per query (p50/p95/p99)',
    'p50/p95/p99 of how many rows each query returned. A latency climb that tracks a row-count climb is just more work; a climb that does not is a regression.',
    DS, SEL, w=12,
  ),
];

// 4. Reliability ----------------------------------------------------------
local reliability = [
  o.row('Reliability'),
  o.timeseries(
    'Server: pipeline errors by kind (1h windows)',
    'GKG-side pipeline error counts in rolling 1h windows, one series per error kind. Lets you tell apart authorization failures, content resolution failures, execution failures, security rejections, and streaming failures.',
    [
      o.target('sum(increase(%s{%s}[1h]))' % [pipelineErrAuthZ, SEL], 'authorization_failed', DS, 'A'),
      o.target('sum(increase(%s{%s}[1h]))' % [pipelineErrContent, SEL], 'content_resolution_failed', DS, 'B'),
      o.target('sum(increase(%s{%s}[1h]))' % [pipelineErrExec, SEL], 'execution_failed', DS, 'C'),
      o.target('sum(increase(%s{%s}[1h]))' % [pipelineErrSec, SEL], 'security_rejected', DS, 'D'),
      o.target('sum(increase(%s{%s}[1h]))' % [pipelineErrStream, SEL], 'streaming_failed', DS, 'E'),
    ],
    'short', 12, 8,
  ),
  o.timeseries(
    'Rails: gRPC errors by code (1h windows)',
    'Rails-side gRPC error counts in rolling 1h windows, by gRPC status code. Auth or unavailable spikes here often line up with the server reliability panel on the left.',
    [o.target(
      'sum by (code) (increase(%s{%s}[1h]))' % [railsGrpcErr, RAIL],
      '{{code}}', RDS,
    )],
    'short', 12, 8,
  ),
  o.timeseries(
    'Server: threat counters by kind (1h windows)',
    'Threat-policy rejections grouped by reason. These are not application errors — they are intentional rejections for rate, depth, validation, allowlist, missing-auth-filter, timeout, or validation issues.',
    [
      o.target('sum(increase(gkg_query_engine_threat_rate_limited_total{%s}[1h]))' % SEL, 'rate_limited', DS, 'A'),
      o.target('sum(increase(gkg_query_engine_threat_depth_exceeded_total{%s}[1h]))' % SEL, 'depth_exceeded', DS, 'B'),
      o.target('sum(increase(gkg_query_engine_threat_limit_exceeded_total{%s}[1h]))' % SEL, 'limit_exceeded', DS, 'C'),
      o.target('sum(increase(gkg_query_engine_threat_validation_failed_total{%s}[1h]))' % SEL, 'validation_failed', DS, 'D'),
      o.target('sum(increase(gkg_query_engine_threat_allowlist_rejected_total{%s}[1h]))' % SEL, 'allowlist_rejected', DS, 'E'),
      o.target('sum(increase(gkg_query_engine_threat_auth_filter_missing_total{%s}[1h]))' % SEL, 'auth_filter_missing', DS, 'F'),
      o.target('sum(increase(gkg_query_engine_threat_timeout_total{%s}[1h]))' % SEL, 'timeout', DS, 'G'),
    ],
    'short', 12, 8,
  ),
  o.gaugeStat(
    'Server: in-flight gRPC requests',
    'Current count of active gRPC requests on the GKG webserver, summed across pods. A flat ceiling here is the saturation signal for the gRPC server.',
    'sum(rpc_server_active_requests{%s})' % SEL,
    DS, 'short', 12,
  ),
];

// 5. Traversal compaction (Rails) ----------------------------------------
// Compaction encodes traversal-ID lists into a smaller form before the
// gRPC call. Useful as a separate row because regressions here show up
// as Rails-side cost without changing the server pipeline.
local traversal = [
  o.row('Traversal compaction (Rails)'),
  o.timeseries(
    'Rails: traversal IDs per request (p50/p95)',
    'Number of traversal IDs Rails attaches to each gRPC call (pre-compaction). p95 climbing means Rails is sending bigger authorization payloads.',
    [
      o.target('histogram_quantile(0.50, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsTraversalIds, RAIL], 'p50', RDS, 'A'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsTraversalIds, RAIL], 'p95', RDS, 'B'),
    ],
    'short', 12, 8,
  ),
  o.timeseries(
    'Rails: compaction ratio (p50/p95)',
    'Ratio of compacted size to original. Lower is better — 0.1 means the compaction reduced the payload to 10% of original. Watch for sustained 1.0 (no benefit).',
    [
      o.target('histogram_quantile(0.50, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsCompactRatio, RAIL], 'p50', RDS, 'A'),
      o.target('histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[5m])))' % [railsCompactRatio, RAIL], 'p95', RDS, 'B'),
    ],
    'percentunit', 12, 8,
  ),
  o.timeseries(
    'Rails: compaction fallback / threshold-exceeded per minute',
    'Rate of requests where compaction fell back to the uncompressed form (left) or where the traversal ID list exceeded the threshold (right). Rising fallback often precedes a tail-latency event on the Rails side.',
    [
      o.target('sum(rate(%s{%s}[5m])) * 60' % [railsCompactFallback, RAIL], 'fallback/min', RDS, 'A'),
      o.target('sum(rate(%s{%s}[5m])) * 60' % [railsTraversalThresh, RAIL], 'threshold-exceeded/min', RDS, 'B'),
    ],
    'short', 12, 8,
  ),
];

// 6. Content resolution (Gitaly) ------------------------------------------
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

// 7. Schema watcher -------------------------------------------------------
local schema = [
  o.row('Schema watcher'),
] + o.gaugePanels(o.metric('gkg.webserver.schema.state'), DS, SEL);

// 8. Resources -----------------------------------------------------------
// Mirrors the indexer Resources row exactly. Selector flips to
// container=gkg-webserver. cAdvisor + kube-state-metrics, no app
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
    'Total OOM-killer events across webserver replicas in the last hour. Any non-zero value warrants a look at the memory panel below.',
    'sum(increase(container_oom_events_total{%s}[1h]))' % [SEL],
    DS, 'short', 6,
  ),
  o.gaugeStat(
    'Restarts (1h)',
    'Container restarts across webserver replicas in the last hour. Crash-loops show up here before they show up in the app metrics.',
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

// 9. Reference (collapsed by default) ------------------------------------
local reference =
  o.externalSection('HTTP transport', ext.GKG_HTTP, DS, SEL)
  + o.externalSection('gRPC transport', ext.GKG_GRPC, DS, SEL)
  + o.sectionCollapsed('Query pipeline (reference)', o.metricsInDomain('query.pipeline'), DS, SEL)
  + o.sectionCollapsed('Query engine — threats (reference)', o.metricsInDomain('query.engine'), DS, SEL)
  + o.sectionCollapsed('Content resolution (reference)', o.metricsInDomain('server.content'), DS, SEL)
  + o.sectionCollapsed('Schema watcher (reference)', o.metricsInDomain('server.schema_watcher'), DS, SEL)
  + o.externalSection('Rails KG — request (reference)', ext.RAILS_KG_REQUEST, RDS, RAIL)
  + o.externalSection('Rails KG — traversal (reference)', ext.RAILS_KG_TRAVERSAL, RDS, RAIL);

local items =
  health
  + volume
  + latency
  + reliability
  + traversal
  + content
  + schema
  + resources
  + reference;

o.dashboard(
  'orbit-gkg-webserver',
  'Orbit — GKG webserver',
  ['gkg', 'webserver'],
  'GKG webserver dashboard. Top rows tell the latency story: a Rails request lands on GKG over gRPC, the pipeline runs (authorize → compile → execute → hydrate), and Rails redacts before the response goes out. Bottom rows are reliability, resources, and the per-domain catalog reference (collapsed by default).',
  items,
)
