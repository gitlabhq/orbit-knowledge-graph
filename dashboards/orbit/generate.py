#!/usr/bin/env python3
"""
Generate a set of importable Grafana dashboards for the Orbit stack.

Outputs (one JSON per dashboard, importable via Grafana's UI):
  - orbit-overview.dashboard.json        — golden signals + links to sub-dashboards
  - orbit-gkg-webserver.dashboard.json   — all webserver metrics
  - orbit-gkg-indexer.dashboard.json     — all indexer metrics
  - orbit-siphon.dashboard.json          — producers + consumers
  - orbit-nats.dashboard.json            — NATS JetStream + varz
  - orbit-rails-kg.dashboard.json        — Rails gitlab_knowledge_graph_*
  - orbit-all-metrics.dashboard.json     — the original kitchen-sink dump

Every dashboard has these template variables:
  - $ORBIT_DS   datasource — default "Mimir - Analytics Eventsdot"
  - $RAILS_DS   datasource — default "Mimir - Gitlab Gstg"
  - $cluster    orbit-stg | orbit-prd | orbit-.*
  - $rails_env  gstg | gprd | gstg|gprd

Selectors match the real labels observed on dashboards.gitlab.net:
  GKG:    container=gkg-{webserver,indexer}, cluster=orbit-*, namespace=gkg
  Siphon: namespace=siphon, cluster=orbit-*
  Rails:  env in {gstg,gprd}

Run:  python3 generate.py
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

GRID_WIDTH = 24
PANEL_W = 8
PANEL_H = 8
ROW_H = 1

# ---------------------------------------------------------------------------
# Metric catalogue
# ---------------------------------------------------------------------------

# (prom_name, kind, labels)
# kind: "counter" | "histogram" | "gauge" | "udc"

GKG_HTTP = [
    ("http_server_request_duration_seconds", "histogram", ["http_route", "http_response_status_code", "http_request_method"]),
    ("http_server_active_requests", "udc", ["http_route", "http_request_method"]),
    ("http_server_request_body_size_bytes", "histogram", ["http_route"]),
    ("http_server_response_body_size_bytes", "histogram", ["http_route"]),
]

GKG_GRPC = [
    ("rpc_server_duration_seconds", "histogram", ["rpc_service", "rpc_method", "rpc_grpc_status_code"]),
    ("rpc_server_active_requests", "udc", ["rpc_service", "rpc_method"]),
    ("rpc_server_requests_per_rpc", "histogram", ["rpc_service", "rpc_method"]),
    ("rpc_server_responses_per_rpc", "histogram", ["rpc_service", "rpc_method"]),
]

GKG_ETL = [
    ("gkg_etl_messages_processed_total", "counter", ["topic", "outcome"]),
    ("gkg_etl_message_duration_seconds", "histogram", ["topic"]),
    ("gkg_etl_handler_duration_seconds", "histogram", ["handler"]),
    ("gkg_etl_permit_wait_duration_seconds", "histogram", []),
    ("gkg_etl_permits_active", "udc", []),
    ("gkg_etl_nats_fetch_duration_seconds", "histogram", ["outcome"]),
    ("gkg_etl_destination_write_duration_seconds", "histogram", ["table"]),
    ("gkg_etl_destination_rows_written_total", "counter", ["table"]),
    ("gkg_etl_destination_bytes_written_bytes_total", "counter", ["table"]),
    ("gkg_etl_destination_write_errors_total", "counter", ["table"]),
    ("gkg_etl_handler_errors_total", "counter", ["handler", "error_kind"]),
]

GKG_MIGRATION = [
    ("gkg_schema_migration_total", "counter", ["phase", "result"]),
    ("gkg_schema_migration_completed_total", "counter", []),
    ("gkg_schema_cleanup_total", "counter", ["version", "result"]),
]

GKG_SCHEDULER = [
    ("gkg_scheduler_task_runs_total", "counter", ["task", "outcome"]),
    ("gkg_scheduler_task_duration_seconds", "histogram", ["task"]),
    ("gkg_scheduler_task_requests_published_total", "counter", ["task"]),
    ("gkg_scheduler_task_requests_skipped_total", "counter", ["task"]),
    ("gkg_scheduler_task_query_duration_seconds", "histogram", ["query"]),
    ("gkg_scheduler_task_errors_total", "counter", ["task", "stage"]),
]

GKG_CODE = [
    ("gkg_indexer_code_events_processed_total", "counter", ["outcome"]),
    ("gkg_indexer_code_handler_duration_seconds", "histogram", []),
    ("gkg_indexer_code_repository_fetch_duration_seconds", "histogram", []),
    ("gkg_indexer_code_repository_resolution_total", "counter", ["strategy"]),
    ("gkg_indexer_code_repository_cleanup_total", "counter", ["outcome"]),
    ("gkg_indexer_code_repository_empty_total", "counter", ["reason"]),
    ("gkg_indexer_code_indexing_duration_seconds", "histogram", []),
    ("gkg_indexer_code_files_processed_total", "counter", ["outcome"]),
    ("gkg_indexer_code_nodes_indexed_total", "counter", ["kind"]),
    ("gkg_indexer_code_errors_total", "counter", ["stage"]),
]

GKG_SDLC = [
    ("gkg_indexer_sdlc_pipeline_duration_seconds", "histogram", ["entity"]),
    ("gkg_indexer_sdlc_pipeline_rows_processed_total", "counter", ["entity"]),
    ("gkg_indexer_sdlc_pipeline_errors_total", "counter", ["entity", "error_kind"]),
    ("gkg_indexer_sdlc_handler_duration_seconds", "histogram", ["handler"]),
    ("gkg_indexer_sdlc_datalake_query_duration_seconds", "histogram", ["entity"]),
    ("gkg_indexer_sdlc_datalake_query_bytes_bytes_total", "counter", ["entity"]),
    ("gkg_indexer_sdlc_transform_duration_seconds", "histogram", ["entity"]),
    ("gkg_indexer_sdlc_watermark_lag_seconds", "gauge", ["entity"]),
]

GKG_DELETION = [
    ("gkg_indexer_namespace_deletion_table_duration_seconds", "histogram", ["table"]),
    ("gkg_indexer_namespace_deletion_table_errors_total", "counter", ["table"]),
]

GKG_QUERY = [
    ("gkg_query_pipeline_queries_total", "counter", ["query_type", "status"]),
    ("gkg_query_pipeline_compile_duration_seconds", "histogram", ["query_type"]),
    ("gkg_query_pipeline_duration_seconds", "histogram", ["query_type", "status"]),
    ("gkg_query_pipeline_execute_duration_seconds", "histogram", ["query_type"]),
    ("gkg_query_pipeline_authorization_duration_seconds", "histogram", ["query_type"]),
    ("gkg_query_pipeline_hydration_duration_seconds", "histogram", ["query_type"]),
    ("gkg_query_pipeline_result_set_size", "histogram", ["query_type"]),
    ("gkg_query_pipeline_batch_count", "histogram", ["query_type"]),
    ("gkg_query_pipeline_redacted_count", "histogram", ["query_type"]),
    ("gkg_query_pipeline_ch_read_rows_total", "counter", ["query_type", "label"]),
    ("gkg_query_pipeline_ch_read_bytes_bytes_total", "counter", ["query_type", "label"]),
    ("gkg_query_pipeline_ch_memory_usage_bytes", "histogram", ["query_type", "label"]),
    ("gkg_query_pipeline_error_security_rejected_total", "counter", ["reason"]),
    ("gkg_query_pipeline_error_execution_failed_total", "counter", ["reason"]),
    ("gkg_query_pipeline_error_authorization_failed_total", "counter", ["reason"]),
    ("gkg_query_pipeline_error_content_resolution_failed_total", "counter", ["reason"]),
    ("gkg_query_pipeline_error_streaming_failed_total", "counter", ["reason"]),
]

GKG_CONTENT = [
    ("gkg_content_resolve_duration_seconds", "histogram", ["outcome"]),
    ("gkg_content_resolve_total", "counter", ["outcome"]),
    ("gkg_content_resolve_batch_size", "histogram", []),
    ("gkg_content_blob_size_bytes", "histogram", []),
    ("gkg_content_gitaly_calls_total", "counter", []),
]

GKG_SCHEMA_WATCHER = [
    ("gkg_webserver_schema_state", "gauge", ["state"]),
]

GKG_QENGINE = [
    ("gkg_query_engine_threat_validation_failed_total", "counter", ["reason"]),
    ("gkg_query_engine_threat_allowlist_rejected_total", "counter", ["reason"]),
    ("gkg_query_engine_threat_auth_filter_missing_total", "counter", ["reason"]),
    ("gkg_query_engine_threat_timeout_total", "counter", ["reason"]),
    ("gkg_query_engine_threat_rate_limited_total", "counter", ["reason"]),
    ("gkg_query_engine_threat_depth_exceeded_total", "counter", ["reason"]),
    ("gkg_query_engine_threat_limit_exceeded_total", "counter", ["reason"]),
    ("gkg_query_engine_internal_pipeline_invariant_violated_total", "counter", ["reason"]),
]

SIPHON_PRODUCERS = [
    ("siphon_operations_total", "counter", ["app_id", "container"]),
]

SIPHON_CONSUMERS = [
    ("siphon_clickhouse_consumer_number_of_events", "counter", ["product_app_id"]),
]

NATS = [
    ("nats_varz_in_msgs", "counter", []),
    ("nats_varz_out_msgs", "counter", []),
    ("nats_varz_in_bytes", "counter", []),
    ("nats_varz_out_bytes", "counter", []),
    ("nats_varz_slow_consumers", "counter", []),
    ("nats_stream_total_messages", "gauge", ["stream_name"]),
    ("nats_stream_total_bytes", "gauge", ["stream_name"]),
    ("nats_consumer_num_pending", "gauge", ["stream_name", "consumer_name"]),
    ("nats_consumer_num_redelivered", "counter", ["stream_name", "consumer_name"]),
    ("nats_consumer_num_ack_pending", "gauge", ["stream_name", "consumer_name"]),
]

RAILS_KG_REQUEST = [
    ("gitlab_knowledge_graph_grpc_duration_seconds", "histogram", ["method", "status"]),
    ("gitlab_knowledge_graph_grpc_errors_total", "counter", ["method", "code"]),
    ("gitlab_knowledge_graph_redaction_duration_seconds", "histogram", []),
    ("gitlab_knowledge_graph_redaction_batch_size", "histogram", []),
    ("gitlab_knowledge_graph_redaction_filtered_count", "histogram", []),
    ("gitlab_knowledge_graph_jwt_build_duration_seconds", "histogram", []),
    ("gitlab_knowledge_graph_auth_context_duration_seconds", "histogram", []),
]

RAILS_KG_TRAVERSAL = [
    ("gitlab_knowledge_graph_traversal_ids_count", "histogram", []),
    ("gitlab_knowledge_graph_compaction_ratio", "histogram", []),
    ("gitlab_knowledge_graph_compaction_fallback_total", "counter", []),
    ("gitlab_knowledge_graph_traversal_ids_threshold_exceeded_total", "counter", []),
]


# ---------------------------------------------------------------------------
# Panel builder
# ---------------------------------------------------------------------------


@dataclass
class Layout:
    y: int = 0
    panel_id: int = 1

    def next_id(self) -> int:
        i = self.panel_id
        self.panel_id += 1
        return i


def ds(uid_var: str) -> dict:
    return {"type": "prometheus", "uid": f"${uid_var}"}


def target(expr: str, legend: str, ds_var: str, ref: str = "A") -> dict:
    return {
        "datasource": ds(ds_var),
        "expr": expr,
        "legendFormat": legend,
        "refId": ref,
    }


def row(title: str, layout: Layout, collapsed: bool = False) -> dict:
    r = {
        "id": layout.next_id(),
        "type": "row",
        "title": title,
        "collapsed": collapsed,
        "gridPos": {"h": ROW_H, "w": GRID_WIDTH, "x": 0, "y": layout.y},
        "panels": [],
    }
    layout.y += ROW_H
    return r


def panel(
    layout: Layout,
    title: str,
    targets: list[dict],
    x: int,
    y: int,
    w: int = PANEL_W,
    h: int = PANEL_H,
    unit: str = "short",
    desc: str = "",
    ptype: str = "timeseries",
) -> dict:
    return {
        "id": layout.next_id(),
        "type": ptype,
        "title": title,
        "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "datasource": targets[0]["datasource"] if targets else ds("ORBIT_DS"),
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "smooth",
                    "fillOpacity": 10,
                    "showPoints": "auto",
                },
                "unit": unit,
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
            "tooltip": {"mode": "multi"},
        },
    }


def stat_panel(
    layout: Layout,
    title: str,
    targets: list[dict],
    x: int,
    y: int,
    w: int = PANEL_W,
    h: int = 4,
    unit: str = "short",
    desc: str = "",
) -> dict:
    return {
        "id": layout.next_id(),
        "type": "stat",
        "title": title,
        "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "datasource": targets[0]["datasource"] if targets else ds("ORBIT_DS"),
        "targets": targets,
        "fieldConfig": {
            "defaults": {"unit": unit, "noValue": "—"},
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "value",
            "graphMode": "area",
            "textMode": "value_and_name",
        },
    }


def legend_for(labels: list[str]) -> str:
    if not labels:
        return "{{instance}}"
    return " / ".join("{{" + l + "}}" for l in labels)


def unit_for(metric: str) -> str:
    if metric.endswith("_seconds") or metric.endswith("_seconds_total"):
        return "s"
    if metric.endswith("_bytes") or metric.endswith("_bytes_total"):
        return "bytes"
    return "short"


def panels_for_counter(layout, metric, labels, ds_var, sel, x, y):
    if labels:
        by = ", ".join(labels)
        expr = f"sum by ({by}) (rate({metric}{{{sel}}}[5m]))"
    else:
        expr = f"sum(rate({metric}{{{sel}}}[5m]))"
    unit = "Bps" if metric.endswith("_bytes_total") else "short"
    return [
        panel(
            layout,
            f"{metric} — rate",
            [target(expr, legend_for(labels), ds_var)],
            x=x, y=y, unit=unit,
            desc=f"5m rate of `{metric}`" + (f" grouped by {labels}" if labels else "") + ".",
        )
    ]


def panels_for_histogram(layout, metric, labels, ds_var, sel, x, y):
    unit = "s" if metric.endswith("_seconds") else ("bytes" if metric.endswith("_bytes") else "short")
    bucket = metric + "_bucket"
    count = metric + "_count"
    by_labels = ["le"] + labels
    by = ", ".join(by_labels)
    lg = legend_for(labels) or "overall"
    lat_targets = [
        target(f"histogram_quantile(0.50, sum by ({by}) (rate({bucket}{{{sel}}}[5m])))", f"p50 {lg}", ds_var, "A"),
        target(f"histogram_quantile(0.95, sum by ({by}) (rate({bucket}{{{sel}}}[5m])))", f"p95 {lg}", ds_var, "B"),
        target(f"histogram_quantile(0.99, sum by ({by}) (rate({bucket}{{{sel}}}[5m])))", f"p99 {lg}", ds_var, "C"),
    ]
    lat = panel(
        layout,
        f"{metric} — p50/p95/p99",
        lat_targets,
        x=x, y=y, unit=unit,
        desc=f"Quantiles from `{bucket}` over 5m.",
    )
    if labels:
        by2 = ", ".join(labels)
        rate_expr = f"sum by ({by2}) (rate({count}{{{sel}}}[5m]))"
    else:
        rate_expr = f"sum(rate({count}{{{sel}}}[5m]))"
    rate = panel(
        layout,
        f"{metric} — observation rate",
        [target(rate_expr, legend_for(labels) or "count", ds_var)],
        x=x + PANEL_W, y=y, unit="short",
        desc=f"Observations per second from `{count}`.",
    )
    return [lat, rate]


def panels_for_gauge(layout, metric, labels, ds_var, sel, x, y):
    if labels:
        by = ", ".join(labels)
        expr = f"sum by ({by}) ({metric}{{{sel}}})"
    else:
        expr = f"sum({metric}{{{sel}}})"
    return [
        panel(
            layout,
            f"{metric}",
            [target(expr, legend_for(labels), ds_var)],
            x=x, y=y, unit=unit_for(metric),
            desc=f"Gauge `{metric}`.",
        )
    ]


PANEL_BUILDERS = {
    "counter": panels_for_counter,
    "histogram": panels_for_histogram,
    "gauge": panels_for_gauge,
    "udc": panels_for_gauge,
}


def add_section(panels, layout, title, metrics, ds_var, sel):
    panels.append(row(title, layout))
    cursor_x = 0
    row_y = layout.y
    row_max_h = 0
    for name, kind, labels in metrics:
        built = PANEL_BUILDERS[kind](layout, name, labels, ds_var, sel, cursor_x, row_y)
        for p in built:
            needed = p["gridPos"]["x"] + p["gridPos"]["w"]
            if needed > GRID_WIDTH:
                row_y += row_max_h
                row_max_h = 0
                p["gridPos"]["x"] = 0
                p["gridPos"]["y"] = row_y
            panels.append(p)
            row_max_h = max(row_max_h, p["gridPos"]["h"])
            cursor_x = p["gridPos"]["x"] + p["gridPos"]["w"]
            if cursor_x >= GRID_WIDTH:
                row_y += row_max_h
                row_max_h = 0
                cursor_x = 0
    layout.y = row_y + row_max_h


# ---------------------------------------------------------------------------
# Dashboard assembly
# ---------------------------------------------------------------------------

TEMPLATING_VARS = [
    {
        "name": "ORBIT_DS",
        "label": "Orbit datasource (analytics-eventsdot tenant)",
        "type": "datasource",
        "query": "prometheus",
        "current": {"text": "Mimir - Analytics Eventsdot", "value": "mimir-analytics-eventsdot"},
        "regex": "/Analytics Eventsdot/",
        "hide": 0,
        "refresh": 1,
    },
    {
        "name": "RAILS_DS",
        "label": "Rails datasource (gitlab tenant)",
        "type": "datasource",
        "query": "prometheus",
        "current": {"text": "Mimir - Gitlab Gstg", "value": "mimir-gitlab-gstg"},
        "regex": "/Gitlab Gstg|Gitlab Gprd/",
        "hide": 0,
        "refresh": 1,
    },
    {
        "name": "cluster",
        "label": "Orbit cluster",
        "type": "custom",
        "query": "orbit-stg,orbit-prd,orbit-.*",
        "current": {"text": "orbit-stg", "value": "orbit-stg"},
        "options": [
            {"text": "orbit-stg", "value": "orbit-stg", "selected": True},
            {"text": "orbit-prd", "value": "orbit-prd", "selected": False},
            {"text": "all", "value": "orbit-.*", "selected": False},
        ],
        "includeAll": False,
        "multi": False,
        "hide": 0,
    },
    {
        "name": "rails_env",
        "label": "Rails env",
        "type": "custom",
        "query": "gstg,gprd,gstg|gprd",
        "current": {"text": "gstg", "value": "gstg"},
        "options": [
            {"text": "gstg", "value": "gstg", "selected": True},
            {"text": "gprd", "value": "gprd", "selected": False},
            {"text": "both", "value": "gstg|gprd", "selected": False},
        ],
        "includeAll": False,
        "multi": False,
        "hide": 0,
    },
]

GKG_WEB_SEL = 'container="gkg-webserver", cluster=~"$cluster"'
GKG_IDX_SEL = 'container="gkg-indexer", cluster=~"$cluster"'
SIPHON_SEL = 'namespace="siphon", cluster=~"$cluster"'
NATS_SEL = 'cluster=~"$cluster"'
RAILS_SEL = 'env=~"$rails_env"'


def dashboard_shell(uid: str, title: str, tags: list[str], description: str) -> dict:
    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["orbit"] + tags,
        "schemaVersion": 39,
        "version": 1,
        "time": {"from": "now-3h", "to": "now"},
        "timezone": "utc",
        "refresh": "1m",
        "templating": {"list": json.loads(json.dumps(TEMPLATING_VARS))},
        "links": [
            {"type": "dashboards", "asDropdown": True, "includeVars": True, "keepTime": True, "tags": ["orbit"], "title": "Orbit"},
        ],
        "panels": [],
    }


def build_gkg_webserver() -> dict:
    d = dashboard_shell("orbit-gkg-webserver", "Orbit — GKG webserver", ["gkg", "webserver"],
                        "All metrics emitted by the GKG webserver: HTTP + gRPC transport, query pipeline, content resolution, query-engine threat counters, schema watcher.")
    layout = Layout()
    panels: list = []

    panels.append(row("Overview", layout))
    y = layout.y
    panels.append(stat_panel(layout, "Queries / min",
        [target(f'sum(rate(gkg_query_pipeline_queries_total{{{GKG_WEB_SEL}}}[5m]) * 60)', "qpm", "ORBIT_DS")],
        x=0, y=y, w=6))
    panels.append(stat_panel(layout, "Success rate",
        [target(f'sum(rate(gkg_query_pipeline_queries_total{{{GKG_WEB_SEL}, status="ok"}}[5m])) / clamp_min(sum(rate(gkg_query_pipeline_queries_total{{{GKG_WEB_SEL}}}[5m])), 1)', "ok", "ORBIT_DS")],
        x=6, y=y, w=6, unit="percentunit"))
    panels.append(stat_panel(layout, "Pipeline p95 (s)",
        [target(f'histogram_quantile(0.95, sum by (le) (rate(gkg_query_pipeline_duration_seconds_bucket{{{GKG_WEB_SEL}}}[5m])))', "p95", "ORBIT_DS")],
        x=12, y=y, w=6, unit="s"))
    panels.append(stat_panel(layout, "Security rejects / min",
        [target(f'sum(rate(gkg_query_pipeline_error_security_rejected_total{{{GKG_WEB_SEL}}}[5m]) * 60)', "rejects/min", "ORBIT_DS")],
        x=18, y=y, w=6))
    layout.y += 4

    add_section(panels, layout, "HTTP transport", GKG_HTTP, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "gRPC transport", GKG_GRPC, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "Query pipeline", GKG_QUERY, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "Content resolution (Gitaly)", GKG_CONTENT, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "Schema watcher", GKG_SCHEMA_WATCHER, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "Query engine compiler — threat counters", GKG_QENGINE, "ORBIT_DS", GKG_WEB_SEL)

    d["panels"] = panels
    return d


def build_gkg_indexer() -> dict:
    d = dashboard_shell("orbit-gkg-indexer", "Orbit — GKG indexer", ["gkg", "indexer"],
                        "All metrics emitted by the GKG indexer: ETL engine, code pipeline, SDLC pipeline, namespace deletion, scheduler, schema migration.")
    layout = Layout()
    panels: list = []

    panels.append(row("Overview", layout))
    y = layout.y
    panels.append(stat_panel(layout, "Rows indexed / min",
        [target(f'sum(rate(gkg_etl_destination_rows_written_total{{{GKG_IDX_SEL}}}[5m]) * 60)', "rows/min", "ORBIT_DS")],
        x=0, y=y, w=6))
    panels.append(stat_panel(layout, "Bytes indexed / s",
        [target(f'sum(rate(gkg_etl_destination_bytes_written_bytes_total{{{GKG_IDX_SEL}}}[5m]))', "B/s", "ORBIT_DS")],
        x=6, y=y, w=6, unit="Bps"))
    panels.append(stat_panel(layout, "Handler p95 (s)",
        [target(f'histogram_quantile(0.95, sum by (le) (rate(gkg_etl_handler_duration_seconds_bucket{{{GKG_IDX_SEL}}}[5m])))', "p95", "ORBIT_DS")],
        x=12, y=y, w=6, unit="s"))
    panels.append(stat_panel(layout, "Handler errors / min",
        [target(f'sum(rate(gkg_etl_handler_errors_total{{{GKG_IDX_SEL}}}[5m]) * 60)', "errors/min", "ORBIT_DS")],
        x=18, y=y, w=6))
    layout.y += 4

    add_section(panels, layout, "ETL engine", GKG_ETL, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "Code pipeline", GKG_CODE, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "SDLC pipeline", GKG_SDLC, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "Namespace deletion", GKG_DELETION, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "Scheduler", GKG_SCHEDULER, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "Schema migration", GKG_MIGRATION, "ORBIT_DS", GKG_IDX_SEL)

    d["panels"] = panels
    return d


def build_siphon() -> dict:
    d = dashboard_shell("orbit-siphon", "Orbit — Siphon", ["siphon"],
                        "Siphon producer and ClickHouse consumer metrics (analytics-eventsdot tenant).")
    layout = Layout()
    panels: list = []

    panels.append(row("Overview", layout))
    y = layout.y
    panels.append(stat_panel(layout, "Producer ops / s",
        [target(f'sum(rate(siphon_operations_total{{{SIPHON_SEL}}}[5m]))', "ops/s", "ORBIT_DS")],
        x=0, y=y, w=8))
    panels.append(stat_panel(layout, "Consumer events / s",
        [target(f'sum(rate(siphon_clickhouse_consumer_number_of_events{{{SIPHON_SEL}}}[5m]))', "events/s", "ORBIT_DS")],
        x=8, y=y, w=8))
    panels.append(stat_panel(layout, "Producer apps (distinct)",
        [target(f'count(count by (app_id) (siphon_operations_total{{{SIPHON_SEL}}}))', "apps", "ORBIT_DS")],
        x=16, y=y, w=8))
    layout.y += 4

    add_section(panels, layout, "Producers", SIPHON_PRODUCERS, "ORBIT_DS", SIPHON_SEL)
    add_section(panels, layout, "ClickHouse consumers", SIPHON_CONSUMERS, "ORBIT_DS", SIPHON_SEL)

    d["panels"] = panels
    return d


def build_nats() -> dict:
    d = dashboard_shell("orbit-nats", "Orbit — NATS", ["nats"],
                        "NATS varz + JetStream stream/consumer metrics for the Orbit cluster.")
    layout = Layout()
    panels: list = []
    add_section(panels, layout, "NATS", NATS, "ORBIT_DS", NATS_SEL)
    d["panels"] = panels
    return d


def build_rails_kg() -> dict:
    d = dashboard_shell("orbit-rails-kg", "Orbit — Rails KG integration", ["rails", "gkg"],
                        "Rails monolith gitlab_knowledge_graph_* metrics: Rails → GKG gRPC client, redaction, JWT build, auth-context, traversal-ID compaction.")
    layout = Layout()
    panels: list = []

    panels.append(row("Overview", layout))
    y = layout.y
    panels.append(stat_panel(layout, "gRPC calls / min",
        [target(f'sum(rate(gitlab_knowledge_graph_grpc_duration_seconds_count{{{RAILS_SEL}}}[5m]) * 60)', "calls/min", "RAILS_DS")],
        x=0, y=y, w=8))
    panels.append(stat_panel(layout, "gRPC errors / min",
        [target(f'sum(rate(gitlab_knowledge_graph_grpc_errors_total{{{RAILS_SEL}}}[5m]) * 60)', "errors/min", "RAILS_DS")],
        x=8, y=y, w=8))
    panels.append(stat_panel(layout, "gRPC p95 (s)",
        [target(f'histogram_quantile(0.95, sum by (le) (rate(gitlab_knowledge_graph_grpc_duration_seconds_bucket{{{RAILS_SEL}}}[5m])))', "p95", "RAILS_DS")],
        x=16, y=y, w=8, unit="s"))
    layout.y += 4

    add_section(panels, layout, "gRPC / redaction / JWT / auth context", RAILS_KG_REQUEST, "RAILS_DS", RAILS_SEL)
    add_section(panels, layout, "Traversal IDs / JWT compaction", RAILS_KG_TRAVERSAL, "RAILS_DS", RAILS_SEL)

    d["panels"] = panels
    return d


def build_overview() -> dict:
    d = dashboard_shell("orbit-overview", "Orbit — Overview", ["overview"],
                        "Golden-signal stats across all Orbit components. Drill into component dashboards via the Orbit links menu in the top bar.")
    layout = Layout()
    panels: list = []

    panels.append(row("GKG webserver", layout))
    y = layout.y
    panels.append(stat_panel(layout, "Queries / min",
        [target(f'sum(rate(gkg_query_pipeline_queries_total{{{GKG_WEB_SEL}}}[5m]) * 60)', "qpm", "ORBIT_DS")],
        x=0, y=y, w=6))
    panels.append(stat_panel(layout, "Success rate",
        [target(f'sum(rate(gkg_query_pipeline_queries_total{{{GKG_WEB_SEL}, status="ok"}}[5m])) / clamp_min(sum(rate(gkg_query_pipeline_queries_total{{{GKG_WEB_SEL}}}[5m])), 1)', "ok", "ORBIT_DS")],
        x=6, y=y, w=6, unit="percentunit"))
    panels.append(stat_panel(layout, "Pipeline p95 (s)",
        [target(f'histogram_quantile(0.95, sum by (le) (rate(gkg_query_pipeline_duration_seconds_bucket{{{GKG_WEB_SEL}}}[5m])))', "p95", "ORBIT_DS")],
        x=12, y=y, w=6, unit="s"))
    panels.append(stat_panel(layout, "Security rejects / min",
        [target(f'sum(rate(gkg_query_pipeline_error_security_rejected_total{{{GKG_WEB_SEL}}}[5m]) * 60)', "rejects/min", "ORBIT_DS")],
        x=18, y=y, w=6))
    layout.y += 4

    panels.append(row("GKG indexer", layout))
    y = layout.y
    panels.append(stat_panel(layout, "Rows indexed / min",
        [target(f'sum(rate(gkg_etl_destination_rows_written_total{{{GKG_IDX_SEL}}}[5m]) * 60)', "rows/min", "ORBIT_DS")],
        x=0, y=y, w=6))
    panels.append(stat_panel(layout, "Bytes indexed / s",
        [target(f'sum(rate(gkg_etl_destination_bytes_written_bytes_total{{{GKG_IDX_SEL}}}[5m]))', "B/s", "ORBIT_DS")],
        x=6, y=y, w=6, unit="Bps"))
    panels.append(stat_panel(layout, "ETL errors / min",
        [target(f'sum(rate(gkg_etl_handler_errors_total{{{GKG_IDX_SEL}}}[5m]) * 60)', "errors/min", "ORBIT_DS")],
        x=12, y=y, w=6))
    panels.append(stat_panel(layout, "Handler p95 (s)",
        [target(f'histogram_quantile(0.95, sum by (le) (rate(gkg_etl_handler_duration_seconds_bucket{{{GKG_IDX_SEL}}}[5m])))', "p95", "ORBIT_DS")],
        x=18, y=y, w=6, unit="s"))
    layout.y += 4

    panels.append(row("Siphon", layout))
    y = layout.y
    panels.append(stat_panel(layout, "Producer ops / s",
        [target(f'sum(rate(siphon_operations_total{{{SIPHON_SEL}}}[5m]))', "ops/s", "ORBIT_DS")],
        x=0, y=y, w=8))
    panels.append(stat_panel(layout, "Consumer events / s",
        [target(f'sum(rate(siphon_clickhouse_consumer_number_of_events{{{SIPHON_SEL}}}[5m]))', "events/s", "ORBIT_DS")],
        x=8, y=y, w=8))
    panels.append(stat_panel(layout, "Producer apps (distinct)",
        [target(f'count(count by (app_id) (siphon_operations_total{{{SIPHON_SEL}}}))', "apps", "ORBIT_DS")],
        x=16, y=y, w=8))
    layout.y += 4

    panels.append(row("Rails → KG", layout))
    y = layout.y
    panels.append(stat_panel(layout, "gRPC calls / min",
        [target(f'sum(rate(gitlab_knowledge_graph_grpc_duration_seconds_count{{{RAILS_SEL}}}[5m]) * 60)', "calls/min", "RAILS_DS")],
        x=0, y=y, w=8))
    panels.append(stat_panel(layout, "gRPC errors / min",
        [target(f'sum(rate(gitlab_knowledge_graph_grpc_errors_total{{{RAILS_SEL}}}[5m]) * 60)', "errors/min", "RAILS_DS")],
        x=8, y=y, w=8))
    panels.append(stat_panel(layout, "gRPC p95 (s)",
        [target(f'histogram_quantile(0.95, sum by (le) (rate(gitlab_knowledge_graph_grpc_duration_seconds_bucket{{{RAILS_SEL}}}[5m])))', "p95", "RAILS_DS")],
        x=16, y=y, w=8, unit="s"))
    layout.y += 4

    d["panels"] = panels
    return d


def build_all_metrics() -> dict:
    """Legacy kitchen-sink dashboard. Retained for anyone who wants one page."""
    d = dashboard_shell("orbit-all-metrics", "Orbit — All metrics (kitchen sink)", ["all"],
                        "Every Orbit metric in one scrollable dashboard. Prefer the per-component dashboards; this one is for ad-hoc discovery.")
    layout = Layout()
    panels: list = []

    add_section(panels, layout, "GKG webserver — HTTP", GKG_HTTP, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "GKG webserver — gRPC", GKG_GRPC, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "GKG webserver — query pipeline", GKG_QUERY, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "GKG webserver — content resolution", GKG_CONTENT, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "GKG webserver — schema watcher", GKG_SCHEMA_WATCHER, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "GKG webserver — query engine threats", GKG_QENGINE, "ORBIT_DS", GKG_WEB_SEL)
    add_section(panels, layout, "GKG indexer — ETL", GKG_ETL, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "GKG indexer — code", GKG_CODE, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "GKG indexer — SDLC", GKG_SDLC, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "GKG indexer — namespace deletion", GKG_DELETION, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "GKG indexer — scheduler", GKG_SCHEDULER, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "GKG indexer — schema migration", GKG_MIGRATION, "ORBIT_DS", GKG_IDX_SEL)
    add_section(panels, layout, "Siphon — producers", SIPHON_PRODUCERS, "ORBIT_DS", SIPHON_SEL)
    add_section(panels, layout, "Siphon — consumers", SIPHON_CONSUMERS, "ORBIT_DS", SIPHON_SEL)
    add_section(panels, layout, "NATS", NATS, "ORBIT_DS", NATS_SEL)
    add_section(panels, layout, "Rails — KG integration", RAILS_KG_REQUEST, "RAILS_DS", RAILS_SEL)
    add_section(panels, layout, "Rails — traversal IDs", RAILS_KG_TRAVERSAL, "RAILS_DS", RAILS_SEL)

    d["panels"] = panels
    return d


def main() -> None:
    out_dir = Path(__file__).parent
    dashboards = {
        "orbit-overview.dashboard.json": build_overview(),
        "orbit-gkg-webserver.dashboard.json": build_gkg_webserver(),
        "orbit-gkg-indexer.dashboard.json": build_gkg_indexer(),
        "orbit-siphon.dashboard.json": build_siphon(),
        "orbit-nats.dashboard.json": build_nats(),
        "orbit-rails-kg.dashboard.json": build_rails_kg(),
        "orbit-all-metrics.dashboard.json": build_all_metrics(),
    }
    for name, dash in dashboards.items():
        path = out_dir / name
        path.write_text(json.dumps(dash, indent=2) + "\n")
        print(f"wrote {name:45s}  {path.stat().st_size:>8d} bytes  {len(dash['panels'])} panels")


if __name__ == "__main__":
    main()
