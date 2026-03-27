#!/usr/bin/env python3
"""Compare two or more query-profiler result files side by side.

Usage:
    python3 scripts/devtools/diff-profiler-results.py baseline.json dedup.json
    python3 scripts/devtools/diff-profiler-results.py a.json b.json c.json --labels main,dedup,v2
    python3 scripts/devtools/diff-profiler-results.py a.json b.json --metric elapsed_ms
"""

import argparse
import json
import sys
from pathlib import Path


METRICS = {
    "read_rows": ("total_read_rows", "rows"),
    "read_bytes": ("total_read_bytes", "bytes"),
    "memory": ("total_memory_usage", "bytes"),
    "elapsed_ms": ("total_elapsed_ms", "ms"),
}


def fmt_count(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:,.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:,.0f}K"
    return str(int(n))


def fmt_bytes(b):
    mb = b / (1024 * 1024)
    if mb >= 1000:
        return f"{mb / 1000:.1f}GB"
    return f"{mb:.0f}MB"


def fmt_ms(ms):
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    return f"{ms:.0f}ms"


def fmt_value(val, unit):
    if unit == "rows":
        return fmt_count(val)
    if unit == "bytes":
        return fmt_bytes(val)
    if unit == "ms":
        return fmt_ms(val)
    return str(val)


def fmt_delta(old, new):
    if old == 0:
        return "n/a"
    pct = ((new - old) / old) * 100
    if abs(pct) < 1:
        return "same"
    return f"{pct:+.0f}%"


def load_results(path):
    with open(path) as f:
        return json.load(f)


def extract_metric(result, metric_key):
    if "error" in result:
        return None
    return result["summary"][metric_key]


def main():
    parser = argparse.ArgumentParser(description="Diff query-profiler result files")
    parser.add_argument("files", nargs="+", help="Result JSON files to compare")
    parser.add_argument(
        "--labels",
        help="Comma-separated labels for each file (default: filenames)",
    )
    parser.add_argument(
        "--metric",
        choices=list(METRICS.keys()),
        default="read_rows",
        help="Metric to compare (default: read_rows)",
    )
    parser.add_argument(
        "--all-metrics",
        action="store_true",
        help="Show all metrics in separate tables",
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "csv"],
        default="markdown",
        help="Output format (default: markdown)",
    )
    args = parser.parse_args()

    datasets = [load_results(f) for f in args.files]
    labels = (
        args.labels.split(",") if args.labels else [Path(f).stem for f in args.files]
    )

    if len(labels) != len(datasets):
        print("error: number of labels must match number of files", file=sys.stderr)
        sys.exit(1)

    all_queries = sorted(set().union(*(d.keys() for d in datasets)))

    metrics_to_show = list(METRICS.keys()) if args.all_metrics else [args.metric]

    for metric_name in metrics_to_show:
        metric_key, unit = METRICS[metric_name]

        if args.all_metrics:
            print(f"\n## {metric_name}\n")

        if len(datasets) == 2:
            print_two_way(all_queries, datasets, labels, metric_key, unit, args.format)
        else:
            print_n_way(all_queries, datasets, labels, metric_key, unit, args.format)


def print_two_way(queries, datasets, labels, metric_key, unit, fmt):
    a_data, b_data = datasets
    a_label, b_label = labels

    if fmt == "csv":
        print(f"query,{a_label},{b_label},delta")
        for q in queries:
            a_val = extract_metric(a_data.get(q, {"error": True}), metric_key)
            b_val = extract_metric(b_data.get(q, {"error": True}), metric_key)
            a_str = str(a_val) if a_val is not None else "error"
            b_str = str(b_val) if b_val is not None else "error"
            delta = fmt_delta(a_val, b_val) if a_val and b_val else "n/a"
            print(f"{q},{a_str},{b_str},{delta}")
        return

    header = f"| Query | Type | {a_label} | {b_label} | \u0394 |"
    sep = "|---|---|---|---|---|"
    print(header)
    print(sep)

    for q in queries:
        a_result = a_data.get(q)
        b_result = b_data.get(q)

        qtype = ""
        for d in [a_result, b_result]:
            if d and "compilation" in d:
                qtype = d["compilation"]["query_type"]
                break

        a_val = extract_metric(a_result, metric_key) if a_result else None
        b_val = extract_metric(b_result, metric_key) if b_result else None

        a_str = fmt_value(a_val, unit) if a_val is not None else "ERR"
        b_str = fmt_value(b_val, unit) if b_val is not None else "ERR"
        delta = (
            fmt_delta(a_val, b_val)
            if a_val is not None and b_val is not None
            else "n/a"
        )

        print(f"| {q} | {qtype} | {a_str} | {b_str} | {delta} |")


def print_n_way(queries, datasets, labels, metric_key, unit, fmt):
    if fmt == "csv":
        print(f"query,{','.join(labels)}")
        for q in queries:
            vals = []
            for d in datasets:
                v = extract_metric(d.get(q, {"error": True}), metric_key)
                vals.append(str(v) if v is not None else "error")
            print(f"{q},{','.join(vals)}")
        return

    cols = " | ".join(labels)
    header = f"| Query | Type | {cols} |"
    sep = "|---|---" + ("|---" * len(labels)) + "|"
    print(header)
    print(sep)

    for q in queries:
        qtype = ""
        vals = []
        for d in datasets:
            result = d.get(q)
            if result and "compilation" in result and not qtype:
                qtype = result["compilation"]["query_type"]
            v = extract_metric(result, metric_key) if result else None
            vals.append(fmt_value(v, unit) if v is not None else "ERR")

        row_vals = " | ".join(vals)
        print(f"| {q} | {qtype} | {row_vals} |")


if __name__ == "__main__":
    main()
