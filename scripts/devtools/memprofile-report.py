#!/usr/bin/env python3
"""Render a memory report from one code-index-profiler run directory.

Reads summary.json, samples.jsonl and events.jsonl and writes report.md next
to them, plus prints the same content to stdout.
"""

import argparse
import json
import pathlib
import sys

MIB = 1024 * 1024


def mib(n):
    return f"{n / MIB:,.1f}"


def read_jsonl(path):
    if not path.exists():
        return []
    out = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def chart(samples, key, height=18, width=88):
    if not samples:
        return ""
    values = [s[key] for s in samples]
    times = [s["t_ms"] for s in samples]
    hi = max(values) or 1
    span = max(times) or 1
    cols = [0.0] * width
    for t, v in zip(times, values):
        i = min(width - 1, int(t * width / (span + 1)))
        cols[i] = max(cols[i], v)
    lines = []
    for row in range(height, 0, -1):
        thresh = hi * row / height
        label = f"{thresh / MIB:7,.0f}M |" if row % 3 == 0 or row == height else " " * 8 + "|"
        line = "".join("#" if c >= thresh else " " for c in cols)
        lines.append(label + line)
    lines.append(" " * 8 + "+" + "-" * width)
    lines.append(" " * 9 + f"0s{' ' * (width - 10)}{span / 1000:.0f}s")
    return "\n".join(lines)


def phase_bands(samples, width=88):
    if not samples:
        return ""
    span = max(s["t_ms"] for s in samples) or 1
    row = [" "] * width
    for s in samples:
        i = min(width - 1, int(s["t_ms"] * width / (span + 1)))
        row[i] = (s.get("phase") or "?")[0]
    return " " * 8 + " " + "".join(row)


MEM_FIELDS = [
    "bytes_total",
    "bytes_definitions",
    "bytes_imports",
    "bytes_graph_imports",
    "bytes_refpack",
    "bytes_tail",
    "bytes_nodes",
    "bytes_edges",
    "bytes_node_strings",
    "bytes_defs",
    "bytes_def_metadata",
    "bytes_strings",
    "bytes_index_by_fqn",
    "bytes_index_by_name",
    "bytes_index_nested",
    "bytes_index_ancestors",
    "bytes_index_definition_ranges",
    "bytes_index_construction",
    "bytes_files_with_refs",
    "bytes_pending_edges",
    "bytes_failed_chains",
    "bytes_inventory",
    "bytes_family_inputs",
    "bytes_arrow",
]

COUNT_FIELDS = [
    "total_files",
    "parsable_files",
    "families",
    "file_count",
    "files_held",
    "def_count",
    "import_count",
    "node_count",
    "edge_count",
    "string_count",
    "edges_pending",
    "rows",
]


def render(run_dir):
    run_dir = pathlib.Path(run_dir)
    summary = json.loads((run_dir / "summary.json").read_text())
    samples = read_jsonl(run_dir / "samples.jsonl")
    events = read_jsonl(run_dir / "events.jsonl")

    out = []
    w = out.append
    cfg = summary["config"]
    w(f"# Memory profile: {summary['label']}\n")
    ids = summary.get("project_ids") or [summary.get("project_id")]
    w(f"- project ids: {', '.join(f'`{i}`' for i in ids)}")
    invs = [e["fields"] for e in events
            if e["fields"].get("stage") == "inventory"]
    if invs:
        total = sum(i.get("total_bytes", 0) for i in invs)
        files = sum(i.get("total_files", 0) for i in invs)
        parsable = sum(i.get("parsable_files", 0) for i in invs)
        w(f"- archives: {mib(summary['archive_bytes'])} MiB compressed -> "
          f"{mib(total)} MiB retained on disk, {files:,} files "
          f"({parsable:,} parseable)")
    else:
        w(f"- archives: {mib(summary['archive_bytes'])} MiB compressed")
    w(f"- wall clock: {summary['total_ms'] / 1000:.1f}s "
      f"(index {summary['index_ms'] / 1000:.1f}s, flush {summary['flush_ms'] / 1000:.1f}s)")
    w(f"- allocator: {cfg['allocator']}, worker_threads={cfg['worker_threads']}, "
      f"max_concurrent_languages={cfg['max_concurrent_languages']}, "
      f"write_slice_rows={cfg['write_slice_rows']}")
    w(f"- ok: {summary['ok']}" + (f"  error: {summary['error']}" if summary.get("error") else ""))
    w("")
    p = summary["peaks"]
    w("## Peak")
    w("")
    w("| metric | MiB |")
    w("|---|---:|")
    w(f"| peak RSS | {mib(p['max_rss'])} |")
    w(f"| peak phys_footprint | {mib(p['max_footprint'])} |")
    w(f"| peak live heap (requested bytes) | {mib(p['max_alloc_live'])} |")
    w(f"| cumulative allocated | {mib(summary['final']['alloc']['total_alloc_bytes'])} |")
    w(f"| allocation count | {summary['final']['alloc']['total_allocs']:,} |")
    w(f"| samples | {p['samples']:,} |")
    w("")

    if p.get("exact_max_footprint"):
        w("### Exact high-water marks")
        w("")
        w("The kernel and the allocator each keep their own high-water mark, so these "
          "do not depend on the sampler being awake at the right moment. What the "
          "sampler missed is the difference.")
        w("")
        w("| metric | sampled MiB | exact MiB | missed |")
        w("|---|---:|---:|---:|")
        w(f"| phys_footprint | {mib(p['max_footprint'])} | {mib(p['exact_max_footprint'])} | "
          f"{mib(p['exact_max_footprint'] - p['max_footprint'])} |")
        w(f"| live heap | {mib(p['max_alloc_live'])} | {mib(p['exact_max_alloc_live'])} | "
          f"{mib(p['exact_max_alloc_live'] - p['max_alloc_live'])} |")
        w("")
        w(f"mimalloc peak commit: {mib(p['exact_max_commit'])} MiB. "
          f"Widest sampler gap: {p.get('max_sample_gap_ms', 0)} ms.")
        w("")
        if p.get("max_alloc_live_rounded"):
            waste = p["max_alloc_live_rounded"] - p["max_alloc_live"]
            w(f"Size-class rounding at the peak: {mib(waste)} MiB "
              f"({100.0 * waste / p['max_alloc_live']:.1f}% on top of what was requested).")
            w("")

    rounds = summary.get("rounds") or []
    if len(rounds) > 1:
        w("## Settled memory after each round")
        w("")
        w("A round that gives everything back leaves live heap flat. Footprint and "
          "commit staying high on top of a flat live heap is the allocator holding "
          "pages, not the pipeline holding data.")
        w("")
        w("| round | ms | live MiB | footprint MiB | RSS MiB | commit MiB |")
        w("|---:|---:|---:|---:|---:|---:|")
        for r in rounds:
            w(f"| {r['round']} | {r['round_ms']:,} | "
              f"{mib(r['settled_alloc']['live_bytes'])} | "
              f"{mib(r['settled']['footprint_bytes'])} | "
              f"{mib(r['settled']['resident_bytes'])} | "
              f"{mib(r['settled_allocator']['current_commit_bytes'])} |")
        w("")

    if samples:
        w("## Timeline (phys_footprint)")
        w("")
        w("```")
        w(chart(samples, "footprint"))
        w(phase_bands(samples))
        w("```")
        w("")
        w("## Timeline (live heap, allocator-requested bytes)")
        w("")
        w("```")
        w(chart(samples, "alloc_live"))
        w(phase_bands(samples))
        w("```")
        w("")

    interesting = [e for e in events if e["target"] == "codegraph_mem"]
    threshold = MIB * 5

    def significant(e):
        f = e["fields"]
        return max(f.get("bytes_total", 0), f.get("bytes_arrow", 0),
                   f.get("bytes_files_with_refs", 0)) >= threshold

    interesting = [e for e in interesting if significant(e)]
    if interesting:
        w("## Structure sizes at phase boundaries")
        w("")
        w("Probes holding under 5 MiB are omitted.")
        w("")
        w("| t (s) | family | stage | footprint MiB | live heap MiB | detail |")
        w("|---|---|---|---:|---:|---|")
        for e in interesting:
            f = e["fields"]
            stage = f.get("stage", "")
            family = f.get("family", "")
            detail_parts = []
            for k in COUNT_FIELDS:
                if k in f:
                    detail_parts.append(f"{k}={f[k]:,}")
            for k in MEM_FIELDS:
                if k in f and f[k]:
                    detail_parts.append(f"{k}={mib(f[k])}M")
            if "table" in f:
                detail_parts.insert(0, f"table={f['table']}")
            w(f"| {e['t_ms'] / 1000:.1f} | {family} | {stage} | "
              f"{mib(e['footprint'])} | {mib(e['alloc_live'])} | "
              f"{'  '.join(detail_parts)} |")
        w("")

    if samples:
        peak_sample = max(samples, key=lambda s: s["footprint"])
        peak_t = peak_sample["t_ms"]
        w(f"Peak footprint at t={peak_t / 1000:.1f}s during phase "
          f"`{peak_sample.get('phase')}`.")
        before = [e for e in interesting if e["t_ms"] <= peak_t]
        after = [e for e in interesting if e["t_ms"] > peak_t]
        if before:
            e = before[-1]
            w(f"Last structure probe before the peak: `{e['fields'].get('family', '')}"
              f"/{e['fields'].get('stage')}` at t={e['t_ms'] / 1000:.1f}s.")
        if after:
            e = after[0]
            w(f"First structure probe after the peak: `{e['fields'].get('family', '')}"
              f"/{e['fields'].get('stage')}` at t={e['t_ms'] / 1000:.1f}s.")
        w("")

    rows = summary.get("clickhouse_rows", [])
    if rows:
        w("## ClickHouse rows written")
        w("")
        w("| table | rows |")
        w("|---|---:|")
        for r in rows:
            w(f"| {r['table']} | {r['rows']:,} |")
        w(f"| **total** | **{summary['clickhouse_total_rows']:,}** |")
        w("")

    arrow = [e for e in interesting if e["fields"].get("stage") == "converted"]
    if arrow:
        total = sum(e["fields"].get("bytes_arrow", 0) for e in arrow)
        w(f"Arrow materialisation total: {mib(total)} MiB across {len(arrow)} batches.")
        w("")

    text = "\n".join(out)
    (run_dir / "report.md").write_text(text)
    return text


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("run_dir", nargs="+")
    args = ap.parse_args()
    for d in args.run_dir:
        print(render(d))
        print()


if __name__ == "__main__":
    sys.exit(main())
