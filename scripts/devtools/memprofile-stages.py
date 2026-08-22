#!/usr/bin/env python3
"""Per-stage structure breakdown for one code-index-profiler run.

Prints a markdown table per significant probe: which structures were live at
that stage and how many bytes each held, next to the process readings taken at
the same instant.
"""

import argparse
import json
import pathlib
import sys

MIB = 1024 * 1024

LABELS = {
    "bytes_inventory": "file inventory (path + size + decision per archive entry)",
    "bytes_family_inputs": "per-family input path lists",
    "bytes_definitions": "CanonicalDefinition (name, FQN, metadata)",
    "bytes_imports": "CanonicalImport (path, name, alias)",
    "bytes_refpack": "RefPack (packed refs + per-file string arena)",
    "bytes_tail": "inferred returns + unresolved aliases + extensions",
    "bytes_files_with_refs": "RefPack + per-file node index vectors carried into resolve",
    "bytes_nodes": "petgraph node array",
    "bytes_edges": "petgraph edge array",
    "bytes_node_strings": "Directory/File node path strings",
    "bytes_defs": "GraphDef array",
    "bytes_def_metadata": "boxed GraphDefMeta",
    "bytes_graph_imports": "GraphImport array (pool-backed)",
    "bytes_strings": "StringPool (arena + offsets + intern map)",
    "bytes_index_by_fqn": "index: FQN -> definition nodes",
    "bytes_index_by_name": "index: bare name -> definition nodes",
    "bytes_index_nested": "index: scope -> member -> nodes",
    "bytes_index_ancestors": "index: precomputed ancestor chains",
    "bytes_index_definition_ranges": "index: file -> definition byte ranges",
    "bytes_index_construction": "construction-only dir/file path maps",
    "bytes_pending_edges": "resolved EdgeTriples awaiting merge",
    "bytes_failed_chains": "failed chains queued for phase 3",
    "bytes_arrow": "Arrow RecordBatch",
}


def read_jsonl(path):
    with open(path) as fh:
        return [json.loads(l) for l in fh if l.strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("run_dir")
    ap.add_argument("--min-mib", type=float, default=1.0)
    args = ap.parse_args()

    run = pathlib.Path(args.run_dir)
    events = [e for e in read_jsonl(run / "events.jsonl")
              if e["target"] == "codegraph_mem"]

    # BatchTx does not know which family it is converting, so attribute each
    # Arrow batch to the family whose before_convert probe most recently fired.
    arrow_rollup = {}
    last_family = "-"
    for e in events:
        f = e["fields"]
        if f.get("stage") == "before_convert":
            last_family = f.get("family", "-")
            continue
        if f.get("stage") != "converted":
            continue
        key = (f.get("family") or last_family, f.get("table", ""))
        arrow_rollup[key] = arrow_rollup.get(key, 0) + f.get("bytes_arrow", 0)

    threshold = args.min_mib * MIB
    ramp = [e for e in events if e["fields"].get("stage") == "convert_entity"]
    for e in events:
        f = e["fields"]
        if f.get("stage") in ("converted", "convert_entity"):
            continue
        rows = [(k, v) for k, v in f.items()
                if k.startswith("bytes_") and k != "bytes_total" and v >= threshold]
        if not rows:
            continue
        rows.sort(key=lambda kv: -kv[1])
        total = sum(v for _, v in rows)
        counts = {k: v for k, v in f.items()
                  if k.endswith("_count") or k in ("files_held", "file_count",
                                                   "edges_pending", "total_files",
                                                   "parsable_files", "families")}
        print(f"### t={e['t_ms'] / 1000:.1f}s  {f.get('family', '-')} / {f.get('stage')}")
        print()
        print(f"process at this instant: footprint {e['footprint'] / MIB:,.0f} MiB, "
              f"live heap {e['alloc_live'] / MIB:,.0f} MiB, RSS {e['rss'] / MIB:,.0f} MiB")
        if counts:
            print()
            print("counts: " + ", ".join(f"{k}={v:,}" for k, v in counts.items()))
        print()
        print("| structure | MiB | share |")
        print("|---|---:|---:|")
        for k, v in rows:
            print(f"| {LABELS.get(k, k)} | {v / MIB:,.1f} | {100 * v / total:.0f}% |")
        print(f"| **probed total** | **{total / MIB:,.1f}** | |")
        print()

    if ramp:
        print("### Conversion ramp (graph to Arrow, in call order)")
        print()
        print("| t | entity | rows | Arrow MiB | live heap MiB | footprint MiB |")
        print("|---:|---|---:|---:|---:|---:|")
        for e in ramp:
            f = e["fields"]
            if f.get("bytes_arrow", 0) < threshold:
                continue
            print(f"| {e['t_ms'] / 1000:.1f} s | `{f.get('entity')}` | {f.get('rows', 0):,} | "
                  f"{f.get('bytes_arrow', 0) / MIB:,.1f} | {e['alloc_live'] / MIB:,.0f} | "
                  f"{e['footprint'] / MIB:,.0f} |")
        print()

    if arrow_rollup:
        print("### Arrow materialisation (per family, per table)")
        print()
        print("| family | table | MiB |")
        print("|---|---|---:|")
        grand = 0
        for (fam, table), v in sorted(arrow_rollup.items(), key=lambda kv: -kv[1]):
            if v < threshold:
                continue
            grand += v
            print(f"| {fam or '-'} | {table} | {v / MIB:,.1f} |")
        print(f"| | **total (above threshold)** | **{grand / MIB:,.1f}** |")
        print()


if __name__ == "__main__":
    sys.exit(main())
