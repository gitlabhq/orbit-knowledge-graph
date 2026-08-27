#!/usr/bin/env python3
"""Compare a candidate run against a baseline run and flag regressions.

Exit status is non-zero when a hard regression is detected: a different row
count, or a wall clock outside the tolerance. Memory going up is reported but
does not fail, because a candidate may trade one plateau for another.

Measured run-to-run spread on an M3 Max over three repeats: peak live heap
+/-1% (Elasticsearch) and +/-3% (GitLab), wall clock +/-3%, peak footprint
+/-7%. Treat live-heap moves under 3% as noise; allocation counts are exact and
reproduce to the unit, so they are the sharpest signal for a change that removes
allocations rather than shrinking them.
"""

import argparse
import json
import pathlib
import sys

MIB = 1024 * 1024


def load(run_dir):
    return json.loads((pathlib.Path(run_dir) / "summary.json").read_text())


def faults(run_dir):
    """Files the pipeline gave up on. Two runs that aborted a different number of
    files did not index the same repository, so their memory is not comparable."""
    path = pathlib.Path(run_dir) / "events.jsonl"
    if not path.exists():
        return None
    total = 0
    for line in path.open():
        if not line.strip():
            continue
        e = json.loads(line)
        if e["fields"].get("message") == "files faulted during code indexing":
            total += int(e["fields"].get("error_count", 0))
    return total


def median(values):
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def combine(run_dirs):
    """Median across repeats of one revision. A single run is its own median."""
    runs = [load(d) for d in run_dirs]
    out = dict(runs[0])
    out["peaks"] = {
        k: median([r["peaks"].get(k, 0) for r in runs])
        for k in runs[0]["peaks"]
    }
    for k in ("total_ms", "index_ms", "flush_ms"):
        out[k] = median([r[k] for r in runs])
    out["final"] = dict(runs[0]["final"])
    out["final"]["alloc"] = {
        k: median([r["final"]["alloc"].get(k, 0) for r in runs])
        for k in runs[0]["final"]["alloc"]
    }
    out["ok"] = all(r["ok"] for r in runs)
    out["repeats"] = len(runs)
    return out


def stage_totals(run_dir):
    path = pathlib.Path(run_dir) / "events.jsonl"
    if not path.exists():
        return {}
    out = {}
    with path.open() as fh:
        for line in fh:
            if not line.strip():
                continue
            e = json.loads(line)
            if e["target"] != "codegraph_mem":
                continue
            f = e["fields"]
            stage = f.get("stage")
            if stage in (None, "converted"):
                continue
            key = f"{f.get('family', '-')}/{stage}"
            if stage == "convert_entity":
                key = f"{key}:{f.get('entity')}"
                value = f.get("bytes_arrow", 0)
            else:
                value = f.get("bytes_total", f.get("bytes_files_with_refs", 0))
            if value:
                out[key] = out.get(key, 0) + value
    return out


def pct(new, old):
    if not old:
        return 0.0
    return 100.0 * (new - old) / old


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("baseline", nargs="+")
    ap.add_argument("--candidate", nargs="+", required=True)
    ap.add_argument("--time-tolerance-pct", type=float, default=7.0)
    ap.add_argument("--memory-tolerance-pct", type=float, default=5.0)
    ap.add_argument("--min-mib", type=float, default=5.0)
    args = ap.parse_args()

    b, c = combine(args.baseline), combine(args.candidate)
    failures = []

    print(f"# {c['label']} vs {b['label']}"
          + (f" (median of {b['repeats']} vs {c['repeats']} runs)"
             if max(b["repeats"], c["repeats"]) > 1 else ""))
    print()
    print("| metric | baseline | candidate | delta |")
    print("|---|---:|---:|---:|")
    for name, key in (("peak live heap MiB", "max_alloc_live"),
                      ("exact peak live heap MiB", "exact_max_alloc_live"),
                      ("peak footprint MiB", "max_footprint"),
                      ("exact peak footprint MiB", "exact_max_footprint"),
                      ("peak commit MiB", "exact_max_commit"),
                      ("peak RSS MiB", "max_rss")):
        if not b["peaks"].get(key) and not c["peaks"].get(key):
            continue
        bv, cv = b["peaks"].get(key, 0), c["peaks"].get(key, 0)
        print(f"| {name} | {bv / MIB:,.0f} | {cv / MIB:,.0f} | "
              f"{(cv - bv) / MIB:+,.0f} ({pct(cv, bv):+.1f}%) |")
    for name, key in (("wall clock ms", "total_ms"),
                      ("index ms", "index_ms"),
                      ("flush ms", "flush_ms")):
        bv, cv = b[key], c[key]
        print(f"| {name} | {bv:,} | {cv:,} | {cv - bv:+,} ({pct(cv, bv):+.1f}%) |")
    ba = b["final"]["alloc"]
    ca = c["final"]["alloc"]
    print(f"| cumulative alloc GiB | {ba['total_alloc_bytes'] / 1024 ** 3:,.1f} | "
          f"{ca['total_alloc_bytes'] / 1024 ** 3:,.1f} | "
          f"{pct(ca['total_alloc_bytes'], ba['total_alloc_bytes']):+.1f}% |")
    print(f"| allocations M | {ba['total_allocs'] / 1e6:,.0f} | "
          f"{ca['total_allocs'] / 1e6:,.0f} | "
          f"{pct(ca['total_allocs'], ba['total_allocs']):+.1f}% |")
    print()

    if not c["ok"]:
        failures.append(f"candidate run failed: {c.get('error')}")

    bf = [faults(d) for d in args.baseline]
    cf = [faults(d) for d in args.candidate]
    if None not in bf + cf and set(bf) != set(cf):
        print(f"Files faulted: baseline {bf}, candidate {cf}.")
        print()
        failures.append(
            "the two revisions aborted a different number of files, so they did "
            "not index the same repository; re-run with the per-file budgets at 0")

    brows = {r["table"]: r["rows"] for r in b["clickhouse_rows"]}
    crows = {r["table"]: r["rows"] for r in c["clickhouse_rows"]}
    diffs = [(t, brows.get(t, 0), crows.get(t, 0))
             for t in sorted(set(brows) | set(crows))
             if brows.get(t, 0) != crows.get(t, 0)]
    if diffs:
        print("## Row-count regression")
        print()
        print("| table | baseline | candidate |")
        print("|---|---:|---:|")
        for t, bv, cv in diffs:
            print(f"| `{t}` | {bv:,} | {cv:,} |")
        print()
        failures.append(f"{len(diffs)} table(s) wrote a different number of rows")
    else:
        print(f"Row counts identical across {len(brows)} tables "
              f"({b['clickhouse_total_rows']:,} rows).")
        print()

    if pct(c["total_ms"], b["total_ms"]) > args.time_tolerance_pct:
        failures.append(
            f"wall clock regressed {pct(c['total_ms'], b['total_ms']):+.1f}% "
            f"(tolerance {args.time_tolerance_pct}%)")

    for name, key in (("peak live heap", "max_alloc_live"),
                      ("exact peak live heap", "exact_max_alloc_live")):
        if not b["peaks"].get(key):
            continue
        moved = pct(c["peaks"].get(key, 0), b["peaks"][key])
        if moved > args.memory_tolerance_pct:
            failures.append(
                f"{name} regressed {moved:+.1f}% "
                f"(tolerance {args.memory_tolerance_pct}%)")

    bs, cs = stage_totals(args.baseline[0]), stage_totals(args.candidate[0])
    rows = []
    for key in sorted(set(bs) | set(cs)):
        bv, cv = bs.get(key, 0), cs.get(key, 0)
        if max(bv, cv) < args.min_mib * MIB or bv == cv:
            continue
        rows.append((key, bv, cv))
    if rows:
        print("## Probed structures")
        print()
        print("| stage | baseline MiB | candidate MiB | delta |")
        print("|---|---:|---:|---:|")
        for key, bv, cv in sorted(rows, key=lambda r: r[2] - r[1]):
            print(f"| `{key}` | {bv / MIB:,.1f} | {cv / MIB:,.1f} | "
                  f"{(cv - bv) / MIB:+,.1f} |")
        print()

    if failures:
        print("## REGRESSIONS")
        print()
        for f in failures:
            print(f"- {f}")
        return 1
    print("No regressions detected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
