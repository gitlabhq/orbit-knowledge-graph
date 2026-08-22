#!/usr/bin/env python3
"""Summarise a `footprint -p` poll log captured during a profiler run.

Shows the peak dirty bytes per VM region category, which is where thread
stacks, page tables and allocator metadata become visible. With mimalloc as the
global allocator the heap shows up as untagged VM_ALLOCATE rather than MALLOC_*,
because mimalloc maps its own arenas instead of going through libmalloc.
"""

import argparse
import re
import sys

MIB = 1024 * 1024
UNITS = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
ROW = re.compile(r"^\s*([\d.]+)\s*(B|KB|MB|GB)\s+.*?\s+(\d+)\s{4}(.+?)\s*$")
SNAP = re.compile(r"^=== t=([\d.]+)")


def parse_bytes(value, unit):
    return float(value) * UNITS[unit]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("log")
    ap.add_argument("--top", type=int, default=14)
    args = ap.parse_args()

    peaks = {}
    at_peak_total = 0.0
    peak_snapshot = {}
    current = {}
    total = 0.0

    def close():
        nonlocal at_peak_total, peak_snapshot
        if current and total > at_peak_total:
            at_peak_total = total
            peak_snapshot = dict(current)
        for k, v in current.items():
            peaks[k] = max(peaks.get(k, 0), v)

    for line in open(args.log):
        if SNAP.match(line):
            close()
            current.clear()
            total = 0.0
            continue
        m = ROW.match(line.rstrip())
        if not m:
            continue
        dirty = parse_bytes(m.group(1), m.group(2))
        category = m.group(4)
        if category == "TOTAL":
            total = dirty
            continue
        current[category] = current.get(category, 0) + dirty
    close()

    print(f"# VM regions: {args.log}")
    print()
    print(f"Peak snapshot total dirty: {at_peak_total / MIB:,.0f} MiB")
    print()
    print("| category | dirty at peak snapshot MiB | max over run MiB |")
    print("|---|---:|---:|")
    for cat, v in sorted(peak_snapshot.items(), key=lambda kv: -kv[1])[:args.top]:
        print(f"| {cat} | {v / MIB:,.1f} | {peaks.get(cat, 0) / MIB:,.1f} |")
    print()


if __name__ == "__main__":
    sys.exit(main())
