#!/usr/bin/env python3
"""Compare granule pruning between two profiler result files.

Usage:
    python3 scripts/devtools/compare-granules.py <before.json> <after.json> [--labels before,after]

Prints a markdown table showing per-query, per-scan granule changes with
decrease multiples (e.g. "15.4x fewer").
"""

import json
import re
import sys


def parse_args():
    args = sys.argv[1:]
    labels = ("before", "after")
    files = []
    skip_next = False
    for i, arg in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        if arg == "--labels" and i + 1 < len(args):
            labels = tuple(args[i + 1].split(",", 1))
            skip_next = True
        elif not arg.startswith("--"):
            files.append(arg)
    if len(files) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    return files[0], files[1], labels


def extract_base_granules(result):
    for ex in result.get("executions", []):
        if ex.get("label") == "base":
            plan = ex.get("explain_plan", "")
            return re.findall(r"Granules: (\d+)/(\d+)", plan)
    return []


def main():
    before_path, after_path, labels = parse_args()

    with open(before_path) as f:
        before = json.load(f)
    with open(after_path) as f:
        after = json.load(f)

    rows = []
    for name in after:
        if "error" in after[name] or "error" in before.get(name, {}):
            continue

        b_gran = extract_base_granules(before.get(name, {}))
        a_gran = extract_base_granules(after[name])

        for i in range(max(len(b_gran), len(a_gran))):
            if i >= len(b_gran) or i >= len(a_gran):
                continue
            bs, bt = int(b_gran[i][0]), int(b_gran[i][1])
            aa, at = int(a_gran[i][0]), int(a_gran[i][1])
            if bs == aa:
                continue

            if aa == 0:
                multiple = "∞"
                pct = "-100%"
            elif bs == 0:
                multiple = "n/a"
                pct = "new"
            else:
                ratio = bs / aa
                multiple = f"{ratio:.1f}x"
                pct = f"{(aa - bs) / bs * 100:+.0f}%"

            rows.append((name, i, bs, bt, aa, at, multiple, pct))

    if not rows:
        print("No granule changes detected.")
        return

    print(f"| Query | Scan | {labels[0]} | {labels[1]} | Change | Multiple |")
    print("|---|---|---|---|---|---|")
    for name, scan, bs, bt, aa, at, multiple, pct in rows:
        print(f"| {name} | {scan} | {bs}/{bt} | {aa}/{at} | {pct} | {multiple} fewer |")


if __name__ == "__main__":
    main()
