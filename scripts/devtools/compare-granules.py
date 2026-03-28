#!/usr/bin/env python3
"""Compare granule pruning between two profiler result files.

Usage:
    python3 scripts/devtools/compare-granules.py <before.json> <after.json> [--labels before,after]

Prints a markdown table showing total granules scanned per query with
decrease multiples (e.g. "15.4x fewer"). Sums across all scans in the
query plan so scan reordering doesn't create false positives.
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


def total_granules(result):
    """Sum all Granules: selected/total across the base execution plan."""
    for ex in result.get("executions", []):
        if ex.get("label") == "base":
            plan = ex.get("explain_plan", "")
            matches = re.findall(r"Granules: (\d+)/(\d+)", plan)
            selected = sum(int(m[0]) for m in matches)
            total = sum(int(m[1]) for m in matches)
            return selected, total
    return 0, 0


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

        bs, bt = total_granules(before.get(name, {}))
        aa, at = total_granules(after[name])

        if bs == aa and bt == at:
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

        rows.append((name, bs, bt, aa, at, multiple, pct))

    if not rows:
        print("No granule changes detected.")
        return

    print(f"| Query | {labels[0]} | {labels[1]} | Change | Multiple |")
    print("|---|---|---|---|---|")
    for name, bs, bt, aa, at, multiple, pct in rows:
        print(f"| {name} | {bs}/{bt} | {aa}/{at} | {pct} | {multiple} fewer |")


if __name__ == "__main__":
    main()
