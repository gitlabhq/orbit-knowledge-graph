#!/usr/bin/env python3
"""Summarise a dhat-heap.json by what holds the heap at its global peak.

dhat records, per allocation stack, how many bytes were live at t-gmax. This
rolls those up two ways: by the innermost frame in our own code (which
structure), and by the outermost (which pipeline stage).
"""

import argparse
import json
import re
import sys

MIB = 1024 * 1024

OURS = re.compile(
    r"\b(code_graph|indexer|orbit_utils|code_index_profiler|arrow|arrow_array|"
    r"arrow_buffer|petgraph|tree_sitter|oxc|smol_str|hashbrown|bumpalo|rust_lapper)\b"
)

NOISE = re.compile(
    r"(dhat::|core::alloc::|__rustc::|alloc::alloc::|alloc::raw_vec|"
    r"code_index_profiler::memory::|RawTableInner|<alloc::vec::Vec)"
)

FRAME = re.compile(r"^0x[0-9a-f]+: (.*?) \((.*)\)$")


def parse_frame(raw):
    m = FRAME.match(raw)
    if not m:
        return raw, ""
    return m.group(1), m.group(2)


def pick(frames, innermost):
    order = frames if innermost else reversed(frames)
    for raw in order:
        sym, loc = parse_frame(raw)
        if NOISE.search(sym):
            continue
        if OURS.search(sym) or OURS.search(loc):
            return f"{sym}  [{loc}]"
    for raw in order:
        sym, loc = parse_frame(raw)
        if not NOISE.search(sym):
            return f"{sym}  [{loc}]"
    return "?"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dhat_json")
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()

    d = json.load(open(args.dhat_json))
    ftbl = d["ftbl"]
    pps = d["pps"]

    gmax_total = sum(p["gb"] for p in pps)
    end_total = sum(p["eb"] for p in pps)
    alloc_total = sum(p["tb"] for p in pps)
    blocks_total = sum(p["tbk"] for p in pps)

    print(f"# dhat summary: {args.dhat_json}")
    print()
    print(f"- command: `{d['cmd']}`")
    print(f"- t-gmax at {d['tg'] / 1e6:,.1f} s, t-end at {d['te'] / 1e6:,.1f} s")
    print(f"- live at global peak: {gmax_total / MIB:,.1f} MiB")
    print(f"- live at exit: {end_total / MIB:,.1f} MiB")
    print(f"- cumulative allocated: {alloc_total / MIB:,.1f} MiB in {blocks_total:,} blocks")
    print(f"- distinct allocation stacks: {len(pps):,}")
    print()

    for innermost, title in ((True, "innermost frame in our code (which structure)"),
                             (False, "outermost frame in our code (which stage)")):
        buckets = {}
        blocks = {}
        for p in pps:
            if p["gb"] == 0:
                continue
            frames = [ftbl[i] for i in p["fs"]]
            key = pick(frames, innermost)
            buckets[key] = buckets.get(key, 0) + p["gb"]
            blocks[key] = blocks.get(key, 0) + p["gbk"]
        print(f"## Live at global peak, by {title}")
        print()
        print("| MiB | share | blocks | frame |")
        print("|---:|---:|---:|---|")
        for key, v in sorted(buckets.items(), key=lambda kv: -kv[1])[:args.top]:
            print(f"| {v / MIB:,.1f} | {100 * v / gmax_total:.1f}% | "
                  f"{blocks[key]:,} | `{key}` |")
        print()

    print("## Largest single allocation stacks at the peak")
    print()
    for p in sorted(pps, key=lambda p: -p["gb"])[:10]:
        if p["gb"] == 0:
            break
        print(f"### {p['gb'] / MIB:,.1f} MiB in {p['gbk']:,} blocks "
              f"(cumulative {p['tb'] / MIB:,.1f} MiB)")
        print()
        print("```")
        shown = 0
        for raw in [ftbl[i] for i in p["fs"]]:
            sym, loc = parse_frame(raw)
            if NOISE.search(sym):
                continue
            print(f"  {sym}  [{loc}]")
            shown += 1
            if shown >= 14:
                break
        print("```")
        print()


if __name__ == "__main__":
    sys.exit(main())
