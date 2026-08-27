#!/usr/bin/env python3
"""Prove two indexing runs wrote identical graph data.

Fingerprints every row of every graph table in a ClickHouse database. The
fingerprint is order-independent, because insert order is not part of the
contract: parts land concurrently and row order within a part depends on
scheduling. Three columns are excluded because they are `Utc::now()` at index
time and differ between runs by construction: `_version` on every graph row, and
`indexed_at` and `watermark` on the checkpoint. Everything else, including every
id, path, name, tag array, edge endpoint, and the checkpoint's `cursor_values`,
is compared.

A table matches only when the row count, the additive checksum and the XOR
checksum of its per-row hashes all agree.

Graph tables are `ReplacingMergeTree`, so the read is `FINAL` by default: two
runs that wrote the same rows can differ in how much of their duplicate work
merges have collapsed by the time this runs, and a raw read would report that
as a mismatch. Pass `--no-final` only when comparing part-level state on purpose.

Three modes:

  --baseline-db A --candidate-db B   compare two live databases
  --baseline-db A --dump F           write A's fingerprints to F
  --candidate-db B --expect F        compare B against fingerprints in F

`--dump` exists so a measurement run can drop its graph database immediately and
still be comparable afterwards. Keeping one around costs every later run: it
merges tens of millions of rows in the background, and then the server, not the
indexer, is what runs out of memory.
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request

EXCLUDED_COLUMNS = {"_version", "indexed_at", "watermark"}


def query(url, user, password, database, sql):
    params = urllib.parse.urlencode(
        {"database": database, "user": user, "password": password, "query": sql}
    )
    with urllib.request.urlopen(f"{url}/?{params}", timeout=3600) as response:
        return response.read().decode().strip()


def tables(url, user, password, database):
    sql = (
        "SELECT name FROM system.tables "
        f"WHERE database = '{database}' AND engine NOT LIKE '%View' "
        "ORDER BY name FORMAT TSV"
    )
    out = query(url, user, password, database, sql)
    return [t for t in out.split("\n") if t]


def columns(url, user, password, database, table):
    sql = (
        "SELECT name FROM system.columns "
        f"WHERE database = '{database}' AND table = '{table}' "
        "ORDER BY position FORMAT TSV"
    )
    out = query(url, user, password, database, sql)
    return [c for c in out.split("\n") if c and c not in EXCLUDED_COLUMNS]


def fingerprint(url, user, password, database, table, cols, final=True):
    projection = ", ".join(f"`{c}`" for c in cols)
    modifier = " FINAL" if final else ""
    sql = (
        "SELECT count(), "
        "  sum(cityHash64(row)) AS additive, "
        "  groupBitXor(cityHash64(row)) AS parity "
        f"FROM (SELECT ({projection}) AS row FROM `{table}`{modifier}) FORMAT JSON"
    )
    data = json.loads(query(url, user, password, database, sql))["data"][0]
    return [int(data["count()"]), str(data["additive"]), str(data["parity"])]


def fingerprints(creds, database, final):
    out = {}
    for table in tables(*creds, database):
        cols = columns(*creds, database, table)
        out[table] = {
            "columns": cols,
            "fingerprint": fingerprint(*creds, database, table, cols, final),
        }
    return out


def report(baseline, candidate, baseline_name, candidate_name):
    if set(baseline) != set(candidate):
        print(
            f"table sets differ: only in {baseline_name}: "
            f"{sorted(set(baseline) - set(candidate))}, only in {candidate_name}: "
            f"{sorted(set(candidate) - set(baseline))}"
        )
        return 1

    print("| table | rows | identical |")
    print("|---|---:|---|")
    mismatches = []
    total = 0
    for table in sorted(baseline):
        a, b = baseline[table], candidate[table]
        if a["columns"] != b["columns"]:
            mismatches.append(f"{table}: column list differs")
            print(f"| `{table}` | - | **COLUMNS DIFFER** |")
            continue
        total += a["fingerprint"][0]
        ok = a["fingerprint"] == b["fingerprint"]
        if not ok:
            mismatches.append(
                f"{table}: {baseline_name} {a['fingerprint']} vs "
                f"{candidate_name} {b['fingerprint']}"
            )
        print(f"| `{table}` | {a['fingerprint'][0]:,} | {'yes' if ok else '**NO**'} |")
    print(f"| **total** | **{total:,}** | |")
    print()

    if mismatches:
        print("## MISMATCHES")
        print()
        for m in mismatches:
            print(f"- {m}")
        return 1
    print(
        f"All {len(baseline)} tables byte-identical across {total:,} rows "
        f"(every column except the run timestamps)."
    )
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default="http://localhost:18123")
    ap.add_argument("--user", default="default")
    ap.add_argument("--password", default="memprofile")
    ap.add_argument("--baseline-db")
    ap.add_argument("--candidate-db")
    ap.add_argument("--dump", help="write the baseline database's fingerprints here")
    ap.add_argument("--expect", help="compare the candidate against this fingerprint file")
    ap.add_argument("--no-final", action="store_true")
    args = ap.parse_args()
    final = not args.no_final
    creds = (args.url, args.user, args.password)

    if args.dump:
        if not args.baseline_db:
            ap.error("--dump needs --baseline-db")
        with open(args.dump, "w") as handle:
            json.dump(fingerprints(creds, args.baseline_db, final), handle, indent=1)
        print(f"wrote fingerprints for {args.baseline_db} to {args.dump}")
        return 0

    if not args.candidate_db:
        ap.error("need --candidate-db, or --baseline-db with --dump")

    if args.expect:
        with open(args.expect) as handle:
            baseline = json.load(handle)
        baseline_name = args.expect
    else:
        if not args.baseline_db:
            ap.error("need --baseline-db or --expect")
        baseline = fingerprints(creds, args.baseline_db, final)
        baseline_name = args.baseline_db

    candidate = fingerprints(creds, args.candidate_db, final)
    return report(baseline, candidate, baseline_name, args.candidate_db)


if __name__ == "__main__":
    sys.exit(main())
