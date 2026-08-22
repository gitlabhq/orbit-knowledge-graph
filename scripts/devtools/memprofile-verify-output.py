#!/usr/bin/env python3
"""Prove two indexing runs wrote identical graph data.

Fingerprints every row of every graph table in two ClickHouse databases and
compares them. The fingerprint is order-independent, because insert order is not
part of the contract: parts land concurrently and row order within a part
depends on scheduling. Two columns are excluded because they are `Utc::now()` at index time and differ
between runs by construction: `_version` on every graph row, and `indexed_at` on
the checkpoint. Everything else, including every id, path, name, tag array and
edge endpoint, is compared.

A table matches only when the row count, the additive checksum and the XOR
checksum of its per-row hashes all agree.
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request

EXCLUDED_COLUMNS = {"_version", "indexed_at"}


def query(url, user, password, database, sql):
    params = urllib.parse.urlencode(
        {"database": database, "user": user, "password": password, "query": sql}
    )
    with urllib.request.urlopen(f"{url}/?{params}", timeout=600) as response:
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
    out = query(url, user, password, database, table and sql)
    return [c for c in out.split("\n") if c and c not in EXCLUDED_COLUMNS]


def fingerprint(url, user, password, database, table, cols):
    projection = ", ".join(f"`{c}`" for c in cols)
    sql = (
        "SELECT count(), "
        "  sum(cityHash64(row)) AS additive, "
        "  groupBitXor(cityHash64(row)) AS parity "
        f"FROM (SELECT ({projection}) AS row FROM `{table}`) FORMAT JSON"
    )
    data = json.loads(query(url, user, password, database, sql))["data"][0]
    return (int(data["count()"]), str(data["additive"]), str(data["parity"]))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default="http://localhost:18123")
    ap.add_argument("--user", default="default")
    ap.add_argument("--password", default="memprofile")
    ap.add_argument("--baseline-db", required=True)
    ap.add_argument("--candidate-db", required=True)
    args = ap.parse_args()

    a, b = args.baseline_db, args.candidate_db
    creds = (args.url, args.user, args.password)

    ta, tb = tables(*creds, a), tables(*creds, b)
    if ta != tb:
        print(f"table sets differ: only in {a}: {sorted(set(ta) - set(tb))}, "
              f"only in {b}: {sorted(set(tb) - set(ta))}")
        return 1

    print("| table | rows | identical |")
    print("|---|---:|---|")
    mismatches = []
    total = 0
    for table in ta:
        cols_a, cols_b = columns(*creds, a, table), columns(*creds, b, table)
        if cols_a != cols_b:
            mismatches.append(f"{table}: column list differs")
            print(f"| `{table}` | - | **COLUMNS DIFFER** |")
            continue
        fa = fingerprint(*creds, a, table, cols_a)
        fb = fingerprint(*creds, b, table, cols_b)
        total += fa[0]
        ok = fa == fb
        if not ok:
            mismatches.append(
                f"{table}: baseline {fa} vs candidate {fb}")
        print(f"| `{table}` | {fa[0]:,} | {'yes' if ok else '**NO**'} |")
    print(f"| **total** | **{total:,}** | |")
    print()

    if mismatches:
        print("## MISMATCHES")
        print()
        for m in mismatches:
            print(f"- {m}")
        return 1
    print(f"All {len(ta)} tables byte-identical across {total:,} rows "
          f"(every column except the run timestamps).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
