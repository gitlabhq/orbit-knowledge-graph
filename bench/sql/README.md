# Materialized join table spike

SQL and benchmark scripts for the denormalized join table prototype
(MR !2364). Everything here runs against a provisioned bench cluster.

## Setup

Apply the DDL to the bench ClickHouse. Each file creates a target table,
the materialized views that feed it, and a one-time backfill.

```bash
KCTX=$(bash bench/scripts/infra.sh output -raw kctx)
CH_PASS=$(kubectl --context="$KCTX" get secret ra-ch-credentials -n ra-ch-bench12 \
  -o jsonpath='{.data.default-password}' | base64 -d)

for f in bench/sql/materialized_*.sql; do
  kubectl --context="$KCTX" exec -i -n ra-ch-bench12 clickhouse-0 -- \
    clickhouse-client --password "$CH_PASS" -d gkg --multiquery < "$f"
done
```

| File | Relationship | Sort key | Sources |
|---|---|---|---|
| `materialized_reviewer.sql` | REVIEWER (edge table) | `(traversal_path, mr_id, u_id)` | edge, MR, user |
| `materialized_reviewer_by_user.sql` | REVIEWER (edge table) | `(traversal_path, u_id, mr_id)` | edge, MR, user |
| `materialized_authored.sql` | AUTHORED (FK `author_id`) | `(traversal_path, mr_id)` | MR, user |

The table names are hardcoded to the `v93_` schema prefix. Adjust if the
bench cluster is on a different version.

## Benchmarks

Each script runs a set of queries against both the baseline (edge join or
FK join) and the materialized table, warms once, then reports wall clock
and speedup.

```bash
export KCTX=$(bash bench/scripts/infra.sh output -raw kctx)
export RUN_ID=bench12

bash bench/sql/benchmarks/reviewer_aggregations.sh   # 9 aggregation shapes
bash bench/sql/benchmarks/reviewer_sort_key.sh       # by_mr vs by_user granule pruning
bash bench/sql/benchmarks/authored_fk.sh             # FK join baseline
```

## Results (bench12, gitlab-org scope, 504K REVIEWER rows)

Edge-table relationship (REVIEWER): 20-40x on aggregations, ~400ms -> ~15ms.
FK relationship (AUTHORED): 2-5x, ~50ms -> ~15ms. Both converge on the same
single-table floor; the gap is how expensive the baseline join was.

Full tables in the MR description.
