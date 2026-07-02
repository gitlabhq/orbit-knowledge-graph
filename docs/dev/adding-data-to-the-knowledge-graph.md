# Adding data to the GitLab Knowledge Graph (Orbit) — an agent's playbook

> Audience: a coding agent (Claude Code or similar) tasked with adding a new
> **node** or **edge** to the Orbit knowledge graph. This is the end-to-end,
> cross-repo workflow distilled from shipping the Package Registry + Container
> Registry entities (Package, ContainerRepository, PackageFile, Dependency and
> their edges). Follow it top to bottom; the ordering matters.

---

## 0. Mental model (read this first)

Two repositories are involved. Know which one you're in at every step.

| Repo | Path (typical GDK) | Role |
|------|--------------------|------|
| **gitlab monolith** | `…/gitlab-development-kit/gitlab` | Owns the Postgres source tables, the **Siphon CDC** config that replicates them to ClickHouse, and the **redaction** authorization layer. |
| **knowledge-graph** (`gitlab-org/orbit/knowledge-graph`) | `…/gitlab-development-kit/knowledge-graph` | The `gkg` engine: the **ontology** (YAML) that tells `gkg-indexer` how to turn `siphon_*` ClickHouse tables into graph nodes/edges, plus the indexer, query compiler, and gRPC server. |

**Data flow (one direction):**

```
Postgres table (monolith)
   └─ Siphon CDC (db/siphon/tables/*.yml + ClickHouse migration)
        └─ siphon_<table> ClickHouse table  ── replicated to prod CH ──┐
                                                                        │
knowledge-graph ontology YAML (config/ontology/{nodes,edges})          │
   └─ gkg-indexer reads siphon_<table> ◄───────────────────────────────┘
        └─ materializes gl_<node> tables + gl_edge rows
             └─ gRPC (GetGraphSchema / query) ──► monolith Orbit API
                  └─ Authz::RedactionService gates every node per-viewer
```

**Two rules that fall out of this model:**

1. **Entity tables become NODES; join tables become EDGES.** A table like
   `packages_packages` (an entity) → a `Package` node. A join table like
   `packages_build_infos(package_id, pipeline_id)` → a `BUILT_BY` edge.
2. **Nothing is hardcoded in the monolith.** The Orbit API re-exposes whatever
   the ontology advertises over gRPC. The *only* monolith touchpoints for a new
   entity are (a) Siphon CDC of the source table and (b) redaction registration
   for new nodes. No feature flag (Orbit is gated by `OrbitLicense`).

---

## 1. Orient yourself before writing code

- [ ] Read the issue / plan. These ship in **phases** — confirm which slice you own.
- [ ] Confirm **both repos** are present. If `…/knowledge-graph` is missing, stop and ask — you cannot do the ontology half without it.
- [ ] Inspect the source Postgres table(s) in `db/structure.sql`:
  - Columns + types + nullability.
  - **`CHECK (… IS NOT NULL)` constraints** (decides Nullable vs not in ClickHouse).
  - Is there a `project_id`? (decides `--with-traversal-path`.) Check `db/docs/<table>.yml` for `sharding_key` and `table_size`.
  - `bytea` columns → map to ClickHouse `String`.
- [ ] Look at the **most recently merged** sibling for an exact template. Conventions drift between phases; the newest merged MR wins over older ones and over this doc.

---

## 2. Decide the shape

Answer these before touching files:

- **Node or edge?** Entity table → node. Join table → edge.
- **Sharded on `project_id`?** Yes → generate Siphon table `--with-traversal-path`, and the replicated `project_id` is `Int64` (not `Nullable`) because of the CHECK constraint.
- **Is the node redactable?** Almost always yes. Any node that maps to a project-scoped Rails resource needs a policy + redaction registration (Section 4). If you skip this, the node **fails closed and returns zero rows** — a silent, confusing failure.
- **Edge ownership:** reuse the shared **`IN_PROJECT`** edge for "belongs to a project". Do **not** invent `HAS_X` edges for ownership — every project-owned node uses `IN_PROJECT`, and "what does this project own?" is the inverse traversal.

---

## 3. Monolith — Siphon CDC (one MR per source table)

> The generic Siphon lifecycle — adding the migration and replicating the table —
> is already documented in [ClickHouse table design with Siphon → Table replication example](https://docs.gitlab.com/development/database/clickhouse/clickhouse_table_design_with_siphon/#table-replication-example)
> (the CDC service itself lives at [`gitlab-org/analytics-section/siphon`](https://gitlab.com/gitlab-org/analytics-section/siphon)).
> Read that first. This section only records the **review-tested conventions**
> specific to Orbit nodes/edges — the deltas on top of the canonical flow.

Branch off **fresh `master`**. One table per MR keeps `main.sql` conflicts small.

### 3.1 Generate

```bash
bundle exec rails generate gitlab:click_house:siphon <table> --with-traversal-path
```

This emits three files:
- `db/click_house/migrate/main/<TS>_create_siphon_<table>.rb` — main `ReplacingMergeTree`.
- `db/click_house/migrate/main/<TS>_create_siphon_<table>_pg_pkey_ordered.rb` — companion id-ordered table + materialized view (for reconciliation).
- `db/siphon/tables/<table>.yml` — CDC config (`dedup_by`, `dedup_by_columns_lookup_table`, `reconcile`).

> If `bundle exec rails …` fails with a missing gem (e.g. a `gdk-toogle` version
> mismatch — yes, that gem is intentionally spelled "toogle"), your local bundle is
> behind `master`. Run `bundle install` and retry.

### 3.2 Post-generation edits (the review-tested conventions)

- **`project_id Int64`, not `Nullable(Int64)`** — justified by the `CHECK (project_id IS NOT NULL)` constraint. Same for any other CHECK-guarded or `NOT NULL` FK column. Leave genuinely nullable columns `Nullable`.
- **Add an explicit `ORDER BY` to the main table** matching the `PRIMARY KEY` (e.g. `ORDER BY (traversal_path, id)`). The generator emits `PRIMARY KEY` only; the latest merged tables add the explicit `ORDER BY`.
- **Keep the generator's CODECs.** Do **not** "normalize" `CODEC(DoubleDelta, ZSTD)` → `ZSTD(1)` or `Delta` → `Delta(8)`. The bare forms are the generator default; ClickHouse normalizes them at table creation and that normalized form is what `main.sql` records. Reviewers (and GitLab Duo) will suggest the explicit form — decline it.
- **`smallint` → `Int16`**, no default (mirror existing tables).
- Don't hand-tune the PK to a composite like `(package_id, id)`. The `id`-leading companion table is what reconciliation needs; premature tuning has been reverted in review.
- Confirm `# frozen_string_literal: true` and every PG column is represented.

### 3.3 Regenerate `main.sql` (NEVER hand-edit)

`db/click_house/main.sql` must be regenerated against the **CI-pinned** ClickHouse
version, in Docker, because the local GDK ClickHouse is a newer version that emits
a slightly different dump (e.g. single-column-key parens) that fails `clickhouse:check-schema`.

```bash
gdk stop clickhouse                       # free port 8123
docker rm -f gl-ch-dump 2>/dev/null
docker run -d --name gl-ch-dump -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:25.12.3-alpine
until curl -sf http://localhost:8123/ping | grep -q Ok; do sleep 1; done   # wait for CH to be ready
curl -s http://localhost:8123/ --data "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_development"
bundle exec rake gitlab:clickhouse:migrate:main
bundle exec rake gitlab:clickhouse:schema:dump:main   # writes main.sql + schema_cache/*.yml
git diff db/click_house/main.sql          # MUST be only your new table's blocks
docker rm -f gl-ch-dump && gdk start clickhouse
```

> Pin version note: CI uses `clickhouse/clickhouse-server:25.12.3-alpine`. There is
> no pinned macOS ClickHouse binary, so Docker is the only faithful way. Verify the
> CI image tag hasn't moved before relying on `25.12.3`.

### 3.4 What to commit (and what NOT to)

Stage **only**:
- the two migration `.rb` files,
- `db/siphon/tables/<table>.yml`,
- `db/click_house/main.sql`,
- `db/click_house/schema_cache/main/siphon_<table>.yml` **and** `…_pg_pkey_ordered.yml`.

Do **not** commit / explicitly discard:
- `db/click_house/schema_migrations/main/<TS>` markers — these land in a **separate batch commit**, not the feature MR.
- Drift in unrelated `schema_cache/main/*.yml` (e.g. `siphon_ci_pipeline_metadata.yml` flipping the dict-name qualification). `git checkout --` those. The dump regenerates *all* tables; only your table's files are in scope.

> Maintainers **do** require the `schema_cache/main/*.yml` for your new table inline
> in the feature MR — a Phase 2 MR was blocked for omitting them.

### 3.5 Validate

```bash
bundle exec rubocop --no-server db/click_house/migrate/main/<TS>_create_siphon_<table>*.rb
# Run the siphon spec against the pinned CH while the Docker container is still up:
bundle exec rspec spec/db/clickhouse_siphon_tables_spec.rb
```

CI authority: `clickhouse:check-schema` (validates `main.sql`), the siphon spec
(replication target, `reconcile.expression_key_columns` == `sharding_key`,
column→type mapping), and `db:check-migrations`.

---

## 4. Monolith — redaction registration (one MR, per new NODE)

Skip this for edges — an edge needs no registration as long as both endpoint
**nodes** are registered. New nodes, however, **fail closed** unless registered in
`ee/app/services/ee/authz/redaction_service.rb`.

1. **Policy.** The resource class must resolve the read ability. If a sibling
   policy already exists, mirror it and **reuse an existing ability** to avoid the
   custom-permission documentation regeneration pipeline. Example
   (`app/policies/packages/dependency_policy.rb`):

   ```ruby
   # frozen_string_literal: true
   module Packages
     class DependencyPolicy < BasePolicy
       delegate { @subject.project&.packages_policy_subject }
     end
   end
   ```

   This reuses the project-level `read_package` ability — **no new permission**, so
   no `custom_roles` / GraphQL / OpenAPI doc regen.
2. **Register** in `redaction_service.rb`:
   - `EE_RESOURCE_CLASSES`: `your_type: ::Fully::Qualified::Model`
   - `EE_PRELOAD_ASSOCIATIONS`: `your_type: [{ project: [:namespace, :project_feature, :group, :organization] }]`
   - **Right-size the preloads — measure, don't guess.** Redaction runs the policy
     per record, so a missing preload becomes an N+1. Exercise the redaction locally
     against a realistic multi-record set and **count the PG queries** it issues
     (`ActiveRecord::QueryRecorder`, query logging, or watch `development.log`).
     **Dump the actual SQL and its `EXPLAIN` plan** for the queries that repeat.
     Every association the policy touches per record must appear in
     `EE_PRELOAD_ASSOCIATIONS` until the query count stays flat as the set grows.
3. The registration **key string** (`your_type`) must exactly match the
   `redaction.resource_type` advertised by the ontology node (Section 5). Coordinate
   this string with the Orbit team / ontology MR.
4. **Spec**: add a context in `ee/spec/services/ee/authz/redaction_service_spec.rb`
   proving an accessible vs inaccessible resource authorizes correctly, and add the
   type to the `supported_types` assertion. Mirror the existing `with packages` block.

---

## 5. knowledge-graph — the ontology (one MR)

Branch off **`main`** (the prior phase's branch is merged). The ontology is
YAML-driven; the indexer and query compiler are Rust.

### 5.1 Node YAML — `config/ontology/nodes/<domain>/<node>.yaml`

Model it on the closest existing node (e.g. `nodes/packages/package.yaml`). Required pieces:
- `node_type`, `domain`, `description`, `label`, `destination_table: gl_<node>`, `default_columns`.
- `redaction: { resource_type: <type>, id_column: id, ability: <ability> }` — **must match the monolith registration** (Section 4).
- `properties:` — **every property needs a `description`** (CI-enforced). **`nullable` must match the siphon source column**: don't mark a `NOT NULL` source nullable, and don't mark a `Nullable(...)` source `nullable: false` without a comment explaining the NULL→default coercion. Reviewers flag both directions.
- `etl: { type: table, scope: namespaced, source: siphon_<table>, order_by: [traversal_path, id], edges: { project_id: { to: Project, as: IN_PROJECT, direction: outgoing } } }`.
  - **Which edge mechanism to use:** the inline `edges:` block here is for edges derived from an **FK column on this node's own source table** (like `IN_PROJECT` from `project_id`). Use a **separate edge YAML (§5.2)** only for **join-table edges** (e.g. `DECLARES_DEPENDENCY` from `packages_dependency_links`), where the relationship lives in its own table.
  - **How the ETL uses this (read before adding a mapping):** the `etl:` block declares an extraction plus a row-wise transform (source columns → graph columns, FK-edge resolution, type discriminators). Most nodes are a straight column projection, but if a property needs a **mapping** — an Integer-to-Enum column, a computed value, or a non-trivial rename — you must declare it here, not discover it fails at index time. See [SDLC indexing → ETL](../design-documents/indexing/sdlc_indexing.md#etl) (and the Integer-to-Enum mapping note in that doc) for how plans, transforms, and column mappings are derived from the ontology.
- `storage:` — `primary_key`, `columns`, `indexes`, `projections`. Copy the shape from the template node.

### 5.2 Edge YAML — `config/ontology/edges/<edge>.yaml`

For a **join-table edge**, model on `edges/built_by.yaml`:

```yaml
description: <Subject> <verbs> <object>
variants:
  - from_node: { type: <From>, id: id }
    to_node:   { type: <To>,   id: id }
    scope: same_namespace
    description: "..."
etl:
  - scope: namespaced
    source: siphon_<join_table>
    order_by: [traversal_path, id]
    from: { id: <from_fk_column>, type: <From> }
    to:   { id: <to_fk_column>,   type: <To> }
```

If either FK column is `Nullable` in the source, the ETL can emit null-target edges —
filter or document it (reviewers will ask). Prefer NOT-NULL join columns.

### 5.3 Register in `config/ontology/schema.yaml` (the step that's easy to miss)

Node/edge files are **NOT auto-discovered** — they're loaded from a registry in
`schema.yaml`. If you skip this, your YAML is silently ignored and the DDL/indexer
won't see your entity (you'll burn time wondering why `gl_<node>` never appears).

```yaml
# under the matching domain:
  <domain>:
    nodes:
      <ExistingNode>: nodes/<domain>/<existing>.yaml
      <NewNode>: nodes/<domain>/<new>.yaml     # ← add
# in the edges: map:
edges:
  …
  <NEW_EDGE>: edges/<new_edge>.yaml            # ← add
```

### 5.4 Bump the schema version + regenerate DDL

```bash
# bump the single integer in:
config/SCHEMA_VERSION        # e.g. 64 -> 65

# regenerate (do NOT hand-edit graph.sql):
mise run schema:generate:ddl
```

This rewrites `config/graph.sql` (remote/ClickHouse) and `config/graph_local.sql`
(local/DuckDB). Expect:
- `config/graph.sql`: a new `CREATE TABLE … gl_<node>` block + the version stamp.
- **Edges have no per-edge table** — they're rows in the shared `gl_edge` table, so a new edge produces *no* new `CREATE TABLE`. That's correct.
- `config/graph_local.sql`: only the version-stamp line changes for SDLC nodes — the local/DuckDB graph doesn't contain the namespace-graph `gl_*` node tables (so `gl_<node>` being absent there is expected, same as `gl_package`).

> **`include_dir`/`rust_embed` gotcha:** the embedded ontology is read live from
> `config/ontology` at runtime in debug builds, but only via the `schema.yaml`
> registry — so 5.3 is what actually wires it in, not the file's presence on disk.

### 5.5 Test fixtures + SDLC scenario

- **`fixtures/siphon.sql`** — add a `CREATE TABLE … siphon_<table>` for each new source table, using the simplified fixture form (mirror the existing `siphon_packages_build_infos` block: no CODECs, `PROJECTION pg_pkey_ordered`). The scenario harness seeds rows into these.
- **SDLC scenario YAML** — entity-ETL coverage lives in `crates/integration-tests/tests/indexer/scenarios/sdlc/<domain>/`, executed by the `scenario_indexing` test. These moved from Rust to **YAML**; add a `.yaml` scenario, not a Rust function. Mirror `processes_packages.yaml` (node + IN_PROJECT) and `processes_package_built_by_pipeline.yaml` (edge from a join row):

  ```yaml
  description: ...
  scope: { namespace: 100 }
  seed:
    namespaces: [{ id: 100, traversal_path: "1/100/" }]
    projects:   [{ id: 1000, namespace_id: 100, traversal_path: "1/100/1000/" }]
    siphon_<table>: [ { id: …, project_id: 1000, …, traversal_path: "1/100/1000/" } ]
  expect:
    nodes:
      gl_<node>:
        rows: [ { id: …, <prop>: <value> } ]
    edges:
      - { kind: <EDGE>, from: <From>, to: <To>, traversal_path: "1/100/1000/", count: 1 }
  ```

### 5.6 Document

Add a row to `docs/design-documents/data_model.md`: the node table, the mermaid
relationship diagram, the `IN_PROJECT` source list, and a new relationship-types row.

### 5.7 Validate (toolchain matters)

```bash
# The repo pins rustc via rust-toolchain.toml; a BARE `cargo` may pick up an older
# toolchain on PATH. Use mise's env or an explicit +version.
cargo +<pinned> test -p ontology                          # ~128 tests: load, constants, references
cargo +<pinned> test -p integration-tests scenario_indexing   # end-to-end siphon->graph (needs Docker ClickHouse)
```

- Find the pinned version in `rust-toolchain.toml` (e.g. `1.95.0`). Running via
  `mise run …` uses the right toolchain automatically.
- `integration-tests` pulls in heavy code-graph deps that require the **newer**
  rustc — a bare `cargo test` on an older toolchain fails at dependency resolution.
- Local hooks may reference CI-only env vars (e.g. `CI_MERGE_REQUEST_DIFF_BASE_SHA`)
  and crash on commit; `git commit --no-verify` is acceptable here since the real
  checks run in CI. Note it in your summary.

---

## 6. Sequencing & cross-repo coordination

- **`main.sql` conflicts:** every monolith Siphon MR regenerates `db/click_house/main.sql`. Concurrent in-flight Siphon MRs will conflict — expect a one-time rebase on whichever merges later.
- **resource_type contract:** the ontology `redaction.resource_type` (KG repo) and the `EE_RESOURCE_CLASSES` key (monolith) are a hand-shake. They must be identical strings, and the redaction MR must merge for the node to return data in production.
- **Order of operations across a phase:** Siphon CDC tables can merge in any order; the redaction MR and the ontology MR are independent but both must land before the node is queryable + authorized. Define a node in `schema.yaml` *before* an edge that references it.
- **Push & MRs:** by default, **stage + validate locally and let the human push and open MRs.** Confirm before pushing. `glab` may be unauthenticated (`401`) — check `glab auth status` before assuming you can create/edit MRs via the API.

---

## 7. End-to-end checklist

Monolith, per Siphon table:
- [ ] Branch off fresh `master`; generator run with correct `--with-traversal-path`.
- [ ] `project_id Int64`, explicit `ORDER BY`, CODECs untouched.
- [ ] `main.sql` regenerated via pinned-CH Docker; diff is only your blocks.
- [ ] Committed: migrations + siphon yaml + main.sql + schema_cache yml. Markers excluded; unrelated drift reverted.
- [ ] RuboCop + siphon spec green. Commit body ≤72 cols, full URLs (no `#123`/`!123`).

Monolith, per new node:
- [ ] Policy added (reuse an existing ability if possible).
- [ ] Registered in `EE_RESOURCE_CLASSES` + `EE_PRELOAD_ASSOCIATIONS`; key matches ontology `resource_type`.
- [ ] Redaction spec context + `supported_types` updated; green.

knowledge-graph:
- [ ] Node YAML (every property has `description`; nullable matches source).
- [ ] Edge YAML (join FKs handled).
- [ ] **Registered in `schema.yaml`** (nodes map + edges map).
- [ ] `config/SCHEMA_VERSION` bumped; `mise run schema:generate:ddl` run; `graph.sql` shows the new `gl_<node>`.
- [ ] `fixtures/siphon.sql` updated; SDLC YAML scenario(s) added.
- [ ] `data_model.md` updated.
- [ ] `cargo test -p ontology` + `scenario_indexing` green (correct toolchain).

---

## 8. Failure modes you will actually hit

| Symptom | Cause | Fix |
|---------|-------|-----|
| Node never appears in `graph.sql`; DDL "succeeds" | New node/edge not registered in `config/ontology/schema.yaml` | Add it to the registry (5.3). |
| Queries return **zero rows** for the new node, no error | Node not registered in `EE::Authz::RedactionService` → fails closed | Add the redaction registration + policy (Section 4). |
| `clickhouse:check-schema` fails on `main.sql` | Hand-edited `main.sql`, or dumped from non-pinned ClickHouse | Regenerate with the `25.12.3` Docker image (3.3). |
| Siphon spec: `reconcile`/sharding mismatch | `expression_key_columns` ≠ `db/docs/<table>.yml` `sharding_key` | Align them; usually `[project_id]`. |
| `bundle exec rails …` can't find a gem | Local bundle behind `master` | `bundle install`. |
| `cargo test -p integration-tests` fails at dep resolution (rustc too old) | Bare `cargo` used an older PATH toolchain | Use `mise run` or `cargo +<pinned>`. |
| `git commit` aborts on an unbound `CI_*` var | Local hook expects CI env | `git commit --no-verify`; note it. |
| `glab mr` returns `401` | `glab` token expired | `glab auth login`, or let the human drive MRs. |

---

## 9. Reflect — keep this playbook current

When you finish adding a table / node / edge, **close the loop on this document**.
Conventions drift between phases (§1), so if anything you hit diverged from what's
written here — a new generator flag, a changed CODEC default, an extra file
maintainers required, a redaction wrinkle, a toolchain gotcha — update the relevant
section **in the same MR** (or an immediate fast-follow). A stale playbook is how
the next agent re-burns the time you just spent. If nothing changed, say so
explicitly in your summary so the next reader trusts the doc.
