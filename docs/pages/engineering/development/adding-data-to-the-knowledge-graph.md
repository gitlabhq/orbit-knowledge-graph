# Adding data to GitLab Orbit — an agent's playbook

> Audience: a coding agent (Claude Code or similar) tasked with adding a new
> **node** or **edge** to the Orbit graph. This is the end-to-end,
> cross-repo workflow distilled from shipping the Package Registry + Container
> Registry entities (Package, ContainerRepository, PackageFile, Dependency and
> their edges). Follow it top to bottom; the ordering matters.

---

## 0. Mental model (read this first)

Two repositories are involved. Know which one you're in at every step.

| Repo | Path (Caproni edit mode) | Role |
|------|--------------------------|------|
| **GitLab monolith** | `…/gitlab-caproni/repos/gitlab` | Owns the Postgres source tables, the **Siphon CDC** config that replicates them to ClickHouse, and the **redaction** authorization layer. |
| **knowledge-graph** (`gitlab-org/orbit/knowledge-graph`) | `…/gitlab-caproni/repos/knowledge-graph` (or your own checkout) | The `gkg` engine: the **ontology** (YAML) that tells `gkg-indexer` how to turn `siphon_*` ClickHouse tables into graph nodes/edges, plus the indexer, query compiler, and gRPC server. |

**Data flow (one direction):**

```plaintext
Postgres table (monolith)
   └─ Siphon CDC (db/siphon/tables/*.yml + ClickHouse migration)
        └─ siphon_<table> ClickHouse table  ── replicated to prod CH ──┐
                                                                        │
knowledge-graph ontology YAML (config/ontology/{nodes,edges})          │
   └─ gkg-indexer reads siphon_<table> ◄───────────────────────────────┘
        └─ materializes gl_<node> tables + edge-table rows
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

- [ ] Read the issue / plan. These ship in **phases**. Confirm which slice you own.
- [ ] Confirm **both repos** are present. If `…/knowledge-graph` is missing, stop and ask. You cannot do the ontology half without it.
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
- **Is the node redactable?** Almost always yes. Any node that maps to a project-scoped Rails resource needs a policy + redaction registration (Section 4). If you skip this, the node **fails closed and returns zero rows**. This is a silent, confusing failure.
- **Edge ownership:** reuse the shared **`IN_PROJECT`** edge for "belongs to a project". Do **not** invent `HAS_X` edges for ownership. Every project-owned node uses `IN_PROJECT`, and "what does this project own?" is the inverse traversal.

---

## 3. Monolith — Siphon CDC (one MR per source table)

> The generic Siphon lifecycle (adding the migration and replicating the table)
> is already documented in [ClickHouse table design with Siphon → Table replication example](https://docs.gitlab.com/development/database/clickhouse/clickhouse_table_design_with_siphon/#table-replication-example).
> The CDC service itself lives at [`gitlab-org/analytics-section/siphon`](https://gitlab.com/gitlab-org/analytics-section/siphon).
> Read that first. This section only records the **review-tested conventions**
> specific to Orbit nodes/edges, the deltas on top of the canonical flow.

Branch off **fresh `master`**. One table per MR keeps `main.sql` conflicts small.

### 3.1 Generate

```shell
bundle exec rails generate gitlab:click_house:siphon <table> --with-traversal-path
```

This emits three files:

- `db/click_house/migrate/main/<TS>_create_siphon_<table>.rb`: main `ReplacingMergeTree`.
- `db/click_house/migrate/main/<TS>_create_siphon_<table>_pg_pkey_ordered.rb`: companion ID-ordered table + materialized view (for reconciliation).
- `db/siphon/tables/<table>.yml`: CDC config (`dedup_by`, `dedup_by_columns_lookup_table`, `reconcile`).

> If `bundle exec rails …` fails with a missing gem, your local bundle is
> behind `master`. Run `bundle install` and retry.

### 3.2 Post-generation edits (the review-tested conventions)

- **`project_id Int64`, not `Nullable(Int64)`**, justified by the `CHECK (project_id IS NOT NULL)` constraint. Same for any other CHECK-guarded or `NOT NULL` FK column. Leave genuinely nullable columns `Nullable`.
- **Add an explicit `ORDER BY` to the main table** matching the `PRIMARY KEY` (e.g. `ORDER BY (traversal_path, id)`). The generator emits `PRIMARY KEY` only; the latest merged tables add the explicit `ORDER BY`.
- **Keep the generator's CODECs.** Do **not** "normalize" `CODEC(DoubleDelta, ZSTD)` → `ZSTD(1)` or `Delta` → `Delta(8)`. The bare forms are the generator default; ClickHouse normalizes them at table creation and that normalized form is what `main.sql` records. Reviewers (and GitLab Duo) will suggest the explicit form. Decline it.
- **`smallint` → `Int16`**, no default (mirror existing tables).
- Don't hand-tune the PK to a composite like `(package_id, id)`. The `id`-leading companion table is what reconciliation needs; premature tuning has been reverted in review.
- Confirm `# frozen_string_literal: true` and every PostgreSQL column is represented.

### 3.3 Regenerate `main.sql` (NEVER hand-edit)

`db/click_house/main.sql` must be regenerated against the **CI-pinned** ClickHouse
version, in Docker. The Caproni dev-stack ClickHouse is a newer version that emits
a slightly different dump (e.g. single-column-key parens) that fails `clickhouse:check-schema`.
Run this from `repos/gitlab` with the monolith in edit mode. Port 8123 must be free on the
host, so stop `mise run dev` (its ClickHouse port-forward uses it) first.

```shell
docker rm -f gl-ch-dump 2>/dev/null
docker run -d --name gl-ch-dump -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:25.12.3-alpine
until curl -sf "http://localhost:8123/ping" | grep -q Ok; do sleep 1; done   # wait for CH to be ready
curl -s "http://localhost:8123/" --data "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_development"
bundle exec rake gitlab:clickhouse:migrate:main
bundle exec rake gitlab:clickhouse:schema:dump:main   # writes main.sql + schema_cache/*.yml
git diff db/click_house/main.sql          # MUST be only your new table's blocks
docker rm -f gl-ch-dump
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

- `db/click_house/schema_migrations/main/<TS>` markers: these land in a **separate batch commit**, not the feature MR.
- Drift in unrelated `schema_cache/main/*.yml` (e.g. `siphon_ci_pipeline_metadata.yml` flipping the dict-name qualification). `git checkout --` those. The dump regenerates *all* tables; only your table's files are in scope.

> Maintainers **do** require the `schema_cache/main/*.yml` for your new table inline
> in the feature MR. A Phase 2 MR was blocked for omitting them.

### 3.5 Validate

```shell
bundle exec rubocop --no-server db/click_house/migrate/main/<TS>_create_siphon_<table>*.rb
# Run the siphon spec against the pinned CH while the Docker container is still up:
bundle exec rspec spec/db/clickhouse_siphon_tables_spec.rb
```

CI authority: `clickhouse:check-schema` (validates `main.sql`), the siphon spec
(replication target, `reconcile.expression_key_columns` == `sharding_key`,
column→type mapping), and `db:check-migrations`.

---

## 4. Monolith — redaction registration (one MR, per new NODE)

Skip this for edges. An edge needs no registration as long as both endpoint
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

   This reuses the project-level `read_package` ability, with **no new permission**, so
   no `custom_roles` / GraphQL / OpenAPI doc regen.
2. **Register** in `redaction_service.rb`:
   - `EE_RESOURCE_CLASSES`: `your_type: ::Fully::Qualified::Model`
   - `EE_PRELOAD_ASSOCIATIONS`: `your_type: [{ project: [:namespace, :project_feature, :group, :organization] }]`
   - **Right-size the preloads: measure, don't guess.** Redaction runs the policy
     per record, so a missing preload becomes an N+1. Exercise the redaction locally
     against a realistic multi-record set and **count the PostgreSQL queries** it issues
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

Model it on the closest existing node. [`nodes/agents/agent_session.yaml`](../../../../config/ontology/nodes/agents/agent_session.yaml)
is a small, complete example with a `.sql.j2` extract. [`nodes/packages/package.yaml`](../../../../config/ontology/nodes/packages/package.yaml)
is one with a generated extract. [`config/schemas/ontology.schema.json`](../../../../config/schemas/ontology.schema.json)
is the authority, and `mise ontology:validate` checks every file against it.

A node file has these top-level keys. `node_type`, `domain`, `description`, `label`, `destination_table`,
`default_columns`, `style`, `properties` and `redaction` are required:

- `node_type`, `domain`, `description` (200 characters at most), `label` (the property shown as the node's name), `destination_table: gl_<node>` and `default_columns`.
- `style`: the size and color used by graph views.
- `redaction: { resource_type: <type>, id_column: id, ability: <ability> }`: **must match the monolith registration** (Section 4).
- `properties:` block. A column-backed property needs `type`, `source` and `description`, and the schema enforces all three. **`nullable` must match the siphon source column.** Don't mark a `NOT NULL` source nullable. Don't mark a `Nullable(...)` source `nullable: false` without a comment that explains the NULL-to-default coercion. Reviewers flag both directions. Enum properties list their `values`. Namespaced nodes carry a hidden `traversal_path` property, and path-scoped nodes declare a `traversal_path_lookup` on `id`.
- `indexer.reindex`: the datalake tables whose Siphon changes trigger a re-index of the namespace.
- `pipelines:` the extract and transform that build the node (see below).
- `storage:` the ClickHouse DDL: `primary_key`, `columns` with codecs, `indexes`, `projections` and table `settings`. Copy the shape from the template node.

There is no top-level `etl:` block, and the schema rejects one. Each entry in `pipelines:` pairs an `extract` with a `transform`:

```yaml
pipelines:
  - name: Package
    extract:
      type: clickhouse
      tables: [siphon_packages_packages]
      order_by: [traversal_path, package_type, id]
      query: generated
    transform:
      type: datafusion
      edges:
        - from: { field: id, kind: Package }
          to: { field: project_id, kind: Project }
          label: IN_PROJECT
```

- **`extract`:** `query: generated` builds the SQL from the declaration. It suits a single-table projection, and it also takes `filter`, `lookups` and `partition_count`. A node extract can't set `fields`; only standalone edge extracts can (§5.2). Use `query: <name>.sql.j2` only for genuinely complex joins or materialized arrays. The template sits next to the YAML, as `agent_session.sql.j2` does. The indexer renders `{{version_column}}`, `{{deleted_column}}` and `{{watermark_column}}` in it. The query must alias its columns `AS _version` and `AS _deleted`, and `{{filters}}` and `{{batch_size}}` pass through to query time. The "Extraction SQL" item in [`crates/indexer/AGENTS.md`](../../../../crates/indexer/AGENTS.md#before-writing-a-new-handler-reuse-existing-infra) has the full contract.
- **`transform`:** Nodes and standalone edges always use `type: datafusion`, a row-wise projection of the extracted rows. The key is required. `transform.edges` emits edges from an **FK column on this node's own source table**, for example `IN_PROJECT` from `project_id`. Each `label` must name an edge that declares a variant for that node pair (§5.2). Set `mutable: true` on an edge whose FK can change after the row is created (a milestone or an assignee, for example). Without it the old edge is never tombstoned and stays in the graph. When SQL can't express the graph shape, put the logic in a derived entity (`domains.<domain>.derived` in `schema.yaml`, such as `SystemNote`) with a registered Rust transform. A node can't use one. See [When a Rust transform is justified](../../../../crates/indexer/AGENTS.md#when-a-rust-transform-is-justified-adr-015).
- Use a **separate edge pipeline (§5.2)** for **join-table edges**, such as `DECLARES_DEPENDENCY` from `packages_dependency_links`, where the relationship lives in its own table.

### 5.2 Edge YAML — `config/ontology/edges/<edge>.yaml`

An edge file declares the allowed source and target pairs (`variants`). It can also carry the pipelines that emit the edge.

- **FK-derived edge** (the FK is a column on one node's table): add a variant to the edge file. Emit the edge from the node's `transform.edges` (§5.1). [`edges/in_project.yaml`](../../../../config/ontology/edges/in_project.yaml) holds one variant per node, each with an `fk_column`. Reuse `IN_PROJECT` for ownership and don't invent `HAS_X` edges.
- **Join-table edge** (the relationship has its own table): the edge file also holds the extract. [`edges/built_by.yaml`](../../../../config/ontology/edges/built_by.yaml) emits `BUILT_BY` from two join tables, and [`edges/declares_dependency.yaml`](../../../../config/ontology/edges/declares_dependency.yaml) is the single-table form:

```yaml
description: <Subject> <verbs> <object>
table: gl_ci_edge                     # optional, see §5.3
variants:
  - from_node: { type: <From>, id: id }
    to_node: { type: <To>, id: id }
    scope: same_namespace
    description: "..."
indexer:
  reindex: [siphon_<join_table>]
pipelines:
  - name: <EDGE>_siphon_<join_table>_<To>
    extract:
      type: clickhouse
      tables: [siphon_<join_table>]
      fields: [<from_fk_column>, <to_fk_column>]
      order_by: [traversal_path, id]
      query: generated
    transform:
      type: datafusion
      edges:
        - from: { field: <from_fk_column>, kind: <From> }
          to: { field: <to_fk_column>, kind: <To> }
          label: <EDGE>
```

A variant's `scope` is `namespace_anchor`, `same_namespace`, `prune_to_source` or `prune_to_target`. It tells the query compiler how a `traversal_path` crosses the edge, and `namespace_anchor` also requires an `fk_column`. Its target must be a namespace anchor (one with a `traversal_path_lookup`), and one FK column name must always map to the same anchor. If either FK column is `Nullable` in the source, the pipeline can emit null-target edges. Filter them or document it, because reviewers will ask. Prefer NOT-NULL join columns.

Hot query shapes can be pre-joined by declaring a chain under `settings.denormalized_joins`
in `schema.yaml` (see the [Denormalized joins](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/querying/graph_engine.md#denormalized-joins) section of the pre-rewrite graph engine doc).
This emits a `gl_denorm_<name>` table composed from the chain's own DDL plus one feeding
materialized view per table. The table stores every column of every table in the chain and
each declaration is a schema bump, so weigh write amplification before adding one.

### 5.3 Register in `config/ontology/schema.yaml` (the step that's easy to miss)

Node/edge files are **NOT auto-discovered**. They're loaded from a registry in
`schema.yaml`. If you skip this, your YAML is silently ignored and the DDL/indexer
won't see your entity (you'll burn time wondering why `gl_<node>` never appears).

```yaml
domains:
  <domain>:
    nodes:
      <ExistingNode>: nodes/<domain>/<existing>.yaml
      <NewNode>: nodes/<domain>/<new>.yaml     # ← add
edges:
  …
  <NEW_EDGE>: edges/<new_edge>.yaml            # ← add
```

**Which edge table an edge lands in.** `gl_edge` is only the default (`settings.default_edge_table`). `settings.edge_tables` defines several tables: `gl_edge`, `gl_code_edge`, `gl_ci_edge`, `gl_sec_edge` and `gl_diff_edge`. The loader picks the table for each variant in this order, and the first match wins:

1. `table:` on the variant.
2. `table:` at the top level of the edge file.
3. The edge table whose `domains:` list holds the domain of both endpoints, when both are in the same domain. For example, `gl_code_edge` lists `source_code`.
4. `settings.default_edge_table`.

So one edge kind can span several tables. `IN_PROJECT` sets `table:` per variant. If the table you need doesn't exist, add it under `settings.edge_tables` with its `sort_key`, `columns` and `storage`.

### 5.4 Record the schema change: migration ledger and archive

An ontology change that alters the generated DDL, an extract or a transform changes the fingerprint snapshot. It must ship with exactly one schema-version bump and a migration ledger entry. The dispatcher reads the entry to decide how much of the graph to rebuild or re-index. Don't edit the `schema:` pin in `config/versions.yaml` by hand.

```shell
mise schema:bump              # derive the scope, bump the pin, write the ledger entry and the archive, re-snapshot
mise run schema:generate:ddl  # regenerate the DDL (do NOT hand-edit the generated SQL)
mise run schema:ledger:check  # confirm the ledger and snapshot match the ontology
```

`mise schema:bump` runs `cargo xtask migration-ledger bump` ([`crates/xtask/src/migration_ledger.rs`](../../../../crates/xtask/src/migration_ledger.rs)). It:

- derives the migration scope from the drift against the committed fingerprints in `config/schema-migrations.fingerprint.yaml`;
- prepends an entry to `config/schema-migrations.yaml`, or amends the newest entry when your branch has already bumped past `origin/main`;
- bumps `schema:` in `config/versions.yaml`;
- writes the ontology archive `config/ontology-archives/v<N>.tar.gz`.

`--scope` (`*`, `sdlc` or `code`) and `--entities` (with `sdlc` only) can widen the derived scope but never narrow it. With no ontology drift they declare it. `--scope none` is the exception: it replaces the derived scope, marks a change as output-neutral and requires a `--note` that certifies it. Use it only when the produced rows and tables are byte-identical. `--base` sets the ref that decides between a new bump and an amend (default `origin/main`). `--amend` and `--new` force that decision only when the base ref can't be read.

Each archive holds the ontology sources of one schema version, and the server embeds them. Commit the new archive, and never edit or relabel an existing one. [`config/ontology-archives/README.md`](../../../../config/ontology-archives/README.md) explains the retention rules.

`mise run schema:generate:ddl` rewrites `config/graph.sql` and `config/graph_persistent.sql` (ClickHouse) and `config/graph_local.sql` (DuckDB, for the local graph). Expect:

- `config/graph.sql`: a new `CREATE TABLE … gl_<node>` block for a new node, and the version stamp.
- A new edge in an existing edge table produces no new `CREATE TABLE`, because edges are rows in that table (§5.3).
- `config/graph_local.sql`: only the version stamp changes for SDLC nodes. The local graph holds no namespace-graph `gl_*` node tables, so `gl_<node>` is absent there.

CI runs `scripts/checks/check-migration-ledger.sh` and `scripts/checks/check-ddl-freshness.sh`. The first fails when a fingerprint change lacks a `base + 1` bump, or when the entry under-declares the change. The second fails when a DDL file is stale.

The "Schema migration" section of [`crates/indexer/AGENTS.md`](../../../../crates/indexer/AGENTS.md#schema-migration) owns how the dispatcher migrates, the re-index campaign and dead-version cleanup. Read it before you choose a scope, and don't copy it into this guide.

> **`include_dir`/`rust_embed` gotcha:** the embedded ontology is read live from
> `config/ontology` at runtime in debug builds, but only via the `schema.yaml`
> registry. So 5.3 is what actually wires it in, not the file's presence on disk.

### 5.5 Test fixtures + SDLC scenario

- **`config/seeds/siphon.sql`**: add a `CREATE TABLE … siphon_<table>` for each new source table, using the simplified fixture form (mirror the existing `siphon_packages_build_infos` block: no CODECs, `PROJECTION pg_pkey_ordered`). The scenario harness seeds rows into these.
- **SDLC scenario YAML**: entity-ETL coverage lives in `crates/integration-tests/tests/indexer/scenarios/sdlc/<domain>/`, executed by the `scenario_indexing` test. Add a `.yaml` scenario, not a Rust function (see [Testing](../testing/README.md#principles)). Mirror `processes_packages.yaml` (node + IN_PROJECT) and `processes_package_built_by_pipeline.yaml` (edge from a join row):

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

After the code MR merges, open a separate docs MR. Add the node to the user
schema reference,
[`docs/docs-gitlab-com/schema.md`](../../../docs-gitlab-com/schema.md), in the table for
its domain. Update the node count in the page description and the intro.

### 5.7 Validate (toolchain matters)

```shell
# The repo pins rustc via rust-toolchain.toml; a BARE `cargo` may pick up an older
# toolchain on PATH. Use mise's env or an explicit +version.
cargo +<pinned> test -p ontology                           # load, constants, references
mise test:integration:indexer:sdlc:scenario <domain>/      # SDLC scenarios on a ClickHouse testcontainer
```

- Find the pinned version in `rust-toolchain.toml` (e.g. `1.98.1`). Running via
  `mise run …` uses the right toolchain automatically.
- `integration-tests` pulls in heavy code-graph deps that require the **newer**
  rustc. A bare `cargo test` on an older toolchain fails at dependency resolution.

---

## 6. Sequencing & cross-repo coordination

- **`main.sql` conflicts:** every monolith Siphon MR regenerates `db/click_house/main.sql`. Concurrent in-flight Siphon MRs will conflict. Expect a one-time rebase on whichever merges later.
- **resource_type contract:** the ontology `redaction.resource_type` (KG repo) and the `EE_RESOURCE_CLASSES` key (monolith) are a hand-shake. They must be identical strings, and the redaction MR must merge for the node to return data in production.
- **Order of operations across a phase:** Siphon CDC tables can merge in any order. The redaction MR and the ontology MR are independent, but both must land before the node is queryable + authorized. Define a node in `schema.yaml` *before* an edge that references it.
- **Push & MRs:** by default, **stage + validate locally and let the human push and open MRs.** Confirm before pushing. `glab` may be unauthenticated (`401`). Check `glab auth status` before assuming you can create/edit MRs via the API.

---

## 7. End-to-end checklist

Monolith, per Siphon table:

- [ ] Branch off fresh `master`; generator run with correct `--with-traversal-path`.
- [ ] `project_id Int64`, explicit `ORDER BY`, CODECs untouched.
- [ ] `main.sql` regenerated via pinned-CH Docker; diff is only your blocks.
- [ ] Committed: migrations + siphon YAML + main.sql + schema_cache YAML. Markers excluded; unrelated drift reverted.
- [ ] RuboCop + siphon spec green. Commit body ≤72 cols, full URLs (no `#123`/`!123`).

Monolith, per new node:

- [ ] Policy added (reuse an existing ability if possible).
- [ ] Registered in `EE_RESOURCE_CLASSES` + `EE_PRELOAD_ASSOCIATIONS`; key matches ontology `resource_type`.
- [ ] Redaction spec context + `supported_types` updated; green.

knowledge-graph:

- [ ] Node YAML (every property has `description`; nullable matches source).
- [ ] Edge YAML (join FKs handled).
- [ ] **Registered in `schema.yaml`** (nodes map + edges map).
- [ ] `mise schema:bump` run; `mise run schema:generate:ddl` run; `graph.sql` shows the new `gl_<node>`.
- [ ] `config/seeds/siphon.sql` updated; SDLC YAML scenario(s) added.
- [ ] Query-side YAML scenario(s) added under `crates/integration-tests/tests/server/data_correctness/scenarios/<category>/` to verify the new entity is queryable end-to-end.
- [ ] `cargo test -p ontology` + `mise test:integration:indexer:sdlc:scenario <domain>/` green (correct toolchain).

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
| `glab mr` returns `401` | `glab` token expired | `glab auth login`, or let the human drive MRs. |

---

## 9. Reflect — keep this playbook current

When you finish adding a table / node / edge, **close the loop on this document**.
Conventions drift between phases (§1). So if anything you hit diverged from what's
written here, update the relevant section in a separate docs MR. Examples are a new
generator flag, a changed CODEC default, an extra file maintainers required, a
redaction wrinkle, or a toolchain gotcha. A stale playbook is how
the next agent re-burns the time you just spent. If nothing changed, say so
explicitly in your summary so the next reader trusts the doc.
