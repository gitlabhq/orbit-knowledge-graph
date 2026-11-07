---
title: "Orbit ADR 023: A self-contained engine for indexing work"
creation-date: "2026-11-07"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Proposed. This ADR amends [ADR 017](017_clone_based_non_blocking_migrations.md).
The clone scope stays.
Campaigns replace checkpoint seeding and the checkpoint promotion gate.
A schema version no longer needs its own NATS streams or pods.

## Date

2026-11-07

## Context

The dispatcher decides what runs. The engine runs it. Both grew one flow at a time.

The dispatcher holds cron schedules, CDC routing, backfill sweeps and schema migrations.
Each new trigger adds a new pattern.
Schema migrations run when the dispatcher starts. They block the rollout.

The code handler builds its own lanes and frees its slot by hand.
The engine, KEDA and the logs cannot see these lanes.

Nothing records what ran.
We cannot explain a stale namespace or replay a run.

Checkpoints share the lifecycle of the graph.
A new checkpoint column forces a full graph rebuild.
During a full rebuild, freshness decays for hours.

[Orbit Next](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/16) and [third-party data](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/20) need new flows without engine changes.
They also need new ontology versions at runtime, with no deploy.

## Decision

**One library, `orbit-engine`, runs all indexing work. A flow is a planner and a worker. The engine owns lanes, retries, scaling, the ledger and migrations.**

![Orbit Engine V2 in one picture](../../../assets/adr/023-indexing-engine.svg)

A flow has up to three pieces of code. Helm configures where they run.

| Piece | Question it answers | Runs on |
|---|---|---|
| Worker | How is one input done? | Worker pods, in a lane |
| Planner | Which inputs run now, and in which lane? | Planner pod |
| Reconciler | Is a recorded fact still true? | Planner pod |
| Helm lanes | Where does a lane run, with how many slots, and what happens on a timeout? | Config |

The design follows six rules.

1. The planner plans. The worker executes. Helm sets slots, timeouts and retries.
2. The engine has no domain terms: no project, namespace, Gitaly or ontology. A CI check enforces it.
3. Tasks run at least once. Every write is idempotent.
4. Inputs are small. A task carries IDs. The worker reads the current state when it runs.
5. Work is a task. A decision is a reconciliation.
6. NATS holds no facts. A NATS wipe costs one recovery campaign.

### Workers

A worker runs one input and returns one outcome. It does not know its lane.

```rust
pub trait Worker: FromResources + Send + Sync + 'static {
    const NAME: &'static str;
    const EXCLUSIVE: bool = false;
    type Input: Payload;
    type Output: Payload;

    fn key(input: &Self::Input) -> Key;
    async fn run(&self, ctx: &TaskContext, input: Self::Input) -> Result<Outcome<Self::Output>, TaskError>;
}

impl TaskContext {
    pub fn tenant(&self) -> &TenantId;
    pub async fn pool(&self, name: &'static str) -> Result<PoolPermit, TaskError>;
    pub fn detach(&self);
}

impl<O: Payload> Outcome<O> {
    pub fn done(output: O) -> Self;
    pub fn no_change() -> Self;
    pub fn spawn<W: Worker>(self, inputs: impl IntoIterator<Item = W::Input>) -> Self;
}
```

The key is the identity of an input.
The engine keeps one pending task for each worker and key.
A burst of 50 pushes to one project gives one task.
The marker clears when the task starts. A push during a run queues one more task.
With `EXCLUSIVE`, the engine also takes a lease on the key. Two runs of one key never overlap.

`ctx.detach()` frees the slot while the task waits.
The main use is the wait for ClickHouse async inserts, up to 1 s.
The message and the lease stay held until `run` returns.

`spawn` splits the work into child tasks in the same job.
A job ends only when all its tasks end. One job lookup tells if a namespace is done.
Work for another team uses a `done` trigger, not `spawn`.
The producer then does not know its consumers.

| Use | When |
|---|---|
| `spawn` | Your own work, split into parts. You want to know when all the parts end. |
| `Trigger::done` | Another team's work starts after yours. You do not wait for it. |

Errors tell the engine what to do next.

```rust
impl TaskError {
    pub fn permanent(e: impl Into<anyhow::Error>) -> Self;
    pub fn defer(after: Duration, reason: &'static str) -> Self;
    pub fn backoff(after: Duration, reason: &'static str) -> Self;
    pub fn incompatible(reason: &'static str) -> Self;
}
```

| Error | Engine action | Uses an attempt |
|---|---|---|
| Any error, from `?` | Retry with backoff. Dead-letter after the last attempt. | Yes |
| `defer` | This input is not ready. Redeliver it after the delay. | No |
| `backoff` | A shared dependency pushes back. Redeliver. Stop pulling from the lane on this pod until the delay ends. | No |
| `incompatible` | This binary lacks a feature that the input needs. Redeliver to another pod. | No |
| Lane timeout | Move to the `on_timeout` lane, if Helm sets one. Else retry. | Yes |
| `permanent` | Dead-letter now. | — |

A shared client, such as the ClickHouse writer, returns a `Backoff` error when its breaker is open.
The worker only writes `?`. The engine finds the error with `downcast_ref`.
A backoff never dead-letters a task. Tasks wait as long as the outage lasts.

### Planners

A planner runs each time one of its triggers fires. It submits worker inputs.

```rust
pub trait Planner: FromResources + Send + Sync + 'static {
    const NAME: &'static str;

    fn triggers() -> Vec<Trigger>;
    async fn plan(&self, ctx: &mut PlanContext) -> Result<(), PlanError>;
}

pub enum Trigger {
    Cron(&'static str),
    Stream { subjects: &'static [&'static str], batch: usize, linger: Duration },
    Done { worker: &'static str },
}

pub enum Fired {
    Cron,
    Events(EventBatch),
    Manual { keys: Vec<Key> },
    Sweep { target: Option<String>, cursor: Option<String> },
}

impl PlanContext {
    pub fn fired(&self) -> &Fired;
    pub fn submit<W: Worker>(&mut self, tenant: TenantId, input: W::Input) -> &mut Submit;
    pub fn next_page(&mut self, cursor: Option<String>);
}
```

`Manual` comes from the admin API or a reconciler.
`Sweep` comes from the engine, one page at a time, for a campaign or a NATS recovery.
Every planner handles both. The testkit checks it.
A planner reads only events and Orbit storage.
A split that reads a source is a worker.
For example, `entity.split` probes the datalake. It spawns one `entity.index` task for each range.

An event is a hint. The source keeps the fact.
If NATS loses an event, a sweep or a reconciler fixes the result.

### Lanes

A lane is a name in Helm. The planner picks it. The worker never sees it.

```yaml
engine:
  lanes:
    code.index:
      default:  { slots: 8, timeout: 15m, on_timeout: big }
      big:      { slots: 1, timeout: 2h }
      backfill: { slots: 2, timeout: 2h }
```

| Task source | Lane |
|---|---|
| A submit | `.lane()`, else `default` |
| A submit during a sweep | `backfill`, unless `.lane()` is set |
| A submit from a `done` event | The lane of the source task, unless `.lane()` is set |
| A spawned child | The lane of its parent |
| A lane timeout with `on_timeout` | The `on_timeout` lane |

Every worker has a `default` and a `backfill` lane.
Each worker and lane pair is one NATS consumer and one KEDA trigger.
The code planner routes a repository by its size in the code checkpoint.
A first index has no size. It runs in `default`. On a timeout, `on_timeout` moves it to `big`.

### Reconcilers

A reconciler compares the wanted state with the observed state. It writes the difference.
It runs on the planner pod under a lease, with no slot.

```rust
pub trait Reconciler: FromResources + Send + Sync + 'static {
    const NAME: &'static str;
    const WRITES: &'static [&'static str];
    const EVERY: Duration;

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError>;
}

impl ReconcileContext {
    pub async fn last_job<W: Worker>(&self, key: &Key) -> Result<Option<JobSummary>, ReconcileError>;
    pub async fn replan<P: Planner>(&self, keys: Vec<Key>) -> Result<(), ReconcileError>;
    pub async fn campaign<P: Planner>(&self, target: &str) -> Result<CampaignState, ReconcileError>;
    pub async fn cancel_campaign<P: Planner>(&self, target: &str) -> Result<(), ReconcileError>;
}
```

One reconciler writes each table.
A tick is idempotent and has a 30 s limit.
A failed tick has no retry. The next tick runs again.
A check with heavy reads is a task. The reconciler reads its result on a later tick.

The namespace status is a reconciler.
A namespace is indexed when its `entity.split` job at the active version is done.
A job with only `no_change` tasks writes no ledger row.
On its first run at a version, `entity.split` returns `done` with zero ranges, not `no_change`.

### Two kinds of schema

| Class | Owner | Changes | Examples |
|---|---|---|---|
| System | The binary | Only with a deploy | Engine ledger, checkpoints, namespace status |
| Indexed data | The ontology registry | At any time | Graph nodes and edges |

**The engine migrates the system schema at startup.**
`orbit-store` is a small crate. A store is a folder of numbered SQL steps. Each dialect has its own folder.

```plaintext
stores/engine/
  clickhouse/
    001_expand_namespace_status.sql
    002_expand_first_indexed_at.sql
    003_contract_drop_legacy_state.sql
  sqlite/
    001_expand_namespace_status.sql
    002_expand_first_indexed_at.sql
    003_contract_drop_legacy_state.sql
```

```rust
pub trait Backend: Send + Sync + 'static {
    fn dialect(&self) -> &'static str;
    async fn applied(&self, store: &str) -> Result<Vec<Applied>, StoreError>;
    async fn apply(&self, store: &str, step: &Step) -> Result<(), StoreError>;
}

impl EngineBuilder {
    pub fn system_stores(self, stores: Stores, backend: Arc<dyn Backend>) -> Self;
}
```

An `expand` step only adds. Old binaries keep working.
A `contract` step drops or narrows. It waits one release after the first binary that sees it.
A `build.rs` embeds the folders. It fails on a missing or duplicate step.
Each backend records the applied steps and their hashes inside its own database.
The engine adds its own store, `ledger`, for its tables.
No role serves before the system schema matches its binary.
A changed step file stops every role.
A sharded store has many databases, such as one SQLite file for each tenant.
The worker migrates each file when it opens it.

**The ontology registry owns the indexed data schema. The engine has no part in it.**
A new version can come from any team at any time.

```plaintext
the registry adds gitlab@115
 → SchemaMigration creates the gitlab@115 tables: clone what the scope keeps, create the rest empty
 → ctx.campaign::<P>("gitlab@115") for each planner that the scope invalidates
 → workers load gitlab@115 from the registry, in the backfill lanes
 → all campaigns done → SchemaMigration promotes gitlab@115
```

The clone scope of ADR 017 stays. The version declares it.
A campaign of the invalidated planners replaces checkpoint seeding and the checkpoint gate.
During a campaign, live work writes to the active and the migrating version.
Backfills are disposable.
If N+2 arrives during the N+1 campaign, `SchemaMigration` cancels N+1 and starts N+2. N stays active.
A worker reads any version whose ontology format it supports.
One set of lanes serves all versions. A version needs no streams or pods of its own.

### The ledger

The ledger records what ran. An admin can show and replay it.
Metrics record how fast and how much.
The ledger holds no domain context and no stats.

| Table | One row for each | Retention |
|---|---|---|
| `engine_task` | Task that did work or failed | 30 days |
| `engine_job` | Job that did work or failed | 30 days |
| `engine_campaign` | Campaign, with its cursor and progress | Forever |
| `engine_generation` | NATS install | Forever |

A `no_change` task writes no row. Most runs do nothing. This rule keeps the volume low.
Each task event carries its child count.
The ledger sink ends a job when no task of it is open.
The ledger uses the graph ClickHouse cluster for now.
If it adds too much load there, it moves to SQLite on S3.
The ledger is a system store. The move needs only a second backend.

The admin API on the planner pod shows and replays jobs. It also runs campaigns.
A webserver reads job history through a read-only client. It needs no NATS and no planner pod.

```rust
impl LedgerReader {
    pub fn new(clickhouse: ClickHouseClient) -> Self;
    pub async fn jobs(&self, filter: JobFilter) -> Result<Vec<JobSummary>, LedgerError>;
    pub async fn job(&self, id: JobId) -> Result<Option<JobDetail>, LedgerError>;
    pub async fn task(&self, id: TaskId) -> Result<Option<TaskDetail>, LedgerError>;
}
```

<details>
<summary>Ledger DDL</summary>

```sql
CREATE TABLE engine_task
(
    task_id       UUID,
    job_id        UUID,
    parent_id     Nullable(UUID),
    tenant        LowCardinality(String),
    worker        LowCardinality(String),
    lane          LowCardinality(String),
    status        Enum8('running' = 1, 'detached' = 2, 'done' = 3, 'failed' = 4, 'lost' = 5),
    attempts      UInt16,
    error_class   LowCardinality(String),
    error_message String CODEC(ZSTD(3)),
    input         String CODEC(ZSTD(3)),
    pod           LowCardinality(String),
    enqueued_at   DateTime64(3),
    finished_at   Nullable(DateTime64(3)),
    updated_at    DateTime64(3),
    INDEX idx_task task_id TYPE bloom_filter GRANULARITY 4,
    INDEX idx_job  job_id  TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(enqueued_at)
ORDER BY (worker, toStartOfHour(enqueued_at), task_id)
TTL toDateTime(enqueued_at) + INTERVAL 30 DAY;

CREATE TABLE engine_job
(
    job_id      UUID,
    planner     LowCardinality(String),
    worker      LowCardinality(String),
    tenant      LowCardinality(String),
    key         String,
    campaign_id Nullable(UUID),
    replay_of   Nullable(UUID),
    cause_job   Nullable(UUID),
    status      Enum8('running' = 1, 'done' = 2, 'failed' = 3, 'lost' = 4),
    created_at  DateTime64(3),
    finished_at Nullable(DateTime64(3)),
    updated_at  DateTime64(3),
    INDEX idx_job job_id TYPE bloom_filter GRANULARITY 4,
    INDEX idx_key key    TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (planner, toStartOfHour(created_at), job_id)
TTL toDateTime(created_at) + INTERVAL 30 DAY;

CREATE TABLE engine_campaign
(
    campaign_id UUID,
    planner     LowCardinality(String),
    target      Nullable(String),
    status      Enum8('running' = 1, 'paused' = 2, 'done' = 3, 'failed' = 4, 'cancelled' = 5),
    cursor      String,
    submitted   UInt64,
    done        UInt64,
    failed      UInt64,
    started_at  DateTime64(3),
    finished_at Nullable(DateTime64(3)),
    updated_at  DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id;

CREATE TABLE engine_generation
(
    generation UInt32,
    binary     LowCardinality(String),
    created_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY generation;
```

</details>

### NATS holds no facts

| Object | Kind | Contents | After a wipe |
|---|---|---|---|
| `ORBIT_TASKS` | Work queue | One subject for each worker and lane | The recovery campaign submits the work again. |
| `<worker>__<lane>` | Pull consumer | One for each lane | The provisioner creates it again. |
| `ORBIT_TASK_EVENTS` | Stream, 24 h | Task events for the ledger sink and `done` triggers | Running jobs become `lost`. |
| `orbit_pending` | KV | One dedupe marker for each pending worker and key | Recovery submits the work again. |
| `orbit_leases` | KV with TTL | Key leases, pool permits, trigger and reconciler leases, the generation | Pods restart. The new generation fences old owners. |

The provisioner can find a generation in ClickHouse, but no NATS objects.
Then it writes a new generation and creates the objects.
Every pod restarts.
The engine starts one recovery campaign for each planner, in the backfill lanes.
Workers return `no_change` for inputs that are up to date.

### Host API

The host embeds the engine. For Orbit, the host is the `indexer` crate.

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load()?;
    Engine::builder()
        .config(config.engine.clone())
        .resources(indexer::resources(&config))
        .system_stores(orbit_store::embed!("stores"), indexer::clickhouse(&config))
        .module(indexer::code::register)
        .module(indexer::entity::register)
        .module(indexer::status::register)
        .module(indexer::migration::register)
        .build()?
        .serve(Role::from_env()?)
        .await
}
```

One binary has three roles.
The `planner` role runs on two pods. It runs system migrations, triggers, reconcilers, the ledger sink and the admin API.
The `worker` role serves the lanes in its Helm `serve` list. KEDA scales it from zero.
The `all` role runs everything in one process, for local development and tests.

`build()` fails on a duplicate name, a missing resource or a worker without a `default` or `backfill` lane.
It also fails on an unknown `on_timeout` lane or a cycle of `done` triggers.

<details>
<summary>Example: the code flow</summary>

```rust
impl Planner for CodePlanner {
    const NAME: &'static str = "code";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::Stream { subjects: &["siphon.push"], batch: 500, linger: Duration::from_secs(2) }]
    }

    async fn plan(&self, ctx: &mut PlanContext) -> Result<(), PlanError> {
        let inputs: Vec<CodeIndexInput> = match ctx.fired().clone() {
            Fired::Events(batch) => batch.decode::<RepositoryPushed>().map(|p| p.map(CodeIndexInput::from)).collect::<Result<_, _>>()?,
            Fired::Manual { keys } => self.projects.inputs(&keys).await?,
            Fired::Sweep { cursor, .. } => {
                let page = self.projects.page(cursor.as_deref(), 1000).await?;
                ctx.next_page(page.next);
                page.items
            }
            Fired::Cron => Vec::new(),
        };
        for input in inputs {
            let lane = match self.checkpoints.parseable_files(input.project_id).await? {
                Some(files) if files > self.big_repo_min_files => BIG,
                _ => Lane::DEFAULT,
            };
            ctx.submit::<CodeIndex>(input.tenant.clone(), input).lane(lane);
        }
        Ok(())
    }
}

impl Worker for CodeIndex {
    const NAME: &'static str = "code.index";
    const EXCLUSIVE: bool = true;
    type Input = CodeIndexInput;
    type Output = Indexed;

    fn key(input: &CodeIndexInput) -> Key {
        Key::new("project", input.project_id)
    }

    async fn run(&self, ctx: &TaskContext, input: CodeIndexInput) -> Result<Outcome<Indexed>, TaskError> {
        let archive = {
            let _permit = ctx.pool("gitaly_archives").await?;
            self.gitaly.archive(&input).await?
        };
        let inserts = self.writer.insert(self.parser.parse(archive).await?).await?;
        ctx.detach();
        let written = inserts.await?;
        self.checkpoints.save(&input, written.parseable_files).await?;
        Ok(Outcome::done(Indexed::from(written)))
    }
}
```

</details>

<details>
<summary>Example: the namespace status reconciler</summary>

```rust
impl Reconciler for NamespaceStatus {
    const NAME: &'static str = "namespace.status";
    const WRITES: &'static [&'static str] = &["engine.namespace_status"];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        let version = self.ontologies.active("gitlab").await?;
        for ns in self.status.not_indexed(&version).await? {
            let key = EntitySplitInput::key_for(ns.id, &version);
            match ctx.last_job::<EntitySplit>(&key).await? {
                Some(job) if job.is_done() => self.status.indexed(&ns, &version).await?,
                Some(job) if job.is_failed() => self.status.failed(&ns, &version).await?,
                Some(_) => {}
                None => ctx.replan::<EntityPlanner>(vec![key]).await?,
            }
        }
        Ok(())
    }
}
```

</details>

## Benefits of this approach

A new flow is a planner and a worker, with no engine change.
Orbit Next adds a Git lake fetch. Its readers subscribe with `done` triggers.
Third-party data adds an ontology version and a source, with no Orbit deploy.

Lanes, slot release and retries move out of the code handler into the engine.
KEDA and the logs can then see them.
Every job that did work is in the ledger. An admin can replay it.

A migration no longer blocks a rollout.
A system table change is one new step file. It starts no graph rebuild.
An indexed data migration runs in the backfill lanes. Their pods scale to zero when no campaign runs.

NATS becomes disposable.
A wipe costs one recovery campaign. No fact is lost.

## Consequences

- Every write must be idempotent. Tasks run at least once. Recovery repeats work.
- A planner must handle `Sweep` and `Manual`. Without them, NATS recovery and `replan` cannot reach its work.
- KEDA scales on message count, not cost. The planner must split work into lanes by size.
- During a backoff, the backlog grows. KEDA scales up pods that only wait. Lane `max` replicas cap the scale-up.
- Live work writes to two versions during a campaign. Code jobs run two times while it lasts.
- The ledger shares the graph cluster. A ledger that loads it too much must move to its own backend.
- A job with only `no_change` tasks has no ledger row. A worker that a reconciler watches must return `done` on its first run at a version.
- A `contract` that drops a filled column must ship one release after the fill ends. Review enforces it.
- The version scope of ADR 017 must travel with the version in the registry. A reviewed file cannot follow a runtime change.

These items are out of scope:

- The query side, authorization of the admin API and billing emission
- Fair scheduling for each tenant inside a lane
- Connectors for third-party data

## Why not the alternatives

### Grow the current dispatcher and engine

Each flow would keep its own lanes, retries and migration path.
The dispatcher already has one pattern for each trigger. The next two projects would add two more.

### An external workflow engine

Temporal, Dagster and Restate each bring a stateful service with its own database.
Self-Managed and Dedicated would have to run it.
They also keep a full history for each run. Most of our runs do nothing.
We already run NATS and ClickHouse. The engine adds only lanes, leases and a ledger on top of them.

### Joins after children

A `then` step after all children end needs a counter in NATS for each open job.
No flow needs a fan-in now. The namespace status uses a reconciler.
The ledger already detects the end of a job. A `Trigger::job_end` can add fan-in later, with no NATS state.

### Workers that start other teams' work

The fetch worker could spawn the trigram and graph builds.
It would then know every consumer of the lake. Each new consumer would change it.
A `done` trigger keeps the producer unaware of its consumers.

### A declarative diff for system tables

A tool could compare the declared `CREATE TABLE` with the live table. It would compute the `ALTER` statements.
Each backend would need its own shadow database and diff rules.
Numbered steps work on any SQL backend. A SQLite file restored from S3 knows its own version.

### Deferred features

The API has no feature that no flow uses. Each deferred feature can come back without a breaking change.

| Deferred | How to add it later |
|---|---|
| Fan-in after children | `Trigger::job_end::<W>()`, from the ledger |
| Reconciler wake-up on a job end | A `WATCH` constant with a default |
| An ingest HTTP trigger | The host mounts a route that publishes to a subject. A `Stream` trigger reads it. |
| Task IDs, attempt and deadline on `TaskContext` | New getters |
| A clock on contexts | A `Clock` resource. The testkit provides a fake one. |
| A history of failed attempts | A new ledger table from the same task events |
| A data fill as a store step | A step kind that names a planner. Today a reconciler starts the campaign. |
| A Prometheus scaler during a backoff | A KEDA trigger on the backoff gauge |

## References

- [Orbit Engine v2 epic](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/21)
- [Orbit Code Indexing v2 with Object Store](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/16)
- [Support third-party data sources in Orbit](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/20)
- [Non-blocking migrations and blue/green deployment](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/7)
- [ADR 017: Clone-based non-blocking schema migrations](017_clone_based_non_blocking_migrations.md)
- [ADR 020: Billing SOX scope](020_billing_sox_scope.md)
