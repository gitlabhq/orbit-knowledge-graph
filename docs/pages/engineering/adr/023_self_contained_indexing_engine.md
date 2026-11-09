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

## Context and problem

Orbit indexing has two parts today.
The dispatcher decides what runs. The indexer engine runs each task from NATS.
Both parts grew with each new flow.

This shape causes three problems:

- **Each flow brings its own mechanics.** The dispatcher has one pattern for each kind of trigger: cron, CDC, sweeps and migrations. The code handler adds its own lanes and slot release. KEDA and the logs cannot see them.
- **Migrations block indexing.** Schema migrations run when the dispatcher starts. A rollout waits for them.
- **Runs are not visible.** Nothing records what ran. We cannot explain a stale namespace or replay a run.

The next two projects make these problems larger.
[Orbit Next](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/16) and [third-party data](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/20) need new flows without engine changes.
They also need new ontology versions at runtime, with no deploy.

## Decision

**One library, `indexing-engine`, runs all indexing work. A flow is a planner and a worker. The engine owns lanes, retries, scaling, the ledger and migrations.**

This section walks through the engine from the top down.
It follows one push to project 42, from the trigger to the ledger.

### The engine in one picture

![Orbit Engine V2 in one picture](../../../assets/adr/023-indexing-engine.svg)

A flow has up to three pieces of code. Helm configures where they run.

| Piece | Question it answers | Runs on |
|---|---|---|
| Planner | Which inputs run now, and in which lane? | Planner pod |
| Worker | How is one input done? | Worker pods, in a lane |
| Reconciler | Is a recorded fact still true? | Planner pod |

The engine has no domain terms: no project, namespace, Gitaly or ontology.
A CI check enforces it.
Tasks run at least once. Every write is idempotent.
NATS holds no facts, and every system store can be rebuilt. A failure costs a reindex, never querying.

### Triggers: what wakes the engine

A trigger tells the engine when to call a planner.
The code planner wakes on the Siphon stream of pushes.

```rust
fn triggers() -> Vec<Trigger> {
    vec![Trigger::Stream {
        subjects: &["siphon.push"],
        batch: 500,
        linger: Duration::from_secs(2),
    }]
}
```

Each kind of trigger replaces one dispatcher pattern.

```rust
pub enum Trigger {
    Cron(&'static str),
    Stream { subjects: &'static [&'static str], batch: usize, linger: Duration },
    Done { worker: &'static str },
}

impl Trigger {
    pub fn done<W: Worker>() -> Self;
}
```

`Done` fires when a task of another worker ends with `done`.
The engine also calls every planner in two more cases.
The admin API or a reconciler sends `Manual`. A campaign or a NATS recovery sends `Sweep`.

An event only says that something changed. The data stays in its source system, such as GitLab or the Git lake.
If NATS loses an event, a sweep or a reconciler reads the source again.

### Planners: what runs, and where

A planner turns a trigger into worker inputs. It also picks the lane of each input.

```rust
#[async_trait]
pub trait Planner: FromResources + Send + Sync + 'static {
    const NAME: &'static str;

    fn triggers() -> Vec<Trigger>;
    async fn plan(&self, ctx: &mut PlanContext) -> Result<(), PlanError>;
}
```

The code planner reads the push events and routes each repository by its size.
The size comes from the source of truth, not from a checkpoint.

```rust
async fn plan(&self, ctx: &mut PlanContext) -> Result<(), PlanError> {
    let projects: Vec<ProjectRef> = match ctx.fired().clone() {
        Fired::Events(batch) => batch.decode()?,
        Fired::Manual { keys } => self.projects.find(&keys).await?,
        Fired::Sweep { cursor, .. } => self.projects.page(ctx, cursor).await?,
        Fired::Cron => return Ok(()),
    };
    for project in projects {
        let size = self.projects.repository_size(project.id).await?;
        let lane = if size > BIG_REPOSITORY_BYTES { BIG } else { Lane::DEFAULT };
        ctx.submit::<CodeIndex>(project.tenant.clone(), project).lane(lane);
    }
    Ok(())
}
```

A planner reads only events and Orbit storage. It never calls a source.
A split that reads a source is a worker.
Every planner handles `Manual` and `Sweep`. The testkit checks it.

### Lanes: where work runs

A lane is a queue for one worker. It sets how its tasks behave on a timeout.

```yaml
engine:
  lanes:
    code.index:
      default:  { timeout: 15m, on_timeout: big }
      big:      { timeout: 2h }
      backfill: { timeout: 2h }
```

An instance is a set of worker pods. It sets the number of pods, the slots of each lane on one pod, and the pod pools.

```yaml
engine:
  pools:
    gitaly_archives: { per_key: 8, unknown_key: 4, max_wait: 60s }
  instances:
    code:
      role: worker
      pods: { min: 1, max: 20 }
      serve: { code.index/default: 16, code.index/big: 6 }
      pools: { parse: 10, parse_big: 4, insert: 32 }
    backfill:
      role: worker
      pods: { min: 0, max: 40 }
      serve: { code.index/backfill: 2 }
```

Each `code` pod admits up to 16 small and 6 big repositories at once.
Each lane has its own slots on a pod. A burst of small repositories cannot take the big slots.
An instance is one Deployment and one KEDA ScaledObject. Helm renders both from `engine.instances`.
The Deployment sets `ORBIT_INSTANCE` on its pods. A pod serves the lanes in the `serve` list of its instance.
Every worker has a `default` and a `backfill` lane. One instance serves each lane.
Each lane is one NATS consumer and one KEDA trigger. KEDA scales the pods of an instance on their pending tasks.

| Task source | Lane |
|---|---|
| A submit | `.lane()`, else `default` |
| A submit during a sweep | `backfill`, unless `.lane()` is set |
| A submit from a `done` event | The lane of the source task, unless `.lane()` is set |
| A spawned child | The lane of its parent |
| A lane timeout with `on_timeout` | The `on_timeout` lane |

`timeout` is the longest time one attempt may run. The clock starts with `run`. It includes the time after `ctx.detach()`.
When the time is up, the engine drops `run`. It frees the slot and releases the lease.
`on_timeout` moves the task to another lane of the same worker. Without it, the task retries in its lane.
A repository can be slower than its size suggests. On a timeout, it moves from `default` to `big`.

### Tasks: keys, dedupe and leases

Each input has a key. The engine uses it for dedupe and for the lease.

```rust
const EXCLUSIVE: bool = true;

fn key(project: &ProjectRef) -> Key {
    Key::new("project", project.id)
}
```

The engine keeps one pending task for each worker and key.
A burst of 50 pushes to project 42 gives one task.
The marker clears when the task starts. A push during a run queues one more task.
With `EXCLUSIVE`, the engine takes a lease on the key. Two runs of project 42 never overlap.

A lease is a key in the `orbit_leases` bucket. Its value says who holds it and until when.

```rust
pub struct Lease {
    pub holder: PodId,
    pub expires_at: DateTime<Utc>,
}
```

NATS TTLs are not used. Every write is a compare-and-swap on the revision of the key.
The engine renews the lease every third of `engine.leases.ttl`, which is 60 s by default.
A failed renewal means another pod holds the key. The engine then drops `run`.
If a pod dies, its `expires_at` passes. The next pod that pulls the redelivered message takes the lease.
Until then, that pod defers the task to `expires_at`.
A pod treats a lease as expired only 5 s after `expires_at`. This margin covers clock skew between pods.
At the end of `run`, the engine deletes the lease. A dropped `run` also releases it, on a best-effort basis.

The input holds IDs, not data.
The worker reads the current state when it runs. A pending task always indexes the newest push.

### Workers: doing one input

A worker runs one input and returns one outcome. It does not know its lane.

```rust
#[async_trait]
pub trait Worker: FromResources + Send + Sync + 'static {
    const NAME: &'static str;
    const EXCLUSIVE: bool = false;
    type Input: Payload;
    type Output: Payload;

    fn key(input: &Self::Input) -> Key;
    async fn run(&self, ctx: &TaskContext, input: Self::Input) -> Result<Outcome<Self::Output>, TaskError>;
}
```

The engine builds each worker from its resources.
The code worker fetches under a cluster pool and parses under a pod pool.
It frees its slot while ClickHouse confirms the inserts.

```rust
async fn run(&self, ctx: &TaskContext, project: ProjectRef) -> Result<Outcome<Indexed>, TaskError> {
    let repository = {
        let _permit = ctx.pool("gitaly_archives").key(&project.storage).await?;
        self.gitaly.fetch(&project).await?
    };
    let pool = if repository.files > SMALL_REPO_MAX_FILES { "parse_big" } else { "parse" };
    let inserts = {
        let _permit = ctx.pool(pool).await?;
        self.graph.write(repository).await?
    };
    ctx.detach();
    Ok(Outcome::done(inserts.confirmed().await?))
}
```

A pool limits one stage of a task. The code names the pool. Helm sets its scope and size.
A pool in `engine.pools` is cluster-wide, with a limit for each key, such as a Gitaly storage.
`unknown_key` is the limit for inputs with no key.
A pool in an instance limits a stage inside one pod. Here, 16 tasks fetch ahead while 10 parse.
The worker picks `parse` or `parse_big` after the fetch, from the real file count.
A task can hold several permits of one pool, one for each part of its parallel work.
A task that waits longer than `max_wait` defers. It frees its slot and lease, and uses no attempt.
A cluster permit is a lease. If the engine loses it, it drops `run`.

`ctx.detach()` frees the slot. The lane pulls the next task.
The message and the lease stay held until `run` returns.
The engine sends heartbeats and renews the lease. The worker never does.

A worker that indexes an ontology gets the version in its input.
It loads the version from the registry. The engine never reads it.

### Outcomes and errors

The result of `run` decides the next step.

```rust
impl<O: Payload> Outcome<O> {
    pub fn done(output: O) -> Self;
    pub fn no_change() -> Self;
    pub fn spawn<W: Worker>(self, inputs: impl IntoIterator<Item = W::Input>) -> Self;
}
```

`no_change` writes no ledger row and fires no `done` trigger.
The code worker returns it when its checkpoint already covers the newest commit.

```rust
if self.checkpoints.covers(&project).await? {
    return Ok(Outcome::no_change());
}
```

Errors tell the engine how to retry.

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

### Chaining work

A job is a task plus every task it spawned. A planner's submit starts a new job.

#### A worker may start only parts of its own work

`spawn` splits the input of a worker into child tasks in the same job.
When a namespace is turned on, `namespace.enable` spawns its SDLC split and one code task for each project.

```rust
let projects = self.projects.in_namespace(namespace.id).await?;
Ok(Outcome::done(())
    .spawn::<EntitySplit>([namespace])
    .spawn::<CodeIndex>(projects))
```

The SDLC split spawns one `entity.index` task for each range, in the same job.
A job ends only when all its tasks end. One job lookup tells if the whole namespace is indexed.
Children run in the lane of their parent.

#### A planner starts all new work, also after another worker

A `done` trigger calls a planner when a task of another worker ends.
The trigram build of Orbit Next starts when the Git lake fetch ends.

```rust
fn triggers() -> Vec<Trigger> {
    vec![Trigger::done::<GitFetch>()]
}
```

The planner reads each output of the fetch with its real type.

```rust
if let Fired::Events(batch) = ctx.fired().clone() {
    for update in batch.outputs::<GitFetch>()? {
        ctx.submit::<TrigramBuild>(update.project.tenant.clone(), update.project);
    }
}
```

`outputs::<GitFetch>()` returns `GitFetch::Output`. A change to that type breaks the build of the planner, not its runs.
The fetch does not know its readers. A new reader adds one planner and one worker.
The new job records the source job in `cause_job`.

### The ledger: what ran

The ledger records what ran. An admin can show and replay it.
Metrics record how fast and how much.
The ledger holds no domain context and no stats.

| Table | One row for each | Retention |
|---|---|---|
| `engine_task` | Task that did work or failed | 30 days |
| `engine_job` | Worker and key, with its last job | 30 days after that job |
| `engine_campaign` | Campaign of one planner: its tag, status and next page | Forever |
| `engine_generation` | NATS install | Forever |

A `no_change` task writes no row.
The history of a job is its root task row and its children.
When no task of a job is open, the sink writes the job's status to `engine_job`.
`engine_job` keeps only the newest job for each worker and key.
The ledger uses the graph cluster for now. It can move to SQLite on S3 later.

The admin API on the planner pod shows and replays jobs. It also runs campaigns.
A webserver reads job history through a read-only client.

```rust
impl LedgerReader {
    pub async fn jobs(&self, filter: JobFilter) -> Result<Vec<JobSummary>, LedgerError>;
    pub async fn job(&self, id: JobId) -> Result<Option<JobDetail>, LedgerError>;
    pub async fn task(&self, id: TaskId) -> Result<Option<TaskDetail>, LedgerError>;
}
```

<details>
<summary>Ledger DDL</summary>

The tables use `ReplacingMergeTree(_version)`.
Sort key columns never change. Each task keeps one row. Reads use `FINAL`.
In `engine_job`, `_version` is the `created_at` of the job. A newer job always wins.

```sql
CREATE TABLE IF NOT EXISTS engine_task (
    task_id UUID,
    job_id UUID,
    parent_id Nullable(UUID),
    planner LowCardinality(String),
    worker LowCardinality(String),
    lane LowCardinality(String),
    tenant LowCardinality(String),
    key String CODEC(ZSTD(1)),
    tag LowCardinality(String) DEFAULT '',
    replay_of Nullable(UUID),
    cause_job Nullable(UUID),
    status Enum8('running' = 1, 'detached' = 2, 'done' = 3, 'failed' = 4, 'lost' = 5),
    attempts UInt16,
    error_class LowCardinality(String),
    error_message String CODEC(ZSTD(3)),
    input String CODEC(ZSTD(3)),
    pod LowCardinality(String),
    created_at DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    finished_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
    _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(Delta(8), ZSTD(1)),
    INDEX idx_task task_id TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_job job_id TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_tag tag TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_status status TYPE set(8) GRANULARITY 4
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (worker, toStartOfHour(created_at), task_id)
TTL toDateTime(created_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS engine_job (
    worker LowCardinality(String),
    key String CODEC(ZSTD(1)),
    job_id UUID,
    status Enum8('running' = 1, 'done' = 2, 'failed' = 3, 'lost' = 4),
    created_at DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    finished_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
    _version DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1))
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (worker, key)
TTL toDateTime(created_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS engine_campaign (
    tag LowCardinality(String),
    planner LowCardinality(String),
    status Enum8('running' = 1, 'paused' = 2, 'done' = 3, 'failed' = 4, 'cancelled' = 5),
    next_page Nullable(String) CODEC(ZSTD(1)),
    started_at DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    finished_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
    _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(Delta(8), ZSTD(1))
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (tag, planner);

CREATE TABLE IF NOT EXISTS engine_schema_migrations (
    store LowCardinality(String),
    version UInt32,
    name String,
    checksum FixedString(64),
    status Enum8('seen' = 1, 'applied' = 2, 'failed' = 3),
    error String DEFAULT '',
    binary LowCardinality(String),
    applied_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY (store, version);

CREATE TABLE IF NOT EXISTS engine_generation (
    generation UInt32,
    binary LowCardinality(String),
    created_at DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(Delta(8), ZSTD(1))
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (generation);
```

</details>

### Reconcilers: keeping facts true

A reconciler compares the wanted state with the observed state. It writes the difference.
It runs on the planner pod under a lease, with no slot.

```rust
#[async_trait]
pub trait Reconciler: FromResources + Send + Sync + 'static {
    const NAME: &'static str;
    const WRITES: &'static [&'static str];
    const EVERY: Duration;

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError>;
}
```

A timer is the only trigger of a reconciler. The engine runs it every `EVERY`, on one planner replica.

```rust
const EVERY: Duration = Duration::from_secs(60);
```

Each tick checks the full state. A lost event only delays a fact by one period.
A reconciler never submits a task. It calls `replan` or `campaign` on a planner, so the planner keeps all routing.

| Reconciler | Wanted state | Action |
|---|---|---|
| Namespace status | Every enabled namespace is indexed at the active version | Writes `indexed` or `failed`. With no job, it replans the namespace. |
| Schema migration | The newest ontology version is active | Creates the tables, starts a campaign, then promotes the version |
| Code freshness | Each checkpoint matches the newest commit of its project | Replans the stale projects. This heals lost push events. |
| Namespace removal | A disabled namespace has no graph data | Replans the deletion of its data |

Here is the namespace status reconciler.
A namespace is indexed when its `namespace.enable` job at the active version is done.

```rust
let version = self.registry.active("gitlab").await?;
for ns in self.status.not_indexed(&version).await? {
    let key = Key::new("namespace", format!("{}@{version}", ns.id));
    match ctx.last_job::<NamespaceEnable>(&key).await? {
        Some(job) if job.is_done() => self.status.indexed(&ns, &version).await?,
        Some(job) if job.is_failed() => self.status.failed(&ns, &version).await?,
        Some(_) => {}
        None => ctx.replan::<NamespacePlanner>(vec![key]).await?,
    }
}
```

One reconciler writes each table.
A tick is idempotent and has a 30 s limit.
A failed tick has no retry. The next tick runs again.

### Migrations

Data has two classes, with opposite migration rules.

| Class | Owner | Changes | Examples |
|---|---|---|---|
| System | The binary | Only with a deploy | Engine ledger, checkpoints, namespace status |
| Indexed data | The ontology registry | At any time | Graph nodes and edges |

**The engine migrates the system schema at startup.**
`orbit-store` keeps numbered SQL steps, with one folder for each dialect.
There is one store for now, `engine`. It holds the checkpoints, the namespace status and the ledger.

```plaintext
stores/engine/
  clickhouse/
    000_baseline.sql
    022_expand_parseable_files.sql
  sqlite/
    000_baseline.sql
    022_expand_parseable_files.sql
```

```rust
Engine::builder().system_stores(orbit_store::embed!("stores"), clickhouse)
```

| Step | What it may do | When it runs |
|---|---|---|
| `baseline` | Create every table of the store in its current shape | Only when the store is created or rebuilt |
| `expand` | Add a table, a column with a default, a TTL or a comment | At startup, before indexing serves |
| `contract` | Drop a column or a table | One release after the first binary that sees it |

A step may only change metadata. `build.rs` rejects a step that rewrites data, such as a type change or a new sort key.
Such a change is a new table, filled by a campaign.
CI runs every step on each supported ClickHouse version. At startup, the engine checks the version against the minimum.
`engine_schema_migrations` records the state of each step. Its shape never changes.

When a step fails, the engine rebuilds the store:

```plaintext
for each engine table: create it fresh under a temporary name, in the full current shape
 → EXCHANGE TABLES with the live table → rename the old copy to <table>_migration_v<step>_backup
 → mark every step as applied
 → start one recovery campaign for each planner, in the backfill lanes
```

Table names never change, and the recovery campaign refills the empty tables. The cost is a full reindex and the ledger history, but graph queries keep working. The engine rebuilds at most once for each release. A second failure, or a failed `CREATE`, stops indexing and alerts the admin. This works because every table can be rebuilt from the source, or can start empty.

**The ontology registry owns the indexed data schema. The engine has no part in it.**
A new version can come from any team at any time.
A `SchemaMigration` reconciler in the indexer starts a campaign for it.

```rust
for version in self.registry.migrating().await? {
    self.graph.create_tables(&version).await?;
    if ctx.campaign::<NamespacePlanner>(&version.to_string()).await?.is_done() {
        self.registry.promote(&version).await?;
    }
}
```

A failed `create_tables` or campaign marks the version `failed` in the registry, with its error.
The reconciler stops retrying it. The active version keeps serving queries. An admin can press Reindex to retry it.
A campaign calls `plan()` with `Sweep`, one page at a time, in the backfill lanes.
Every task it starts carries its tag, such as `migration:199`.
After a restart, it resumes from its saved `next_page`.
It is done when its last page is submitted and no tagged task is open.
Live work writes to both versions until promotion. A newer version cancels an older campaign.

### When NATS is lost

NATS holds work in flight, dedupe markers and leases. It holds no fact.

| Object | Contents | After a wipe |
|---|---|---|
| `ORBIT_TASKS` | One subject for each worker and lane | The recovery campaign submits the work again. |
| `ORBIT_TASK_EVENTS` | Task events for the ledger sink and `done` triggers | Running jobs become `lost`. |
| `orbit_pending` | One dedupe marker for each pending worker and key | Recovery submits the work again. |
| `orbit_leases` | Key leases with their holder and `expires_at`, pool permits, trigger and reconciler leases, the generation | Pods restart. The new generation fences old owners. |

The provisioner can find a generation in ClickHouse, but no NATS objects.
Then it writes a new generation and creates the objects.
The engine starts one recovery campaign for each planner.
Workers return `no_change` for inputs that are up to date.

### Putting it together

The host embeds the engine. For Orbit, the host is the indexer binary.

```rust
Engine::builder()
    .config(config.engine.clone())
    .resources(resources)
    .system_stores(orbit_store::embed!("stores"), clickhouse)
    .module(code::register)
    .build()?
    .serve(Role::from_env()?)
    .await
```

One binary has three roles.
The `planner` role runs on two pods. It runs system migrations, triggers, reconcilers, the ledger sink and the admin API.
The `worker` role runs the pods of one instance. It serves the lanes in its `serve` list.
The `all` role runs everything in one process, for local development and tests.

`build()` fails on a duplicate name, a missing resource or a worker without a `default` or `backfill` lane.
It also fails on an unknown `on_timeout` lane or a cycle of `done` triggers.

The testkit runs the engine in memory, with a fake clock.

```rust
#[tokio::test]
async fn routes_a_big_repository_to_the_big_lane() {
    let kit = Testkit::new().provide(projects_with_repository_size(42, 8 * GIB)).module(code::register);
    assert_eq!(kit.plan::<CodePlanner>(push(42)).await.lane_of::<CodeIndex>(), BIG);
}
```

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
- A failed migration costs a full reindex and the ledger history, at most once for each release. Graph queries keep working.
- A step may only change metadata. A change that rewrites data is a new table and a campaign.
- V2 replaces the V1 concurrency and timeout keys. Both config blocks are accepted until V1 is removed.
- A planner must handle `Sweep` and `Manual`. Without them, NATS recovery and `replan` cannot reach its work.
- KEDA scales on message count, not cost. The planner must split work into lanes by size.
- During a backoff, the backlog grows. KEDA scales up pods that only wait. The `pods.max` of the instance caps the scale-up.
- Live work writes to two versions during a campaign. Code jobs run two times while it lasts.
- The ledger shares the graph cluster. A ledger that loads it too much must move to its own backend.
- A worker that a reconciler watches must return `done` on its first run at a version.
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

### A declarative diff for system tables

A tool could compare the declared `CREATE TABLE` with the live table. It would compute the `ALTER` statements.
Each backend would need its own shadow database and diff rules.
Numbered steps work on any SQL backend. A SQLite file restored from S3 knows its own version.

## References

- [Orbit Engine v2 epic](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/21)
- [Orbit Code Indexing v2 with Object Store](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/16)
- [Support third-party data sources in Orbit](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/20)
- [Non-blocking migrations and blue/green deployment](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/7)
- [ADR 017: Clone-based non-blocking schema migrations](017_clone_based_non_blocking_migrations.md)

## Appendix: NATS load today and with the engine

Production numbers are from the 7 days to 2026-11-09. Siphon streams are not part of the engine.

| | Today | With the engine |
|---|---|---|
| Orbit streams | 10: `GKG_INDEXER` and `GKG_DEAD_LETTERS`, with a copy for each release | 2: `ORBIT_TASKS` and `ORBIT_TASK_EVENTS` |
| Subjects in the task stream | Up to 83,000: one for each project branch or namespace | One for each worker and lane, about 15 |
| Consumers | 17, up to 21 during a release | One for each worker and lane, plus planner triggers and the ledger sink |
| Tasks published | 1.9 a second | About the same. Task events add two or three messages for each task. |
| Tasks pending | Up to 83,000 on the code consumer | At most one for each worker and key, plus `max_pending` for each campaign |
| Tasks waiting for an ack | Up to 1,660 | At most the slots of all running pods |
| Lock entries | 940,000 in 6 buckets, one for each schema version | One lease for each running exclusive task. The engine deletes it at the end. |
| Retired KV data | 669,000 progress entries, no longer written | None |
| Dead letters | 1,280 messages in 5 streams | `failed` rows in `engine_task` |
| Siphon | 13 streams, 18.9 M messages, 22 messages a second | No change |

## Appendix: V1 config keys and their V2 equivalents

| V1 key | V2 equivalent |
|---|---|
| `topics` | `engine.lanes` |
| `concurrency_groups` | The slots in `engine.instances.<name>.serve` |
| `fetch_concurrency` | The lane slots in `serve` |
| `small_indexing_slots`, `big_indexing_slots` | The pod pools `parse` and `parse_big` |
| `write_max_concurrent` | The pod pool `insert` |
| `archive_storage_budget`, `archive_tenant_budget` | `per_key` and `unknown_key` of the cluster pool `gitaly_archives` |
| `archive_lease_max_wait_secs` | `max_wait` of the pool |
| `job_timeout_secs` | `timeout` on the lane |
