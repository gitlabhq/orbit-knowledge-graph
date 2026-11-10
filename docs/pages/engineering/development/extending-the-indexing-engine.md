# Extending the indexing engine

The engine is being built in [epic 21](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/21).
This guide shows its API, and it changes with the code.
[The indexing engine](../architecture/indexing/indexing-engine.md) explains how the engine works.
[ADR 023](../adr/023_self_contained_indexing_engine.md) records why.

A flow has up to three pieces of code. Helm configures where they run.

| Piece | Question it answers | Runs on |
|---|---|---|
| Planner | Which inputs run now, and in which lane? | Planner pod |
| Worker | How is one input done? | Worker pods, in a lane |
| Reconciler | Is a recorded fact still true? | Planner pod |

The examples follow the code flow: one push to project 42.

## Step 1: Declare the triggers

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

```rust
pub enum Trigger {
    Cron(&'static str),
    Stream { subjects: &'static [&'static str], batch: usize, linger: Duration },
    Done(DoneTrigger),
}

impl Trigger {
    pub fn done<W: Worker>() -> Self;
}
```

`Done` fires when a task of another worker ends with `done`.
`Trigger::done` is the only way to build it, so the build fails when the worker does not exist.
The engine also calls every planner with `Manual` and `Sweep`.
The admin API or a reconciler sends `Manual`. A campaign or a NATS recovery sends `Sweep`.

## Step 2: Write the planner

A planner turns a trigger into worker inputs. It also picks the lane of each input.

```rust
#[async_trait]
pub trait Planner: FromResources + Send + Sync + 'static {
    const NAME: &'static str;

    fn triggers() -> Vec<Trigger>;
    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError>;
}
```

The code planner routes each repository by its size in the source of truth.

```rust
async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
    let projects: Vec<ProjectRef> = match fired {
        Fired::Events(batch) => batch.decode()?,
        Fired::Manual { keys } => self.projects.find(&keys).await?,
        Fired::Sweep { cursor, .. } => {
            let page = self.projects.page(cursor).await?;
            ctx.continue_from(page.next);
            page.projects
        }
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

A planner reads only events and Orbit storage, never a source. Work that must read a source to split itself is a worker.

A planner matches every `Fired` variant, with no wildcard arm. A planner that has no use for a variant returns from that arm, so the decision shows in the code. A sweep with more pages calls `ctx.continue_from`, and the engine saves the cursor. During a sweep the engine ignores `.lane()` and uses the `backfill` lane.

## Step 3: Write the worker

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

### Keys and leases

```rust
const EXCLUSIVE: bool = true;

fn key(project: &ProjectRef) -> Key {
    Key::new("project", project.id)
}
```

The engine keeps one pending task for each worker and key, and the marker clears when the task starts. With `EXCLUSIVE`, it also takes a lease on the key, so two runs of one key never overlap. The input holds IDs, and the worker reads the current state when it runs.

A lease is a key in the `orbit_leases` bucket. Its value says who holds it and until when.

```rust
pub struct Lease {
    pub holder: PodId,
    pub expires_at: DateTime<Utc>,
}
```

The engine renews the lease every third of `engine.leases.ttl`, which is 60 s by default. If a renewal fails, the engine drops `run`. If a pod dies, the next pod waits until `expires_at`, then takes the lease. NATS TTLs are not used.

### Pools and `ctx.detach()`

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

The code names a pool, and Helm sets its scope and size. A cluster pool has a limit for each key, such as a Gitaly storage. A pod pool limits one stage on a pod, so fetches can run ahead of parsing. A task that waits past `max_wait` defers, and uses no attempt. `ctx.detach()` frees the slot, but the message and the lease stay held until `run` returns.

## Step 4: Return an outcome or an error

```rust
impl<O: Payload> Outcome<O> {
    pub fn done(output: O) -> Self;
    pub fn no_change() -> Self;
    pub fn spawn<W: Worker>(self, inputs: impl IntoIterator<Item = W::Input>) -> Self;
}
```

`no_change` writes no ledger row and fires no `done` trigger.

```rust
if self.checkpoints.covers(&project).await? {
    return Ok(Outcome::no_change());
}
```

```rust
impl TaskError {
    pub fn permanent(e: impl Into<anyhow::Error>) -> Self;
    pub fn defer(after: Duration, reason: &'static str) -> Self;
    pub fn throttle(after: Duration, reason: &'static str) -> Self;
    pub fn incompatible(reason: &'static str) -> Self;
}
```

| Error | Engine action | Uses an attempt |
|---|---|---|
| Any error, from `?` | Retry with backoff. Dead-letter after the last attempt. | Yes |
| `defer` | This input is not ready. Redeliver it after the delay. | No |
| `throttle` | A shared dependency pushes back. Redeliver. Stop pulling from the lane on this pod until the delay ends. | No |
| `incompatible` | This binary lacks a feature that the input needs. Redeliver to another pod. | No |
| Lane timeout | Move to the `on_timeout` lane, if Helm sets one. Else retry. | Yes |
| `permanent` | Dead-letter now. | — |
| Older than `max_age` | Dead-letter now, with the error class `expired`. | — |

A shared client returns a `Throttle` error when its breaker is open. The worker only writes `?`.

`defer`, `throttle` and `incompatible` use no attempt, so `max_age` on the lane sets the limit. The age counts from the first delivery to a pod, not from the submit. A task that waits in a backfill lane does not age.

## Step 5: Chain work

A job is a task plus every task it spawned. A planner's submit starts a new job.

`spawn` splits the input of a worker into child tasks in the same job. Children run in the lane of their parent. One outcome spawns at most 10,000 children. A worker with more inputs spawns children that split the rest.

```rust
let projects = self.projects.in_namespace(namespace.id).await?;
Ok(Outcome::done(())
    .spawn::<EntitySplit>([namespace])
    .spawn::<CodeIndex>(projects))
```

Work for another team starts in its planner, with a `done` trigger.

```rust
fn triggers() -> Vec<Trigger> {
    vec![Trigger::done::<GitFetch>()]
}
```

```rust
match fired {
    Fired::Events(batch) => {
        for update in batch.outputs::<GitFetch>()? {
            ctx.submit::<TrigramBuild>(update.project.tenant.clone(), update.project);
        }
    }
    Fired::Manual { .. } | Fired::Sweep { .. } | Fired::Cron => {}
}
```

`outputs::<GitFetch>()` returns `GitFetch::Output`. A change to that type breaks the build of the planner, not its runs. A payload change only adds fields, because old events stay in the stream during a rollout.

## Step 6: Add a reconciler

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
The first tick runs when the engine starts. If a planner pod crash-loops, each restart ticks again, with no backoff. Dedupe keeps those replans cheap, so we accept this risk until we hit it.

```rust
const EVERY: Duration = Duration::from_secs(60);
```

The namespace status reconciler reads the newest job for each namespace.

```rust
let version = self.registry.active("gitlab").await?;
for ns in self.status.not_indexed(&version).await? {
    let key = NamespaceEnable::key(&ns.at(&version));
    match ctx.last_job::<NamespaceEnable>(&key).await? {
        Some(job) if job.is_done() => self.status.indexed(&ns, &version).await?,
        Some(job) if job.is_failed() => self.status.failed(&ns, &version).await?,
        Some(_) => {}
        None => ctx.replan::<NamespacePlanner>(vec![key]).await?,
    }
}
```

A reconciler never submits a task. It calls `replan` or `campaign` on a planner. One reconciler writes each table. A tick is idempotent and has a 30 s limit, and a failed tick has no retry.

## Step 7: Configure what each pod pulls

Each pod reads only its own config. It lists the lanes and sinks that the pod pulls, with their slots and pools. The pod knows nothing about other pods.

```yaml
engine:
  lanes:
    code.index/default: { slots: 16, timeout: 15m, max_age: 24h, on_timeout: code.index/big }
  pools: { parse: 10 }
```

Helm decides which configs run, and how many pods run each one.

```yaml
shared:
  engine:
    pools: { gitaly_archives: { cluster: true, per_key: 8, unknown_key: 4, max_wait: 60s } }
deployments:
  code-small:
    pods: { min: 1, max: 20 }
    config:
      engine:
        lanes: { code.index/default: { slots: 16, timeout: 15m, on_timeout: code.index/big } }
        pools: { parse: 10 }
  code-big:
    pods: { min: 0, max: 4 }
    resources: { cpu: 8, memory: 32Gi }
    config:
      engine:
        lanes: { code.index/big: { slots: 6, timeout: 2h } }
        pools: { parse_big: 4 }
```

A test instance can run one deployment whose config lists every lane.

```yaml
deployments:
  universal:
    pods: { min: 1, max: 1 }
    config:
      engine:
        lanes:
          code.index/default: { slots: 4, timeout: 15m, on_timeout: code.index/big }
          code.index/big: { slots: 1, timeout: 2h }
          code.index/backfill: { slots: 1, timeout: 2h }
```

| Task source | Lane |
|---|---|
| A submit | `.lane()`, else `default` |
| A submit during a sweep | `backfill`. The engine ignores `.lane()`. |
| A submit from a `done` event | The lane of the source task, unless `.lane()` is set |
| A spawned child | The lane of its parent |
| A lane timeout with `on_timeout` | The `on_timeout` lane |

Helm renders each deployment as one Deployment and one KEDA ScaledObject, with one trigger for each lane in its config. The chart merges `shared` into every config, so all pods agree on cluster pools and leases. Every worker has a `default` and a `backfill` lane. `mise engine:check-helm` warns when no deployment pulls a lane, and fails when two deployments give one lane a different `timeout` or `max_age`. It also fails when no deployment sizes a registered pool. `timeout` limits one attempt, including the time after `ctx.detach()`. `max_age` limits all the attempts and deferrals of one task.

## Step 8: Change a system table

The system tables are in one store, `engine`, with one folder for each dialect.

```plaintext
stores/engine/
  clickhouse/
    000_baseline.sql
    022_expand_task_attempts.sql
  sqlite/
    000_baseline.sql
    022_expand_task_attempts.sql
```

| Step | What it may do | When it runs |
|---|---|---|
| `baseline` | Create every table of the store in its current shape | Only when the store is created or rebuilt |
| `expand` | Add a table, a column with a default, a TTL or a comment | At startup, before indexing serves |
| `contract` | Drop a column or a table | One release after the first binary that sees it |

A step may only change metadata, and `build.rs` rejects a step that rewrites data. A change that rewrites data is a new table, filled by a campaign. CI runs every step on each supported ClickHouse version. If a step fails at startup, the engine rebuilds the store and reindexes, at most once for each release. So every table must be rebuildable from the source, or safe to start empty. The active version pointer is neither. It lives outside the `engine` store, and the engine never rebuilds it. A failed step on it stops indexing and alerts the admin.

## Step 9: Wire it into the host

A module registers its pieces, its lanes and its pools.

```rust
pub const BIG: Lane = Lane::new("big");

pub fn register(engine: &mut EngineBuilder) {
    engine
        .worker::<CodeIndex>()
        .planner::<CodePlanner>()
        .pool("gitaly_archives")
        .pool("parse")
        .pool("parse_big");
}
```

The host adds each module to the engine.

```rust
Engine::builder()
    .config(config.engine.clone())
    .resources(resources)
    .system_stores(orbit_store::embed!("stores"), clickhouse)
    .module(code::register)
    .build()?
    .serve()
    .await
```

A pod does what its config lists. A config with `planner: true` runs the triggers, the reconcilers, the system migrations, the ledger sink and the admin API. A config with lanes or sinks pulls them. A local config can list everything, for development and tests.

`build()` fails on a duplicate name, a missing resource or a worker without a `default` or `backfill` lane.
It also fails on an unknown `on_timeout` lane, a cycle of `done` triggers or a config pool that no module registered.
`ctx.pool` with a name that no module registered is a permanent error.

## Step 10: Test it

The testkit runs the engine in memory, with a fake clock.

```rust
#[tokio::test]
async fn routes_a_big_repository_to_the_big_lane() {
    let kit = Testkit::new().provide(projects_with_repository_size(42, 8 * GIB)).module(code::register);
    assert_eq!(kit.plan::<CodePlanner>(push(42)).await.lane_of::<CodeIndex>(), BIG);
}
```

## Planned: sinks

This API does not exist yet. It shows how a worker writes to a store with one writer for each partition, such as SlateDB.

A sink is a store with fixed partitions. Workers stage records for a partition. One pod owns each partition, keeps its writer open, and writes the records in batches.

```rust
#[async_trait]
pub trait Sink: FromResources + Send + Sync + 'static {
    const NAME: &'static str;
    type Record: Payload;
    type Writer: Send;

    async fn open(&self, partition: Partition) -> Result<Self::Writer, SinkError>;
    async fn write(&self, writer: &mut Self::Writer, records: Vec<Self::Record>) -> Result<(), SinkError>;
}
```

The trigram index of Orbit Next is a sink with 16 partitions in SlateDB.

```rust
impl Sink for TrigramSink {
    const NAME: &'static str = "trigram";
    type Record = TrigramShard;
    type Writer = Db;

    async fn open(&self, partition: Partition) -> Result<Db, SinkError> {
        Ok(Db::open(partition.path(), Arc::clone(&self.store)).await?)
    }

    async fn write(&self, db: &mut Db, shards: Vec<TrigramShard>) -> Result<(), SinkError> {
        db.write(TrigramShard::to_write_batch(&shards)).await?;
        db.flush().await?;
        Ok(())
    }
}
```

A worker stages its records with one call. The build task is done when the call returns.

```rust
let partition = partition_of(&project, self.partitions);
ctx.stage::<TrigramSink>(partition, shards).await?;
Ok(Outcome::done(()))
```

The module registers the sink. Helm says which pods own its partitions.

```rust
engine.worker::<TrigramBuild>().planner::<TrigramPlanner>().sink::<TrigramSink>()
```

```yaml
shared:
  engine:
    sinks:
      trigram: { partitions: 16, batch: { max_bytes: 64MiB, max_wait: 30s } }
deployments:
  trigram-build:
    pods: { min: 1, max: 40 }
    config:
      engine:
        lanes: { trigram.build/default: { slots: 8, timeout: 15m } }
  trigram-writer:
    pods: { min: 1, max: 5 }
    config:
      engine:
        sinks: { trigram: { write_at_once: 4 } }
```

A sink is not a worker. Its pods run a pull loop for each partition they own, and call `write` for each batch. `write_at_once: 4` lets a pod write four of its partitions at once.

Records travel through object storage, and NATS carries only a pointer to them. `ctx.stage` returns once the records are durable, so the producer's pod can die safely. The writer pods share the partitions, and a lease makes sure only one pod writes each one. That pod opens its writer once, then calls `write` for each batch. It deletes staged records only after `write` succeeds, so `write` must be idempotent. A DuckLake sink works the same way, with one transaction for each batch.

## Reference

### Ledger

| Table | One row for each | Retention |
|---|---|---|
| `engine_task` | Task that did work or failed | 30 days |
| `engine_job` | Worker and key, with its last job | 30 days after that job |
| `engine_campaign` | Campaign of one planner: its tag, status and next page | Forever |
| `engine_schema_migrations` | Step of a system store | Forever |
| `engine_generation` | NATS install | Forever |

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

### NATS objects

| Object | Contents | After a wipe |
|---|---|---|
| `ORBIT_TASKS` | One subject for each worker and lane | The recovery campaign submits the work again. |
| `ORBIT_TASK_EVENTS` | Task events for the ledger sink and `done` triggers | Running jobs become `lost`. |
| `orbit_pending` | One dedupe marker for each pending worker and key | Recovery submits the work again. |
| `orbit_leases` | Key leases, pool permits, trigger and reconciler leases, the generation | Pods restart. The new generation fences old owners. |

### V1 config keys

Both config blocks are accepted until V1 is removed.

| V1 key | V2 equivalent |
|---|---|
| `topics` | `engine.lanes` in a pod config |
| `concurrency_groups` | The `slots` of each lane in a pod config |
| `fetch_concurrency` | The `slots` of the lane |
| `small_indexing_slots`, `big_indexing_slots` | The pod pools `parse` and `parse_big` |
| `write_max_concurrent` | The pod pool `insert` |
| `archive_storage_budget`, `archive_tenant_budget` | `per_key` and `unknown_key` of the cluster pool `gitaly_archives`, in `shared` |
| `archive_lease_max_wait_secs` | `max_wait` of the pool |
| `job_timeout_secs` | `timeout` on the lane |
