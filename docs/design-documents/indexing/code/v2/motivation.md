
## Motivation

### Historical context

GitLab Orbit was first launched to users in May 2026 after 3 months of development. Customers have requested support for branches and commits, which we cannot do at scale with ClickHouse.

> As of November 2025, the [GitLab monolith](https://gitlab.com/gitlab-org/gitlab) has over 4000 branches considered "active" (committed to within the last 3 months) and even more that are considered "stale" (last committed more than 3 months ago).
>
> Locally, with the limited support for Ruby. We currently index about 300,000 definitions and over 1,000,000 relationships.
>
>For simplicity's sake, let's say we want to keep an active code index for branches that are considered "active". This would require us to index (300,000 definitions x 4000 branches) = 1.2 billion definitions and (1,000,000 relationships x 4000 branches) = 4 billion relationships just for the GitLab monolith. This is simply not feasible if we extrapolate this to all the repositories in `.com`.

In the initial design, we also identified Object Storage as the right candidate to scale the system beyond what we can do with ClickHouse.

>After the initial deployment, metrics and customer feedback will determine whether branch-level indexing is worth the storage and compute cost. The approach below outlines one viable path.
>
>As stated above GitLab has the concept of a branch being "active" or "stale". An active branch is one that has been committed to within the last 3 months. A stale branch is one that has not been committed to in the last 3 months.
>
>For the amount of data and uneven query distribution (some branches are never going to be queried), it's best we don't keep the data against the main branches in the same database since that would result in a lot of wasted storage and compute resources.
>
>Ideally, we would re-use the same indexing strategy as the main branch where we can index the active branches by listening to code indexing tasks from NATS, but instead of loading the data into ClickHouse, we would store the data in cold storage (like S3 or GCS).
>
>On request, we would load the data into ClickHouse from cold storage in materialized tables. This would allow us to then query the data in ClickHouse during the current session and then unload the data from ClickHouse after the session is complete (based on a variable TTL).

### Functional Limitations of Today

Additionally, today's models are largely trained on `grep` and content-matching tools. Orbit v1 has no way to search the physical content of files. In our [evaluations](https://gitlab.com/gitlab-org/orbit/orbit-evals-harness) and work with [Orbit Ask](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2329), we've found that a combination of standard content matching and code graph capabilities is required to fall into the standard training distribution and achieve maximum effectiveness.

## Data store considerations

### Why not ClickHouse

Why not ClickHouse can be broken down into the following points:

1. The code graph write pattern is delete-heavy, and ClickHouse merges cannot keep up at current scale,
2. Branches and commits multiply that scale by orders of magnitude,
3. ClickHouse full-text search does not hold at group-wide scope or with scoring, and
4. It is serverful, so RPS does not scale linearly.

#### Code indexing is delete-heavy; ClickHouse is not

ClickHouse is built for append-heavy analytics. Code indexing is the opposite. In the current design, every push replaces a project's whole graph, so we delete the old graph and insert the new one. We are at about 50 billion rows across the graph (12 billion in the code edge table alone) in production. Deletes are where it hurts:

- ReplacingMergeTree stops merging large parts.
- ReplacingMergeTree does not delete rows for us.

Even if we move to an append-only model with commits & branches, we still need to clean up old revisions. While sharding buys time, it is not a fix.

#### Branches and commits multiply the problem

In the monolith, 300,000 definitions times 4,000 active branches is 1.2 billion definitions and 4 billion relationships. Additionally, the monolith has 46,685 branches, and 3,586 of them (7.7%) had a push in the last 30 days. If we extrapolate this to `.com`, node and edge counts head into the hundreds of billions and some projects need 1000x the storage they use today.

#### Testing blob storage in ClickHouse

We ran a [benchmark](https://gitlab.com/gitlab-org/orbit/experiments/orbit-next/-/blob/main/docs/clickhouse/code-search-benchmark.md) loading blobs into ClickHouse. High-level details:

- We used ClickHouse Cloud 26.4.1, 2 replicas at 30 vCPU / 120 GiB each.
- We stored blobs once with `text` and `ngrambf_v1` indexes, plus a `refs` table mapping (project, ref, path) to a blob.
- The data consisted of 3,351 projects, 33.4 million refs, 1.92 million blobs, 5.67 GiB after 17.4x dedup.

While single-query latency looked ok, we can observe that throughput becomes an issue:

| Profile | Peak RPS | p50 | p99 |
|---|---|---|---|
| BEST (light, narrow) | ~81 | 171 ms | 420 ms |
| REALISTIC | ~27 | 307-699 ms | 5-7 s |
| WORST (all heavy) | ~3 | 1.3-2.7 s | 4-6 s |

It saturated at about 16 concurrent queries, with Group-level trigram queries topping out around 2 RPS.

#### ClickHouse limitations for what we need

Additional limitations:

- ClickHouse can't run `ast-grep` queries; regex only accelerates when there's a literal substring to pull out.
- We need to be able to co-locate graph data with trigrams and content in the same database.
- We need to be mindful of operational overhead and COGS as we scale to content, branches, and commits.

### Why not Zoekt

Why not Zoekt can be broken down into the following points:

- It indexes only the default branch, and branches and commits are our top customer ask,
- It is memory-resident and stateful,
- We need graph data, trigrams, and content in one store with authz baked into the layout,
- Most of the logic around archive fetching, locking, task queues, and backfill is already built for Orbit, which is 90% of the Zoekt code.

#### Memory-resident and stateful

Zoekt keeps its index and cache in RAM on PVCs with mmap, and a central coordinator pins repos to nodes. On `.com` it sits on 60 to 80 TiB and the trigram index runs about 3x the corpus. We want an SSD cache in front of object storage and stateless workers to reduce COGS.

#### Zoekt limitations for what we need

Additional limitations:

- We need fine-grained access control, and the database must respect authz rules out of the box. Zoekt is not built that way.
- Zoekt is written in Go, and we are using Rust.
- We need to co-locate graph data, trigrams, and content in the same database. Zoekt has trigrams and content only, no graph and no `ast-grep`.
- We don't want to require customers to deploy and operate Zoekt to use Orbit. Orbit must own the vertical stack and not rely on external services.

Zoekt does not go away. Orbit code indexing will ship as another engine and an alternative path for large Zoekt users.

## We're betting on Cloud-native Object Storage (e.g. S3, GCS, Minio)

As you may have guessed by now, we're betting on Object Storage. Recent advancements in Object Storage and querying engines make querying from cold storage very efficient. Furthermore, with Object Storage, we can scale horizontally and replace the graphs without creating a noisy-neighbor problem on the database.
