
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

While Zoekt is a solid system for trigram matching, it is not a good base for Orbit. The reasons:

1. GitLab Zoekt as of today is a Rails and Gitaly appliance, not a modular engine. Orbit runs in a separate GCP project and cannot reach Gitaly.
2. It indexes the default branch only, caps at 64 branches, and has no commit dimension. Branches and commits are our top customer ask.
3. It is memory-resident and stateful. Orbit wants stateless workers over object storage.
4. Authorization is a per-request list of project ids from Rails, not a property of the storage layout.
5. The shard format has no place for graph data, so content and graph would live in two engines.
6. Orbit's graph indexer is Rust. The text matcher must sit in the same process and read the same layout.

#### A Rails and Gitaly appliance

Everything that makes Zoekt a service lives in Rails: node registry, replicas, indices, tasks, and watermarks. The indexer polls the Rails internal API for tasks. Each task carries a Gitaly address and token, and the indexer dials Gitaly gRPC directly. Zoekt nodes must sit on the Gitaly network with Gitaly credentials.

Orbit runs in its own GCP project and reaches GitLab only through the Rails internal API and the Siphon and NATS pipeline. To run Zoekt there, we would open Gitaly gRPC across projects or move Orbit into the production cluster. Both break the [minimal Gitaly load](functional_requirements.md#minimal-load-on-gitaly) requirement. The reusable part of Zoekt is the shard builder and the matcher. We would replace everything else.

#### Default branch only, no commits

The GitLab indexer hard-codes one branch named `HEAD`. Upstream allows at most 64 branches per repository, and a delta build refuses a changed branch set. The monolith has 4,000 active branches. Commits have no representation, so a search at a commit is not expressible.

#### Memory-resident and stateful

Zoekt maps shards into RAM from persistent disks, and Rails pins each top-level namespace to StatefulSet nodes. The index is about 3.5x the corpus and needs RAM above 1.2x the corpus. Memory is the recurring failure mode on `.com`. Orbit wants memory to be a cache, not the index.

#### Authorization is a filter, not a layout

Rails expands the caller's permissions into a list of project ids and sends it with each query. Orbit requires authorization [before candidate selection](functional_requirements.md#authorization-and-namespace-isolation), encoded in the layout as traversal paths, with no Rails call per query.

#### No place for the graph

A Zoekt shard holds file content, filenames, posting lists, branch masks, ctags symbols, and metadata. Offsets are 32-bit, so a shard stays under 4 GB and about 1 GB of content. There is no extension point for definitions, references, edges, or partition metadata. Symbols come from a universal-ctags subprocess and serve ranking and the `sym:` filter. Orbit builds its graph with tree-sitter in Rust and needs `ast-grep` matching.

The functional requirements need [content and graph in one query](functional_requirements.md#graph-filtering-by-file-content), at the same revision, under the same access scope. With Zoekt, the graph lives in a second engine. Every combined query becomes two engines, two revision anchors, two coverage statements, and a join in the application. The per-repository shard SHA and the graph revision drift apart on every push.

#### Vertical integration in Rust

Orbit's graph indexer, tree-sitter parsing, `ast-grep`, authorization, and ingest are Rust crates. The text matcher must run in the same process, share the same object storage layout, and commit at the same revision. Then one query can filter by content and walk the graph in one pass with one coverage statement. Calling out to a separate, stateful Go system gives up all of that. It adds a network hop or a CGo boundary on every query, a second scheduler, and a second consistency point. The memory work we need is also easier in Rust. Sourcegraph's 5x RAM reduction came from working around Go map and garbage collector overhead. Rust gives explicit layout and zero-copy reads over object storage bytes by default.

#### References

- [Zoekt design document (GitLab handbook)](https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/code_search_with_zoekt/)
- [gitlab-zoekt-indexer: task polling against the Rails internal API](https://gitlab.com/gitlab-org/gitlab-zoekt-indexer/-/blob/c6de94b111d4b1d078ec922085ad0effe7b47035/internal/task_request/task_request.go#L26)
- [gitlab-zoekt-indexer: direct Gitaly gRPC dial](https://gitlab.com/gitlab-org/gitlab-zoekt-indexer/-/blob/c6de94b111d4b1d078ec922085ad0effe7b47035/internal/gitaly/gitaly.go#L89)
- [gitlab-zoekt-indexer: single `HEAD` branch](https://gitlab.com/gitlab-org/gitlab-zoekt-indexer/-/blob/c6de94b111d4b1d078ec922085ad0effe7b47035/internal/indexer/indexer.go#L142)
- [gitlab-zoekt-indexer: `repo_ids` authorization filter](https://gitlab.com/gitlab-org/gitlab-zoekt-indexer/-/blob/c6de94b111d4b1d078ec922085ad0effe7b47035/internal/search/query.go#L336)
- [Zoekt upstream: delta builds refuse a changed branch set](https://github.com/sourcegraph/zoekt/blob/0e022b711109/index/builder.go#L721)
- [Zoekt upstream: design notes (index size, shard limits)](https://github.com/sourcegraph/zoekt/blob/main/doc/design.md)
- [Sourcegraph: 5x reduction in Zoekt RAM usage](https://sourcegraph.com/blog/zoekt-memory-optimizations-for-sourcegraph-cloud)
- [Orbit v1 code indexing design](../v1/code_indexing.md)

Zoekt does not go away. Orbit code indexing will ship as another engine and an alternative path for large Zoekt users.

## We're betting on Cloud-native Object Storage (e.g. S3, GCS, Minio)

As you may have guessed by now, we're betting on Object Storage. Recent advancements in Object Storage and querying engines make querying from cold storage very efficient. Furthermore, with Object Storage, we can scale horizontally and replace the graphs without creating a noisy-neighbor problem on the database.
