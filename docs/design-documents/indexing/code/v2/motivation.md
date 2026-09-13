
## Motivation

### Historical context

GitLab Orbit first launched in May 2026 to users after 3 months of developement. Customers have requested support for branches and commits, which we cannot do at scale with ClickHouse.

> As of November 2025, the [GitLab monolith](https://gitlab.com/gitlab-org/gitlab) has over 4000 branches considered "active" (committed to within the last 3 months) and even more that are considered "stale" (last committed more than 3 months ago).
>
> Locally, with the limited support for Ruby. We currently index about 300,000 definitions and over 1,000,000 relationships.
>
>For simplicity's sake, let's say we want to keep an active code index for branches that are considered "active". This would require us to index (300,000 definitions *4000 branches) = 1.2 billion definitions and (1,000,000 relationships* 4000 branches) = 4 billion relationships just for the GitLab monolith. This is simply not feasible if we extrapolate this to all the repositories in `.com`. 

In the initial design, we already had identified S3 as a potential candidate to scale the system beyond what we can do with ClickHouse. 
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

Additionally, models of today are largely trained on `grep` and content matching tools. Orbit v1 does not have a way to search physical content of files. In our [evaluations](https://gitlab.com/gitlab-org/orbit/orbit-evals-harness) and work with [orbit ask](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2329), we've found that a combination of both standard content matching and code graph capabilities are required to fall into the standard training distribution and achieve maximum effectiveness.


## Why not Zoekt? Why not ClickHouse or Elasticsearch?

### ClickHouse limitations

ClickHouse is made for append only, quick updates workflow. It is not designed for heavy churn on historical data. In the past 3 months of operating in production, we have reached a scale of 50B rows and already are experiencing the pain of updating and deleting data in ClickHouse. 

Here are some examples of issues we are seeing:
- ReplacingMergeTree not rewriting big parts, meaning some duplicate row stay forever.
- ReplacingMergeTree does not autormatically delete rows, we need to run complex deletion strategies at scale which is limiting.

### Zoekt Limitations

Few things that come to mind:
- We need fine grained access control to the data, and for the database to respect authz rules of the box. 
- Zoekt is written in Go, and we are using Rust. 
- We need to be able to co-locate graph data with trigrams and content in the same database.
- Zoekt is costly to run and maintain and is not a good fit for our use case.
- We want to own the vertical stack and not rely on external services.

## We're betting on Cloud-native Object Storage (e.g. S3, GCS, Minio)

As you may have guessed by now, we're betting on S3. Recent advancements in S3 storage and querying engines make querying from cold storage very efficient. Furthermore, since its S3, we can easilly scale horizontally and replace the graphs without creating a noisy neighbours problem on the database.
