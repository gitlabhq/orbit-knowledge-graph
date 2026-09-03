---
title: "GKG ADR 018: Gitaly proxy transport"
creation-date: "2026-09-03"
authors: [ "@dgruzd" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-09-03

## Context

Orbit Remote currently downloads repository archives through a GitLab Rails
internal endpoint. Rails authorizes each request and delegates the archive to
Gitaly through Workhorse send-data. This adds a new Rails endpoint whenever
Orbit needs another read-only Gitaly RPC and keeps Rails in the data path.

## Decision

The code indexer can use Workhorse's Gitaly proxy to call the generated Gitaly
`RepositoryService.GetArchive` client over gRPC tunneled through WebSocket. The
proxy keeps Rails as the authorization authority while Workhorse enforces a
read-only, single-repository profile and prevents Orbit from receiving Gitaly
addresses or credentials.

The transport is selected by `gitlab.gitaly_transport`:

- `rails_http` keeps the existing path and remains the default.
- `workhorse_ws_with_fallback` falls back to Rails HTTP only when the proxy is
  unavailable before an archive stream starts.
- `workhorse_ws` surfaces proxy availability failures.

An archive stream that fails after delivering data restarts from zero over the
proxy with bounded jittered backoff. It never switches to Rails HTTP midway.
The complete archive is buffered before downstream extraction so bytes from a
failed attempt cannot be combined with bytes from its replacement.

## Consequences

The default is inert. Rollout can select the fallback mode before making the
proxy mandatory. Proxy policy drift remains visible while the fallback keeps
indexing available.

Restarting a large archive repeats Gitaly work. Attempts are therefore bounded,
observable, and handed back to NATS redelivery after exhaustion. Buffering a
proxy archive uses temporary disk space comparable to the existing extraction
path, but guarantees attempt atomicity.

## Non-goals

- Caching proxy channels for query-time repository reads.
- Proxy admission control and per-project cool-down.
- Replacing Rails HTTP for project metadata.
- Changing the Workhorse or Rails proxy protocol.

## References

- [Related Orbit issue](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1252)
- [Transport foundation](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2381)
- [Workhorse proxy core](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/253414)
- [Workhorse proxy route](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/253431)
- [Rails authorization endpoint](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/253417)
