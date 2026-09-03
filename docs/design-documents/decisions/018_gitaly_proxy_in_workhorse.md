---
title: "GKG ADR 018: Gitaly proxy in Workhorse"
creation-date: "2026-09-03"
authors: [ "@dgruzd" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-09-03

## Context

Orbit reads repository archives through a GitLab Rails internal endpoint. Rails
authorizes the request, then remains in the data path while Workhorse streams
the archive from Gitaly. Each additional Gitaly RPC needs another Rails and
Workhorse endpoint with its own request and response translation.

Other services avoid that translation by receiving Gitaly addresses and tokens
from Rails. Zoekt task payloads and the KAS `agent_info` response use this
pattern. It exposes storage topology and credentials to each consumer, expands
the effect of a compromised consumer, and couples the consumer to Gitaly
routing. We do not want to repeat that approach for Orbit.

## Decision

Add a consumer-generic Gitaly proxy in Workhorse. A consumer opens one internal
WebSocket route for a project and speaks ordinary gRPC through the tunnel. It
uses generated Gitaly clients and never receives a Gitaly address or token.

Rails remains the only authorization authority. Workhorse preauthorizes the
WebSocket upgrade with Rails once per connection. Rails authenticates the
consumer, authorizes the project, and returns the resolved Gitaly connection
details and a named policy profile to Workhorse. The authorization stays in
Workhorse memory and is bound to that connection.

Workhorse does not decide consumer policy. It applies the compiled
`readonly_repository` profile to every stream, then applies the narrower method
allowlist supplied by Rails. The profile permits only accessor RPCs scoped to
one repository, requires exactly one target repository matching the connection,
rejects additional repositories and client-streaming methods, and fails closed
for unknown methods or unresolved repository scope. A stream without its
connection's session cannot run, so it cannot inherit another session's policy.

The route and Workhorse implementation do not name Orbit. A consumer is added
through one credential-header to authorizer registration in Rails, where its
authentication, project policy, method allowlist, and Gitaly client identity
live. Orbit is the first registered consumer.

## Session model

Connection age moves a session from active to draining and gates new streams
only. Streams already admitted may finish under a separate per-stream deadline,
while the client proactively opens a freshly preauthorized connection before
the old one expires. We do not use grpc-go connection age for this because its
GOAWAY grace ends by closing the connection even when an archive is still in
flight. Since Gitaly archives cannot resume, that behavior can repeatedly cut a
large archive and restart it from zero.

The accepted cost is delayed revocation for an admitted read. With the proposed
defaults, a caller revoked just after admission may finish an already-started
stream for roughly 70 minutes in the worst case: up to 10 minutes of connection
age plus a 60-minute stream deadline. Whether that is acceptable for the first
version is an open question below.

## Orbit as the first consumer

The code indexer calls Gitaly `GetArchive` through the proxy. Transport selection
is opt-in: the existing Rails HTTP path remains the default, a proxy-only mode
surfaces proxy failures, and a rollout mode can fall back to Rails when the
proxy is unavailable before streaming starts.

Once proxy archive bytes have started, a transport failure never switches to
Rails. The indexer retries the proxy within a bounded budget and restarts the
archive from zero because the RPC has no resume operation. It spools each full
attempt to a temporary file before extraction so bytes from a failed attempt
cannot be consumed or combined with its replacement.

Rollout has two controls. The instance-level `workhorse_gitaly_proxy` operations
flag disables the mechanism. The `orbit_gitaly_proxy` flag enables the Orbit
consumer per root namespace. Turning either off prevents new preauthorizations;
it does not terminate an already-admitted stream.

## Alternatives considered

### Give Orbit Gitaly connection details

Rails could return the Gitaly address and token to Orbit as it does for some
other services. This is the shortest data path, but it places routable storage
details and credentials in another cluster and makes Orbit responsible for
storage routing and credential handling.

### Add one Rails endpoint per RPC

This keeps the current archive pattern and preserves Rails authorization, but
each new RPC adds another endpoint and translation layer. Rails also remains in
the data path for every repository read.

### Use a shared-server gRPC proxy with grpc-go connection aging

A shared server with standard age and GOAWAY behavior is simpler than the
session lifecycle proposed here. Its grace period can terminate in-flight
archives, causing restart loops, and a shared server cannot drain one transport
through grpc-go's public API.

### Add consumer-specific routes

Separate routes make the consumer explicit in the path. They also require a
Workhorse route and deployment routing work for every consumer, while the
credential already selects and authenticates the Rails authorizer.

## Consequences

Adding an RPC normally becomes a Rails allowlist change, plus a Gitaly module
bump in Workhorse if its pinned descriptors do not yet contain the RPC. Adding
a consumer becomes one Rails authorizer registration rather than a new proxy
route or Workhorse policy implementation. Consumers continue using generated
Gitaly clients without a bespoke HTTP representation for each operation.

Workhorse takes on a ported transparent proxy package, WebSocket adaptation,
per-connection session state, per-stream policy enforcement, and connection
draining. Rails receives a preauthorization request for each new connection.
Workhorse's Gitaly module pin becomes an explicit coupling point: methods newer
than the pin fail closed until the module is updated.

## Open questions for the team

Is the roughly 70-minute worst-case window for an already-started read
acceptable, or must the first version include a Redis-backed hook that lets
Rails force Workhorse to drain a session immediately?

Should Workhorse preserve wire ordering between final HTTP/2 trailers and the
WebSocket close by tracking HTTP/2 frame boundaries, or should it use GOAWAY
and change the client contract so the final close frame is best effort?

Can Orbit's production cluster reach the new internal route through GitLab.com
Ingress, and does that Ingress pass WebSocket upgrades on the route? This must
be verified before staging because Orbit currently reaches its existing
internal prefix, not this new shared prefix.

Is a registry keyed by one credential header per consumer the right Rails seam,
or is one Rails endpoint per consumer preferable despite requiring matching
Workhorse and deployment routes?

## References

- [Workhorse proxy core](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/253414)
- [Workhorse route, sessions, and director](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/253431)
- [Rails preauthorization and authorizer](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/253417)
- [Orbit client transport](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2381)
- [Orbit indexer archive transport](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2382)
- [Gitaly direct access for Orbit](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1252)
