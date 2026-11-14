---
title: "Orbit ADR 025: An Orbit-owned front door for request routing"
creation-date: "2026-11-12"
authors: [ "@veberechi" ]
toc_hide: true
---

## Status

Proposed.

This ADR is stage 1 of moving the Orbit request path off GitLab Rails.
It keeps authentication and redaction in Rails, as [ADR 022](022_queries_in_rails.md) decided.

## Date

2026-11-12

## Context

GitLab Rails is the only caller of the Orbit web server.
Rails decides which Orbit server gets a request.
On GitLab.com, the address is one setting, `grpcEndpoint: "tls://gkg.orbit-prd.gke.gitlab.net:50054"`, in the `gitlab-com/gl-infra/k8s-workloads/gitlab-com` values.
`Analytics::KnowledgeGraph::GrpcClient.configured_endpoint` reads it for every call, and `QueryRunner.params_for` reads it for every query stream.

The Orbit side of that address is a GKE internal TCP load balancer (`webserver.service.type: LoadBalancer` in `argocd/apps` `services/gkg/values.yaml`).
TLS ends in the web server pod.
The load balancer passes encrypted connections through, so it cannot route on a request header or a token claim.

So every change to who reaches which Orbit server needs a change in the GitLab monolith.
Testing a schema change on production data needs exactly that.
A canary deployment with its own graph database can index the production datalake.
But the only way to send chosen users to it today is a new endpoint setting and a feature flag in Rails.
That change waits for monolith review and the monolith deployment cadence, and the code ships to GitLab Self-Managed and GitLab Dedicated.
The same is true of any later traffic split, such as a gradual promotion or a regional pool.

Rails does more than forward requests.
It authenticates the user, signs a token with the groups the user can access, and answers redaction requests during the query stream.
[ADR 022](022_queries_in_rails.md) moved the query stream back into Rails for those reasons, after [ADR 008](008_workhorse_query_acceleration.md) put it in Workhorse.
This ADR does not move that work.

## Decision

**We put an Orbit-owned layer 7 gRPC proxy, the front door, in front of the Orbit web servers.
The front door owns the decision of which Orbit server gets a request.**

### Rails points at the front door

Rails keeps one address and does not change its code.
The front door takes over the existing hostname, `gkg.orbit-prd.gke.gitlab.net`, so the Rails setting does not change either.
Until the hostname moves, the front door can run on a new hostname, and a change to `grpcEndpoint` in `k8s-workloads` moves GitLab.com to it.
That is a configuration change, not a monolith change, and rolling it back is the same change in reverse.

The address keeps the `tls://` scheme.
Rails sends the token only to a TLS or private address (`GrpcClient#auth_metadata` and `QueryRunner.params_for`).
The front door certificate must chain to a CA in the Rails CA bundle, like the current one.

### The front door routes on token claims

Every request carries the token that Rails signs.
The token has the claims the front door needs: `user_id`, `root_namespace_id`, `is_gitlab_team_member`, and `group_traversal_ids`.
The front door reads the claims and picks a backend pool:

| Rule | Backend pool |
| ---- | ------------ |
| The claim matches a canary rule, for example `user_id` in a list or `is_gitlab_team_member` | Canary |
| Anything else | Production |

The rules are front door configuration in `argocd/apps`, which the Orbit team owns.
No Rails change is needed to add, change, or remove a rule.

The front door routes on claims but does not authorize on them.
The web server behind it still checks the token, so every pool enforces the same authorization and a routing mistake cannot widen access.

### Backend pools

Each pool is one Orbit web server deployment with its own graph database.
The production pool is the current deployment.
A canary pool runs an MR image, indexes the production datalake into its own graph database, and keeps its NATS state apart from production.
The canary needs one Orbit change first: the NATS key-value bucket `orbit_ontology_archives` has a fixed name.
Every dispatcher writes its active version to that bucket, and every web server follows it.
The bucket name must take a per-deployment prefix before a canary runs on the production NATS.

### What stays in Rails

- Authentication, and signing the token
- Redaction callbacks during the query stream
- The REST, MCP, and GraphQL endpoints that users and agents call

Moving these is stage 2 and stage 3, and each needs its own ADR.

## Benefits of this approach

- A canary, a gradual promotion, or a regional split is an Orbit configuration change with Orbit review.
- No monolith code changes, so nothing new ships to GitLab Self-Managed or GitLab Dedicated.
- Routing on claims needs no new header or flag in Rails.
- The front door is the entry point that stage 2 needs when clients call Orbit directly.

## Consequences

- Every production query takes one more network hop.
  We measure the latency before and after the cutover against the ADR 022 numbers: 45, 179, and 757 ms at p50, p95, and p99.
- The front door is a new component on the production request path, with its own on-call and capacity.
  When it is down, Orbit is down for GitLab.com.
- TLS ends at the front door.
  It encrypts again to the web servers, so the certificate setup changes.
  The [service boundary](../architecture/security/README.md#service-boundary) section must then describe two TLS hops instead of one.
  The front door is inside the Orbit boundary, so the rule that no caller connects to Orbit directly still holds.
- The query stream is a long, two-way gRPC stream with redaction messages.
  The front door must support two-way streaming, the client keepalive (every 60 seconds, also without calls), and the 120 second maximum query time.
- The canary rules live outside the GitLab feature flag system, so ChatOps does not show or change them.
- GitLab Self-Managed and GitLab Dedicated do not need a front door.
  They keep calling the web server directly, and this ADR does not change them.

## Why not the alternatives

### Route in Rails with a feature flag

Rails can already keep one connection per address (`GrpcClient.stub(endpoint)`).
A flag and a second endpoint setting would route chosen users to a canary.
It keeps every routing change in the monolith, which is the cost this ADR removes.

### Route on a header that Rails sets

Rails already sends `x-gitlab-enabled-feature-flags`.
A per-user flag in that header could drive the front door.
It still needs a monolith change for each new kind of rule.
The token already has the claims.

### Keep the TCP load balancer and add a second one

A second internal load balancer gives the canary its own address.
Something must still choose the address for each request, and only Rails can.

## Open questions

- Which proxy: a GKE Gateway with an internal Application Load Balancer, or an Envoy deployment that we run.
- How Rails reaches the front door.
  GitLab.com reaches Orbit over Private Service Connect today.
  The front door needs its own service attachment, or the current one moves to it.
- Whether the front door checks the token signature before it routes.
  The token uses HS256 with a shared key, so a check needs the key at the front door.
  Routing on unchecked claims is safe while the web server checks the token, but it lets a caller pick a pool.

## References

- [ADR 001: gRPC communication protocol for Rails-GKG integration](001_grpc_communication.md)
- [Security architecture: service boundary](../architecture/security/README.md#service-boundary)
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)
- [ADR 017: Clone-based non-blocking schema migrations](017_clone_based_non_blocking_migrations.md)
- [ADR 022: Run Orbit queries in Rails instead of Workhorse](022_queries_in_rails.md)
- [Rebuild and reindex runbook](../runbooks/manual_rebuild.md)
