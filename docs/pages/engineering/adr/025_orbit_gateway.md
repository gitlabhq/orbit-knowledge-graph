---
title: "ADR 025: The Orbit Gateway"
description: A proposal for the Orbit Gateway, an Orbit-owned Envoy proxy in front of the Orbit web servers in every deployment.
creation-date: "2026-11-12"
authors: [ "@veberechi" ]
toc_hide: true
---

## Status

Proposed.

This ADR is stage 1 of moving the Orbit request path off GitLab Rails.
It adds the Orbit Gateway, which forwards every request unchanged.
Routing decisions, such as canary pools, come in later ADRs and issues.
Authentication and redaction stay in Rails, as [ADR 022](022_queries_in_rails.md) decided.

## Date

2026-11-12

## Context

GitLab Rails is the only caller of the Orbit web server.
Rails reads one endpoint setting and sends every call to that address.
On GitLab.com, the address is a private hostname that resolves to a Private Service Connect (PSC) endpoint.
On the Orbit side, the PSC service attachment points at an internal passthrough load balancer, which sends traffic straight to the web server pods.
TLS ends in the web server pod.

This path has no place where Orbit can decide which server gets a request:

- The load balancer works at layer 4.
  It passes encrypted connections through, so it cannot route on a request header or a token claim.
- It balances connections, not requests.
  gRPC carries every query from a Rails process on one long-lived HTTP/2 connection, so the pod that gets the connection serves all of its queries.
  To spread load, the web server closes connections when they reach a maximum age.
  Those forced closes killed in-flight query streams ([#971](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/971)) and hit a gRPC library panic ([#511](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/511)).
- Any change to who reaches which Orbit server needs a change in the GitLab monolith.
  That change waits for monolith review and the monolith deployment cadence, and ships to GitLab Self-Managed and GitLab Dedicated.

Several planned features need a routing point that Orbit owns:

- Canary pools that test changes on production data with chosen users.
- Cache and version affinity for code indexing ([#1277](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1277), [#1278](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1278)).
- Direct client access without a Rails request in the path ([#1498](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1498)).

## Decision

**We add the Orbit Gateway, an Orbit-owned Envoy proxy, in front of the Orbit web servers in every deployment.
In stage 1, the Gateway forwards every request unchanged to the web servers.**

### Request path

```plaintext
Today:     Rails -> PSC -> internal load balancer -> web server pods

Gateway:   Rails -> PSC -> internal load balancer -> Gateway pods -> web server pods
```

- The internal load balancer stays in front of the Gateway, because a PSC service attachment needs a load balancer forwarding rule.
  It spreads Rails connections across the Gateway replicas.
- Behind the Gateway there is no load balancer.
  The Gateway finds the web server pods through a headless Kubernetes Service and balances each gRPC stream across them.
  A regular Service balances connections, like the load balancer does today, so it must be headless.
- Request-level balancing replaces forced connection closes as the way to spread load.

### Cutover on GitLab.com

The PSC service attachment moves from the web server load balancer to the Gateway load balancer.
The hostname, the PSC endpoint, and the Rails setting do not change, so the cutover needs no monolith change.
Rolling back points the service attachment at the web servers again.

A new hostname would need a new PSC endpoint, a new DNS record, and a Rails configuration change, so we do not use one.

The address keeps the `tls://` scheme, because Rails sends the token only to a TLS or private address.
The Gateway ends TLS with a certificate that chains to a CA in the Rails CA bundle, and encrypts again to the web servers.

### Every deployment

The Gateway is part of the Orbit Helm chart, so GitLab.com, GitLab Self-Managed, and GitLab Dedicated run the same topology by default.

The Gateway arrives in two chart releases:

| Chart release | Gateway |
| ------------- | ------- |
| A minor release | Optional, off by default |
| The next major release | A proxy is always in front of the web servers: the Gateway by default, or a proxy the administrator brings |

GitLab.com turns the Gateway on in the minor release first.
The major release's upgrade notes ask administrators to turn it on in the minor release and upgrade once it runs cleanly.
Rolling back from the major release means returning to the minor release.
Which releases these are is decided when the work lands.

An administrator who already runs a proxy, or does not want Envoy, can turn off the chart's Gateway and put their own proxy in front of the web servers.
That proxy must meet the [requirements](#requirements) below, because the web server relies on it to balance requests.
Features that build on the Gateway, such as cache and version affinity, need the chart's Gateway.
With their own proxy, administrators do not get them, unless their proxy implements the same routing.

Shipping the Gateway is an Orbit team decision.
We tell the GitLab Dedicated team before the major release.

### The proxy

The Gateway is a plain Envoy deployment in the chart, scoped to the Orbit namespace.
It needs no custom resource definitions and no cluster-wide controller, so administrators without cluster-admin rights can install it.
The Envoy Gateway controller needs cluster-wide resources, which already blocked concurrent end-to-end test runs ([!1356](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1356)).

In stage 1 the routing configuration is static.
Runtime changes to routing, which [#1278](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1278) plans, come later.

### Requirements

The query stream is a long, two-way gRPC stream with redaction messages.
The Gateway must:

- Support two-way streaming, the client keepalive (every 60 seconds, also without calls), and the 120 second maximum query time.
- Accept and forward HTTP/2 header lists of at least 64 KiB.
  Tokens with many group traversal IDs exceed 16 KiB, and many proxies cap headers lower by default.
  Envoy's default request header limit is 60 KiB, so the chart sets the limit explicitly, and a test sends a token larger than 16 KiB through the Gateway.
- Run at least two replicas, with a PodDisruptionBudget or `maxUnavailable: 0`.
- Drain before a pod stops: send GOAWAY, and set the termination grace period above the 120 second maximum query time.
  Replicas alone do not protect in-flight streams.

### Prerequisites

Each is a separate code change, made before the GitLab.com cutover:

- The web server drains on shutdown ([#1505](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1505)).
  On SIGTERM it sends GOAWAY, waits for running queries for longer than the 120 second maximum query time, and then exits.
  The chart sets the pod termination grace period above that wait.
  Today the web server stops at once on SIGTERM, so a deploy ends the queries running on the pods it replaces.
  The Gateway moves new queries away from a stopping pod, but it cannot save running ones.
- The web server's maximum connection age can be turned off.
  Today the configuration requires it.

### One component, several uses

The Gateway is the single routing point for Orbit.
Each use is delivered by its own ADR or issue:

| Use | Routing key | Delivered by |
| --- | ----------- | ------------ |
| Forward every request | None | This ADR |
| Canary pools for testing on production data | Token claims | A later ADR |
| Cache and version affinity for code indexing | Cache shard and version | [#1277](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1277), [#1278](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1278) |
| Direct client access, rate limits, client IP | Path | [#1498](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1498) |

### What stays in Rails

- Authentication, and signing the token
- Redaction callbacks during the query stream
- The REST, MCP, and GraphQL endpoints that users and agents call

[#1498](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1498) plans moving these.

## Benefits of this approach

- Orbit owns the routing point, so later traffic changes are Orbit configuration with Orbit review, not monolith changes.
- Load spreads per request, so the web server no longer needs forced connection closes.
- One default topology for GitLab.com, GitLab Self-Managed, and GitLab Dedicated, with room for administrators who bring their own proxy.
- Canary pools, cache affinity, and direct client access share one component instead of three.
- TLS can end at the Gateway, so the web server no longer has to handle TLS itself.
  Request observability, such as access logs and per-request metrics, can move to the Gateway in the same way.

## Consequences

- Every query takes one more network hop.
  We measure latency before and after the cutover against the ADR 022 numbers: 45, 179, and 757 ms at p50, p95, and p99.
- The Gateway is a new component on the request path.
  When it is down, Orbit is down.
  It needs its own metrics, alerts, and capacity planning.
- At first, TLS ends at the Gateway and again at the web server.
  Ending TLS only at the Gateway leaves the hop to the web server unencrypted inside the cluster, so that step needs a security review.
  The [service boundary](../architecture/security/README.md#service-boundary) section must describe the TLS setup either way.
  The Gateway is inside the Orbit boundary, so the rule that no caller connects to Orbit directly still holds.
- Every deployment runs one more component, and administrators get a chart major release.
- The web server closes connections at a maximum age to rebalance them behind the load balancer.
  Behind the Gateway, the Gateway keeps connections to every web server pod and picks a pod for each request, so closing a connection rebalances nothing and can end a running query.
  Where the Gateway runs, the maximum age is turned off.
  Once the chart requires a proxy in front of the web servers, the setting is removed.
- The internal load balancer balances Rails connections across Gateway replicas per connection.
  The Gateway recycles its connections to Rails only so that replicas can join and leave: at a long interval, with GOAWAY and a drain longer than the maximum query time.

## Why not the alternatives

### Route in Rails with a feature flag

Rails could keep one connection per address and pick an address with a feature flag.
It keeps every routing change in the monolith, which is the cost this ADR removes.

### Route on a header that Rails sets

Rails could send a header that a proxy routes on.
Each new kind of rule still needs a monolith change.

### Add a second load balancer per pool

A second internal load balancer gives a pool its own address.
Something must still choose the address for each request, and only Rails can.
It also keeps connection-level balancing.

### GKE Gateway

GKE Gateway with an internal Application Load Balancer runs only on Google Cloud, so GitLab Self-Managed and GitLab Dedicated cannot use it.
It also routes on paths and headers, not on token claims, so canary routing would need an extra callout.

### Wait until a feature needs the Gateway

Each feature would add its own proxy, or wait for another feature to add one.
Forwarding first lets us measure the extra hop and run the Gateway in production before any routing depends on it.

## Related issues

| Issue | Relation |
| ----- | -------- |
| [#1277](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1277) | Envoy in front of the code indexing web servers, routing by cache shard and version |
| [#1278](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1278) | Runtime control of Envoy routing |
| [#1275](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1275) | Web servers with a warm in-memory cache, which pays off only when requests reach the same instance |
| [#1498](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1498) | Direct client access, with Envoy as the entry point for clients |
| [#954](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/954) | Server-side query cache for pagination, which needs later pages to reach the same instance |
| [#389](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/389) | Sticky sessions for per-pod query caching, closed without adoption. Affinity by cache shard (#1277) replaces it |
| [#182](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/182) | Rate limiting per namespace, which the Gateway can enforce at the edge |
| [#971](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/971) | Forced connection closes killed in-flight query streams |
| [#1505](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1505) | The web server ends running queries on shutdown instead of draining |
| [#511](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/511) | gRPC library panic when connections reached the maximum age |
| [#985](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/985) | Blue-green deployments, which switch traffic between versions |

## References

- [ADR 001: gRPC communication protocol for Rails-GKG integration](001_grpc_communication.md)
- [Security architecture: service boundary](../architecture/security/README.md#service-boundary)
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)
- [ADR 022: Run Orbit queries in Rails instead of Workhorse](022_queries_in_rails.md)
