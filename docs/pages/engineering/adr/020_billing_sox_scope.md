---
title: "Orbit ADR 020: SOX Scoping for Billing Event Emission"
creation-date: "2026-05-15"
last-updated: "2026-10-15"
authors: [ "@snachnolkar" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-05-15

## Context

Orbit emits a billable usage event for each eligible graph query that succeeds.
The event conforms to the shared GitLab billable usage schema and goes out through Snowplow.
Emission happens only when tracking is on and the request carries valid attribution.
Schema responses do not emit billing events.
A missing or unknown realm value prevents the event.
On GitLab.com, missing namespace attribution also prevents the event.
The Data Insights Platform delivers events to CustomersDot asynchronously.
A successful query does not guarantee delivery.

Before Orbit runs a query, it asks CustomersDot whether the caller has quota left.
The quota gate supports two authentication modes, an admin token and a license checksum.
The gate skips the check in two cases:

- A license-checksum request that has no license checksum
- A GitLab.com request that has no namespace attribution

Any other quota-check failure denies the request.

We can build Orbit with billing enforcement turned on.
An enforced build validates its configuration at startup.
It refuses to start unless billing and quota checks are on, the endpoints match an approved environment pair, and the authentication modes are compatible.
A build without enforcement can turn off billing and quota checks through configuration.

Orbit emits its own billable usage events, so the work falls under Sarbanes-Oxley (SOX) IT general controls.
The compliance team told us that two scopes are possible:

- The whole repository, as AI Gateway does, so that every change is a SOX-controlled change
- A limited scope at the folder or component level

The billing code started as a module inside the server.
We then moved it into an isolated billing component.
Event construction, delivery integration, and quota logic now live in that component.
One claims-conversion adapter in the server turns the authenticated request claims into billing and quota inputs.
Configuration, delivery credentials, and pipeline observer callbacks also cross into the billing component.
They do not pass through the adapter.

A limited scope at the component level is therefore possible, but only if we also control the hook points around it.
The adapter centralizes claim mapping.
It does not isolate every dependency that can change billing correctness.

## Decision

**We scope SOX to the billing component, the claims-conversion adapter, and a list of extended hook points.**

The SOX-scoped section of the code owners file is the list of in-scope files.
Each file in that section has a required-reviewer rule.

A component outside the billing component is in scope when a change to it can change billing correctness without a change to billing code.

### Primary scope

The billing component is in scope as a whole.
It covers event construction and tracking, quota checks and their cache, delivery authentication, and startup enforcement.
It also covers its constants, metrics, and tests.

The claims-conversion adapter is in scope.
It is the one declared conversion point from authenticated request claims to billing inputs.

### Extended hook points

These parts live outside the billing component.
Each can change billing correctness without a change to billing code, so each is in scope.
A change that adds or moves a hook point must update the file list in the SOX-scoped section of the code owners file.
A change that adds a new kind of hook point must also add a row to this table.

| Hook point | Why it is in scope |
|---|---|
| Pipeline observer interface | It defines how the pipeline reports success and errors, and the order in which observers run. A change decides whether billing events fire for a query outcome. |
| Request claims | They carry billing identity and attribution, including realm, root namespace, and instance identity. A change can alter attribution, stop emission, or reject a request. |
| Token validation gate | It decides whether a request gets claims at all, and so whether a billable call can reach the pipeline. |
| Authorization gate | It decides whether a query can complete. A rejection before completion prevents emission. |
| Billing observer construction | It builds the billing observer from the adapter inputs and attaches it to the pipeline. |
| Pipeline billing wiring | It connects the billing tracker to the query pipeline. A removal or a new order silently stops emission. |
| Tracker and quota startup | It validates configuration, builds the tracker and the quota service, wires them into the service, and drains queued events at shutdown. A change can turn off emission or quota enforcement, prevent startup, or lose queued events. |
| Pipeline security gate | It decides whether the pipeline gets far enough to report success. |
| Billing configuration and defaults | They set emission and quota enablement, endpoint selection, and authentication modes. They also hold other settings that affect delivery or quota enforcement. |
| Delivery token cache | It caches and refreshes the Cloud Connector token for event delivery. A change can prevent delivery without a change to the billing component. |

### Out of scope

Files outside the SOX-scoped section of the code owners file are outside this declared scope.
The section is a maintained review boundary.
It does not prove that other code cannot affect billing.
The controls below reduce the risk that an unknown dependency expands the surface, but they do not remove it.

### Controls

The SOX-scoped code owners section belongs to the Orbit team.
A change to any file in it needs code-owner approval on the protected default branch.
The repository has no default ownership rule for other files.
The code owners file and the dependency boundary test are in the same section, so a change to these controls needs the same approval.
Ownership is per file, so an unrelated change inside a scoped file also needs approval.

The adapter has a header comment that names it the one claims-conversion seam between the server and the billing component.
The comment links to this ADR and names the extended hook points.
It does not claim that configuration, credentials, or observer callbacks pass through claim conversion.

The agent instruction files link to this ADR.
Agents and contributors who change billing emission or quota-check code see the policy first.

A continuous integration (CI) job checks the package dependency declarations in the workspace.
It fails if any package other than the server declares a direct dependency on the billing component.
A passing run shows that this direct dependency surface did not grow.
It does not check new imports inside the server, re-exports, or changes to existing dependencies.

GitLab Duo review instructions flag new billing importers, re-exports, claim-mapping bypasses, and event emission outside the billing component.
These checks are advisory and do not gate a merge.
Human reviewers stay responsible for new hook points and for updates to the code owners file.

A new member of the SOX approver group needs recorded manager approval.
A formal maintainer nomination is optional, as the compliance discussion clarified.
The operational process and the retention of approval evidence are still follow-up work.
The code owners rules and CI checks do not show that this access process is in place.

## Benefits of this approach

Compliance can audit one declared SOX surface without a review of every file in the repository.
Work outside the billing surface has no SOX merge requirements.
The ADR, the adapter header, and the code owners rules all point at the same set of files.
Contributors and agents can find the boundary from any of them.

## Consequences

The hook-point list needs upkeep.
A future change can add a new path that silently affects billing correctness.
The author must add that path to the code owners section, and update this ADR only for a new kind of hook point.
Reviewers of in-scope code must notice when a refactor expands the hook-point surface.

A cross-component refactor of the pipeline observer interface needs SOX-reviewer approval, even when its intent has no relation to billing.
The dependency boundary test adds a small CI step.

## Why not the alternatives

### Whole-repository SOX scope

This scope matches AI Gateway and leaves no doubt about the surface.
We rejected it because every change in the repository would carry SOX requirements, even when it cannot affect billing correctness.
That slows non-billing work and gives no compliance benefit.

## References

- [Determine SOX compliance scope and controls for Orbit billing event emission](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/507)
- [Emit Snowplow billing events on successful queries](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937)
- [SOX scoping discussion on the billing events MR](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937#note_3268652539)
- [Add a CustomersDot usage quota gate for MCP and REST queries](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1304)
- [Add a SOX boundary check for billing dependents](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1372)
- [Add SOX billing-boundary review instructions for GitLab Duo](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1468)
- [Compliance discussion on SOX approver access](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1358#note_3413138557)
- [Add instance and deployment claims to the Orbit JSON Web Token](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/232123)
- [AI Gateway billing events code](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/tree/main/lib/billing_events)
- [AI Gateway billing events documentation](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/blob/main/docs/billing_events.md)
- [Billable usage schema version 1-0-2](https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/billable_usage/jsonschema/1-0-2)
- [Add metrics, tracing, and alerting for the billing event pipeline](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/488)
