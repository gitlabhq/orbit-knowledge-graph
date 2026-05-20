# Autonomous Operations Agents for Orbit (Knowledge Graph)

## Status

- **Author**: @dgruzd
- **Status**: Draft — seeking feedback from Infrastructure DRIs
- **Created**: 2026-05-20

## Summary

This document proposes deploying autonomous AI agents (a supervisor + worker pool)
to continuously monitor, diagnose, and remediate operational issues in the Orbit
(Knowledge Graph) production infrastructure. The goal is to reduce Tier-1 on-call
burden by automating the detection, triage, and resolution of common production
incidents — freeing SREs and the KG team to focus on higher-leverage work.

**Terminology:** Throughout this document, **"`gitlab-org` cluster"** refers to the
production GKE cluster that hosts Orbit services operating on `gitlab-org` namespace
data only. This cluster has no network path to clusters containing customer data.

## Motivation

### Current Operational Reality

Orbit is a distributed system with multiple failure domains:

| Component | Failure modes |
|-----------|---------------|
| **Siphon / Postgres replication** | Connectivity failures (PSC mismatches), snapshot-based onboarding requires hands-on monitoring |
| **NATS JetStream** | Stuck consumers, DLQ accumulation, stream retention headroom, lock contention |
| **ClickHouse** | DELETE/query slowness under production load (5B+ row tables), PK lookup pathology, OOMs during ETL joins |
| **GKG Indexer** | Silent partial indexing (37% failure rate on code repos), stack overflows on large ASTs, watermark lag |
| **GKG Webserver** | Query timeouts, redaction exchange latency spikes, schema version mismatches |

These have manifested as real production incidents:

1. **Siphon startup failure**: Disaster archive Postgres node reported as replica, preventing Siphon from starting.
2. **ClickHouse production-load pathology**: `merge_request_diff_files` built up ~2 days of lag because projection-based PK lookups took 3-4s per lookup on 5B rows.
3. **Code indexing failures**: 4/11 public repos failed indexing completely; partial indexing returned no results with no user-facing warning.
4. **MergeRequestDiffFile ETL OOMs**: Joins caused out-of-memory errors in the indexing pipeline.

### Current On-Call Model

- **Tier 1**: Production Engineering SRE (24x7 follow-the-sun via Incident.io) — still being finalized
- **Tier 2**: Best-effort SME coverage — discussion about expanding Analytics Tier 2 to include GKG
- **Gap**: Production runbook, Grafana dashboards, alerting rules, and SLIs/SLOs are still incomplete

The 5-engineer Knowledge Graph team is expected to actively monitor the service during the first 0-100 days — a model borrowed from Global Search launch. This is unsustainable as the service scales.

### Why Autonomous Agents

Manual monitoring, runbook execution, and incident response are fundamentally repetitive, pattern-matchable tasks:

- **Detect**: Watch metrics, logs, alerts for known failure patterns
- **Diagnose**: Follow a decision tree (check NATS consumers → check ClickHouse lag → check Siphon → ...)
- **Remediate**: Execute runbook steps (restart stuck consumers, clear DLQ, dispatch reindexing, ...)
- **Communicate**: Create issues, leave comments, notify on-call

An autonomous agent system can execute these loops continuously, 24x7, with perfect recall of every runbook and historical incident. When the agent can resolve an issue, it does. When it cannot, it escalates with full context already gathered.

## Architecture

### Supervisor-Worker Model

```
                    ┌───────────────────────────┐
                    │        Supervisor          │
                    │  (Frontier reasoning model) │
                    │                            │
                    │  - Watches alert channels  │
                    │  - Plans investigation     │
                    │  - Delegates to workers    │
                    │  - Reviews worker output   │
                    │  - Decides: fix / escalate │
                    └─────────┬─────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
              ▼               ▼               ▼
    ┌─────────────┐  ┌───────────────┐  ┌──────────────────┐
    │  Monitor    │  │  Diagnostics  │  │  Reporting       │
    │  Worker     │  │  Worker       │  │  Worker          │
    │             │  │               │  │                  │
    │ - Scrape    │  │ - Query CH    │  │ - Create issues  │
    │   metrics   │  │ - Inspect     │  │   with diagnosis │
    │ - Watch     │  │   NATS state  │  │ - Open MRs       │
    │   logs      │  │ - Check       │  │   (config fixes) │
    │ - Parse     │  │   Siphon      │  │ - Comment on     │
    │   alerts    │  │ - Trace data  │  │   existing issues│
    │             │  │   pipeline    │  │ - Recommend       │
    │             │  │               │  │   remediation    │
    └─────────────┘  └───────────────┘  └──────────────────┘
```

### Operating Modes

**1. Continuous Monitoring (always-on)**

The system runs a monitoring loop that:
- Scrapes Prometheus metrics endpoints and Grafana dashboards
- Watches Elasticsearch/Logstash log streams for error patterns
- Monitors NATS JetStream consumer lag and DLQ depth
- Checks ClickHouse system tables for query pathology
- Tracks Siphon replication state and connectivity

**2. Alert-Triggered Investigation**

When an alert fires (PagerDuty/Incident.io webhook, Slack alert channel, metric threshold breach):
- Supervisor receives the alert context
- Plans an investigation (which workers to spawn, what to check)
- Workers execute diagnostic steps in parallel
- Supervisor synthesizes findings and decides next action

**3. Proactive Issue Detection**

Beyond reactive alerting, the system proactively scans for:
- Gradual degradation trends (slowly increasing query latency, growing consumer lag)
- Silent failures (partial indexing with no error, stale watermarks)
- Configuration drift (schema version mismatches across pods)
- Capacity warnings (ClickHouse disk/memory approaching limits)

### Capabilities and Tool Access

Workers operate through well-defined tool interfaces:

| Capability | Tools / APIs | Scope |
|------------|-------------|-------|
| **Metrics** | Prometheus API, Grafana API | Read-only: query SLIs, dashboard panels, alert state |
| **Logs** | Elasticsearch API, `kubectl logs` | Read-only: search error patterns, trace request IDs. `kubectl logs` only on `gitlab-org` cluster |
| **NATS** | NATS CLI / monitoring API | Read-only: inspect streams/consumers, consumer lag, DLQ depth. `gitlab-org` cluster only |
| **ClickHouse** | `clickhouse-client` | Read-only: `system.*` tables, graph tables for diagnostics. `gitlab-org` instance only — no customer data access |
| **Kubernetes** | `kubectl` (scoped RBAC) | Read-only: pod status, events, logs. `gitlab-org` cluster only — no customer data clusters |
| **GitLab** | `glab` CLI, GitLab API | Create issues, open MRs, leave comments, read pipeline status |
| **Siphon** | Siphon health API, logs | Read-only: replication status, table onboarding state |
| **GKG Server** | Health check endpoint, gRPC | Read: schema version, readiness, query test execution |

### Credential Model

Agents receive fine-grained, scoped credentials — never broad admin tokens.

| Credential | Scope | Access level |
|------------|-------|-------------|
| **GitLab PAT(s)** | Fine-grained project/group tokens | Read + write for specific actions (create issues, open MRs, leave comments). Scoped to `gitlab-org/orbit` and related projects |
| **`gitlab-org` cluster kubeconfig** | `gitlab-org` cluster only | Read-only: pod status, events, logs. No access to customer data clusters |
| **ClickHouse credentials** | `gitlab-org` ClickHouse instance | Read-only: `system.*` tables, graph tables for diagnostics. No customer data access |
| **NATS credentials** | `gitlab-org` NATS deployment | Read-only: stream/consumer inspection, monitoring API |
| **Orbit logs access** | TBD — possibly all Orbit logs across environments | Read-only via Elasticsearch/Grafana APIs |

### Data Isolation Constraint

**Direct infrastructure access (kubectl, ClickHouse, NATS) is restricted to the
`gitlab-org` cluster only.** This cluster contains only GitLab-org internal data —
no customer data. The agent must never have credentials or network access to
clusters that hold customer data.

Observability data (metrics, logs) may span broader environments — the scope of
log access is TBD, but would always be read-only through aggregated APIs
(Grafana, Elasticsearch), never through direct cluster access to customer-facing
infrastructure.

### Safety Boundaries

**Prohibited** (enforced at the tooling/RBAC level, not by the agent's judgement):

- No access to clusters containing customer data — `gitlab-org` cluster only for direct infra access
- No writes to ClickHouse (read-only credentials)
- No modification of NATS stream or consumer configuration
- No deletion of Kubernetes resources
- No force-push to protected branches
- All MRs require human review before merge
- Escalation required for S1/S2 severity (agent gathers context, human decides)

**Allowed actions** (GitLab only, via fine-grained PATs):

- Creating GitLab issues with diagnosis and recommended remediation
- Opening MRs (e.g., config fixes, runbook updates)
- Leaving comments on existing issues/MRs with investigation findings
- Reading pipeline status, logs, and CI artifacts

All infrastructure write operations (restarting pods, dispatching re-indexing,
resetting consumers) are out of scope. The agent diagnoses and recommends —
humans execute. If a remediation is needed, the agent creates an issue with
the exact commands to run.

### Escalation Protocol

```
Agent detects anomaly
    │
    ├── Known pattern?
    │   ├── Yes → Create issue with diagnosis + exact remediation commands
    │   └── No  → Continue investigation
    │
    ├── Root cause identified?
    │   ├── Yes → Create issue with full context + recommended action
    │   └── No  → Create issue with investigation findings, suggest next steps
    │
    └── Severity assessment
        ├── S1/S2 → Page on-call immediately with gathered context
        ├── S3 → Create issue, notify team channel
        └── S4 → Create issue, add to backlog
```

### Failure Containment

The agent system must fail safe. These mechanisms are enforced externally (not
by the agent's own judgement):

- **Global kill switch**: A single ConfigMap key (or feature flag) that any on-call
  SRE can flip to pause the supervisor immediately without redeploying.
- **Rate limits on GitLab writes**: Hard ceiling on issues created, comments posted,
  and MRs opened per hour. The Reporting worker stops and alerts when the limit is
  hit rather than queuing or retrying.
- **Issue deduplication**: Each diagnosis is fingerprinted. Re-creation of an issue
  with the same fingerprint is blocked within a TTL window to prevent alert-storm
  spam.
- **Quarantine mode**: If the safety-violations counter exceeds a threshold, the
  supervisor self-suspends and alerts the team. Manual intervention required to
  resume.
- **Bot identity**: All agent actions use a dedicated bot user (`@orbit-ops-agent`)
  so that human responders can distinguish agent-generated content from human
  updates and filter accordingly.

## Concrete Automation Targets (Phase 1)

Based on documented incidents and existing runbooks, these are the highest-value
automations:

### 1. NATS Consumer Health

**Current manual process** (from `sdlc_indexing.md` runbook):
- Check consumer info for pending/redelivery counts
- Identify stuck messages via `nats consumer info`
- Reset consumer if needed, potentially with JetStream purge

**Agent automation**:
- Continuous monitoring of consumer lag and redelivery rates
- DLQ depth monitoring with automatic issue creation when depth grows
- Full JetStream health report generated on schedule
- When consumer reset is needed: create issue with diagnosis and recommended commands for human execution

### 2. ClickHouse Query Pathology Detection

**Current manual process**:
- Ad-hoc investigation when query latency increases
- Manual `system.query_log` inspection
- Hypothesis-driven debugging of projection/PK issues

**Agent automation**:
- Periodic scan of `system.query_log` for slow queries (> p99 baseline)
- Automatic query plan analysis for newly slow queries
- Detection of table mutation backlogs (`system.mutations`)
- Capacity monitoring (disk, memory, merge backlog)
- Issue creation with query fingerprint, plan, and suggested optimization

### 3. Indexing Pipeline Health

**Current manual process**:
- Check watermark lag via metrics
- Manually inspect code indexing failure rates
- Re-dispatch failed indexing jobs

**Agent automation**:
- Continuous watermark lag monitoring with trend detection
- Code indexing failure rate tracking per repository
- Issue creation for transiently failed indexing jobs with re-dispatch commands
- Detection of silent partial indexing (indexed file count vs expected)
- Weekly indexing health report

### 4. Siphon Replication Monitoring

**Current manual process**:
- Monitor replication slot lag
- Check Siphon pod health after deployments
- Verify table snapshot completion

**Agent automation**:
- Replication slot lag monitoring with early warning
- Post-deployment health verification (Siphon pods connecting, consuming)
- Table onboarding progress tracking
- Connectivity check between Siphon and all Postgres endpoints

### 5. Schema Version and Deployment Verification

**Current manual process**:
- Check schema version matches across indexer and webserver pods
- Verify health check endpoint after deployment
- Run smoke queries post-deployment

**Agent automation**:
- Post-deployment schema version consistency check
- Automated smoke query execution against production
- Health endpoint monitoring during rolling deployments
- Automatic rollback recommendation if health degrades post-deploy

## Implementation Approach

### Execution Model

Unlike interactive chat-based agent systems, this system operates autonomously:

- **No human-in-the-loop for routine operations**: The supervisor plans and executes
  without waiting for human approval (within safety boundaries)
- **Continuous execution**: The supervisor runs in a loop — monitor, detect, investigate,
  act, report
- **Asynchronous communication**: Results are posted to GitLab issues, Slack channels,
  and incident timelines — not to a chat interface
- **Persistent memory**: The supervisor cannot run indefinitely — it needs a durable
  memory layer in GitLab (issues, project wiki, repository files, or a dedicated
  state store) to carry context across sessions: what was investigated, what actions
  were taken, what is still pending. Design of this memory layer is a separate topic.
  Phase 0 tolerates stateless restarts (each monitoring cycle is self-contained).

### Infrastructure

- **Compute**: Sandboxed containers running in a dedicated namespace on the
  `gitlab-org` cluster (or a separate agent-only cluster with no network path
  to customer data clusters). One supervisor, N workers.
- **Orchestration**: Supervisor spawns/terminates workers based on workload
- **State**: PostgreSQL for session history, action log, and escalation tracking
- **Secrets**: Fine-grained GitLab PATs, read-only `gitlab-org` cluster kubeconfig,
  read-only ClickHouse/NATS credentials (see Credential Model above)

### Agent Skills and Knowledge

The agents are equipped with:

1. **Runbooks as skills**: Existing runbooks (`sdlc_indexing.md`, `code_indexing.md`,
   `server_configuration.md`) are loaded as agent context, providing step-by-step
   procedures the agent can follow
2. **Architecture context**: Design documents and `AGENTS.md` provide system understanding
3. **Operational history**: Past incidents and their resolutions inform pattern matching
4. **Tool proficiency**: Pre-configured MCP servers for Prometheus, Elasticsearch, NATS,
   ClickHouse, GitLab, and Kubernetes APIs

### Observability of the Agent System Itself

The autonomous agent system needs its own observability:

| Signal | Implementation |
|--------|---------------|
| Agent health | Dedicated health endpoint; alert if supervisor is down > 5 min |
| Action audit log | Every agent action logged to PostgreSQL with timestamp, reasoning, outcome |
| Cost tracking | LLM token usage tracked per investigation cycle |
| Effectiveness metrics | Diagnosis accuracy, issues caught proactively, MTTD comparison, false positive rate |
| Safety violations | Alert on any action that hits a hard safety boundary |

## Rollout Plan

### Phase 0: Monitoring + Reporting (weeks 1-4)

- Deploy supervisor in monitoring-only mode
- Generate diagnostic reports, create issues with findings
- Validate detection accuracy against known incidents
- Tune alert thresholds and pattern matching
- **Exit criteria**: >90% of real issues detected, <10% false positive rate

### Phase 1: Active Diagnosis (weeks 5-8)

- Enable alert-triggered investigation (agent investigates when paged)
- Agent creates issues with root cause analysis + exact remediation commands
- Humans execute recommended actions, feed back results
- **Exit criteria**: >80% of diagnoses confirmed accurate by humans

### Phase 2: Proactive Detection (weeks 9-12)

- Enable trend detection and early warning (degradation before alert fires)
- Capacity planning recommendations
- Performance regression detection
- Configuration drift detection across pods/deployments
- **Exit criteria**: Agent catches issues before they page Tier-1

### Phase 3: Knowledge Building (weeks 13+)

- Agent proposes runbook updates based on novel investigations
- Opens MRs to improve alerting rules, dashboards, documentation
- Builds a searchable knowledge base of past investigations and resolutions
- Feeds back into persistent memory for future sessions

## Success Metrics

| Metric | Baseline (current) | Target (6 months) |
|--------|--------------------|--------------------|
| Tier-1 pages per week (KG-related) | TBD (measure during Phase 0) | 50% reduction |
| Mean time to detection (MTTD) | Minutes to hours (human-dependent) | < 2 minutes |
| Mean time to diagnosis (root cause identified) | 30-60 min (manual investigation) | < 10 min |
| After-hours engineer wake-ups for KG | TBD | 80% reduction |
| Diagnosis accuracy | N/A | > 80% confirmed correct by humans |
| False positive rate | N/A | < 5% |
| Issues caught before Tier-1 page | 0% | > 30% (proactive detection) |

## Open Questions

1. **LLM cost model**: What is the acceptable monthly spend for continuous agent
   operation? Need to model token usage for monitoring loops vs investigation cycles.

2. **Shared infrastructure**: Should the agent platform be shared across teams
   (e.g., could Siphon, NATS, or ClickHouse teams also use autonomous agents)?
   Or should this be Orbit-specific?

3. **Integration with Incident.io**: How does autonomous remediation interact with
   the existing incident management workflow? Should the agent create/update
   incidents directly?

4. **Scope of k8s RBAC**: What is the minimal RBAC set needed for useful remediation
   without creating security risk?

5. **On-call relationship**: Does this replace Tier-2 SME coverage, augment Tier-1,
   or create a new "Tier-0" autonomous layer?

6. **Supervisor persistent memory**: The supervisor cannot run indefinitely and needs
   durable state across sessions. Options include GitLab issues as a structured log,
   a dedicated repository with state files, project wiki pages, or an external store.
   The memory layer needs to support both structured state (current investigations,
   pending actions, known patterns) and unstructured context (investigation notes,
   incident timelines). This is a separate design topic.

## References

- [Orbit observability design](../observability.md)
- [SDLC indexing runbook](../../dev/runbooks/sdlc_indexing.md)
- [Code indexing runbook](../../dev/runbooks/code_indexing.md)
- [Server configuration runbook](../../dev/runbooks/server_configuration.md)
- [Orbit launch super document](https://docs.google.com/document/d/1UD5E_53bMfX6IYRVu41KGZ7NGh-wObXrOA0EizI_d0U)
- [Knowledge Graph offsite notes](https://docs.google.com/document/d/1BLfJGqyHtaNSdf_OO_YFoNaQFcaewMc1IgKKytPGGEg)
