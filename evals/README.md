# GKG Agent Evaluation Harness

Compares AI agent performance on graph tasks: **Orbit** (structured Knowledge Graph queries via GitLab REST API) vs **glab CLI** (manual GitLab REST/GraphQL access). Both arms hit the same GitLab instance -- the variable is the access method.

Uses OpenCode's HTTP API for agent orchestration with full glass-box trace capture.

## Setup

```bash
cd evals/
cp .env.example .env
# fill in GITLAB_TOKEN and ANTHROPIC_API_KEY
mise setup        # or: uv sync --extra dev
```

## Usage

```bash
# Validate config, tasks, fixtures without running
mise dry-run

# Run full evaluation (all arms)
mise run

# Run a single arm
mise run:orbit
mise run:glab

# Score + report
mise score
mise report

# All three in sequence
mise full
```

## Architecture

```
eval.yaml (SSOT config)
    │
    ▼
harness/runner.py
    │  per arm: spawn opencode serve on dedicated port
    │  per task: create session → send prompt → capture snapshot
    │
    ├── tools/orbit_query.py     Orbit arm: REST wrapper for /api/v4/orbit/
    └── glab CLI                 glab arm: glab commands + GraphQL
    │
    ▼
harness/store.py  ──►  .eval-servers/eval.duckdb
    │                     ├── task_results    per-task results + session stats
    │                     ├── snapshots       full session traces (JSON, end-of-task)
    │                     ├── live_events     raw SSE events (real-time)
    │                     ├── live_messages   message info extracted from SSE (real-time)
    │                     ├── live_parts      tool calls + text extracted from SSE (real-time)
    │                     ├── scores          evaluator scores per task
    │                     ├── run_configs     config snapshots + file content hashes
    │                     ├── runs            run metadata
    │                     └── servers         OpenCode server process state
    │
    ▼
harness/evaluators/     per-task scoring (graph, efficiency, behavior)
harness/aggregators/    cross-arm analysis (descriptive, comparative, distributional)
harness/report.py       markdown report
```

## State management

All harness state lives in a single DuckDB file at `.eval-servers/eval.duckdb`. Schema is defined in `harness/sql/ddl.sql`, reusable query macros in `harness/sql/helpers.sql`.

You can connect directly for ad-hoc analysis:

```bash
duckdb .eval-servers/eval.duckdb
```

### Useful macros

```sql
-- Overview of all runs with config hash, cost, success count
FROM run_overview();

-- Per-arm summary for a run (successes, cost, avg duration, etc.)
FROM arm_summary('20260423_120000');

-- Head-to-head task comparison between two arms
FROM compare_arms('20260423_120000', 'orbit', 'glab');

-- Task results with evaluator scores
FROM task_detail('20260423_120000', 'orbit');

-- Find all runs that used the same config
FROM runs_by_config('a1b2c3d4e5f6g7h8');

-- Scalar helpers
SELECT run_cost('20260423_120000');
SELECT success_rate('20260423_120000', 'orbit');
```

### Real-time observability

SSE events are streamed to DuckDB as they arrive. Message and tool-call data is extracted into queryable tables so you can observe agent behavior mid-task:

```sql
-- Per-task progress (messages, tool calls, last activity)
FROM task_progress('20260423_120000');

-- Watch tool calls for a specific task as they happen
FROM part_stream('20260423_120000', 'orbit', 'mr-neighbors');

-- See message costs accumulating live
FROM msg_stream('20260423_120000', 'orbit', 'mr-neighbors');

-- Raw event stream
FROM live('20260423_120000', 'orbit');

-- Quick event count (is anything happening?)
SELECT event_count('20260423_120000');
```

Data flows in three tiers:
1. **Real-time** -- `live_events`, `live_messages`, `live_parts` written per-SSE-event
2. **Per-task** -- `task_results` written when each task completes
3. **End-of-task** -- `snapshots` captured via API call after task finishes (full trace for scoring/replay)

### Config versioning

Each run snapshots the full eval config (parsed YAML), all referenced files (agent prompts, skills, task YAMLs, fixtures), and a SHA256 hash of the bundle. This lets you detect config drift between runs:

```sql
-- Compare config hashes across runs
SELECT run_id, config_name, config_version, config_hash FROM run_configs;

-- See what files were used in a specific run
SELECT json_keys(files) FROM run_configs WHERE run_id = '20260423_120000';
```

The `run.version` field in `eval.yaml` is a semver string you bump manually when making intentional config changes. The `config_hash` is computed automatically and catches any change (including file content).

## Key design decisions

- **All Python.** OpenCode SDK is 10 HTTP endpoints; hand-written httpx client.
- **Full agent trace.** SessionSnapshot captures every message, tool call, event, diff, todo.
- **Incremental snapshots.** SSE events write message/part data to DuckDB in real-time; full snapshot captured at task end for scoring.
- **Skill pre-loading.** Skill content is inlined into the system prompt so agents don't waste a turn calling the skill tool.
- **Prompt not retried.** Non-deterministic; only session create + data extraction retried.
- **Shared SSE.** One connection per arm, EventDemuxer routes events by session_id with per-session callbacks.
- **DuckDB for all state.** Results, snapshots, scores, config snapshots, server state -- single file, queryable with SQL macros. Read queries use macros from `helpers.sql`.
- **Resume via completed_task_ids.** Queries DuckDB for already-completed tasks, skips them on restart.
- **4-state error model.** SUCCESS | TIMEOUT | AGENT_ERROR | INFRA_ERROR, raw error always stored.

## Directory layout

```
evals/
├── eval.yaml                SSOT config (name, version, arms, tasks, evaluators)
├── pyproject.toml           dependencies
├── mise.toml                task runner
├── .env                     secrets (not committed)
├── harness/                 core framework
│   ├── config.py            pydantic models + env var resolution
│   ├── db.py                shared DuckDB connection management
│   ├── sql/
│   │   ├── ddl.sql          table definitions
│   │   └── helpers.sql      reusable scalar + table macros
│   ├── opencode.py          httpx async client for OpenCode API
│   ├── session.py           snapshot capture + SSE event demuxer
│   ├── store.py             DuckDB-backed result/snapshot/score storage
│   ├── server.py            OpenCode server process manager (DuckDB-backed)
│   ├── runner.py            orchestration loop
│   ├── cli.py               CLI entry point
│   ├── report.py            report generation
│   ├── evaluators/          per-task scoring
│   └── aggregators/         cross-arm analysis
├── tools/
│   └── orbit_query.py       agent-callable REST wrapper
├── agents/                  agent system prompts per arm
├── tasks/                   task YAML definitions
├── fixtures/                expected results + params per task
├── tests/                   smoke tests
└── .eval-servers/           runtime state (gitignored)
    ├── eval.duckdb          all harness state
    └── logs/                server stdout/stderr
```

## Adding tasks

Create a YAML file in `tasks/`:

```yaml
id: my-task
prompt: |
  Find all {{entity}} in project {{project_id}}.
category: search
difficulty: easy
structured_output_schema:
  type: object
  required: [results]
  properties:
    results:
      type: array
```

Then add fixtures:

```
fixtures/my-task/
├── params.json       {"entity": "Issue", "project_id": 278964}
└── expected.json     {"results": [...]}
```

## Tests

```bash
uv run --extra dev pytest tests/ -v
```
