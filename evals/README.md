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
harness/store.py
    ├── results/<run_id>/<arm>.jsonl        TaskResult per task
    └── results/<run_id>/sessions/<id>.json Full SessionSnapshot
    │
    ▼
harness/evaluators/     per-task scoring (graph, efficiency, behavior)
harness/aggregators/    cross-arm analysis (descriptive, comparative, distributional)
harness/report.py       markdown + JSON report
```

## Key design decisions

- **All Python.** OpenCode SDK is 10 HTTP endpoints; hand-written httpx client.
- **Full agent trace.** SessionSnapshot captures every message, tool call, event, diff, todo.
- **Prompt not retried.** Non-deterministic; only session create + data extraction retried.
- **Shared SSE.** One connection per arm, EventDemuxer routes events by session_id.
- **Resume via JSONL.** Reads existing lines, skips completed task IDs.
- **4-state error model.** SUCCESS | TIMEOUT | AGENT_ERROR | INFRA_ERROR, raw error always stored.

## Directory layout

```
evals/
├── eval.yaml               SSOT config
├── pyproject.toml           dependencies
├── mise.toml                task runner
├── .env                     secrets (not committed)
├── harness/                 core framework
│   ├── config.py            pydantic models + env var resolution
│   ├── opencode.py          httpx async client for OpenCode API
│   ├── session.py           snapshot capture + SSE event demuxer
│   ├── store.py             JSONL + snapshot writer
│   ├── runner.py            orchestration loop + server lifecycle
│   ├── cli.py               CLI entry point
│   ├── report.py            report generation
│   ├── evaluators/          per-task scoring
│   └── aggregators/         cross-arm analysis
├── tools/
│   └── orbit_query.py       agent-callable REST wrapper
├── agents/                  agent system prompts per arm
├── skills/                  SKILL.md files loaded by agents
├── tasks/                   task YAML definitions
├── fixtures/                expected results + params per task
├── tests/                   smoke tests
└── results/                 output (gitignored)
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
uv run pytest tests/ -v
```
