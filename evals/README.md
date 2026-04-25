# GKG Agent Evaluation Harness

Compares AI agent performance on graph tasks: **Orbit** (structured Knowledge Graph queries) vs **glab CLI** (GitLab REST/GraphQL). Both arms hit the same GitLab instance -- the variable is the access method.

Each arm runs in an isolated Docker container. All state lives in DuckDB via a proxy server. Runs auto-resume on crash.

## Setup

```bash
cp evals/.env.example evals/.env
# fill in GITLAB_TOKEN and ANTHROPIC_API_KEY
mise eval:build    # build Docker image (requires colima)
```

## Usage

From the repo root:

```bash
mise eval:dry-run       # validate config, tasks, fixtures, Docker
mise eval:run           # run full evaluation
mise eval:run:bg        # run detached in background
mise eval:score         # score latest run
mise eval:report        # generate markdown report
mise eval:full          # build -> run -> score -> report
```

Or from `evals/`:

```bash
mise run                # same as eval:run
mise run:orbit          # single arm
mise run:glab
```

## Architecture

```
eval.yaml (SSOT)
    │
    ▼
EvalRunner (runner.py)
    │
    ├── db_server.py (:5555)          DuckDB proxy (single writer, MVCC reads)
    │       │
    │       ▼
    │   eval.duckdb                   all state (7 tables, ~20 macros)
    │
    ├── Docker: eval-orbit (:4096)    isolated container, ephemeral workspace
    │       │
    │       ▼ SSE events
    │   EventDemuxer (session.py)  ──► live_events table (real-time)
    │
    └── Docker: eval-glab (:4097)     same image, different agent/skills
            │
            ▼ SSE events
        EventDemuxer               ──► live_events table (real-time)
```

### Container isolation

Each arm runs in a Docker container built from `container/` (alpine + mise + opencode + glab). The host bind-mounts the eval workspace read-only at `/mnt/workspace`. The entrypoint copies it to `/workspace` (writable, ephemeral) and starts `opencode serve`. The agent discovers skills naturally via `.opencode/skills/` in the copied workspace. Everything the agent writes dies with `--rm`.

### DuckDB proxy

During runs, all reads and writes go through `db_server.py` (FastAPI on :5555). Writes are batched (flush every 100ms or 200 items). Reads use separate MVCC cursors for snapshot isolation. This allows concurrent CLI queries during active runs.

Offline (no server running), CLI commands fall back to `DirectClient` for direct file access.

### State tables

| Table | Written when | Purpose |
|---|---|---|
| `runs` | begin/end of run | run lifecycle |
| `run_configs` | before first task | config + files + SHA256 hash + Docker image hash |
| `servers` | each state change | container lifecycle |
| `live_events` | per SSE event | real-time agent activity |
| `task_results` | per task completion | results + token/cost stats |
| `snapshots` | per task completion | full session trace for scoring |
| `scores` | score command | evaluator scores |

### Auto-resume

If a run crashes, the next `mise eval:run` with the same `eval.yaml` detects the incomplete run via `config_hash` match and resumes from where it stopped. `completed_task_ids` per arm means only the in-flight task is re-run.

### Real-time observability

```sql
-- connect during a run:
-- curl localhost:5555/query -d '{"sql": "FROM task_progress(...)"}'
-- or after: duckdb .eval-servers/eval.duckdb

FROM task_progress('20260423_120000');
FROM part_stream('20260423_120000', 'orbit', 'mr-neighbors');
FROM msg_stream('20260423_120000', 'orbit', 'mr-neighbors');
FROM arm_summary('20260423_120000');
FROM compare_arms('20260423_120000', 'orbit', 'glab');
FROM run_overview();
```

## Tasks

19 tasks across 4 difficulty levels:

| Category | Count | Examples |
|---|---|---|
| search | 1 | search-user |
| traversal | 2 | mr-review-chain, mr-neighbors |
| aggregation | 4 | mr-count-by-project, label-hotspots, reviewer-workload, pipeline-failure-authors |
| multi-hop | 3 | vulnerability-blast-radius, issue-to-deployment, cross-project-collaborators |
| path-finding | 1 | path-between-users |
| research | 7 | gatekeeper-analysis, god-model-archaeology, co-change-coupling, most-contentious-features, security-scan-coverage, release-velocity, pipeline-stage-bottleneck |

Research tasks require crossing code graph and SDLC boundaries (DiffFile -> MR -> User -> Label) and are where orbit's graph advantage is most pronounced.

## Directory layout

```
evals/
├── eval.yaml                config (arms, tasks, evaluators)
├── container/
│   ├── Dockerfile           alpine + mise bootstrap
│   ├── mise.toml            tool versions (SSOT)
│   └── entrypoint.sh        cp workspace + exec opencode
├── harness/
│   ├── runner.py            EvalRunner class (orchestration)
│   ├── db.py                DbClient + DirectClient
│   ├── db_server.py         FastAPI DuckDB proxy
│   ├── store.py             ResultStore (reads/writes via macros)
│   ├── server.py            ServerManager (Docker lifecycle)
│   ├── session.py           SSE demuxer + snapshot capture
│   ├── cli.py               click CLI + Ctx class
│   ├── config.py            pydantic models
│   ├── report.py            markdown generation
│   ├── sql/{ddl,helpers}.sql
│   ├── evaluators/          graph, efficiency, behavior
│   └── aggregators/         descriptive, comparative, distributional
├── tools/orbit_query.py     orbit API wrapper (toon format, query-schema)
├── agents/                  system prompts per arm
├── .opencode/skills/        orbit-query, glab-data
├── tasks/*.yaml             task definitions
├── fixtures/<task>/         params.json + expected.json
├── tests/test_smoke.py      28 tests
└── .eval-servers/           runtime (gitignored)
    ├── eval.duckdb
    └── logs/
```

## Tests

```bash
cd evals && uv run --extra dev pytest tests/ -v
```
