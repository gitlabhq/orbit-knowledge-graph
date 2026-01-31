# GitLab Load Testing Framework (Rust)

A Rust tool that hammers your GitLab instance with fake user activity. It spawns a bunch of agents that create projects, open issues, make merge requests, and generally act like real users - except they never take coffee breaks.

No Ruby dependencies. Just compile and run.

## What it does

Each agent:
- Gets its own GitLab user account
- Creates projects with Java code (Maven structure, valid syntax)
- Opens and closes issues
- Creates merge requests, pushes commits, merges them
- Comments on things

The agents pick actions randomly based on weights, so you get a mix of activity that looks vaguely realistic. File creation happens more often than project creation, commenting happens more often than merging, etc.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Orchestrator                          │
│  - Creates 2 namespaces (load-test-namespace-1, -2)         │
│  - Creates N users (load-test-user-001 through -100)        │
│  - Spawns agent tasks                                       │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌─────────┐     ┌─────────┐     ┌─────────┐
        │ Agent 1 │     │ Agent 2 │     │ Agent N │
        │         │     │         │     │         │
        │ State:  │     │ State:  │     │ State:  │
        │ -Projects│    │ -Projects│    │ -Projects│
        │ -Issues │     │ -Issues │     │ -Issues │
        │ -MRs    │     │ -MRs    │     │ -MRs    │
        └────┬────┘     └────┬────┘     └────┬────┘
             │               │               │
             └───────────────┼───────────────┘
                             ▼
                    ┌─────────────────┐
                    │   GitLab API    │
                    │ localhost:3000  │
                    └─────────────────┘
```

## Actions

| Action | Weight | What it does |
|--------|--------|--------------|
| create_project | 5 | New project with pom.xml |
| create_file | 20 | Adds a Java class |
| update_file | 15 | Edits an existing file |
| create_issue | 15 | Opens an issue |
| close_issue | 10 | Closes one |
| link_issues | 5 | Links two issues |
| create_milestone | 5 | Creates a milestone |
| attach_milestone | 5 | Attaches milestone to issue |
| create_merge_request | 8 | New branch, new file, opens MR |
| push_to_merge_request | 10 | Adds commits to an open MR |
| comment_on_issue | 5 | Leaves a comment |
| comment_on_merge_request | 5 | Leaves a comment |
| approve_merge_request | 5 | Approves an open MR |
| merge_merge_request | 3 | Merges it |
| close_merge_request | 2 | Closes without merging |
| reply_to_issue_comment | 5 | Replies to another agent's comment on an issue |
| reply_to_mr_discussion | 5 | Replies to another agent's discussion thread on an MR |

Higher weight = happens more often.

## Requirements

- Rust 1.70+
- A GitLab instance (GDK works fine)
- An admin API token

## Building

```bash
cargo build --release
```

Binary lands at `target/release/gitlab-load-testing`.

## Usage

```bash
export GITLAB_QA_ADMIN_ACCESS_TOKEN="glpat-xxx"

# Defaults: 100 agents, 60 minutes
./target/release/gitlab-load-testing

# Smaller test
./target/release/gitlab-load-testing \
  --agent-count 5 \
  --duration-minutes 5

# Different GitLab instance
./target/release/gitlab-load-testing \
  --base-url http://gdk.test:3000
```

### Options

| Option | Env var | Default |
|--------|---------|---------|
| `--base-url` | `LOAD_TEST_BASE_URL` | `http://localhost:3000` |
| `--admin-token` | `GITLAB_QA_ADMIN_ACCESS_TOKEN` | (required) |
| `--agent-count` | `LOAD_TEST_AGENTS` | 100 |
| `--duration-minutes` | `LOAD_TEST_DURATION_MINUTES` | 60 |
| `--min-action-delay` | - | 0.5s |
| `--max-action-delay` | - | 3.0s |
| `--verbose` | `LOAD_TEST_VERBOSE` | false |
| `--dry-run` | `LOAD_TEST_DRY_RUN` | false |

## Dry-Run Mode

Test the framework without making actual API calls:

```bash
./target/release/gitlab-load-testing \
  --agent-count 5 \
  --duration-minutes 1 \
  --dry-run
```

Dry-run mode:
- Simulates all actions without hitting GitLab
- Shows what API endpoints would be called
- Reports action distribution across agents
- Saves detailed report to `dry_run_report_YYYYMMDD_HHMMSS.json`

Use this to verify:
- The flow works correctly
- Expected API connectivity requirements
- Action distribution matches expectations

Example dry-run output:

```
=== DRY-RUN REPORT ===
(No actual API calls were made)

Total Actions Simulated: 45

--- Actions by Type ---
  create_file                    :    12
  create_issue                   :     8
  create_project                 :     5
  ...

--- API Endpoints Required ---
    15x  POST /projects/:id/repository/files/:path
     8x  POST /projects/:id/issues
     5x  POST /projects
  ...

--- Agent Activity ---
  Agents active: 5
  Avg actions per agent: 9
```

## Cross-Agent Interaction

Agents share a common resource pool, so they interact with each other's work:

- **Comments**: Any agent can comment on issues/MRs created by other agents
- **Replies**: Agents reply to comments made by other agents, creating threaded conversations
- **Approvals**: Agents approve MRs from other agents (can't approve your own)
- **Issue links**: Issues from different agents can be linked together

Example flow:
```
Agent 1 creates issue #5
Agent 3 comments on issue #5: "Looks good!"
Agent 7 replies to Agent 3's comment: "> Looks good!\n\nI agree with this."
Agent 2 also comments on issue #5
```

This simulates realistic multi-user collaboration on shared projects.

## Output

You get progress updates every minute:

```
[Progress] 55min remaining | Requests: 1523 | Success: 98.5%
```

And a summary at the end:

```
=== LOAD TEST RESULTS ===
Duration: 3600.0s
Total Requests: 45230
Success Rate: 98.7%

--- By Action Type ---
create_project              | Total:  1200 | Success:  1195 | Fail:     5 | Avg:   234.5ms | P95:   456.2ms
create_file                 | Total:  8500 | Success:  8450 | Fail:    50 | Avg:   125.3ms | P95:   298.1ms
```

A JSON report also gets written to `load_test_report_YYYYMMDD_HHMMSS.json`.

## Generated code

The Java files have actual structure:

```
project-name/
├── pom.xml
└── src/main/java/com/example/loadtest/
    ├── service/
    │   └── UserService.java
    ├── controller/
    │   └── OrderController.java
    └── repository/
        └── ProductRepository.java
```

They compile. Whether they do anything useful is a different question.

## Resource reuse

Namespaces and users have predictable names (`load-test-namespace-1`, `load-test-user-001`, etc.). On repeat runs, the tool finds existing ones and reuses them. It only creates new PATs since old ones might have expired.

This means the first run takes a while to set up, but subsequent runs start faster.

## Logging

```bash
# More output
RUST_LOG=debug ./target/release/gitlab-load-testing

# Way more output
RUST_LOG=trace ./target/release/gitlab-load-testing
```
