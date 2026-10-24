---
name: orbit-cli
description: >
  Index and query a local checkout offline with the GitLab Orbit CLI (`orbit`, or
  `glab orbit`). Reach for it when a question names a symbol or spans code
  structure. Examples: who calls or extends this, where it is defined, what
  a file defines, how definitions are distributed. One call replaces many
  file reads and text greps, including matches in config, templates, and docs.
  Works on the working tree and unpushed branches, and also searches and reads
  the project's GitLab. Not a fit: reading one known file, or counts and
  relationships across GitLab (use the `orbit` skill).
version: 0.33.1
license: MIT
compatibility: Requires the GitLab Orbit CLI (directly or through glab); local indexing needs filesystem access to the checkout.
metadata:
  audience: developers
  keywords: orbit, orbit-cli, local-graph, knowledge-graph, code-graph, workspace
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-cli
---

# GitLab Orbit CLI skill

The GitLab Orbit CLI parses a checkout into a local graph. `grep` finds
every matching line in code, tests, config, and docs, labeled with its enclosing definition. `context` reads their source and relationships. Both
index the working tree on first use and refresh the index when it is stale, so
you never need to index by hand. A new commit alone does not trigger an index.
A read waits up to 10 minutes for a running index and then exits with code 75.
To index ahead of time without blocking a short shell timeout, run
`workspace index --async`, which returns once the background index holds its locks.
It prints `{"status": ...}` with `indexing`, `indexed`, or `queued` (another index
of the repository already runs). If open readers hold the locks for 30 s, it exits
with code 75 while the background index keeps waiting.
`workspace index --wait` waits for a running index (`--timeout SECONDS`) and then
indexes in the foreground, which is the default. For GitLab data across projects, use the `orbit` skill, which queries the GitLab Orbit API.

The binary is `orbit`, or `glab orbit` through the wrapper. In non-interactive
shells, put glab's `--yes` before the subcommand so glab skips its install and
run prompts: `glab orbit --yes grep "..."`. The binary rejects `--yes` after
other subcommands. Only `setup` and `uninstall` take their own `--yes`
(`glab orbit --yes setup --yes`). Run `orbit <command> --help` before you guess
a flag. Run `orbit skills` to list available skills. Use
`orbit skills get orbit [path]` to print the composed skill (`SKILL.md` by
default), or specify a path to print a file from the skill tree.

Wrapper details: [`references/local/cli.md`](references/local/cli.md).

## Fix inaccurate guidance

If guidance is wrong or outdated (command, flag, or behavior), tell the user.
With their confirmation, open a focused MR against `metadata.source-project` fixing `metadata.source-path` (one fix per MR, Conventional Commits).
If they decline, note the discrepancy in one line and continue with the corrected command.

## Find, then read

```shell
orbit grep "method:rate limit" src
orbit grep 'query_arrow|insert_batch|execute' crates/duckdb-client
orbit context duckdb_client::search::DuckDbSearch::grep src/lib.rs:120-180 crates/duckdb-client
```

Quote `a|b|c` for OR alternatives. Plain words ignore case, `_`, and `-`; terms
with regex characters match as regex. Output follows rg's heading layout: each
file's path once, then `line:text` per match, in full. A `line-» Kind name:start-end`
line precedes the matches inside each definition. `line-» imports`
names what an import line brings in, and `line-» outside indexed definitions` marks
code no definition encloses. A path that does not exist searches tracked paths with the same last segment.
A scoped search with no matches widens one directory at a time. A `»` line says what was searched.
A search that matches more than 20 files lists them by directory, with matching lines per file,
before the matches. Pipe through `head` to bound output; `-m N` caps matches
per file and `-M N` hides lines wider than N. Files that define
a term come first, then code, tests, and config or docs. A definition the query names prints first with its source.
The usual rg and grep flags work: `-A`/`-B`/`-C`, `-l`, `-c`, `-w`, `-F`, `-g`.
A kind and a colon start a search for the words after it, up to the next kind, so
`orbit grep "mr:rate limit def:throttle"` runs two searches. Kinds are graph nodes
or short names: `def:`, `class:`, `method:`, or `function:` for definitions, and
`file:` or `dir:` alone list where matches are. A trailing `def:` applies to the
words before it. `file:x` keeps paths containing x, and alone lists them.
Unknown names such as `std::io` stay literal, and `-F` turns qualifiers off.

A plain search with no paths or file filters also searches the GitLab group
behind `origin`. It finds issues, merge requests, and notes; `commit:words` searches commits, and
`orbit context repo:group/project symbol:Name` reads another project's code.
Those results print after the local ones as tokens such as `MergeRequest[42]`.
Name `group/project`, `Project[id]`, or `Group[id]` as a path to search another
project or group, including its code when there is no local checkout.
`mr:`, `issue:`, `commit:`, and `comment:` search GitLab only.
`branch:` finds branches with their merge requests and pipelines, and
`repo:group/project` names the project.

```shell
orbit grep "mr:rate limit repo:gitlab-org/gitlab"
orbit grep "branch:approval rules"
orbit context mr:42 issue:7 src/lib.rs:120-180
orbit context branch:fix-ci pipeline:123 job:456 commit:0123abc repo:gitlab-org/cli
```

Pass names as printed by `grep`, paths, ranges, directories, or GitLab targets
as grep prints them: `mr:42`, `issue:7`, `branch:name`, `pipeline:123`,
`job:456`, `commit:sha`. They resolve in the `origin` project unless
`repo:group/project` names another; `mr:group/project!42` names one per target.
Merge requests and issues return description, comments, and review threads, and
merge requests add changed files and failed CI jobs. A branch reads its merge
request, or its latest pipeline. Pipelines list failed jobs, and jobs show
their logs; commits show message, changed files, and merge requests. Bare
`orbit context` reads the current branch. A `Definition:<id>` from another indexed checkout reads that checkout.
Definition targets show full source and indexed relationships. File targets show
a compact map and at most ten connections per section, with omitted counts.
Line ranges show only the numbered lines.
`<--` is a caller and `-->` is a callee. Reuse the returned source.

<!-- orbit:section quick-start -->
## Choose the command

- `orbit grep 'a|b'` searches code, tests, config, and docs and labels each match with its enclosing definition. A plain query also searches the GitLab group behind `origin`.
- `orbit context <name> <path>:<lines> mr:42 job:9` reads several targets in one call. Definitions come with their callers and tests, and GitLab merge requests, issues, branches, pipelines, and jobs come with the local code they point at. A bare `orbit context` reads the current branch's merge request and CI failures.
- `orbit query` answers relationship and aggregation questions across GitLab entities through the GitLab Orbit API. Examples: who reviewed which merge requests, or which changes to an area failed CI. Write the query in the language below.

```shell
orbit grep 'retry_backoff|RetryBackoff'
orbit context src/retry.rs:40-90 retry_backoff mr:42
```

<!-- /orbit:section -->

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/local/cli.md) |
