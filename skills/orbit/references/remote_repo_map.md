# Orbit remote repo map reference

`remote_repo_map.py` is the remote counterpart to the local [`glab orbit repo-map`](local_repo_map.md) command. It maps code structure for any project indexed in Orbit Remote by shelling out to `glab orbit query`. It needs no local checkout or Orbit Local index.

Use it for inheritance trees, ancestor chains, included concerns, class members, directory API surfaces, or call sites in a project that Orbit Remote has indexed. Prefer the local command when you are in a checkout and need branch-local or unmerged code. Prefer a raw query from [`recipes.md`](recipes.md) for a single known entity or a cross-domain SDLC question.

## Prerequisites

- glab v1.117.0 or later, authenticated against GitLab.com.
- The `knowledge_graph` feature flag enabled for the user.
- The target project and branch indexed in Orbit Remote.
- A token with access to the target project.

## Invocation

The script is at `./scripts/remote_repo_map.py` relative to the Orbit skill root. The default target is `gitlab-org/gitlab` (`project_id = 278964`) on `master`. Override with global flags before the subcommand.

```bash
python3 ./scripts/remote_repo_map.py extends BasePolicy
python3 ./scripts/remote_repo_map.py extends ApplicationRecord --depth 3
python3 ./scripts/remote_repo_map.py ancestors Ci::Build
python3 ./scripts/remote_repo_map.py ancestors Issue --filter-prefix app/models/concerns
python3 ./scripts/remote_repo_map.py includes Noteable app/models/concerns
python3 ./scripts/remote_repo_map.py class MergeRequestPolicy
python3 ./scripts/remote_repo_map.py api app/services/merge_requests
python3 ./scripts/remote_repo_map.py callers execute
python3 ./scripts/remote_repo_map.py callers "MergeRequests::RefreshService#execute"
python3 ./scripts/remote_repo_map.py --project-id 77960826 --branch main api crates/orbit-cli
```

## Subcommands

| Subcommand | What it does |
|---|---|
| `extends NAME [--depth N]` | Walks `EXTENDS` from a base type down to descendants in one server-side multi-hop traversal. Depth is capped at 3 by the server. Prints definition type, FQN, and `file_path:line`, ordered by file path. Results are not labelled per hop, because the response has no reliable per-node hop count. |
| `ancestors NAME [--depth N] [--filter-prefix PREFIX]` | Walks `EXTENDS` upward from a class in one traversal. Accepts short names such as `Build` or FQNs such as `Ci::Build`. `--filter-prefix` keeps only ancestors whose `file_path` starts with the prefix, applied client-side at no extra query cost. |
| `includes BASE PREFIX [--depth N]` | For every descendant of `BASE`, lists the concerns it includes directly from `PREFIX`, in two queries. `--depth` (1 to 3, default 1) applies to the descendant leg only. Transitively inherited concerns are not reported, and a base under `PREFIX` is not reported as its own descendants' concern. |
| `class NAME` | Lists members defined through `DEFINES` edges, with member kind, name, and `file_path:line`. |
| `api PATH_PREFIX` | Lists type-like and callable definitions under the prefix. Use a narrow prefix such as `app/services/merge_requests`, not `app/`. |
| `callers NAME` | Lists definitions that call a method or function through `CALLS` edges. A qualified target such as `MergeRequests::RefreshService#execute` narrows common names. |

## Output format and caveats

Output is plain text with `file_path:line` locators that file-reading tools accept directly.

Known limits of Orbit Remote's Code Graph coverage:

- `EXTENDS` depth is capped at 3 server-side, so large inheritance trees can be incomplete.
- `CALLS` edges are not fully indexed for every language and project.
- Each query requests the maximum result cap (`limit` 1000). A traversal with more than 1000 rows, such as `ApplicationRecord` at depth 2 or more, is truncated. The server applies no stable order before truncation, so the truncated subset differs between runs. Narrow the base or depth, or treat the result as a sample.
- A branch filter is required. The default is `master`; pass `--branch main` for projects that use `main`.

## Budget and anti-patterns

- Start with one targeted subcommand. This helper has no broad `overview` equivalent.
- Keep path prefixes narrow for `api`.
- Do not use it for local uncommitted or branch-local code. Use Orbit Local.
