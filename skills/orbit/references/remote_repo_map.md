# Orbit remote repo map reference

`remote_repo_map.py` is the remote counterpart to the local
[`local_repo_map.py`](local_repo_map.md) helper. It maps source-code structure
for any project indexed in Orbit Remote by shelling out to
`glab orbit remote query`, so it does not require a local checkout or an Orbit
Local index.

Use it when the user asks to inspect code shape in a GitLab project that is
available in Orbit Remote: inheritance trees, ancestor chains, class members,
directory API surfaces, or call sites.

## When to use it

Use the remote repo map when you need to:

- Trace descendants of a base class, trait, or interface through `EXTENDS`.
- Walk the parent chain for a class with `ancestors` (optionally filtered to a
  path prefix, e.g. only included concerns).
- List, for every descendant of a base, the concerns it includes from a path
  prefix with `includes`.
- Inspect a class/module/member surface with `class`.
- Map types and callables under a path prefix with `api`.
- Find callers of a method or function with `callers`.

Prefer the local [`local_repo_map.py`](local_repo_map.md) helper when you are already in a
checkout and need branch-local or unmerged code. Prefer paste-ready raw Orbit
Remote JSON from [`recipes.md`](recipes.md) when the question is a single known
entity lookup or a cross-domain SDLC query.

## Prerequisites

- `glab` >= v1.94.0 authenticated against GitLab.com.
- The `knowledge_graph` feature flag must be enabled for the user.
- The target project and branch must be indexed in Orbit Remote.
- The token must have access to the target project.

## Invocation

From the Orbit skill root (the directory containing `SKILL.md`), the script is
at:

```text
./scripts/remote_repo_map.py
```

Resolve that path relative to the skill root, not the user's current repository.
When running from another directory, either `cd` to the skill root first or use
the absolute path to the loaded skill directory.

The default target is `gitlab-org/gitlab` (`project_id = 278964`) on `master`:

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
```

Override the project or branch with global flags before the subcommand:

```bash
python3 ./scripts/remote_repo_map.py --project-id 77960826 --branch main api crates/orbit-local
```

## Subcommands

### `extends NAME [--depth N]`

Walks the `EXTENDS` graph from a base type down to descendants. Depth is capped
at 3 because Orbit Remote caps traversal depth server-side. The helper issues a
single server-side multi-hop traversal (`min_hops`/`max_hops`) regardless of
depth, and prints the definition type, FQN, and `file_path:line` for each
descendant, ordered by file path. The result set is the same regardless of how
the depth is reached, so results are not labelled per hop (the Orbit response
does not expose a reliable per-node hop count for variable-length traversals).

### `ancestors NAME [--depth N] [--filter-prefix PREFIX]`

Walks the `EXTENDS` graph upward from a class to parents and ancestors. This is
useful when you know a concrete class and need its inheritance chain. The helper
accepts short names such as `Build` and FQNs such as `Ci::Build`; FQNs are
resolved with the `fqn` filter. Like `extends`, it issues a single multi-hop
traversal.

Pass `--filter-prefix` to keep only ancestors whose `file_path` starts with the
prefix — for example `--filter-prefix app/models/concerns` to list only the
concerns a class includes. The filter is applied client-side to the single
traversal result, so it costs no extra query.

### `includes BASE PREFIX [--depth N]`

For every descendant of `BASE`, lists the concerns it directly includes from
`PREFIX`, as a per-descendant breakdown. This answers "which concerns does each
`Noteable` model mix in?" — replacing the old "one `extends` call followed by an
`ancestors` call per descendant" pattern with exactly two queries: one to find
the descendants of `BASE`, one to map those descendants to the concerns under
`PREFIX` that they extend. `BASE` accepts a short name or an FQN.

The descendant traversal honours `--depth` (1-3, default 1); the
descendant→concern leg is a single hop, so only directly-included (not
transitively-inherited) concerns are reported. A base that itself lives under
`PREFIX` is not reported as a concern of its own descendants.

### `class NAME`

Finds a class/module/type and lists members defined through `DEFINES` edges.
Output includes member kind, name, and `file_path:line` locators.

### `api PATH_PREFIX`

Lists type-like and callable definitions whose `file_path` starts with the
prefix. Use a narrow prefix, such as `app/services/merge_requests`, rather than
broad roots like `app/`.

### `callers NAME`

Finds definitions that call a method or function through `CALLS` edges. The
argument can be a bare name such as `execute` or a qualified target such as
`MergeRequests::RefreshService#execute` to narrow common method names.

## Output format and caveats

Output is plain text with `file_path:line` locators that can be passed directly
to file-reading tools.

Orbit Remote's Code Graph coverage is not exhaustive. Treat results as graph
coverage, not as an authoritative source of truth. Known limitations:

- `EXTENDS` depth is capped at 3 server-side, and large inheritance trees can be
  incomplete.
- `CALLS` edges are not fully indexed for every language/project combination.
- `extends` and `ancestors` issue a single server-side multi-hop traversal;
  `includes` issues two. None of them fan out into per-hop round trips. Each
  query requests the maximum result cap (`limit` 1000). A traversal whose result
  genuinely exceeds 1000 rows (a very wide base such as `ApplicationRecord` at
  depth ≥ 2) is truncated, and because the server applies no stable ordering
  before truncation, the truncated subset is non-deterministic between runs.
  Narrow the base or depth for such roots, or treat the result as a sample.
- A branch filter is required. The default is `master`; pass `--branch main` for
  projects that use `main`.

## Budget and anti-patterns

- Start with one targeted subcommand; this helper does not have a broad
  `overview` equivalent.
- Keep path prefixes narrow for `api`.
- Do not use this for local uncommitted or branch-local code; use Orbit Local.
- Do not present graph-only inheritance or caller counts as complete; flag them
  as graph coverage.
