# Orbit Local development quickstart

Build and test the `orbit` CLI and related crates without GDK, NATS, Siphon,
ClickHouse, or PostgreSQL. Many contributions only need the tools on this
page: language parser additions, `orbit-cli` changes, docs, unit
tests, and the code-graph integration tests.

For anything that touches the server pipeline (SDLC indexing, the query
REST API), you need the full setup in
[Local development](local-development.md). The same applies to most
ontology YAML changes: `mise run ontology:validate` catches schema problems
locally, but verifying their end-to-end behavior requires the full stack.

## Prerequisites (5 minutes)

- Git
- [`mise`](https://mise.jdx.dev/) for tool management:

  ```shell
  curl "https://mise.jdx.dev/install.sh" | sh
  ```

## Clone and set up

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise trust && mise install
```

`mise install` supplies the repository's pinned `protoc` compiler for the
code-graph integration tests, along with the other managed tools.

If `mise install` errors on first run (the Rust toolchain post-install step
can fail before the toolchain is fully linked), re-run it once.

## Build the orbit CLI

```shell
mise run build:cli
./target/release/orbit help
```

The first release build compiles every dependency and takes a few minutes
(about 5 minutes on an Apple Silicon laptop). Incremental rebuilds are much
faster.

## Read the agent skill

`orbit skills` lists the agent skills deployed by the selected GitLab instance.
`orbit skills get orbit [path]` validates and caches the instance's whole remote
tree, composes it with local CLI guidance, and prints `SKILL.md` when `path` is
omitted. The cache uses the operating system's user cache directory and keeps
instance origins isolated.

When the glab-provided Orbit API and authentication environment is absent or
incomplete, the command serves the embedded local tree. It makes no network or
credential-helper call. This makes the local guidance available in offline
development builds:

```shell
./target/release/orbit skills
./target/release/orbit skills get orbit references/local/sql.md
```

## Index a repository and run a query

Index the knowledge-graph repository itself as a test target, then query the
resulting DuckDB graph with SQL:

```shell
./target/release/orbit index .

# Count the extracted definitions:
./target/release/orbit sql 'SELECT count(*) FROM gl_definition'

# Find up to three definitions named "main":
./target/release/orbit sql "SELECT name, definition_type, file_path
  FROM gl_definition WHERE name = 'main' LIMIT 3"

# Structured output for scripts:
./target/release/orbit sql -F json 'SELECT path, language FROM gl_file LIMIT 5'
```

The graph is written to `~/.gitlab/orbit/graph.duckdb`. `orbit schema` lists every
table and column in it. Orbit Local is queried with DuckDB SQL only; the JSON
query DSL documented under `docs/source/remote/` applies to Orbit Remote.

## Run tests without infrastructure

```shell
mise run test:fast                    # unit tests (~1900 tests, no Docker)
mise run test:local                   # local integration tests, no Docker
mise run test:integration:codegraph   # code-graph fixture tests (needs protoc)
mise run ontology:validate            # validate ontology YAML changes
mise run lint:code                    # clippy, warnings as errors
mise run lint:docs                    # markdownlint + Vale + lychee
```

`test:fast` runs in a few seconds once the test binaries are compiled; the
first invocation pays the compile cost.

## What you can't test without GDK

- SDLC indexing (requires ClickHouse, NATS, and Siphon)
- The query REST API and authorization paths

The full server integration suite (`mise run test:integration`) also runs
without GDK. It needs Docker (`mise run colima:start` on macOS), not the
GDK stack.

For the rest, follow [Local development](local-development.md).
