# Orbit Local development quickstart

Build and test the `orbit` CLI and related crates without GDK, NATS, Siphon,
ClickHouse, or PostgreSQL. Many contributions only need the tools on this
page: language parser additions, `orbit-local` CLI changes, docs, unit
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

- `protoc` (the protobuf compiler), only if you plan to run the code-graph
  integration tests. It is not managed by `mise`:

  ```shell
  # macOS
  brew install protobuf

  # Debian/Ubuntu
  sudo apt-get install -y protobuf-compiler

  # Fedora
  sudo dnf install -y protobuf-compiler

  # Windows
  winget install protobuf
  ```

## Clone and set up

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise trust && mise install
```

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

The graph is written to `~/.orbit/graph.duckdb`. `orbit schema` lists every
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
without GDK — it needs Docker (`mise run colima:start` on macOS), not the
GDK stack.

For the rest, follow [Local development](local-development.md).
