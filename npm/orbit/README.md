# @gitlab/orbit

The [GitLab Knowledge Graph (Orbit)](https://gitlab.com/gitlab-org/orbit/knowledge-graph)
local CLI. Indexes a repository on your machine into a local DuckDB property
graph and answers code-structure queries (definitions, references, imports,
files, directories) over it.

## Install

```shell
npm install -g @gitlab/orbit
```

This wrapper package selects and installs the prebuilt binary for your
platform via optional dependencies:

- `@gitlab/orbit-darwin-arm64`
- `@gitlab/orbit-darwin-x64`
- `@gitlab/orbit-linux-arm64`
- `@gitlab/orbit-linux-x64`
- `@gitlab/orbit-win32-x64`

Linux binaries are fully static (musl) and run on both glibc and musl
distributions.

## Usage

```shell
orbit index /path/to/your/repo
orbit help
```

See the [Orbit Local documentation](https://docs.gitlab.com/orbit/local/getting-started/)
for access methods (direct CLI, `glab`, MCP) and query examples.
