# tree-dsl-cli

Code indexing CLI for tree-dsl. Parses source files into a tagged property graph with definitions, imports, references, and cross-file call edges.

## Build

```
cargo build -p tree-dsl-cli
```

The binary is `tree-dsl`.

## Commands

### `parse` -- inspect a single file

Parse one file through the tree-dsl pipeline and print the annotation stream. No cross-file resolution. Useful for debugging rewrites, colorings, and tag assignment.

```
tree-dsl parse main.py
tree-dsl parse main.ts --stage cst     # raw tree-sitter CST
tree-dsl parse main.ts --stage ast     # after rewrites, before coloring
tree-dsl parse main.ts --stage tagged  # after coloring (default)
tree-dsl parse --stdin --lang python   # read from stdin
```

Stages:
- **cst** -- raw tree-sitter parse (before any rewrites)
- **ast** -- after canonical rewrites (K_MEMBER, K_CALL, K_IVAR, __supertype), before coloring
- **tagged** -- after coloring, with tags (def/import/ref/binding/branch/loop/scope)
- **ssa** -- same as tagged (SSA is part of the pipeline, not a separate stage)

Example:

```
$ tree-dsl parse example.py
   0  module                         tag=scope sym="example.py" [0-106]
   1  class_definition               tag=def sym="class User:..." [0-66]
   3  identifier                     field=name sym="User" [6-10]
   6  function_definition            tag=def sym="def __init__..." [16-66]
   8  identifier                     field=name sym="__init__" [20-28]
  18  assignment                     tag=binding sym="self.name = name" [50-66]
  19  ____ivar                       field=left tag=ref sym="name" [50-59]
  22  function_definition            tag=def sym="def greet..." [68-105]
  33  ____member                     tag=ref sym="user.name" [96-105]
  34  identifier                     field=object sym="user" [96-100]
  36  identifier                     field=member sym="name" [101-105]
edges:
  User[1] --[Defines]--> __init__[6]
```

Each line: node index, kind (prefixed `__` for synthetic/canonical kinds), field name, tag, interned symbol text, byte span.

### `index` -- index a file or directory

Run the full pipeline including cross-file import resolution. Prints per-file summaries to stdout and stats to stderr.

```
tree-dsl index src/
tree-dsl index main.py
tree-dsl index src/ --lang python   # override language detection
```

Example:

```
$ tree-dsl index project/
models.py: 4 defs, 0 imports, 2 refs, 4 edges
services.py: 2 defs, 1 imports, 1 refs, 3 edges

cross-file edges: 1
  services.py:create_user[13] --> models.py:User[1]

--- stats ---
files:        2
definitions:  6
imports:      1
refs:         3
intra edges:  7
cross edges:  1
time:         0.01s
```

### `test` -- run YAML test suites

Run integration tests defined in YAML. Same format as the test fixtures in `crates/tree-dsl-tests/fixtures/`.

```
tree-dsl test fixtures/python/simple_call.yaml
tree-dsl test --inline '<yaml>'
```

YAML format:

```yaml
name: "example test"
pipeline: python                      # optional: python, typescript, js, rust
fixtures:
  - path: main.py
    content: |
      def foo():
          pass
      foo()
tests:
  - name: foo is defined
    query: |
      MATCH (d:Definition)
      WHERE d.name = 'foo'
      RETURN d.fqn AS fqn
    assert:
      - { row_count: 1 }
      - { row: { fqn: "main.foo" } }
```

Example:

```
$ tree-dsl test --inline '
name: quick check
fixtures:
  - path: main.py
    content: |
      def hello(): pass
tests:
  - name: hello exists
    query: "MATCH (d:Definition) WHERE d.name = '\''hello'\'' RETURN d.name AS n"
    assert:
      - { row_count: 1 }
'
---
suite: "quick check"
tests: 1
passed: 1
failed: 0
skipped: 0
```

## Architecture

All three commands route through the same library functions in `tree-dsl`:

| Command | Library function | Resolver |
|---|---|---|
| `parse` | `tree_dsl::parse(lang_id, path, source)` | No |
| `index` | `tree_dsl::index(lang_id, files)` | Yes |
| `test` | `tree_dsl::index(lang_id, files)` + query/assert | Yes |

The pipeline stages per file:

1. **Parse** -- tree-sitter CST (`bridge::parse`)
2. **Rewrite** -- canonical vocabulary (K_MEMBER, K_CALL, K_IVAR, __supertype, __decorator)
3. **Color** -- tag nodes as def/import/ref/binding/branch/loop/scope
4. **SSA fold** -- reaching definitions, E_CALLS/E_DEFINES/E_IMPORTS edges
5. **Post-SSA** -- return type propagation, callable dispatch, MRO inheritance
6. **Resolve** (index/test only) -- cross-file import resolution via the resolver DSL

## Supported languages

Full pipeline (rewrites + colorings + resolver): **Python**

Rewrites + colorings (no resolver yet): **TypeScript**, **JavaScript**, **Rust**

Parsing only (tree-sitter grammar, no colorings): Bash, C, C++, C#, Elixir, Go, HCL, Java, Kotlin, Lua, PHP, Ruby, Scala, Swift, Zig
