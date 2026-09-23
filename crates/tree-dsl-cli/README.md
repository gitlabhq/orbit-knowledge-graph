# tree-dsl-cli

Code indexing CLI for tree-dsl. Parses source files into a canonical tree with definitions, imports, call edges, and cross-file resolution.

## Build

```
cargo build -p tree-dsl-cli
```

The binary is `tree-dsl`.

## Commands

### `parse` -- inspect a single file

Parse one file through the tree-dsl pipeline and print the annotation stream. No cross-file resolution.

```
tree-dsl parse main.py
tree-dsl parse main.ts --stage cst     # raw tree-sitter CST
tree-dsl parse main.ts --stage ast     # after rewrites, before linking
tree-dsl parse main.ts --stage ssa     # full pipeline with edges (default)
tree-dsl parse --stdin --lang python   # read from stdin
```

Stages:
- **cst** -- raw tree-sitter parse (before any rewrites)
- **ast** -- after canonical rewrites, before SSA linking
- **ssa** -- full pipeline: rewrites + SSA linking + prune (default)

Example:

```
$ tree-dsl parse example.py --stage ssa
   1  __def                          [0-66]
   2  __defname                      sym="User" [6-10]
   3  __class                        [0-66]
   5  __def                          [16-66]
   6  __defname                      sym="__init__" [20-28]
   7  __function                     [16-66]
   8  __self_method                  [16-66]
  15  __binding                      sym="name" [50-66]
  16  __ivar                         sym="name" [50-59]
edges:
  ?1 --[Defines]--> ?5
```

Each line: node index, kind (`__` prefix for canonical kinds), field name if present, interned symbol text, byte span.

### `rewrite` -- test pattern rules

Apply match/replace patterns to a file. Useful for developing new rewrite rules.

```
tree-dsl rewrite main.py --match '(return_statement $$$V)' --replace '(__ssa_return $$$V)'
tree-dsl rewrite main.py --after all --match '...' --replace '...'
```

### `index` -- index a file or directory

Run the full pipeline including cross-file import resolution.

```
tree-dsl index src/
tree-dsl index main.py
tree-dsl index src/ --lang python
tree-dsl index src/ --no-save        # skip saving the serialized graph
```

Example:

```
$ tree-dsl index project/

--- stats ---
files:        2
definitions:  6
imports:      1
intra edges:  7
cross edges:  1
parse:        0.01s
resolve:      0.00s
total:        0.01s
saved:        ~/.orbit/var/graphs/project.bin (0.1 MB, 0.00s)
```

### `test` -- run YAML test suites

Run integration tests defined in YAML.

```
tree-dsl test fixtures/python/simple_call.yaml
tree-dsl test --inline '<yaml>'
```

Suites use the shared fixture format (Cypher queries against the exported
graph); the full vocabulary is in the
[fixtures README](../integration-tests-codegraph/README.md).

```yaml
name: "example test"
pipeline: python
fixtures:
  - path: main.py
    content: |
      def foo():
          pass
      foo()
tests:
  - name: foo is defined
    query: |
      MATCH (d:Definition) WHERE d.fqn = 'main.foo'
      RETURN d.name AS name, d.definition_type AS type
    assert:
      - { row: { name: foo, type: Function } }
  - name: foo is called from the file
    query: |
      MATCH (f:File)-[:CALLS]->(d:Definition) WHERE d.fqn = 'main.foo'
      RETURN f.path AS path
    assert:
      - { row: { path: main.py } }
```

## Architecture

Every command composes phases from `tree_dsl::pipeline`. A `Pipeline<T>`
carries one artifact; `then(phase)` swaps it for the next, and the artifact's
type decides which phases may follow.

| Command | Pipeline |
|---|---|
| `parse --stage cst` | `Each(Parse)` |
| `parse --stage ast` | `Each(Parse.pipe(Rewrite))` |
| `parse --stage ssa` | `pipeline::index` |
| `parse --stage display` | `pipeline::index` then `Display` |
| `index` | `pipeline::index` |
| `test` | `pipeline::index` then `Display`, `Export`; `pipeline::reindex` per step |

`pipeline::index` is `Prepare`, then `Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link))`,
then `Insert`, then `Resolve`. The per-file steps run fused, one file at a
time per worker, so a file's source and raw tree are gone before the next
file starts. `pipeline::reindex` swaps `Prepare` for `Remap` and runs the
same tail over the changed files only.

## Supported languages

Full pipeline (rewrites + linking + resolver): **Python**, **TypeScript**, **JavaScript**, **Rust**, **Go**, **PHP**, **Java**, **Kotlin**, **Ruby**, **C#**, **C**, **C++**, **Scala**, **Bash**, **Elixir**, **Lua**, **Swift**, **Zig**

Parsing only (tree-sitter grammar, no rewrites): HCL, Haskell, OCaml. See `../tree-dsl/GAPS.md` for engine gaps the rule files hit.
