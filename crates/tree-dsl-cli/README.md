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

YAML format:

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
    entity: Definition
    expect:
      - fqn: main.foo
        name: foo
        definition_type: Function
  - name: foo is called
    entity: DefinitionToDefinition
    expect:
      - caller: main.foo
        callee: main.foo
        edge_kind: Calls
```

## Architecture

All commands route through the same library functions in `tree-dsl`:

| Command | Library function | Resolver |
|---|---|---|
| `parse` | `tree_dsl::parse(lang_id, path, source)` | No |
| `index` | `tree_dsl::index(lang_id, files)` | Yes |
| `test` | `tree_dsl::index(lang_id, files)` + query/assert | Yes |

Pipeline stages per file:

1. **Parse** -- tree-sitter CST to indextree arena
2. **Rewrite** -- YAML rules transform CST nodes into canonical `__def`, `__call`, `__import`, etc.
3. **Link** -- SSA-based value flow emits Defines/Calls/Imports/Extends edges
4. **Prune** -- remove non-canonical nodes, promote their children
5. **Compact** -- rebuild dense arena for cache-friendly resolution
6. **Resolve** (index/test only) -- parallel cross-file import and call resolution

## Supported languages

Full pipeline (rewrites + linking + resolver): **Python**, **TypeScript**, **JavaScript**, **Rust**

Parsing only (tree-sitter grammar, no rewrites): Bash, C, C++, C#, Elixir, Go, HCL, Haskell, Java, Kotlin, Lua, OCaml, PHP, Ruby, Scala, Swift, Zig
