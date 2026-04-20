# Code Graph Integration Tests

YAML-driven integration tests for the v2 code-graph pipeline. Each fixture defines source files, runs them through the full pipeline (parse, build graph, resolve), and asserts on Cypher query results.

## Running

```bash
cargo nextest run -p integration-tests-codegraph
```

## Fixture structure

```yaml
name: Test suite name
pipeline: generic              # optional: "generic" (default) or a named pipeline like "js" / "ruby_prism"
fixtures:
  - path: main.py
    content: |
      from utils import helper
      def run():
          helper()
  - path: utils.py
    content: |
      def helper():
          pass

tests:
  - name: Test name
    severity: error            # "error" (default) or "warning"
    skip: false                # skip this test
    query: |
      MATCH (caller:Definition)-[:DefinitionToDefinition]->(callee:Definition)
      WHERE caller.name = 'run'
      RETURN caller.fqn AS caller, callee.fqn AS callee
    assert:
      - { row_count: 1 }
      - { row: { caller: "main.run", callee: "utils.helper" } }
```

A test can also use `queries` for multiple query blocks:

```yaml
  - name: Multi-query test
    queries:
      - query: |
          MATCH (d:Definition) RETURN count(d) AS n
        assert:
          - { count_equals: { field: n, value: 3 } }
      - query: |
          MATCH (f:File) RETURN count(f) AS n
        assert:
          - { count_equals: { field: n, value: 2 } }
```

## Assertion reference

Every assertion is a YAML map. Optional `where` pre-filters rows. Optional `not` inverts the check.

### Row checks

```yaml
- { row: { name: "Foo", fqn: "app.Foo" } }         # at least one row matches all columns
- { not: true, row: { name: "Foo" } }               # no row matches
```

### Row count

```yaml
- { row_count: 3 }                                  # exact row count
- { empty: true }                                    # 0 rows
- { empty: false }                                   # >0 rows
```

### Glob pattern matching

```yaml
- { match: { field: fqn, pattern: "com.example.*" } }       # every row's field matches
- { not: true, match: { field: file, pattern: "main.py" } }  # no row's field matches
```

### Column integrity

```yaml
- { unique: fqn }                                    # no duplicate values in column
- { no_nulls: fqn }                                  # no NULL values in column
- { column_values: { field: name, values: ["a", "b", "c"] } }  # exact set of distinct values
```

### Aggregate checks

```yaml
- { count_equals: { field: n, value: 5 } }           # first row's field == value
- { count_gte: { field: n, value: 3 } }              # first row's field >= value
```

### `where` filter

Pre-filters the result to rows matching all specified column=value pairs before evaluating the assertion. Composes with every assertion type.

```yaml
- { where: { kind: "Defines" }, row_count: 3 }
- { where: { kind: "Calls" }, row: { file: "main.py", def: "UserService" } }
- { where: { kind: "Defines" }, not: true, match: { field: file, pattern: "main.py" } }
```

### `not` negation

Inverts any assertion. The check must fail for the assertion to pass.

```yaml
- { not: true, row: { name: "Foo" } }               # row must NOT exist
- { not: true, match: { field: f, pattern: "*.py" } } # no row may match the glob
```

## Fixture directories

```
fixtures/
  containment.yaml          # definition nesting (class > method > inner class)
  structural.yaml           # file/directory structure, edge kinds, imports
  java_resolution.yaml      # Java same-class and cross-file call resolution
  kotlin_resolution.yaml    # Kotlin call resolution and package scoping
  python_resolution.yaml    # Python cross-file import + call resolution
  java/                     # additional Java-specific test suites
  kotlin/                   # additional Kotlin-specific test suites
  python/                   # additional Python-specific test suites
  javascript/               # JS-v2 custom pipeline fixtures (use pipeline: js)
  typescript/               # TS-v2 custom pipeline fixtures (use pipeline: js when mixing .ts/.js/.vue)
  vue/                      # Vue SFC fixtures exercised through the JS-v2 pipeline
  examples/                 # example/reference fixtures (e.g. ruby_custom_pipeline)
```

## Adding a test

1. Create or edit a YAML file in `fixtures/`
2. Define source files under `fixtures`
3. Write Cypher queries and assertions under `tests`
4. Run `cargo nextest run -p integration-tests-codegraph` to verify
