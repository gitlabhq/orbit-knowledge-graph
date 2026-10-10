# orbit-fuzz

Fuzz testing for Orbit with [Bolero](https://github.com/camshaft/bolero).

## Fuzz targets

### Query compiler

| Target | What it exercises |
|---|---|
| `fuzz_compile` | Unstructured byte input to `compile()` — tests JSON parsing edge cases |
| `fuzz_compile_structured` | Structured JSON query generation via `FuzzQuery` `TypeGenerator` — generates valid/semi-valid queries that reach deeper compiler logic (normalization, lowering, optimization, security enforcement, codegen) |

### Orbit query frontend

| Target | What it exercises |
|---|---|
| `fuzz_gql` | Unstructured text into `compile()` with the Orbit query frontend; every failure must be client-safe |
| `fuzz_gql_grammar` | Derivations of `query.pest` itself (`grammar::Grammar`), so the text is in the language by construction; the syntax tree must consume it or reject it with a lowering error, never a syntax error or pipeline invariant |

`grammar::Grammar` parses the grammar with `pest_meta` and uses input bytes to choose productions while walking the rule AST. The real parser decides whether a derivation is faithful: `gql::pair_outline` must return the same rule sequence the walk produced, which discards derivations that PEG ordered choice or greedy repetition would read differently. Leaf overrides supply nonempty identifiers.

### Language parsers

| Target | What it exercises |
|---|---|
| `fuzz_ruby` | Ruby parser + DSL extraction (`RubyDsl` spec, `parse_full_collect`) |
| `fuzz_python` | Python parser + DSL extraction (`PythonDsl` spec, `parse_full_collect`) |
| `fuzz_typescript` | TypeScript and JavaScript tree-sitter parsers (`Language::parse_ast`) |
| `fuzz_java` | Java parser + DSL extraction (`JavaDsl` spec, `parse_full_collect`) |
| `fuzz_kotlin` | Kotlin parser + DSL extraction (`KotlinDsl` spec, `parse_full_collect`) |
| `fuzz_csharp` | C# parser + DSL extraction (`CSharpDsl` spec, `parse_full_collect`) |
| `fuzz_rust_parser` | Rust tree-sitter parser (`Language::parse_ast`) |

### Indexer messages

| Target | What it exercises |
|---|---|
| `fuzz_indexer_messages` | Deserialization of all indexer NATS message types (`GlobalIndexingRequest`, `NamespaceIndexingRequest`, `CodeIndexingTaskRequest`, `NamespaceDeletionRequest`) |

## Running

With mise (recommended):

```sh
mise fuzz:compile              # fuzz the query compiler (unstructured)
mise fuzz:compile-structured   # fuzz the query compiler (structured)
mise fuzz:gql                  # fuzz the Orbit query frontend (unstructured)
mise fuzz:gql-grammar          # fuzz the Orbit query frontend (grammar derivations)
mise fuzz:ruby                 # fuzz the Ruby parser
mise fuzz:python               # fuzz the Python parser
mise fuzz:typescript           # fuzz the TypeScript/JS parser
mise fuzz:java                 # fuzz the Java parser
mise fuzz:kotlin               # fuzz the Kotlin parser
mise fuzz:csharp               # fuzz the C# parser
mise fuzz:rust-parser          # fuzz the Rust parser
mise fuzz:indexer-messages     # fuzz indexer message deserialization
```

Or directly with cargo-bolero:

```sh
cargo +nightly bolero test <target_name> -p orbit-fuzz
```

If your default toolchain is nightly, you can omit `+nightly`.

CI does not run these targets. See [Known gaps](../../docs/design-documents/testing.md#known-gaps).
