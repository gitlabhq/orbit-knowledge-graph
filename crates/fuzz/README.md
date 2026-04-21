# gkg-fuzz

Fuzz testing for GKG using [Bolero](https://github.com/camshaft/bolero).

## Fuzz targets

### Query compiler

| Target | What it exercises |
|---|---|
| `fuzz_compile` | Unstructured byte input to `compile()` — tests JSON parsing edge cases |
| `fuzz_compile_structured` | Structured JSON query generation via `FuzzQuery` `TypeGenerator` — generates valid/semi-valid queries that reach deeper compiler logic (normalization, lowering, optimization, security enforcement, codegen) |

### Language parsers

| Target | What it exercises |
|---|---|
| `fuzz_ruby` | Ruby parser (`RubyAnalyzer::parse_and_analyze`) |
| `fuzz_python` | Python parser + analyzer (tree-sitter → FQN/import/reference extraction) |
| `fuzz_typescript` | TypeScript/JavaScript parser (swc) + analyzer, tests both `.ts` and `.js` dialects |
| `fuzz_java` | Java parser + analyzer (tree-sitter → AST extraction) |
| `fuzz_kotlin` | Kotlin parser + analyzer |
| `fuzz_csharp` | C# parser + analyzer |
| `fuzz_rust_parser` | Rust parser + analyzer |

### Indexer messages

| Target | What it exercises |
|---|---|
| `fuzz_indexer_messages` | Deserialization of all indexer NATS message types (`GlobalIndexingRequest`, `NamespaceIndexingRequest`, `CodeIndexingTaskRequest`, `NamespaceDeletionRequest`) |

## Running

With mise (recommended):

```sh
mise fuzz:compile              # fuzz the query compiler (unstructured)
mise fuzz:compile-structured   # fuzz the query compiler (structured)
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
cargo bolero +nightly test <target_name> -p gkg-fuzz
cargo +nightly bolero test <target_name> -p gkg-fuzz

If toolchain is already defaulted to nightly, you can omit the `+nightly`.

## Future work

- **Crash triage and corpus management**: no CI integration or persistent corpus yet.
- **Property-based testing**: Bolero supports property tests with shrinking — useful for round-trip invariants (e.g. compile then validate output SQL shape).
- **Additional compiler entry points**: fuzzing individual pipeline stages or specific query features (aggregations, filters, traversals) with targeted generators.
- **Scheduled extended fuzzing runs**: CI jobs for longer-running fuzz sessions (e.g. nightly) to catch issues that short local runs miss.
