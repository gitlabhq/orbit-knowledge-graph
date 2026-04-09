# gkg-fuzz

Fuzz testing for GKG using [Bolero](https://github.com/camshaft/bolero).

## Fuzz targets

| Target | What it exercises |
|---|---|
| `fuzz_compile` | Query compiler (`compile()`) — JSON parsing, ontology validation, SQL generation with a real ontology and security context |
| `fuzz_ruby` | Ruby tree-sitter parser (`RubyAnalyzer::parse_and_analyze`) — tests for panics or crashes on arbitrary input |

Both targets currently use unstructured byte input (`&[u8]` interpreted as UTF-8).

## Running

With mise (recommended):

```sh
mise fuzz:compile   # fuzz the query compiler
mise fuzz:ruby      # fuzz the Ruby parser
```

Or directly with cargo-bolero:

```sh
cargo bolero +nightly test fuzz_compile -p gkg-fuzz
cargo bolero +nightly test fuzz_ruby -p gkg-fuzz
```
If toolchain is already defaulted to nightly, you can ommit the `+nightly`

## Future work

- **Structured input generation**: use Bolero's `TypeGenerator` derive or custom generators to produce valid/semi-valid JSON query structures and source code, getting past early parsing rejection and into deeper logic.
- **Coverage of other parsers**: only Ruby is targeted; the remaining language analyzers could benefit from the same treatment.
- **Crash triage and corpus management**: no CI integration or persistent corpus yet.
- **Property-based testing**: Bolero supports property tests with shrinking — useful for round-trip invariants (e.g. compile then validate output SQL shape).
- **Additional compiler entry points**: fuzzing individual pipeline stages or specific query features (aggregations, filters, traversals) with targeted generators.
- **Scheduled extended fuzzing runs**: CI jobs for longer-running fuzz sessions (e.g. nightly) to catch issues that short local runs miss.
