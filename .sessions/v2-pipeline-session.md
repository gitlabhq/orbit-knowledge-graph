# V2 Pipeline Session Summary

## Branch: `feat/v2-cli-wiring`

## What we did

### 1. Consolidated language config (one file per language)

**Before:** Two files per language in two directories:
- `parser-core/src/v2/langs/python.rs` — `PythonDsl` (DSL spec)
- `code-graph/src/v2/lang_rules/python.rs` — `PythonRules` (resolution rules)

Scopes and references were duplicated between them.

**After:** One file per language in `code-graph/src/v2/langs/`:
- `PythonDsl` + `PythonRules` + tests together
- Scopes derived from `language_spec` via `derive_scopes()` (maps `DefKind.is_type_container()` → `is_type_scope: bool`)
- References removed from `ResolutionRules` (walker uses `language_spec.refs`)
- `parser-core/src/v2/` and `code-graph/src/v2/lang_rules/` deleted

### 2. Structural cleanup

- Killed dual binding system: `ParseBindingRule` + `CanonicalBinding` removed. Bindings handled only by the walker's `BindingRule` + SSA.
- Merged `resolve_import` and `resolve_import_to_defs` into one function.
- Added `ReceiverExtract::resolve()` method, deduplicated chain extraction in engine.rs.
- Replaced `ScopeKind` enum (3 variants, 2 behaviors) with `is_type_scope: bool`.
- Added `SsaResolver::seal_remaining()` safety net.
- Removed dead code: `ImportIndex`, `ImportRef`, `by_file`, empty `resolvers/mod.rs`.
- Removed `auto_scopes`/`auto_refs`/`auto_imports` from DSL (no language used them).

### 3. Streaming pipeline architecture

**Before:**
```
parse ALL → store all ASTs → build context (with ASTs) → walk ALL → resolve ALL → graph
```

**After:**
```
parallel per-file: parse → walk → drop AST
sequential: build indexes → parallel resolve → graph
```

Key changes:
- Per-file SSA (`FileWalkResult` owns its `SsaResolver`)
- ASTs dropped after walking, never stored in `ResolutionContext`
- `ResolutionContext` lost its generic `A` type parameter (no AST storage)
- `GenericPipeline<P, R>` where `R: HasRules` (was `R: ReferenceResolver<A>`)
- Pipeline calls `build_edges()` directly instead of going through trait indirection

Memory profile:
- Old: ALL ASTs in memory simultaneously (could be 100MB+ for large repos)
- New: At most N ASTs (rayon worker count) at any time, each dropped after walking

### 4. Resolver restructure

Split monolithic `reaching.rs` (615 lines, 12 free functions with 5-7 parameters each) into:
- `edge_builder.rs`: `Resolver` struct holding `ctx/ssa/rules/sep`, methods for `resolve_bare`, `resolve_chain`, `value_to_types` (shared primitive), `walk_step`, `compound_key_step`. `chain_fallback` reuses `resolve_bare`.
- `imports.rs`: Self-contained import strategy functions with single `apply()` entry point.

### 5. CLI wiring

- Added `--v2` flag to `orbit index` command
- `index_repo_v2` runs `Pipeline::run()`, converts `CodeGraph` → Arrow → DuckDB via `convert_v2_graph`
- Same DuckDB tables as v1, same `orbit query` works on output
- Fixed Arrow column names to match ontology (`import_path`, `identifier_name`, `relationship_kind`, etc.)

### 6. Observability

- indicatif progress bars for parse+walk and resolution phases
- Per-language stats: file count, definitions, references, imports, errors
- Thread count printed at start
- Red `[SLOW]` warning when any single resolve takes >100ms

---

## Performance optimizations

### Allocation reduction

| What | Before | After |
|---|---|---|
| SSA variable names | `String` key per write (heap alloc every time) | `Intern<str>` (one alloc per unique name) |
| `Value::Type` | `String` (24 bytes, clone = heap alloc) | `Intern<str>` (8 bytes, clone = pointer copy) |
| Block predecessors | `Vec<BlockId>` (always heap) | `SmallVec<[BlockId; 2]>` (stack for ≤2) |
| Phi operands | `Vec<Value>` (always heap) | `SmallVec<[Value; 2]>` (stack for ≤2) |
| ReachingDefs | `Vec<Value>` | `SmallVec<[Value; 2]>` |
| Phi trivial check | `operands.clone()` (full Vec) | Iterate by index, clone one Value |
| SsaResolver init | Empty vecs | Pre-sized: 32 blocks, 8 phis, 64 vars |
| Resolver FQN construction | `format!()` per lookup | Reusable `String` buffer |
| Fqn.to_string() | Join parts every call | Cached as `IStr` at construction |
| Chain type threading | `Vec<String>` per step | `SmallVec<[IStr; 2]>` (zero-cost clone) |

### Algorithmic fixes

| What | Before | After |
|---|---|---|
| `build_containment_edges` | O(D²) linear scan for parent | O(D) with FQN→index hashmap |
| `lookup_member` | O(members) linear scan | O(1) nested hashmap |
| `lookup_member_with_supers` | Returns `Vec<DefRef>` (alloc per call) | Writes into caller's `&mut Vec<DefRef>` |
| Member+super lookups | No caching, repeated BFS | `RwLock<FxHashMap>` cache on `MemberIndex` |
| Chain depth | Unbounded (30+ steps for builders) | Capped at 10 steps |
| Chain type accumulation | Exponential duplicates | Dedup with `FxHashSet` per step |
| Unresolved reads | Try all 6 import strategies, find nothing | Early exit if name not in `DefinitionIndex` |
| Type name resolution | Bare names resolved at resolution time (linear import scan) | FQNs resolved at parse time (O(1) hashmap) |
| Resolution phase | Sequential (single-threaded) | Parallel across files (rayon `par_iter_mut`) |

### Elasticsearch benchmark (22,935 Java files)

| Metric | Before optimizations | After |
|---|---|---|
| Parse + walk | ~2.9s | ~2.85s |
| Index build | ~0.26s | ~0.26s |
| Resolution | ~69s (sequential) | ~6.4s (parallel + caches + early exit) |
| Total | ~72s | ~10.8s |
| Edges | 3,025,870 (with false positives) | 2,647,724 (correct) |
| Stalls | 3.5s per builder chain | Zero >100ms |

---

## Architecture (current state)

```
pipeline.rs: GenericPipeline<P, R>
│
├── PARALLEL (rayon): for each file
│   ├── P::parse_file() → (CanonicalResult, AST)
│   ├── walk_file() → FileWalkResult { ssa, reads }
│   └── drop AST
│
├── SEQUENTIAL: collect results
│   └── ResolutionContext::build(results) → indexes
│       ├── DefinitionIndex (by_fqn, by_name, fqns reverse map)
│       ├── MemberIndex (class→member→defs, supers, RwLock cache)
│       └── FileScopes (per-file interval trees)
│
├── PARALLEL (rayon): build_edges()
│   └── for each file's walks:
│       Resolver { ctx, ssa, rules, buf }
│       ├── resolve_chain() — walk ExpressionStep chain
│       │   ├── resolve_base() → types via SSA
│       │   ├── walk_step() → member lookup (cached)
│       │   └── compound_key_step() → Python self.db fallback
│       └── resolve_bare() — SSA → import strategies → implicit this
│
└── SEQUENTIAL: GraphBuilder → CodeGraph (petgraph)
    ├── structural nodes/edges
    └── + resolved call edges
```

### File layout

```
linker/src/v2/
├── ssa.rs           — Braun et al. SSA (Intern<str> keys, SmallVec)
├── walker.rs        — AST walker driving SSA per file
├── edge_builder.rs  — Resolver struct + parallel edge building
├── imports.rs       — Import strategy functions
├── context.rs       — DefinitionIndex, MemberIndex (with cache), FileScopes
├── rules.rs         — ResolutionRules, IsolatedScopeRule, BranchRule, etc.
├── builder.rs       — GraphBuilder (structural edges, O(D) containment)
├── graph.rs         — CodeGraph (petgraph, AsRecordBatch impls)
├── edges.rs         — ResolvedEdge, EdgeSource
├── resolver.rs      — ReferenceResolver trait, NoResolver, GlobalBacktracker
└── mod.rs           — re-exports

code-graph/src/v2/
├── langs/
│   ├── python.rs    — PythonDsl + PythonRules + tests
│   ├── java.rs      — JavaDsl + JavaRules + tests
│   ├── kotlin.rs    — KotlinDsl + KotlinRules + tests
│   └── csharp.rs    — CSharpDsl + tests (no resolver yet)
├── pipeline.rs      — GenericPipeline, Pipeline::run, progress bars
├── custom/          — custom pipeline support (e.g. Ruby/Prism)
└── mod.rs
```

---

## Determinism

Fully deterministic results. Same repo → same set of nodes, edges, definitions, imports.

- SSA is deterministic (Braun et al.)
- Parse-time import resolution is deterministic (first matching import wins)
- All caches are pure function caches (same input → same output)
- Parallel resolution: edge *set* is deterministic, edge *order* in the output vec is non-deterministic (rayon thread scheduling). This is fine — edge order is irrelevant for the graph database.

---

## Remaining work

### High priority

| Item | Notes |
|---|---|
| Wire v2 into CI | Add `--v2` to benchmark scenario in `code-indexing-benchmark.yaml`. Binary already supports the flag. |
| `convert_v2_graph` edge iteration | Currently works but uses `graph.edge_indices()` — could use a more direct method. |
| Stats JSON output | `index_repo_v2` returns a minimal `RepositoryIndexingResult` with zeros for graph stats. Should populate from `PipelineResult.stats`. |
| Remove debug `eprintln!` | The `[v2]` prefixed timing lines are useful for dev but should be behind a verbose flag for production. |
| Remove `[SLOW]` warning | Or make it configurable. Useful for profiling, noisy for users. |

### Medium priority

| Item | Notes |
|---|---|
| `self.db.query()` instance attr SSA across sibling methods | Block predecessor wiring needs debugging for Python. |
| `ImportStrategy::FilePath` for Python relative imports | Path manipulation + file tree lookup, currently returns empty. |
| C# resolution rules | Currently `NoRules` (parse-only, no SSA/resolution). |
| TypeScript/Ruby/Rust/Go v2 support | Currently skipped with "not supported" message. |
| Interprocedural SSA | `$return` binding rule + cross-function SSA reads. Compatible with per-file SSA architecture. |

### Low priority

| Item | Notes |
|---|---|
| Walker dispatch table | `FxHashMap<&str, NodeAction>` for O(1) node kind matching. Currently 4 linear scans per node (~5 items each). Measured as negligible vs resolution. |
| Kotlin extension function receivers | Needs `receiver_type` on function defs. |
| Generic/type parameter resolution | Type argument extraction + substitution. |
| Stdlib type sets | Per-language `FxHashSet<&str>` of known stdlib types for instant skip. Currently handled by "name not in DefinitionIndex" early exit. |
| Third-party dependency resolution | Stub indexing from JAR/wheel/gem type signatures. Big project. |

---

## Commits on this branch

| Commit | What |
|---|---|
| `74252734` | Consolidate lang config (one file per language) |
| `41731dce` | Kill dual binding system, merge imports, dead code |
| `c7dccf1c` | Split reaching.rs → edge_builder.rs + imports.rs |
| `b4a2f2fd` | Remove auto_scopes/auto_refs/auto_imports |
| `4f4fc244` | Per-file SSA, drop ASTs, streaming pipeline |
| `7a34acd2` | Wire v2 into CLI with --v2 flag |
| `7eb6e67b` | Add gitlab-xtasks as mise tool, bench:v2 task |
| `abe5056f` | Add indicatif progress bars |
| `c9718c38` | Fix Arrow column names, add [SLOW] warnings |
| `103ff409` | Intern SSA vars, SmallVec blocks/phis, pre-size |
| `89db83b7` | Cache Fqn.to_string() as IStr |
| `4a023058` | O(1) member lookup, IStr type threading |
| `27f1992b` | Fix O(D²) containment edges |
| `5f147382` | Cache member+super lookups |
| `a218be30` | Limit chain depth to 10 |
| `efb732b8` | Dedup chain types, resolve bare types locally |
| `1001f025` | Parallelize resolution, early exit for unknown names |
| `d5f670f9` | Parse-time FQN resolution via imports |
| `2c5ab752` | O(1) import map in parser |
