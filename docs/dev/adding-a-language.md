# Adding a new language to the code indexer

Canonical worked example:
[C-language MR !1133](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1133).
Mirror its structure when in doubt.

## Prerequisites

```shell
mise install                # rustc, cargo, nextest, lefthook, etc.
mise trust                  # in fresh worktrees
mise build
mise test:fast              # ~30 s; no Docker required
```

No GDK, ClickHouse, or NATS needed. The v2 pipeline tests run in-process
against an in-memory graph.

## Architecture overview

The code indexer produces **Code Graph** nodes (`Definition`, `File`,
`Directory`, `ImportedSymbol`) connected by relationships (`Contains`,
`Defines`, `Imports`, `Calls`, `Extends`):

```plaintext
crates/
  code-graph/
    src/v2/
      langs/
        generic/      # tree-sitter DSL languages (C, C++, C#, Go, Java, Kotlin, Python, Ruby)
        custom/       # JS/TS via OXC, Rust via rust-analyzer
      registry.rs     # register_v2_pipelines! — one line per language
      config/lang.rs  # define_languages! — Language enum + metadata
      pipeline.rs     # GenericPipeline<Dsl, Rules>, FamilyPipeline
      dsl/            # ScopeRule / ReferenceRule / ImportRule builders
      linker/         # ResolutionRules, HasRules, CodeGraph
    treesitter-visit/ # sub-crate: SupportLang enum + grammar loading

  integration-tests-codegraph/
    fixtures/<lang>/  # YAML test fixtures, one directory per language
    tests/suites.rs   # yaml_test!(name, "path.yaml") — must register each fixture
    README.md         # fixture format reference
```

Use the **generic DSL path** for new languages. Go is ~310 lines of declarative
scope, reference, and import rules. Custom pipelines (JS, Rust) exist for
ecosystem-specific module resolution — not a starting point.

## Files touched

Sizes from the C and C++ MRs. All paths relative to `crates/`.

| File | Change | LoC |
|---|---|---:|
| `treesitter-visit/Cargo.toml` | +1 optional `tree-sitter-<lang>` dep; +1 `builtin-parser` feature entry | +2 |
| `treesitter-visit/src/languages.rs` | +1 `SupportLang` variant; +1 `get_ts_language` arm | +8 |
| `code-graph/src/v2/config/lang.rs` | +1 `define_languages!` row | +7 |
| `code-graph/src/v2/langs/generic/<lang>.rs` | `<Lang>Dsl` + `<Lang>Rules` | 240–500 |
| `code-graph/src/v2/langs/generic/mod.rs` | +1 `pub mod` | +1 |
| `code-graph/src/v2/registry.rs` | +2 `use`; +1 `register_v2_pipelines!` row | +3 |
| `integration-tests-codegraph/fixtures/<lang>/*.yaml` | 3+ fixtures | ~150 |
| `integration-tests-codegraph/tests/suites.rs` | **+1 `yaml_test!` per fixture** | +N |
| `docs/design-documents/indexing/code_indexing.md` | Add language to tree-sitter bullet | +2 |

Total: ~700–1,400 LoC across 12–16 files.

## `DslLanguage` trait

[`crates/code-graph/src/v2/dsl/types.rs:427`](../../crates/code-graph/src/v2/dsl/types.rs#L427).
Only `name()` and `language()` are required. Implement `scopes()` too —
without it, no definitions are emitted.

```rust
pub trait DslLanguage: Send + Sync + Default {
    fn name() -> &'static str;                           // required
    fn language() -> crate::v2::config::Language;        // required

    fn scopes()       -> Vec<ScopeRule>      { vec![] }
    fn refs()         -> Vec<ReferenceRule>  { vec![] }
    fn imports()      -> Vec<ImportRule>     { vec![] }
    fn chain_config() -> Option<ChainConfig> { None  }
    fn package_node() -> Option<(&'static str, Extract)> { None }
    fn file_scope()   -> bool                { false }
    fn hooks()        -> LanguageHooks       { LanguageHooks::default() }
    fn bindings()     -> Vec<BindingRule>    { vec![] }
    fn branches()     -> Vec<BranchRule>     { vec![] }
    fn loops()        -> Vec<LoopRule>       { vec![] }
    fn ssa_config()   -> SsaConfig           { SsaConfig::default() }
    fn spec()         -> LanguageSpec        { /* composes the above */ }
}
```

| Method | Purpose | When needed |
|---|---|---|
| `name()` | Lowercase identifier (`"c"`, `"go"`) | always |
| `language()` | `Language` variant | always |
| `scopes()` | Node kinds that produce `Definition` nodes | always |
| `refs()` | Node kinds that produce call/reference edges | recommended |
| `imports()` | Node kinds that produce `ImportedSymbol` nodes | recommended |
| `bindings()` | Local-variable bindings for SSA | if calls flow through aliases |
| `branches()`, `loops()`, `ssa_config()` | Control-flow boundaries for SSA | optional |
| `hooks()` | Return-statement kinds, module scoping, import-path resolution | optional |
| `chain_config()` | How `obj.field.method()` chains resolve | for member access |
| `package_node()` | Node naming the file's package/namespace | for module-style FQN scoping |
| `file_scope()` | Use filename (minus extension) as root scope | for languages without module declarations (C, Bash) |

Builders — `scope(...)`, `reference(...)`, `field(...)`, `child_of_kind(...)`,
`text(...)`, `has_descendant(...)`, `when(...)` — are in
[`extractors.rs`](../../crates/code-graph/src/v2/dsl/extractors.rs) and
[`types.rs`](../../crates/code-graph/src/v2/dsl/types.rs).
For grammars where every construct shares a kind (Elixir: everything is `call`),
use `scope_fn(...)` with a custom label-picker.

## `LanguageFamily`

```rust
pub enum LanguageFamily {
    CFamily,                  // C + C++ share an include-resolution graph
    Jvm,                      // Java + Kotlin share package FQN resolution
    Standalone(Language),     // everything else
}
```

**New languages should be `Standalone`.** Only use a family if the language
shares an FQN or include space with an existing language. See
[C++ MR !1140](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1140)
for a family-pipeline example.

## Step-by-step

Read
[C MR !1133](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1133)
end-to-end before starting.

### Step 1 — Wire the tree-sitter grammar

`crates/code-graph/treesitter-visit/Cargo.toml`:

```toml
[dependencies]
tree-sitter-<lang> = { version = "0.X.Y", optional = true }

[features]
builtin-parser = [
  # ...existing list...
  "tree-sitter-<lang>",
]
```

Check crates.io for the **exact constant** the grammar exports. Most export
`LANGUAGE`, but some differ — `tree-sitter-php` exports `LANGUAGE_PHP`.

`crates/code-graph/treesitter-visit/src/languages.rs`:

```rust
pub enum SupportLang {
    // ...
    Lang,
}

impl LanguageExt for SupportLang {
    fn get_ts_language(&self) -> TSLanguage {
        match self {
            // ...
            #[cfg(feature = "tree-sitter-<lang>")]
            Self::Lang => tree_sitter_<lang>::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-<lang>"))]
            Self::Lang => panic!("tree-sitter-<lang> feature not enabled"),
        }
    }
}
```

Verify:

```shell
cargo check -p treesitter-visit --features builtin-parser
```

Then explore the grammar's node kinds with the AST CLI:

```shell
cargo run -p treesitter-visit --features cli --bin ast -- examples/hello.lang
```

### Step 2 — Declare the language

Add one row to `define_languages!` in `crates/code-graph/src/v2/config/lang.rs`:

```rust
Lang => {
    support_lang: Lang,
    extensions: ["xx", "yy"],          // no leading dot
    exclude: [],                        // Go uses ["_test.go"], JS uses [".min.js"]
    separator: "::",                    // FQN separator
    names: ["lang", "lang-alias"],      // canonical names for query matching
},
```

FQN separator conventions:

- `"."` — module-based (Java, Python, Kotlin, C#, Go)
- `"::"` — namespace-based (C, C++, Ruby, Rust)
- `"\\"` — PHP

The macro generates `file_extensions()`, `exclude_extensions()`,
`fqn_separator()`, `names()`, `to_support_lang()`, and `parse_ast()`.

### Step 3 — Implement `DslLanguage` and `HasRules`

Create `crates/code-graph/src/v2/langs/generic/<lang>.rs`. Use
[`go.rs`](../../crates/code-graph/src/v2/langs/generic/go.rs) (~310 lines)
as reference.

```rust
use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::extract::{field, text, Extract};
use treesitter_visit::predicate::*;

use crate::v2::linker::rules::{ImportStrategy, ReceiverMode, ResolveStage};
use crate::v2::linker::{HasRules, ResolutionRules};

#[derive(Default)]
pub struct LangDsl;

impl DslLanguage for LangDsl {
    fn name() -> &'static str { "lang" }
    fn language() -> Language { Language::Lang }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("function_definition", "Function").def_kind(DefKind::Function),
            scope("class_definition", "Class").def_kind(DefKind::Class),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![reference("call_expression").name_from(field("function"))]
    }

    fn imports() -> Vec<ImportRule> {
        vec![
            // start with a single import_declaration node;
            // refine after the first fixture passes
        ]
    }

    fn file_scope() -> bool { false }
}

pub struct LangRules;

impl HasRules for LangRules {
    fn rules() -> ResolutionRules {
        let spec = LangDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);
        ResolutionRules::new(
            "lang",
            scopes,
            spec,
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![ImportStrategy::ExplicitImport, ImportStrategy::SameFile],
            ReceiverMode::None,
            ".",   // FQN separator — MUST match define_languages!
            &[],
            None,
        )
    }
}
```

Add to `crates/code-graph/src/v2/langs/generic/mod.rs`:

```rust
pub mod lang;
```

### Step 4 — Register the pipeline

`crates/code-graph/src/v2/registry.rs`:

```rust
use crate::v2::langs::generic::lang::{LangDsl, LangRules};

register_v2_pipelines! {
    // ...existing entries...
    Lang    => [GenericPipeline<LangDsl, LangRules>],
}
```

The macro generates `dispatch_language`, `lang_ctx_for`, and
`dispatch_by_tag`.

### Step 5 — Write fixtures

`crates/integration-tests-codegraph/fixtures/<lang>/*.yaml`. Format
documented in
[`integration-tests-codegraph/README.md`](../../crates/integration-tests-codegraph/README.md).

Write at least three covering these areas (names are suggestions — existing
languages use varied names like `resolution.yaml`, `simple_call.yaml`):

1. **Definitions** — scope rules produce `Definition` nodes.
1. **Imports** — import rules produce `ImportedSymbol` nodes.
1. **Cross-file call resolution** — resolver wires calls across files.

#### Example: `definitions.yaml`

```yaml
name: "<Lang>: basic definitions"
fixtures:
  - path: main.lang
    content: |
      function hello() {}
      class Greeter {}

tests:
  - name: "Function and class definitions are extracted"
    query: |
      MATCH (d:Definition)
      WHERE d.name IN ['hello', 'Greeter']
      RETURN d.name AS name, d.definition_type AS kind
      ORDER BY name
    assert:
      - { row: { name: "Greeter", kind: "Class" } }
      - { row: { name: "hello", kind: "Function" } }
```

#### Example: `call_resolution.yaml`

```yaml
name: "<Lang>: cross-file call resolution"
fixtures:
  - path: main.lang
    content: |
      import { helper } from "./util.lang";
      function run() { helper(); }
  - path: util.lang
    content: |
      export function helper() {}

tests:
  - name: "run() calls helper()"
    query: |
      MATCH (a:Definition)-[r:DefinitionToDefinition]->(b:Definition)
      WHERE a.name = 'run' AND r.edge_kind = 'Calls'
      RETURN b.fqn AS target
    assert:
      - { row: { target: "util.helper" } }
```

Full assertion vocabulary (`row`, `row_count`, `empty`, `match`, `unique`,
`no_nulls`, `column_values`, `count_equals`, `count_gte`, `where`, `not`):
[fixtures README](../../crates/integration-tests-codegraph/README.md).

### Step 6 — Register fixtures in `tests/suites.rs`

> **Fixtures are not auto-discovered.** A YAML file in `fixtures/<lang>/`
> does nothing until registered here. This is the most commonly missed step.

`crates/integration-tests-codegraph/tests/suites.rs`:

```rust
yaml_test!(lang_definitions,      "lang/definitions.yaml");
yaml_test!(lang_imports,          "lang/imports.yaml");
yaml_test!(lang_call_resolution,  "lang/call_resolution.yaml");
```

Convention: `<lang>_<fixture_name>`.

### Step 7 — Run tests

```shell
cargo nextest run -p integration-tests-codegraph -E 'test(<lang>)'  # just yours
cargo nextest run -p integration-tests-codegraph                     # full suite
mise lint:code                                                       # must pass for CI
mise lint:code:fix                                                   # auto-fix
```

For debugging: `--no-fail-fast --retries 0`.

### Step 8 — Update the design doc

In
[`docs/design-documents/indexing/code_indexing.md`](../design-documents/indexing/code_indexing.md),
add the language to the tree-sitter bullet under **Parser architecture**:

```diff
- - **Python, Kotlin, Java, C#, Go, Ruby, C, and C++** use tree-sitter grammars.
+ - **Python, Kotlin, Java, C#, Go, Ruby, C, C++, and <Lang>** use tree-sitter grammars.
```

### Step 9 — Open the MR

- Title: `feat(code-graph): add <Lang> language support`
- Body: `Closes #NNN`
- CI checks: `cargo fmt`, clippy (all features), ontology schema, agent file
  sync, markdownlint/Vale/lychee, unit + integration tests

## Pre-flight checklist

- [ ] tree-sitter grammar in `treesitter-visit/Cargo.toml` + `builtin-parser` feature
- [ ] `SupportLang` variant + dispatch in `treesitter-visit/src/languages.rs`
- [ ] `Language` row in `define_languages!`
- [ ] `<Lang>Dsl` implements `DslLanguage` (scopes + refs + imports)
- [ ] `<Lang>Rules` implements `HasRules`
- [ ] `pub mod <lang>;` in `langs/generic/mod.rs`
- [ ] Pipeline registered in `register_v2_pipelines!`
- [ ] 3+ fixtures (definitions, imports, call resolution)
- [ ] **Each fixture registered in `tests/suites.rs`**
- [ ] `cargo nextest run -p integration-tests-codegraph -E 'test(<lang>)'` green
- [ ] `mise lint:code` clean
- [ ] `code_indexing.md` updated
- [ ] MR title in conventional-commit form

## Common traps

### FQN-separator mismatch

The separator appears in both `define_languages!` (`separator: "::"`) and
`ResolutionRules::new(...)`. **They must match.** A mismatch silently breaks
cross-file call resolution.

### `SupportLang` vs `Language`

Two separate enums. `SupportLang` (in `treesitter-visit`) dispatches the
tree-sitter grammar. `Language` (in `code-graph::v2::config`) is the indexer's
identity. Add a variant to both. Connected via `support_lang:` in
`define_languages!`.

### `file_scope()` for module-less languages

Languages without `package`/`namespace`/`module` declarations (C, Bash)
should return `true`. The filename (minus extension) becomes the root scope,
so `util.c::helper` resolves correctly.

### Tree-sitter node names

Always verify with the AST CLI or `grammar.js`. Surprises:

- Bash: function names are `word`, not `identifier`
- Swift: `class`, `struct`, `enum`, `extension` all share `class_declaration`
- Lua: one `function_declaration` for global, local, `M.x`, and `obj:method`
- Elixir: everything is `call` — `defmodule`, `def`, `alias`, `import`, `use`

Use `scope_fn(...)` or `when(...)` predicates for disambiguation.

### Excluded extensions

Go excludes `["_test.go"]`, JS excludes `[".min.js"]`. If the language has a
test-file convention, exclude it.

## When the DSL is not enough

Signs a language needs a custom pipeline:

- Method calls require whole-program type inference
- Module resolution needs a build-system manifest (Cargo, npm, Maven)
- Extensive macros or code generation (Rust)

Custom pipelines live under `crates/code-graph/src/v2/langs/custom/`. Out of
scope for a hackathon contribution — ship the generic DSL pass first and file
a follow-up issue.

## Links

- Hackathon parent issue:
  [#626](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/626)
- Language tracking issues: PHP
  [#339](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/339),
  Shell [#340](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/340),
  PowerShell [#341](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/341),
  HCL [#342](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/342),
  SQL [#343](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/343),
  Swift [#344](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/344),
  COBOL [#345](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/345),
  Scala [#751](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/751)
- Worked examples:
  [C MR !1133](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1133) (canonical reference),
  [C++ MR !1140](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1140) (family-pipeline example)
