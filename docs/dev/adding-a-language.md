# Adding a new language to the code indexer

**Audience:** contributors adding a new language to the v2 code indexer in
`gitlab-org/orbit/knowledge-graph`.
**Estimated effort:**

- definitions-only "minimum viable language": 1–2 focused days
- fully-resolved language (calls, imports, cross-file resolution): 1–2 weeks

This guide walks through every file you must touch, the
[`DslLanguage`](../../crates/code-graph/src/v2/dsl/types.rs) trait surface, the
declarative `define_languages!` and `register_v2_pipelines!` macros, the YAML
fixture format, and the common traps. The most-recent worked example is the
[initial C-language MR (`04433eef`)](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/commit/04433eef78da872995e5df7927e88ab436c63416);
mirror its file-by-file structure when in doubt.

## Prerequisites

```shell
# One-time toolchain setup.
mise install                # rustc, cargo, nextest, lefthook, etc.
mise trust                  # in fresh worktrees

# Sanity-check the workspace builds and passes fast tests.
mise build
mise test:fast              # ~30s; excludes Docker-based suites
```

You do **not** need a GDK, ClickHouse, or NATS for code-indexer work. The v2
pipeline tests run entirely in-process against an in-memory graph.

## The v2 indexer at a glance

The code indexer turns source files into a property graph of `Definition`,
`File`, `Directory`, and `ImportedSymbol` nodes connected by `Contains`,
`Defines`, `Imports`, `Calls`, and `Extends` edges. Everything happens inside a
single Rust crate:

```plaintext
crates/
  code-graph/
    src/v2/
      langs/
        generic/      # tree-sitter-backed DSL languages
                      # C, C++, C#, Go, Java, Kotlin, Python, Ruby
        custom/       # bespoke pipelines (JS/TS via OXC, Rust via rust-analyzer)
      registry.rs     # register_v2_pipelines! { ... } — one line per language
      config/lang.rs  # define_languages! { ... } — Language enum + metadata
      pipeline.rs     # GenericPipeline<Dsl, Rules>, FamilyPipeline
      dsl/            # ScopeRule / ReferenceRule / ImportRule builders
      linker/         # ResolutionRules, HasRules, CodeGraph
    treesitter-visit/ # separate sub-crate: SupportLang enum + grammar loading

  integration-tests-codegraph/
    fixtures/<lang>/  # YAML test fixtures, one directory per language
    tests/suites.rs   # yaml_test!(name, "path.yaml") — must register each fixture
    README.md         # fixture format reference
```

For hackathon-scale contributions, you almost always want the **generic DSL
path**. It's how Go is implemented in ~310 lines: declarative scope, reference,
and import rules over a tree-sitter grammar. The custom pipelines exist because
JavaScript and Rust have ecosystem-specific module resolution requirements;
they should not be your starting point.

## What "adding a language" actually delivers

A typical generic-language MR touches the following files (sizes from the C and
C++ MRs):

| File | What changes | Approx LoC |
|---|---|---:|
| `crates/code-graph/treesitter-visit/Cargo.toml` | +1 optional `tree-sitter-<lang>` dep; +1 entry in `builtin-parser` feature list | +2 |
| `crates/code-graph/treesitter-visit/src/languages.rs` | +1 `SupportLang::<Lang>` variant; +1 `LanguageExt::get_ts_language` arm | +8 |
| `crates/code-graph/src/v2/config/lang.rs` | +1 row inside `define_languages! { ... }` | +7 |
| `crates/code-graph/src/v2/langs/generic/<lang>.rs` | New file: `<Lang>Dsl` and `<Lang>Rules` | 240–500 |
| `crates/code-graph/src/v2/langs/generic/mod.rs` | +1 `pub mod <lang>;` | +1 |
| `crates/code-graph/src/v2/registry.rs` | +2 `use` lines; +1 entry in `register_v2_pipelines! { ... }` | +3 |
| `crates/integration-tests-codegraph/fixtures/<lang>/*.yaml` | 3+ new fixtures: definitions, imports, cross-file call | ~150 |
| `crates/integration-tests-codegraph/tests/suites.rs` | **+1 `yaml_test!(...)` line per fixture** (see [traps](#common-traps)) | +N |
| `docs/design-documents/indexing/code_indexing.md` | List the new language under the tree-sitter parser bullet | +2 |
| `code-indexing-benchmark.yaml` (optional) | A representative benchmark repo for the new language | +20 |

**Total:** ~700–1,400 LoC across roughly 12–16 files for a clean addition.

## The `DslLanguage` trait surface

The trait lives at
[`crates/code-graph/src/v2/dsl/types.rs:427`](../../crates/code-graph/src/v2/dsl/types.rs#L427).
Only `name()` and `language()` are required; everything else has a sensible
default. A minimum-viable language also implements `scopes()` — otherwise no
definitions are emitted.

```rust
pub trait DslLanguage: Send + Sync + Default {
    fn name() -> &'static str;                           // required
    fn language() -> crate::v2::config::Language;        // required

    fn scopes()       -> Vec<ScopeRule>      { vec![] }
    fn refs()         -> Vec<ReferenceRule>  { vec![] }
    fn imports()      -> Vec<ImportRule>     { vec![] }
    fn chain_config() -> Option<ChainConfig> { None  }
    fn package_node() -> Option<(&'static str, Extract)> { None }
    fn file_scope()   -> bool                { false }   // since the C MR
    fn hooks()        -> LanguageHooks       { LanguageHooks::default() }
    fn bindings()     -> Vec<BindingRule>    { vec![] }
    fn branches()     -> Vec<BranchRule>     { vec![] }
    fn loops()        -> Vec<LoopRule>       { vec![] }
    fn ssa_config()   -> SsaConfig           { SsaConfig::default() }
    fn spec()         -> LanguageSpec        { /* composes the above */ }
}
```

| Method | Purpose | When you need it |
|---|---|---|
| `name()` | Lowercase string identifier (`"c"`, `"go"`) | always |
| `language()` | Which `Language` variant this DSL implements | always |
| `scopes()` | Grammar node kinds that produce `Definition` nodes | always (otherwise no definitions) |
| `refs()` | Grammar node kinds that produce call or reference edges | recommended |
| `imports()` | Grammar node kinds that produce `ImportedSymbol` nodes | recommended |
| `bindings()` | Local-variable bindings tracked through SSA | only if calls flow through aliases |
| `branches()`, `loops()`, `ssa_config()` | Control-flow boundaries for SSA | optional refinement |
| `hooks()` | Per-language hooks (return-statement kinds, module scoping, import-path resolution) | optional |
| `chain_config()` | How `obj.field.method()` chains are interpreted | needed for member access |
| `package_node()` | Grammar node that names the file's package or namespace | needed for module-style FQN scoping |
| `file_scope()` | Use the filename (without extension) as the root scope | true for languages without module declarations (C, Bash, single-file Lua) |

`scope(...)`, `reference(...)`, `field(...)`, `child_of_kind(...)`, `text(...)`,
`has_descendant(...)`, and `when(...)` are builders defined in
[`crates/code-graph/src/v2/dsl/extractors.rs`](../../crates/code-graph/src/v2/dsl/extractors.rs)
and [`crates/code-graph/src/v2/dsl/types.rs`](../../crates/code-graph/src/v2/dsl/types.rs).
For languages that cannot be expressed by a single node-kind match (Elixir, for
example, where every construct is a `call` node), `scope_fn(...)` accepts a
custom label-picker function instead.

## Background: `LanguageFamily`

`crates/code-graph/src/v2/config/lang.rs` defines:

```rust
pub enum LanguageFamily {
    CFamily,                  // C + C++ share an include-resolution graph
    Jvm,                      // Java + Kotlin share package FQN resolution
    Standalone(Language),     // everything else
}
```

When two or more languages share an FQN space, the indexer can run them as a
single family pipeline against a shared `CodeGraph`. **New languages should be
`Standalone`** — only opt into a family if the new language genuinely shares an
FQN or include space with an already-supported language. The `family()` method
on `Language` is hand-written; you do not need to touch it unless you are
adding a family member.

## Step-by-step: a new generic language

The cleanest reference is the
[C-language MR `04433eef`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/commit/04433eef78da872995e5df7927e88ab436c63416)
(May 3 2026). Read it once end-to-end before you start. The C++ MR
[`05eac493`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/commit/05eac493)
covers the same shape and demonstrates how to opt into a `LanguageFamily`.

### Step 1 — Wire the tree-sitter grammar

In `crates/code-graph/treesitter-visit/Cargo.toml`:

```toml
[dependencies]
tree-sitter-<lang> = { version = "0.X.Y", optional = true }

[features]
builtin-parser = [
  # ...existing list...
  "tree-sitter-<lang>",
]
```

Check crates.io for the **exact constant name** the grammar exports. Most
grammars export `LANGUAGE`, but a few do not — `tree-sitter-php`, for example,
exports `LANGUAGE_PHP` and `LANGUAGE_PHP_ONLY`. Using the wrong constant
produces a confusing compile error.

In `crates/code-graph/treesitter-visit/src/languages.rs`:

```rust
pub enum SupportLang {
    // ...
    Lang,                 // <- add your variant
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

Verify the grammar loads:

```shell
cargo check -p treesitter-visit --features builtin-parser
```

Once it does, use the AST CLI to explore the grammar's actual node kinds —
treat its output as the contract you are coding against:

```shell
cargo run -p treesitter-visit --features cli --bin ast -- examples/hello.lang
```

### Step 2 — Declare the language with `define_languages!`

`crates/code-graph/src/v2/config/lang.rs` is a **declarative table**. The
`Language` enum, file extensions, FQN separator, language names, and the
`SupportLang` mapping all come from one macro invocation. Adding a language is
a single row:

```rust
Lang => {
    support_lang: Lang,
    extensions: ["xx", "yy"],          // file extensions (no leading dot)
    exclude: [],                        // suffix exclusions (Go uses ["_test.go"])
    separator: "::",                    // FQN separator
    names: ["lang", "lang-alias"],      // canonical names for query matching
},
```

Pick `separator` based on how the language constructs FQNs:

- `"."` — module-based (Java, Python, Kotlin, C#, Go)
- `"::"` — namespace-based (C, C++, Ruby, Rust)
- `"\\"` — PHP-style namespaces

`define_languages!` auto-generates `file_extensions()`, `exclude_extensions()`,
`fqn_separator()`, `names()`, `to_support_lang()`, and `parse_ast()`. Nothing
else in `config/lang.rs` needs to change for a `Standalone` language.

### Step 3 — Implement `DslLanguage` and `HasRules`

Create `crates/code-graph/src/v2/langs/generic/<lang>.rs` with this skeleton.
Read [`go.rs`](../../crates/code-graph/src/v2/langs/generic/go.rs) as a
self-contained, ~310-line reference that covers all the major DSL features
without language-specific gymnastics.

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
            // one entry per language construct that creates a definition
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![reference("call_expression").name_from(field("function"))]
    }

    fn imports() -> Vec<ImportRule> {
        vec![
            // Many languages have a single import_declaration node;
            // start there and refine after the first fixture passes.
        ]
    }

    // Opt in only when the language has no module/namespace construct
    // (C, Bash, single-file Lua scripts). Makes the filename the root scope.
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
            ".",   // FQN separator — MUST match the one in define_languages!
            &[],
            None,
        )
    }
}
```

Add the module to `crates/code-graph/src/v2/langs/generic/mod.rs`:

```rust
pub mod lang;
```

### Step 4 — Register the pipeline

`crates/code-graph/src/v2/registry.rs` uses the declarative
`register_v2_pipelines!` macro. Adding a language is two `use` lines plus one
row inside the macro:

```rust
use crate::v2::langs::generic::lang::{LangDsl, LangRules};

register_v2_pipelines! {
    // ...existing entries...
    Lang    => [GenericPipeline<LangDsl, LangRules>],
    // ...
}
```

The macro auto-generates `dispatch_language`, `lang_ctx_for`, and
`dispatch_by_tag`. You do not need to touch any of the generated dispatch
functions.

### Step 5 — Write fixtures

Fixtures live in `crates/integration-tests-codegraph/fixtures/<lang>/*.yaml`.
The harness and assertion vocabulary are documented in
[`crates/integration-tests-codegraph/README.md`](../../crates/integration-tests-codegraph/README.md).

Ship at least three fixtures so each layer of the pipeline is exercised:

1. **`definitions.yaml`** — proves your scope rules fire.
2. **`imports.yaml`** — proves your import rules produce `ImportedSymbol` nodes.
3. **`call_resolution.yaml`** — proves the resolver wires up end-to-end across files.

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

The full assertion vocabulary (`row`, `row_count`, `empty`, `match`, `unique`,
`no_nulls`, `column_values`, `count_equals`, `count_gte`, `where`, `not`) is
documented in the
[fixtures README](../../crates/integration-tests-codegraph/README.md).

### Step 6 — Register each fixture in `tests/suites.rs`

> **This is the single biggest trap.** Fixture files are **not** auto-discovered.
> A YAML file dropped into `fixtures/<lang>/` is silently inert until you
> register it. Forgetting this step is the most common cause of "my fixture
> passes locally but CI thinks nothing tested it."

Add one `yaml_test!` line per fixture in
`crates/integration-tests-codegraph/tests/suites.rs`:

```rust
yaml_test!(lang_definitions,      "lang/definitions.yaml");
yaml_test!(lang_imports,          "lang/imports.yaml");
yaml_test!(lang_call_resolution,  "lang/call_resolution.yaml");
```

The naming convention is `<lang>_<fixture_name>` so the test list stays
greppable. The C-language MR registers two fixtures
([`c_resolution`, `c_hardcore`](../../crates/integration-tests-codegraph/tests/suites.rs))
and is a good template.

### Step 7 — Run the tests locally

```shell
# Just your language's fixtures
cargo nextest run -p integration-tests-codegraph -E 'test(<lang>)'

# Full code-graph integration suite
cargo nextest run -p integration-tests-codegraph

# Workspace lint — must be clean for CI to accept the MR
mise lint:code
mise lint:code:fix              # auto-fix what it can
```

`cargo nextest` parallelises by default. When debugging a single fixture,
`--no-fail-fast --retries 0` gives you the cleanest output.

### Step 8 — Update the design doc

In
[`docs/design-documents/indexing/code_indexing.md`](../design-documents/indexing/code_indexing.md),
find the **Parser architecture** subsection and add your language to the
tree-sitter grammar bullet:

```diff
- - **Python, Kotlin, Java, C#, Go, Ruby, C, and C++** use tree-sitter grammars.
+ - **Python, Kotlin, Java, C#, Go, Ruby, C, C++, and <Lang>** use tree-sitter grammars.
```

If your language has notable dispatch quirks (excluded extensions, multiple
file extensions), mention them in the surrounding paragraph.

### Step 9 — Open the MR

- Use a conventional-commit title: `feat(code-graph): add <Lang> language support`
- Reference the tracking issue with `Closes #NNN` in the description
- CI gates the MR on: `cargo fmt`, clippy with all features, ontology schema,
  agent file sync, markdownlint / Vale / lychee for docs, the unit and
  integration suites, and the code-indexer benchmark

## Minimum-viable-language checklist

Use this as a pre-flight checklist before you open the MR:

- [ ] tree-sitter grammar added to `treesitter-visit/Cargo.toml` + `builtin-parser` feature
- [ ] `SupportLang::<Lang>` variant + dispatch added to `treesitter-visit/src/languages.rs`
- [ ] `Language::<Lang>` row added to `define_languages!` in `code-graph/src/v2/config/lang.rs`
- [ ] `<Lang>Dsl` implements `DslLanguage` (scopes at minimum, refs and imports recommended)
- [ ] `<Lang>Rules` implements `HasRules`
- [ ] `pub mod <lang>;` added to `langs/generic/mod.rs`
- [ ] Pipeline registered in `register_v2_pipelines!` in `registry.rs`
- [ ] At least 3 fixtures: definitions, imports, cross-file call resolution
- [ ] **Each fixture registered with `yaml_test!(...)` in `tests/suites.rs`**
- [ ] `cargo nextest run -p integration-tests-codegraph -E 'test(<lang>)'` is green
- [ ] `mise lint:code` is clean
- [ ] `docs/design-documents/indexing/code_indexing.md` lists the new language
- [ ] MR title is in conventional-commit form

## Common traps

### Fixtures must be registered in `tests/suites.rs`

Already called out above; worth repeating because it is the most-frequently
missed step. Without a `yaml_test!(name, "path.yaml")` line in
`crates/integration-tests-codegraph/tests/suites.rs`, the YAML file is dead
code. `cargo nextest` will not pick it up. CI will pass. The MR will look done.
Nothing was tested.

### FQN-separator mismatch between `define_languages!` and `ResolutionRules::new(...)`

The separator string appears in two places: in the `define_languages!` row
(`separator: "::"`) and as a parameter to `ResolutionRules::new(...)` inside
your `HasRules::rules()` impl. **They must be identical.** If the table says
`"::"` and the rules pass `"."`, FQNs come out malformed and call resolution
silently fails to match anything across files.

### `SupportLang` and `Language` are distinct enums

`SupportLang` (in `treesitter-visit`) is the tree-sitter grammar dispatcher.
`Language` (in `code-graph::v2::config`) is the indexer's language identity.
You add a variant to both. They are connected via the `support_lang:` field in
`define_languages!`, but they cannot be merged — `treesitter-visit` is a
separate sub-crate for compile-time isolation.

### `file_scope()` for module-less languages

Languages with no `package` / `namespace` / `module` declaration (C, Bash,
single-file Lua scripts) should opt into `fn file_scope() -> bool { true }`.
The indexer then uses the filename (without extension) as the root scope, so
`util.c::helper` resolves correctly. Languages that already have explicit
module constructs (PHP, Swift, Elixir) should leave `file_scope()` at its
default of `false`.

### Tree-sitter node names are not what you think

Always verify against the grammar's `grammar.js` (or the AST CLI) before you
write a scope rule. Common surprises:

- Bash function names are `word` nodes, not `identifier`.
- Swift collapses `class`, `struct`, `enum`, and `extension` into a single
  `class_declaration` kind, disambiguated by a child keyword token.
- Lua emits a single `function_declaration` kind for global, local, `M.x`, and
  `obj:method` forms. Distinguish via children, not kind.
- Elixir parses **everything** as a `call` node — `defmodule`, `def`, `alias`,
  `import`, `use` all share the same kind. Use `scope_fn(...)` or
  `when(...)` predicates that inspect the `identifier` child.

### Excluded extensions matter

Go declares `exclude: ["_test.go"]` so test files don't pollute the production
call graph. If your language has a common test-file naming convention, exclude
it. JavaScript excludes `.min.js`. Match the prevailing style.

## When the DSL is not enough

Some languages cannot be expressed declaratively. Symptoms:

- Resolving method calls requires whole-program type inference
  (e.g. plain JavaScript without TypeScript annotations).
- Module resolution needs a build-system manifest (Cargo, npm workspaces,
  Maven).
- The language uses macros or compile-time code generation extensively (Rust).

In those cases the language gets its own pipeline under
`crates/code-graph/src/v2/langs/custom/`. **This is out of scope for a
hackathon contribution.** File a follow-up issue scoped to "improve
\<Lang\> resolution beyond definitions" and ship the definitions-only generic
DSL pass first — it is still a meaningful, mergeable contribution.

## Where to ask for help

- Hackathon parent issue:
  [knowledge-graph#626](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/626)
- Language-support tracking issues: PHP
  [#339](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/339),
  Shell [#340](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/340),
  PowerShell [#341](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/341),
  HCL [#342](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/342),
  SQL [#343](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/343),
  Swift [#344](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/344),
  COBOL [#345](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/345),
  Scala [#751](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/751)
- Worked examples in `git log`:
  [`04433eef`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/commit/04433eef) (C, canonical reference) and
  [`05eac493`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/commit/05eac493) (C++, family-pipeline example)
