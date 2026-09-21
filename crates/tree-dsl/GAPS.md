# Engine gaps found while adding languages

This file lists what the tree-dsl engine cannot do yet. Each gap blocks
fixture tests that the old pipeline passed. The rule files in `langs/` are
complete for these cases; the fix belongs in `src/`.

Every skipped fixture test carries a `# skip: tree-dsl: <reason>` comment.
The reason text maps to one row below.

## Terms

- Linker: `src/linker.rs`. One forward pass per file. It builds SSA values
  and intra-file edges.
- Resolver: `src/resolver.rs`. It resolves `__import` nodes to files and
  adds cross-file edges.
- Visible names: the per-file map from a local name to the definition it
  denotes. A file's own definitions and every resolved import land in it.

## Closed gaps

These rules now hold in the engine. They are language-neutral; a new rule
file gets them for free.

- Package keys. `build_file_index` indexes every root definition under
  `<package>/<name>`, where `__package` is split on the language's FQN
  separator. An import whose `__source_path` is an FQN resolves like a path.
- Directory globs. An import whose submodule key ends in `/*` resolves to
  every file whose parent directory is the target (`resolve_glob`).
- Cross-file Extends. `resolve_inheritance` adds an Extends edge from a
  definition to each `__supertype` found in the file's visible names and
  defined in another file.
- Inherited dispatch. For each such parent, an implicit-receiver call
  (`__callee (__ivar)`) that has no same-file method resolves to the parent's
  method.
- Many wildcard binders. Every `__name` whose local name (alias, SSA hint,
  or name) is `*` binds unbound bare calls; a file is no longer limited to one.
- Typed-field dispatch. `resolve_field_edges` accepts Interface and Trait
  targets and compares the binding's callee through the visible names, so an
  aliased import matches.
- Canonical markers `__constructor` and `__import_kind` replace the
  `(__alias "<init>")` and `(__visibility "<kind>")` workarounds.

## Gaps by count

| Skipped tests | Gap | Where |
| --- | --- | --- |
| 88 | Package or namespace scope. Same-package siblings without an import never link, and neither do their Extends or inherited-dispatch edges. Go, Java, PHP, Kotlin rely on this. | `resolver.rs` gather_imports_for is import-driven |
| 25 | Include and header pairing. A quoted `#include` resolves to the header. Headers hold no definitions. There is no header-to-source mapping. | `resolver.rs`, `paths.rs` |
| 19 | Path-based import resolution. PHP `use function`, deep namespaces without composer.json, Elixir `lib/` layouts, and Kotlin types in a differently named file do not map to a path. | `resolver.rs`, `paths.rs` |
| 11 | Chained receivers. `f().g()` has no SSA value for the receiver. | `linker.rs` classify_rhs_value, callee_shape |
| 10 | Exotic C and C++ declarators. The rules do not unwrap function-pointer returns, parenthesized declarators, attributes, MSVC modifiers, or conversion operators. | `langs/c.yaml`, `langs/cpp.yaml` |
| 9 | Builtins. A member call on an unbound receiver does not bind to wildcard imports, and the C# and Java matrices disagree on whether it should. A builtin table would settle it. | `linker.rs` resolve_obj |
| 7 | Extension functions and interface members of imported types resolve only through same-file defs. | `linker.rs` find_method_in |
| 5 | References to non-callable definitions. Constant reads produce no edge because `emit` links callable defs only. | `linker.rs` emit |
| 4 | Require attribution. Ruby constants map to autoload paths, not to `require` lines. The transforms cannot camel-case a path. | `langs/ruby.yaml`, `dsl/transform.rs` |
| 3 | Enum scope. `enclosing_class` accepts Class, Impl, and Trait. `self::` and `$this` inside enums do not resolve. | `linker.rs` enclosing_class |
| 3 | Forward references. A call to a name defined later in the file does not resolve in the single pass. Python has the same limit. | `linker.rs` run |
| 2 | Bare builtins. Unbound bare calls bind to the wildcard import. Python has the same limit. | `linker.rs` resolve_name |
| 2 | Multiple targets. `find_method_in` returns the first match; diamond and interface declarations return one edge. | `linker.rs`, `tree/walk.rs` |
| 2 | Nested constructors. `Parent.Child.GrandChild()` has no SSA type, and name lookup finds the nearest `GrandChild`. | `linker.rs` |
| 1 | Flat visible map. An autoload import for a same-file constant shadows the local definition. | `resolver.rs` |
| 8 | Vocabulary and structure counts. Records, test blocks, namespaces reopened in one file, companion members, table wrappers, and synthesized accessors change row counts. | display rules |

## DSL limits

- A rule cannot see the parent of a node. Package-level and local `var`
  declarations get the same rule. Go, Kotlin, and Swift emit a `__def` and an
  SSA `__binding` for both.
- Anonymous tokens with punctuation kinds (`+`, `-`) cannot be matched, so
  Kotlin operator overloading is not rewritten to `plus()`.
- A `tag:` on a `replace:` rule with a literal value does not survive; the
  rules use a structural marker child instead (`(__import_kind "static")`).
- Variadic captures cannot be joined into one string. Scala import paths use
  the node text with `regex_replace`.
- `stale caps`: a failed rule can leave captures from an earlier rule in the
  same stage. Scala import rules live in separate stages for this reason.

## Conventions the rules adopt

- Receiver methods (Go), extension functions (Kotlin), out-of-line methods
  (C++), and table functions (Lua) become an `__impl` wrapper named after the
  receiver type. `find_method_in` reaches them through same-name defs.
- Constructors carry a `(__constructor)` marker so display rules can show
  `Constructor`.
- Language-specific import kinds (PHP include, Ruby require_relative, C#
  static, Scala grouped) carry `(__import_kind "<kind>")` as a marker.
- Ruby constant references, Ruby superclasses, and PHP fully qualified names
  act as inline imports whose path follows the language's file convention.
- Typed parameters carry `(__rhs (__member (__object T)))`, a type reference
  that yields an Imports edge when `T` is imported.

## Not covered

HCL has no rule file. The canonical alphabet has no kind for resource, data
source, output, or local blocks. Its 48 tests do not run.
