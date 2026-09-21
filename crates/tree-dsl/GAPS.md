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
- Wildcard slot: the SSA variable named `*`. A file has one.

## Gaps by count

| Skipped tests | Gap | Where |
| --- | --- | --- |
| 43 | Cross-file Extends. Only the linker emits Extends, from same-file defs. | `linker.rs` handle_def, resolver has no Extends |
| 55 | Package or namespace scope. Same-package siblings without an import never link. Go, Java, PHP, C# rely on this. | `resolver.rs` gather_imports_for is import-driven |
| 28 | Inherited dispatch across files. `find_method_in` follows supertypes only among same-file defs. Traits, mixins, and parent classes in other files do not resolve. | `linker.rs` find_method_in, `resolver.rs` resolve_one_import |
| 25 | Include and header pairing. A quoted `#include` resolves to the header. Headers hold no definitions. There is no header-to-source mapping. | `resolver.rs`, `paths.rs` |
| 21 | Wildcard imports of directories. `import a.b.*` and `using Ns;` name a directory. `resolve_path` resolves files and index files only. Member calls on unresolved names do not fall back to the wildcard. | `paths.rs` resolve_path, `linker.rs` resolve_obj |
| 17 | Path-based import resolution. PHP `use function`, deep namespaces without composer.json, and Kotlin types that live in a differently named file do not map to a path. | `resolver.rs`, `paths.rs` |
| 11 | Chained receivers. `f().g()` has no SSA value for the receiver. | `linker.rs` classify_rhs_value, callee_shape |
| 10 | Exotic C and C++ declarators. The rules do not unwrap function-pointer returns, parenthesized declarators, attributes, MSVC modifiers, or conversion operators. | `langs/c.yaml`, `langs/cpp.yaml` |
| 5 | References to non-callable definitions. Constant reads produce no edge because `emit` links callable defs only. | `linker.rs` emit |
| 4 | Interface-typed properties. `resolve_field_edges` accepts Class and Struct targets only. | `resolver.rs` resolve_field_edges |
| 3 | Enum scope. `enclosing_class` accepts Class, Impl, and Trait. `self::` and `$this` inside enums do not resolve. | `linker.rs` enclosing_class |
| 3 | Forward references. A call to a name defined later in the file does not resolve in the single pass. Python has the same limit. | `linker.rs` run |
| 2 | Aliased type names. `resolve_field_edges` compares the raw type text with the definition name. | `resolver.rs` resolve_field_edges |
| 2 | Builtins. Unresolved bare calls fall back to the wildcard import. Python has the same limit. | `linker.rs` resolve_name |
| 2 | Multiple targets. `find_method_in` returns the first match; diamond and interface declarations return one edge. | `linker.rs`, `tree/walk.rs` |
| 4 | Require attribution. Ruby constants map to autoload paths, not to `require` lines. The transforms cannot camel-case a path. | `langs/ruby.yaml`, `dsl/transform.rs` |
| 8 | Vocabulary and structure counts. Records, singleton methods, namespaces reopened in one file, companion members, and synthesized accessors change row counts. | display rules |

## DSL limits

- A rule cannot see the parent of a node. Package-level and local `var`
  declarations get the same rule. Go, Kotlin, and Swift emit a `__def` and an
  SSA `__binding` for both.
- Anonymous tokens with punctuation kinds (`+`, `-`) cannot be matched, so
  Kotlin operator overloading is not rewritten to `plus()`.
- A `tag:` on a `replace:` rule with a literal value does not survive; the
  rules use a structural marker child instead (`(__visibility "static")`).
- Variadic captures cannot be joined into one string. Scala import paths use
  the node text with `regex_replace`.
- `stale caps`: a failed rule can leave captures from an earlier rule in the
  same stage. Scala import rules live in separate stages for this reason.

## Conventions the rules adopt

- Receiver methods (Go), extension functions (Kotlin), out-of-line methods
  (C++), and table functions (Lua) become an `__impl` wrapper named after the
  receiver type. `find_method_in` reaches them through same-name defs.
- Constructors carry `(__alias "<init>")` so display rules can show
  `Constructor` without a new canonical kind.
- Language-specific import kinds (PHP include, Ruby require_relative, C#
  static, Scala grouped) carry `(__visibility "<kind>")` as a marker.
- Ruby constant references and PHP fully qualified names act as inline
  imports whose path follows the language's file convention.
- Typed parameters carry `(__rhs (__member (__object T)))`, a type reference
  that yields an Imports edge when `T` is imported.

## Not covered

HCL has no rule file. The canonical alphabet has no kind for resource, data
source, output, or local blocks. Its 48 tests do not run.
