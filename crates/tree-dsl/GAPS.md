# Engine gaps found while adding languages

This file lists what the tree-dsl engine cannot do yet. Each gap blocks
fixture tests that the old pipeline passed. The rule files in `langs/` are
complete for these cases; the fix belongs in `src/`.

Every skipped fixture test carries a `# skip: tree-dsl: <reason>` comment.
The reason text maps to one row below. A reason that starts with `bogus:`
marks a test that encodes an old-pipeline artifact, not a language rule.

## Terms

- Linker: `src/linker.rs`. One forward pass per file. It builds SSA values
  and intra-file edges.
- Resolver: `src/resolver.rs`. It resolves `__import` nodes to files and
  adds cross-file edges.
- Visible names: the per-file map from a local name to the definition it
  denotes. A file's own definitions and every resolved import land in it.
- Package key: the file-index key `<package>/<name>` for a root definition,
  with the language's FQN separator replaced by `/`.

## Rules the engine applies

These hold for every language. A rule file gets them by emitting the
canonical nodes named here.

- Package scope. A package or namespace declaration emits a hidden wildcard
  import of `<package>/*` under `__module_export`. A source path that ends
  in `/*` globs the package key space, so files that declare the same
  package see each other's root definitions. The exporter hides the import.
- Directory globs. `FileIndex.dirs` maps a directory to its files, so a glob
  costs one lookup.
- Header contracts. A prototype is a `__decl` marker. Take an import
  `M -> H` where M defines a name that H declares. That definition is
  promoted into H's visible names. Every co-includer of H then reaches it.
  Files that do not include H never take part.
- Forward references. The linker pre-declares every root definition before
  the walk.
- Inheritance. `method_up` climbs `__supertype` chains across files through
  each file's visible names. `resolve_inheritance` adds Extends edges and
  implicit-receiver dispatch; `resolve_receivers` resolves member calls on a
  visible class name; `resolve_field_edges` dispatches through typed bindings,
  field or local.
- Wildcard binders. Every `__name` whose local name is `*` binds unbound bare
  calls. A name whose SSA hint is `*` imports every visible name.
- Builtins. A rule per language empties the callee of a predeclared function
  (`(identifier "/^(len|println|...)$/")` -> `(__callee)`), so it binds to
  nothing.
- Markers. `__constructor` and `__import_kind` are canonical kinds.

## Gaps by count

| Skipped tests | Gap | Where |
| --- | --- | --- |
| 7 | Builtin receivers. `LocalDate.now()` under `import java.time.*` should link, `Console.WriteLine` under `using System` should not. Only a builtin table separates them. | `linker.rs` resolve_obj |
| 7 | Bogus tests. Generic stripping, diamond tie-break, companion counts, reopened namespaces, destructuring. | fixtures |
| 6 | Records. Synthesized accessors shadow explicit members; the rules cannot see the body override. | `langs/java.yaml` |
| 5 | References to non-callable definitions. Constant reads produce no edge because `emit` links callable defs only. | `linker.rs` emit |
| 5 | Same-package siblings that also need another gap (typed local through a supertype, nested types, Ruby concern blocks). | `resolver.rs` |
| 4 | Require attribution. Ruby constants map to autoload paths, not to `require` lines. The transforms cannot camel-case a path. | `langs/ruby.yaml`, `dsl/transform.rs` |
| 3 | Extension functions and interface members of imported types resolve only through same-file defs. | `linker.rs` find_method_in |
| 3 | Annotations. A `__decorator` reference binds only to same-file definitions. | `resolver.rs` resolve_one_import |
| 3 | Nested constructors. `Parent.Child.GrandChild()` has no SSA type, and name lookup finds the nearest `GrandChild`. | `linker.rs` |
| 2 | Sealed `permits`. The rules drop the clause. | `langs/java.yaml` |
| 2 | PHP `use function` to a helper file that mirrors neither namespace nor path. | `langs/php.yaml` |
| 2 | Zig anonymous tests display as Function without a synthesized name. | display rules |
| 1 each | Elixir `lib/` layout, flat visible map shadowing a same-file constant, Lua table wrapper count, Kotlin operator tokens, one target per method name, Kotlin path-based import. | see the skip comment |

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
- A relative import from a root-level file needs `regex_replace("^/", "")`
  after the directory join.

## Conventions the rules adopt

- Receiver methods (Go), extension functions (Kotlin), and table functions
  (Lua) become an `__impl` wrapper named after the receiver type.
  Out-of-line C++ members keep the qualified name as one `__function`.
- Constructors carry a `(__constructor)` marker so display rules can show
  `Constructor`.
- Language-specific import kinds (PHP include, Ruby require_relative, C#
  static, Scala grouped) carry `(__import_kind "<kind>")` as a marker.
- Ruby constant references, Ruby superclasses, and PHP fully qualified names
  act as inline imports. PHP import paths are the full FQN so they hit the
  package key. Elixir modules carry an `exports` tag with their short name.
- Typed parameters carry `(__binding $x (__ssa_typed T) (__rhs (__member
  (__object T))))`, a type reference that dispatches member calls on `$x`.

## Not covered

HCL has no rule file. The canonical alphabet has no kind for resource, data
source, output, or local blocks. Its 48 tests do not run.
