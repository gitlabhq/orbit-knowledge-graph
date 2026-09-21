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
  the walk. The `hoisted` tag also pre-declares direct definitions inside a scope.
- Inheritance. `method_up` climbs `__supertype` chains across files through
  each file's visible names. `resolve_inheritance` adds Extends edges and
  implicit-receiver dispatch; `resolve_receivers` resolves member calls on a
  visible class name; `resolve_field_edges` dispatches through typed bindings,
  field or local.
- Wildcard binders. Every `__name` whose local name is `*` binds unbound bare
  calls in its lexical scope. A name whose SSA hint is `*` imports every visible
  name unless it carries a namespace alias.
- Builtins. A rule per language empties the callee of a predeclared function
  (`(identifier "/^(len|println|...)$/")` -> `(__callee)`), so it binds to
  nothing.
- Markers. `__constructor` and `__import_kind` are canonical kinds.
- Call-site type flow. A binding whose value is a call carries that call's
  identity in SSA. A member call on a call receiver, direct or through such a
  binding, emits a TypeFlow edge to the producing call. Every Calls edge
  records its call site. The resolver repeats return-type resolution over
  call sites to a fixpoint. So `a.b().c().d()` resolves at any depth, in one
  file or across files.
- Decorator references. A `__decorator` resolves like a call through the
  file's visible names. Java `permits` clauses emit decorator references.
- Unbound receivers. Take a member call whose receiver is not bound in the
  enclosing definition, not imported by name, and not a visible definition.
  That receiver binds to the file's wildcard imports, the same rule bare
  calls follow.
- Imports tagged `non_shadowing` do not replace an existing local definition.
  The import remains available for file resolution.
- A call can declare its result type with `returns`. That name resolves in the
  caller file during return-type resolution. The call still targets the method.
- Constructors are never nameable, so they do not shadow their class in the
  visible names. Method lookup covers every same-named definition in the
  class's file, so impl wrappers count.

## Gaps by count

| Skipped tests | Gap | Where |
| --- | --- | --- |
| 13 | Bogus tests. Generic stripping, diamond tie-break, companion counts, reopened namespaces, destructuring, the C# Console matrix, Zig synthesized names, Lua wrapper counts, interface Calls counts, packaged inheritance call count. | fixtures |
| 4 | Same-package siblings that also need another gap (embedding, nested types). | `resolver.rs` |
| 4 | Require attribution. Ruby constants map to autoload paths, not to `require` lines. The fallback row is produced outside tree-dsl. | `langs/ruby.yaml`, code-graph hooks |
| 4 | Static receivers under a namespace import that the resolver cannot see (C# partial classes, await, static using). | `resolver.rs` resolve_receivers |
| 1 | Record constructor reference count. Cross-file export merges distinct call sites with the same caller and target. Other active fixtures require this merged count. | `tree-dsl-tests/src/export.rs` |
| 4 | Nested types. `Parent.Child.GrandChild` collapses to its last segment; SSA lookup picks the nearest same-named definition. Needs a member-chain supertype and nested constructor typing. | `langs/java.yaml`, `langs/kotlin.yaml`, `linker.rs` |
| 3 | Extension functions and interface members of imported types resolve only through same-file defs. | `linker.rs` find_method_in |
| 2 | Inherited dispatch through a static factory chain or `new parent()` in PHP. | `resolver.rs` |
| 1 each | Elixir bare call through `import`, flat visible map shadowing a same-file constant, Kotlin operator tokens, Kotlin path-based import, Go `Save` through embedding. | see the skip comment |

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
