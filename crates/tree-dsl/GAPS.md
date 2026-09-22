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
  name unless it carries a namespace alias. A bare call name that no enclosing
  class declares reads only the wildcard imports tagged `callable`: a C# static
  using or a Java static import supplies callees, a type-only wildcard does not.
- Extensions. A member visible from the caller whose `__impl` wrapper names the
  receiver's class takes part in member lookup, so Kotlin, Swift, and Rust
  extensions resolve in the caller's scope.
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
  file's visible names, or binds to the wildcard imports when unbound. Java
  `permits` clauses emit decorator references.
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
- Supertypes are SSA reads. The linker resolves each `__supertype` at the
  parent block before it enters the class and records the definition, so
  method lookup and Extends edges follow identities, not names.
- Qualified names are member chains. A supertype, a type annotation, or a
  callee may carry `(__member Inner (__object Outer))` at any depth. The
  linker and the resolver walk the chain by definition identity.
- Member lookup respects nesting. `find_method_in` does not enter a nested
  class-like definition unless it is an `__impl` block or carries
  `(__companion)`. Nested class-like definitions are not file-level names; a
  class wins over a same-named non-class in the visible names.
- Branch results meet on the supertype graph. A binding whose value is an
  `if`, `when`, or `try` is a phi of its arms; each arm writes its tail
  expression's value in its own sealed block. A consumer with several reaching
  producers dispatches on the unique least common supertype of their classes;
  a `throw` or `null` arm is bottom and does not take part; without a unique
  meet it emits nothing.
- Module-object members. An Imports edge whose call site is a member call on
  the import's own local name (`import * as ns; ns.foo()`, `import m as ml;
  ml.f()`) resolves that member in the import target's visible names.
- A Kotlin property read in receiver position is a getter call, so the
  property is tagged callable and its getter body attaches to the property.

## Gaps by count

Three skipped tests remain. Assertions that encoded old-pipeline artifacts
are corrected to the language rule and carry a `corrected:` note.

| Skipped tests | Gap | Where |
| --- | --- | --- |
| 1 | Invalid Go. A diamond embedding makes the selector ambiguous, so the language defines no call. | `fixtures/go/multiple_embedding.yaml` |
| 2 | Java record pattern destructuring (`Point(int x, int y)`). The i-th component invokes the record's i-th accessor; positional destructuring has no canonical shape yet. | `langs/java.yaml`, `linker.rs` |

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
- A bare call `N()` in Java, C#, Scala, and Ruby is `(__call (__callee N
  (__implicit)))`. The linker looks up the name in order: a member of the
  enclosing class, then a lexical binding or named import, then a wildcard
  import tagged `callable` such as `import static C.*` or `using static T`. A
  type wildcard supplies no callees (JLS 15.12.1). Every edge carries the site.
- A predeclared identifier such as Go `len` or Kotlin `println` is `(__callee N
  (__predeclared))`: a lexical definition shadows it; otherwise it binds to
  nothing, never to a wildcard import.
- A static import is a member wildcard `(__name "*")` with `(__import_kind
  "static")`, tagged `callable` in the tag-defs stage.
- Java records declare `equals`, `hashCode`, and `toString` (JLS 8.10.3)
  through `(record_component "<name>")` markers; an explicit method of the
  same name suppresses the implicit one. The `equals` guard is name-only, so
  an `equals(Point)` overload also suppresses it.
- Ruby constant references are Zeitwerk-style autoload imports; the exporter
  shows them as `Autoload`. Tests on constant reads follow the import chain.
- Kotlin type paths of four or more segments stay unresolved: the CST stores
  a flat list and the DSL has no list-to-chain reduction.
- An extension found through the caller's visible names resolves only when
  the name is unambiguous in that file; two extensions with one name on
  different receivers resolve neither.

## Base-branch behavior the reviews flagged

These predate this branch and are unchanged here. Each is a search or a cap
where the language defines a scope rule.

- `lookup` retries the root block after an empty local result, so a local
  bound to an unknown value does not shadow a root definition.
- A missing method retries the receiver as the callee.
- Five Calls writes bypass `emit` and its callable gate.
- `ivar_type` takes the first assignment to a field; writes do not join.
- Every root definition is pre-declared without `hoisted`.
- SSA alias reads stop after eight hops; `method_up` stops at depth eight and
  takes the first successful ancestor.
- The flat visible map keeps one definition per name; `visible_from` takes the
  first same-named definition in the corpus; header promotion matches by name.
- A failed import path retries `path/name`; a missing export retries a
  submodule derived from the file name.
- Directory names select source roots in the rule files; a project
  configuration is the right home.
- The `returns` reserved tag has no rule-file user.

## Not covered

HCL has no rule file. The canonical alphabet has no kind for resource, data
source, output, or local blocks. Its 48 tests do not run.
