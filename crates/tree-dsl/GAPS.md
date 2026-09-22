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
- Positional destructuring. A type declares its extraction signature as
  `(__positional ...)`, the ordered header bindings of a Java record. A
  pattern is `(__destructure (__call (__callee T)) slot...)` whose slots are
  bindings or nested destructures. Slot i invokes the member named by
  signature entry i on the named type (JLS 14.30.2); the slot binding carries
  that call's identity, so `x.foo()` later dispatches on the accessor's
  return type. Arity must match; otherwise nothing is emitted.
- Level-order member lookup. Inherited members are searched one supertype
  level at a time with no depth cap; the linker and the resolver share the
  search. Every distinct member found at the shallowest level gets an edge,
  the sound over-approximation call graphs use (class hierarchy analysis).
  Follow-up: pick by the language's rule instead, declared per class (Python
  C3 leftmost, Ruby last include, Scala rightmost trait, Java superclass over
  interface default, Go and Kotlin ambiguity as no edge); prototyped and
  reverted at 2f31390d2.
- Module-object members. An Imports edge whose call site is a member call on
  the import's own local name (`import * as ns; ns.foo()`, `import m as ml;
  ml.f()`) resolves that member in the import target's visible names.
- A Kotlin property read in receiver position is a getter call, so the
  property is tagged callable and its getter body attaches to the property.

## Gaps by count

No skipped tests remain. Assertions that encoded old-pipeline artifacts are
corrected to the language rule and carry a `corrected:` note.
