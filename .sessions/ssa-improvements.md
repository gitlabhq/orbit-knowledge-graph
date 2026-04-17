# SSA/Resolution Improvement Plan

Audit of v2 code-graph resolution capabilities: what works, what doesn't, and what to build next.

## What works today

| Pattern | Mechanism | Languages |
|---------|-----------|-----------|
| Bare function calls (`foo()`) | `ref_dispatch` matches call nodes | All |
| Method chains (`a.b().c()`) | `build_expression_chain` → ExpressionStep chain | All |
| `self`/`this`/`super` calls | `SsaConfig.self_names` → `SsaValue::Type(scope_fqn)` | Python, Ruby, Java, Kotlin |
| Constructor calls (`new Foo()`) | `ChainConfig.constructor` → `ExpressionStep::New` | Java, Kotlin |
| Explicit import resolution | `ImportStrategy::ExplicitImport` | Python, Java, Kotlin, Go |
| Wildcard imports (`import pkg.*`) | `ImportStrategy::WildcardImport` | Java, Kotlin only |
| Same-package implicit resolution | `ImportStrategy::SamePackage` | Java, Kotlin |
| Implicit member calls (no `this.`) | `ImplicitMember` stage walks enclosing scopes | Java, Kotlin, Ruby |
| SSA branch flow (if/else → phi) | `walk_full_branch` fork/join with phi merge | All |
| SSA loop flow | `walk_full_loop` header/body/exit blocks | All |
| Variable reassignment | Last-write-wins within a block | All |
| Type annotation flow | `extract_type_annotation` → `SsaValue::Type` | Java, Kotlin |
| Return type propagation | `metadata.return_type` read during chain resolution | When annotations exist |
| Inheritance traversal | BFS on `Extends` edges via `lookup_nested_with_hierarchy` | All |
| Copy propagation (`b = a`) | `SsaValue::Alias` resolved at read time | All |

## Gaps, ranked by impact

### 1. Instance attributes across methods

```python
class Repo:
    def __init__(self):
        self.db = Database()     # writes to __init__'s SSA block
    def query(self):
        self.db.execute()        # reads from query's block — nothing there
```

**Status:** `skip: true` in test fixtures. Most common Python/Ruby pattern.

**Why it fails:** SSA blocks are per-scope. `__init__` and `query` are sibling blocks with the class block as predecessor. `self.db = Database()` writes to `__init__`'s block. `query`'s block inherits from the class block, not from `__init__`. The SSA read for `self` resolves to `Type(class_fqn)`, then the resolver looks up `db` as a member of the class — but `db` is an instance attribute, not a syntactic definition in the class body.

**Fix:** When a binding has `instance_attr=true`, write to the parent class block instead of the current method block. The compound SSA key `"self.db"` would then be readable by all sibling method blocks. During chain resolution, `self.db.execute()` builds chain `[This, Field("db"), Call("execute")]`. The `This` step resolves `self` to `Type(class_fqn)`. If the field step `db` fails member lookup, fall back to reading the SSA key `"self.db"` from the class block, which would yield `Type("Database")`, then look up `execute` on `Database`.

**Effort:** Medium. Requires changes to `walk_full` (redirect instance attr writes to parent block) and `resolver.rs` (fall back to compound SSA key when field lookup fails).

### 2. Opaque function parameters

```python
def process(handler):    # handler → Opaque
    handler.execute()    # chain dies at base
```

**Why it fails:** Function parameters have no SSA value. The SSA engine writes nothing for them, so any chain rooted in a parameter produces no reaching defs.

**Status:** Works for Java/Kotlin (type annotations in the parameter list produce `SsaValue::Type`). Completely fails for Python/Ruby/Go.

**Fix for typed languages:** Already works via `extract_type_annotation`.

**Fix for untyped languages:** No fix without type stubs, type inference, or user-provided annotations. Could add support for Python type comments (`# type: Handler`) or inline annotations (`handler: Handler`), but those require the user to write them.

**Effort:** N/A for dynamic languages without annotations. Low for Python PEP 484 annotations (already handled by `extract_type_annotation` if the grammar exposes the annotation node).

### 3. Generic type parameters

```java
List<UserService> services = getAll();
services.get(0).query();   // type annotation is "List<UserService>", FQN lookup fails
```

**Why it fails:** `ParseValue::Type` is a bare string. The type annotation `"List<UserService>"` doesn't match any FQN in the graph. No generic parameter extraction.

**Fix:** When a type annotation contains `<...>`, extract the inner type parameter. `List<UserService>` → use `UserService` for member lookups on the result. For the common single-type-parameter case (`List<T>`, `Optional<T>`, `Set<T>`), strip the wrapper and resolve on the inner type.

Implementation: in `extract_type_annotation` or `resolve_base_type_fqns`, if FQN lookup fails and the type string contains `<`, extract the substring between `<` and `>`, split on `,`, take the first element, and try that as the type.

**Effort:** Low-medium. String parsing in `extract_type_annotation` + fallback in resolver.

### 4. Return type inference from function bodies

```python
def make_logger():
    return Logger()

log = make_logger()
log.info()           # chain fails: make_logger has no return_type metadata
```

**Why it fails:** `make_logger` has no return type annotation. The SSA alias `log → make_logger` resolves to the `LocalDef` of `make_logger`, but `make_logger` is not a type container and has no `metadata.return_type`. Chain dies.

**Fix — Phase 2 local inference:** During `walk_full`, when we encounter a `return` statement, evaluate the returned expression's SSA value. If it's `Type("Logger")` or `LocalDef(class_idx)`, record that as the enclosing function's `metadata.return_type`. This is purely local analysis — we're already walking the function body.

**Fix — Phase 3 cross-file propagation:** Phase 2 is parallel, so file A's callers might resolve before file B (containing the callee) has been walked. After Phase 2 completes, do a light Phase 3:

1. Collect all functions with inferred return types from Phase 2
2. Update graph definition metadata
3. Re-resolve stored `PendingChain { target_def, remaining_steps }` entries

The pending chains are small (a def index + a few ExpressionSteps). Phase 3 iterates them, checks if `target_def` now has a return type, and continues chain resolution. Cap at 2-3 iterations for transitive cases (`a()` returns `b()` returns `Logger()`).

**Effort:** Medium. Requires `return` statement handling in `walk_full`, metadata storage, and a Phase 3 loop in `pipeline.rs`.

### 5. Comprehension iteration variables

```python
results = [item.process() for item in get_items()]
```

**Why it fails:** `for_in_clause` binding rule uses `no_value()`. The SSA writes `Opaque` for `item`. The iteration expression `get_items()` is walked (producing a ref event) but its type isn't connected to `item`.

**Fix:** Change `for_in_clause` binding from `no_value()` to `value_from("right")` so `item` gets an `Alias` to `get_items`. Combined with return type inference (#4), if `get_items` returns a typed collection, the chain could partially resolve.

**Effort:** Trivial. One-line change in Python rules.

### 6. Context manager variables

```python
with open('file') as f:
    f.read()           # f → Opaque
```

**Why it fails:** `with_item` binding uses `no_value()`.

**Fix:** Change to `value_from("expression")` so `f` gets an `Alias` to `open`.

**Effort:** Trivial. One-line change in Python rules.

### 7. Exception handler variables

```python
except ValueError as e:
    e.args             # e not bound in SSA
```

**Why it fails:** No binding rule for Python `except_clause`. Java has `catch_formal_parameter` but Python doesn't have an equivalent rule.

**Fix:** Add binding rule: `binding("except_clause", BindingKind::Parameter).name_from(&["name"]).no_value()`. Even without type info, the binding lets the variable participate in SSA (won't resolve but won't crash either). With type extraction from the exception type node, we could write `SsaValue::Type("ValueError")`.

**Effort:** Trivial.

### 8. Python wildcard imports

```python
from module import *
```

**Why it fails:** Python's import strategies list doesn't include `WildcardImport`. Java/Kotlin have it.

**Fix:** Add `ImportStrategy::WildcardImport` to Python's resolution stages.

**Effort:** Trivial. Already implemented for Java/Kotlin.

### 9. `ImportStrategy::FilePath` stub

The `FilePath` strategy (`imports.rs:56`) always returns `vec![]`. Python lists it as a strategy but it never matches.

**Fix:** For Python, match `import foo.bar` against file paths like `foo/bar.py` or `foo/bar/__init__.py` by converting the import path to a file path using the FQN separator and looking up in the file index.

**Effort:** Medium. Needs file path index in the graph.

### 10. Only calls generate reference edges

`obj.attribute` (read without call), `dict['key']`, `array[0]` — none produce reference events. Only call-site nodes in `ref_dispatch` trigger edge creation.

This means field reads, constant accesses, and indexed accesses produce no edges in the graph.

**Fix:** Add `field_access` / `attribute` / `member_access` node kinds to `ref_dispatch`. This would generate edges for every field read, not just method calls. Needs filtering to avoid noise (reading `x.y` shouldn't create an edge to a function named `y`).

**Effort:** Medium. Architectural decision about edge semantics.

### 11. C# has zero resolution

Uses `NoRules`. Parser extracts definitions/imports/references but nothing is resolved cross-file.

**Fix:** Create `CSharpRules` implementing `HasRules` with `TypeFlow` chain mode, `this`/`base` SSA config, and standard import strategies (`ScopeFqnWalk`, `ExplicitImport`, `WildcardImport`, `SameFile`). The parser spec already extracts the right node kinds.

**Effort:** Medium. Needs rules definition + testing.

### 12. Go receiver methods not linked to structs

```go
func (s *Server) Start() { ... }
```

The receiver type `*Server` isn't extracted into metadata, so `Start` isn't associated with `Server` for member lookup.

**Fix:** Extract receiver type from `method_declaration`'s `receiver` field into `metadata.receiver_type`. The metadata field already exists in `GraphDefMeta`. Use it during resolution to look up methods on the struct type.

**Effort:** Low.

### 13. Relative imports in Python

```python
from . import sibling
from ..parent import thing
```

The `module_name` field captures `.` or `..parent`, but the import path becomes `"."` or `"..parent"`. No logic to resolve relative paths against the file's module path.

**Fix:** In import resolution, when path starts with `.`, resolve relative to `module_from_path(file_path)`.

**Effort:** Low-medium.

### 14. Unpacking/destructuring

```python
a, b = get_pair()
first, *rest = items
```

Assignment binding extracts the entire left side as a single name string (`"a, b"`), which doesn't match any definition. Individual variables aren't tracked.

**Fix:** Detect tuple/list patterns on the LHS and create separate bindings per element. Complex — needs grammar-specific handling for each unpacking pattern.

**Effort:** High.

## Architectural constraints

| Constraint | Description | Mitigation |
|-----------|-------------|------------|
| SSA is intra-procedural | No tracing into called function bodies | Return type inference (local, #4) + Phase 3 propagation |
| No type lattice | `Type(String)` can't represent unions, generics, nullable | Generic stripping (#3) handles the most common case |
| Per-scope blocks isolate methods | Sibling methods can't see each other's writes | Instance attr redirect to class block (#1) |
| Only calls generate edges | Field reads, subscripts, type refs produce nothing | Add to `ref_dispatch` (#10), needs edge semantics decision |
| `ExpressionStep::Index` and `MethodRef` are dead | Defined but never constructed by any language | Remove or implement |

## Recommended priority order

1. **Instance attributes across methods** (#1) — highest impact for Python/Ruby, medium effort
2. **Return type inference from function bodies** (#4) — unlocks factory pattern resolution, medium effort
3. **Generic type stripping** (#3) — unlocks Java/Kotlin collection chains, low-medium effort
4. **Comprehension iteration variables** (#5) — trivial fix, common pattern
5. **Context manager variables** (#6) — trivial fix
6. **Exception handler variables** (#7) — trivial fix
7. **Python wildcard imports** (#8) — trivial fix
8. **Go receiver methods** (#12) — low effort, unlocks Go resolution
9. **Relative imports** (#13) — low-medium effort
10. **C# resolution** (#11) — medium effort, new language
11. **FilePath import strategy** (#9) — medium effort
12. **Non-call reference edges** (#10) — medium effort, needs design decision
13. **Unpacking** (#14) — high effort, complex
