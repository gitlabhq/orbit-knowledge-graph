# tree-dsl

YAML-driven tree rewrite engine for multi-language code analysis. Transforms tree-sitter CSTs into a canonical form, runs SSA-based value flow, and emits definition/call/import edges.

## Pipeline

```
source ─→ tree-sitter CST ─→ YAML rewrites (bottom-up) ─→ classify_methods
       ─→ SSA fold (linker) ─→ prune ─→ canonical tree + edges
```

Each language defines its rewrites in `langs/<language>.yaml`. The engine applies them bottom-up in a single pass per stage. All rules use one action: `replace:`.

## Pattern Language Reference

Patterns are S-expressions. The same syntax is used for both match and template sides of a `replace:` rule, with some constructs only valid on one side.

### Nodes

```
(kind)                      Match/create a node of this kind
(kind "literal")            Match/create with exact sym text
(kind children...)          Match/create with child patterns
```

### Captures

```
$N                          Capture any node into slot N
$N:kind                     Capture only if node kind matches
$N:(pattern)                Guarded capture: match full sub-pattern AND capture
$N?                         Optional capture: match continues if not found
```

**Key distinction:**
- `$N:identifier` captures a node whose kind is `identifier` (works on leaves)
- `(identifier $N)` matches a node of kind `identifier` that HAS a child `$N` (fails on leaves)

Use `$N:kind` for leaves like identifiers and literals. Use `(kind $N)` when you need to match a node and capture one of its children.

### Field-rooted matches

Tree-sitter nodes have named fields (`name:`, `body:`, etc.). Match them with:

```
field: $N                   Capture the child in this field
field: (kind ...)           Match a specific kind in this field
field:? $N                  Optional: skip if field absent, capture if present
```

Field order in the pattern must follow CST field order. The matcher scans forward only.

### Variadics

```
$$$X                        Capture all remaining children
$$$X:kind                   Filter: only children of this kind
$$$X:a|b|c                  Filter: multiple kind alternatives
$$$X:(pattern)              Guarded: only children matching sub-pattern
$$$X->__kind                Rekind: copy children, change their kind to __kind
$$$X=>__kind                Leaf-only rekind: emit one node per leaf sym
```

Variadics capture zero or more children. They consume everything up to the next fixed pattern in the match. Position matters: the variadic only sees children that haven't been consumed by earlier patterns.

### Template-side references

```
@$N                         Copy sym of captured node (sets parent's sym)
@$N|transform               Copy sym with text transform applied
@$N->__kind                 Copy captured subtree, change root kind
$N                          Copy entire captured subtree as child
$$$X                        Emit all captured variadic elements
$$$X->__kind                Emit elements with kind changed
$$$X=>__kind                Emit one node per leaf sym value
```

### Assertions (match-side only)

```
!pattern                    Negation: fail if any child matches
**/pattern                  Descendant: succeed if any descendant matches
```

Negation and descendant are zero-width: they don't consume children or advance the cursor.

### Optional template nodes

```
(__kind? @$R)               Emit this node only if $R captured something
(__arm? $A)                 Emit only if $A is non-empty
```

When all child captures inside an optional node are empty, the entire node is skipped.

### Spread operator (template-side)

```
$D { extra_children... }    Copy $D's subtree, inject extra children
```

Copies the captured subtree and appends the template content as additional children of the root. Used for injecting markers into an existing node without reconstructing it.

### Text transforms

Transforms are applied to `@$N|tf` references. They modify the sym text.

```
@$N|lowercase               Lowercase
@$N|strip=@                 Strip prefix "@"
@$N|strip_suffix(".py")     Strip suffix
@$N|split_last("::")        Take text after last "::"
@$N|replace("::", "/")      Replace all occurrences
@$N|prepend("./")           Prepend text
@$N|to_rel(".")             Convert dot-separated to relative path
@$N|tf1|tf2                 Pipeline: apply tf1 then tf2
```

## YAML Rule Structure

```yaml
stages:
  - name: normalize
    rules:
      - match: '(pattern)'
        replace: '(template)'

  - name: classify
    rules:
      - match: '(pattern)'
        replace: '(template)'

resolve:
  lookup_from:
    - __source_root
  stages:
    - name: packages
      rules:
        - match: '(__dir (__file "__init__.py") $$$REST)'
          replace: '(__dir (__package) (__file "__init__.py") $$$REST)'
    - name: roots
      climb:
        while: __package
        mark: __source_root
```

### Stage ordering

Rules within a stage run in a single bottom-up pass. Inner nodes are processed before outer nodes. First-match-wins: only the first matching rule fires per node.

Typical stage sequence:
1. **normalize** -- flatten language quirks, rename nodes, strip wrappers
2. **retag-refs** -- member access (`a.b`) to `__member (__object)`
3. **retag-calls** -- call expressions to `__call (__callee) (__args)`
4. **imports** -- import statements to `__import (__source) (__name)`
5. **classify** -- definitions, bindings, control flow to canonical `__def`, `__binding`, `__branch`, `__loop`

### Bottom-up consequences

Because inner nodes are processed first:
- In classify, `function_definition → __def` fires before `class_definition → __def`
- A class rule can match its body containing already-canonical `__def` children
- Use `$$$ANN:__decorator|__supertype` to capture markers created by earlier rules in the SAME stage
- Use `**/pattern` to detect descendants without capturing them (e.g., `__call__` method detection)

## Canonical Alphabet

After all rewrites and pruning, every surviving node has one of these kinds:

```
__def          Definition (function, class, method, struct, etc.)
  __defname    Name of the definition
  __deftype    Classification: "Function", "Class", "Method", etc.
  __scope      SSA scope boundary
  __return_type  Return type annotation
  __supertype  Inheritance / implements
  __decorator  Decorator reference
  __self_method  Has self/this parameter
  __callable   Has __call__ protocol
  __visibility Access modifier

__import       Runtime import
__import_type  Type-only import
  __source     Display text of source
  __source_path  Resolved path
  __name       Imported name
    __alias    Alias for this name

__call         Call expression
  __callee     What is being called
    __member   Method name
      __object Receiver
    __ivar     Self-method call
  __args       Arguments

__binding      Variable binding
  __rhs        Right-hand side value
__ivar         Instance variable (self.x)
__member       Standalone member access

__branch       SSA fork (if/match/try)
  __arm        Branch arm
__loop         SSA back-edge (for/while)
__return       Return expression
```

## Examples

### Simple rename

```yaml
- match: '(return_statement $$$VALS)'
  replace: '(__return $$$VALS)'
```

### Capture with kind filter

```yaml
# Leaf node: use $N:kind (captures the node)
- match: '(decorator $N:identifier)'
  replace: '(__decorator @$N)'

# Interior node: use (kind $child) (matches kind, captures child)
- match: '(call_expression function: $F arguments: $A)'
  replace: '(__call (__callee @$F) $A)'
```

### Optional fields

```yaml
# One rule handles with/without return type and typed parameters
- match: '(function_definition name: $N parameters:? (parameters $$$P:__binding) return_type:? $R body: $B $$$ANN:__decorator)'
  replace: '(__def (__defname @$N) (__deftype "Function") (__return_type? @$R) (__scope) $$$ANN $$$P $B)'
```

### Descendant assertion

```yaml
# Detect __call__ method anywhere in the class body
- match: '(class_definition name: $N $$$ANN:__supertype|__decorator body: $B **/(__def (__defname "__call__")))'
  replace: '(__def (__defname @$N) (__deftype "Class") (__scope) (__callable "__call__") $$$ANN $B)'
```

### Spread operator

```yaml
# Hoist decorators into the inner definition
- match: '(decorated_definition $$$DECOS:__decorator definition: $D)'
  replace: '$D { $$$DECOS }'
```

### Negation

```yaml
# Match only if the class does NOT have a __call__ method
- match: '(class_definition name: $N body: $B !(__def (__defname "__call__")))'
  replace: '(__def (__defname @$N) (__deftype "Class") (__scope) $B)'
```

### Variadic with rekind

```yaml
# Each import specifier becomes a __name child
- match: '(import_statement (import_clause (named_imports $$$SPECS:import_specifier)) source: (string $S:string_fragment))'
  replace: '(__import (__source @$S) (__source_path @$S) $$$SPECS->__name)'
```

### Text transforms

```yaml
# Rust use path: replace :: with / for source path
- match: '(use_declaration argument: $A:scoped_identifier)'
  replace: '(__import (__source @$A|replace("::","/")) (__source_path @$A|replace("::","/")) (__name @$A|split_last("::")))' 
```
