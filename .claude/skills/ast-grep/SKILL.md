---
name: ast-grep
description: >
  AST-based code search and rewrite via tree-sitter patterns.
  Use instead of Grep/Edit for structural matching, batch rewrites,
  or context-aware queries (e.g. "unwrap inside impl blocks").
allowed-tools: Bash(mise exec -- ast-grep *), Read, Glob
---

# ast-grep

Matches code by parsed AST, not text. Supports metavariable capture, relational rules, and in-place rewrite.

All commands must be run through `mise exec --` since ast-grep is installed via mise:

## Commands

```shell
# Pattern search
mise exec -- ast-grep run -p '$X.unwrap()' -l rust .

# Rewrite: always preview first, then apply with -U
mise exec -- ast-grep run -p '$X.lock().unwrap()' -l rust .
mise exec -- ast-grep run -p '$X.lock().unwrap()' -r '$X.lock().expect("lock poisoned")' -l rust -U .

# Structural search with YAML rules (for relational/composite logic)
mise exec -- ast-grep scan --inline-rules 'id: name
language: rust
rule:
  pattern: $X.unwrap()
  inside:
    kind: impl_item
    stopBy: end' .

# JSON output for programmatic use
mise exec -- ast-grep run -p '$X.unwrap()' -l rust --json .

# Debug: see how ast-grep parses your pattern or target code
mise exec -- ast-grep run -p 'PATTERN' -l rust --debug-query=cst   # concrete syntax tree
mise exec -- ast-grep run -p 'PATTERN' -l rust --debug-query=pattern # metavar detection
```

## Pattern syntax

| Syntax | Meaning |
|---|---|
| `$VAR` | Single named node. Reuse = must match identically (`$A == $A` matches `x == x` only) |
| `$$VAR` | Single unnamed node (operators, punctuation) |
| `$$$` | Zero or more nodes (variadic args, statements) |
| `$_` prefix | Non-capturing (each occurrence matches independently) |

Patterns must be valid parseable code. Bare `.unwrap()` is an ERROR — use `$X.unwrap()`.

Use `--selector KIND` to disambiguate patterns that parse as the wrong node type. Provide surrounding context in the pattern and select the sub-node you actually want: `mise exec -- ast-grep run -p 'struct S { pub $N: $T }' --selector field_declaration -l rust .`

## YAML rule structure

Rules combine three categories:

- **Atomic:** `pattern`, `kind`, `regex`, `nthChild`
- **Relational:** `has`, `inside`, `precedes`, `follows` — always add `stopBy: end`
- **Composite:** `all`, `any`, `not`, `matches`

```yaml
rule:
  all:
    - kind: function_item                    # atomic: match by node type
    - has:                                   # relational: must contain
        kind: await_expression
        stopBy: end                          # required: search entire subtree
    - not:                                   # composite: exclude
        has:
          kind: try_expression
          stopBy: end
```

Use `constraints` in YAML rules to filter metavariable text by regex:

```yaml
rule:
  pattern: $X.$METHOD()
constraints:
  METHOD:
    regex: "^(unwrap|expect)$"
```

## Key Rust node kinds

Use `--debug-query=cst` to discover kinds at runtime. Non-obvious mappings:

| Kind | Note |
|---|---|
| `function_item` | All fn declarations (pub, async, const, etc.) |
| `impl_item` | Both inherent and trait impls |
| `method_call_expression` | `x.foo()` — distinct from `call_expression` (`foo()`) |
| `macro_invocation` | `println!(...)`, `vec![...]` |
| `await_expression` | `expr.await` |
| `try_expression` | The `?` operator |
| `function_modifiers` | Contains `async`, `unsafe`, `const` keywords |
| `visibility_modifier` | `pub`, `pub(crate)`, etc. |

## Shell escaping

Use single-quoted strings for inline rules to avoid `$` expansion. If you must double-quote, escape as `\$VAR`, `\$\$\$`.
