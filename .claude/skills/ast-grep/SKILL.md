---
name: ast-grep
description: >
  AST-based code search and rewrite via tree-sitter patterns.
  Use instead of Grep/Edit for structural matching, batch rewrites,
  or context-aware queries (e.g. "unwrap inside impl blocks").
allowed-tools: Bash(ast-grep *), Read, Glob
---

# ast-grep

Matches code by parsed AST, not text. Supports metavariable capture, relational rules, and in-place rewrite.

## Commands

```shell
# Pattern search (default command)
ast-grep run -p '$X.unwrap()' -l rust .

# Rewrite: always preview first, then apply with -U
ast-grep run -p '$X.lock().unwrap()' -l rust .
ast-grep run -p '$X.lock().unwrap()' -r '$X.lock().expect("lock poisoned")' -l rust -U .

# Structural search with YAML rules (for relational/composite logic)
ast-grep scan --inline-rules 'id: name
language: rust
rule:
  pattern: $X.unwrap()
  inside:
    kind: impl_item
    stopBy: end' .

# JSON output for programmatic use
ast-grep run -p '$X.unwrap()' -l rust --json .

# Debug: see how ast-grep parses your pattern or target code
ast-grep run -p 'PATTERN' -l rust --debug-query=cst   # concrete syntax tree
ast-grep run -p 'PATTERN' -l rust --debug-query=pattern # metavar detection
```

## Pattern syntax

| Syntax | Meaning |
|---|---|
| `$VAR` | Single named node. Reuse = must match identically (`$A == $A` matches `x == x` only) |
| `$$VAR` | Single unnamed node (operators, punctuation) |
| `$$$` | Zero or more nodes (variadic args, statements) |
| `$_` prefix | Non-capturing (each occurrence matches independently) |

Patterns must be valid parseable code. Bare `.unwrap()` is an ERROR — use `$X.unwrap()`.

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

Use `--selector KIND` with `run -p` to extract a sub-node from a pattern match (e.g. report just the function name, not the whole function).

Use `constraints` in YAML rules to filter metavariable text by regex:

```yaml
rule:
  pattern: $F($$$)
constraints:
  F:
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
