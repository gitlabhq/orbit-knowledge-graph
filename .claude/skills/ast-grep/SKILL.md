---
name: ast-grep
description: >
  Structural code search and rewrite using AST patterns.
  Use instead of Grep/Edit when the task involves code structure:
  matching nested expressions, finding functions with specific contents,
  batch-renaming patterns, or any query where text search is too imprecise.
allowed-tools: Bash(ast-grep *), Read, Glob
---

# ast-grep: structural code search and rewrite

ast-grep matches code by its parsed AST (tree-sitter), not by text. It supports metavariable capture, relational rules (has/inside), and in-place rewrite in a single CLI call.

## When to use ast-grep vs existing tools

| Situation | Tool |
|---|---|
| Find files by name or extension | Glob |
| Find a text string in files | Grep |
| Simple one-off string replacement in one location | Edit |
| Match code by structure (nesting, node type, contains/not-contains) | ast-grep |
| Batch rewrite a pattern across many files | ast-grep |
| Find code inside a specific context (impl block, match arm, async fn) | ast-grep |

## Core commands

### 1. Pattern search and rewrite

```bash
# Search
ast-grep run -p 'PATTERN' -l rust [PATHS]

# Search and rewrite (preview first, then apply)
ast-grep run -p 'PATTERN' -l rust .          # preview matches
ast-grep run -p 'PATTERN' -r 'REPLACEMENT' -l rust -U .  # apply all
```

`-U` (`--update-all`) applies rewrites without interactive confirmation.

### 2. Complex structural search with YAML rules

```bash
ast-grep scan --inline-rules "id: my-rule
language: rust
rule:
  kind: function_item
  has:
    pattern: .unwrap()
    stopBy: end" .
```

### 3. Debug AST structure

```bash
ast-grep run -p 'PATTERN' -l rust --debug-query=cst
```

Formats: `cst` (all nodes), `ast` (named only), `pattern` (how ast-grep parses your pattern).

## Pattern syntax

| Syntax | Meaning | Example |
|---|---|---|
| `$VAR` | Single named AST node | `$X.unwrap()` matches `foo.unwrap()` |
| `$$VAR` | Single unnamed node (operators, punctuation) | `a $$OP b` |
| `$$$MULTI` | Zero or more nodes | `fn $F($$$ARGS)` matches any arity |
| `$_` prefix | Non-capturing (each use matches independently) | `$_A == $_A` matches `x == y` |
| Reuse name | Same-name vars must match identical code | `$A == $A` matches `x == x` only |

## Rust node kinds (tree-sitter-rust)

| Kind | Matches |
|---|---|
| `function_item` | `fn foo() {}`, `pub async fn bar() -> T {}` |
| `struct_item` | `struct Foo {}` |
| `enum_item` | `enum Bar {}` |
| `impl_item` | `impl Foo {}`, `impl Trait for Foo {}` |
| `trait_item` | `trait MyTrait {}` |
| `match_expression` | `match x { ... }` |
| `match_arm` | Individual arms in match |
| `call_expression` | `foo()`, `Self::new()` |
| `method_call_expression` | `self.bar()`, `x.clone()` |
| `macro_invocation` | `println!(...)`, `vec![...]` |
| `use_declaration` | `use foo::bar;` |
| `let_declaration` | `let x = ...;` |
| `if_expression` | `if cond { ... }` |
| `for_expression` | `for x in iter { ... }` |
| `async_block` | `async { ... }` |
| `unsafe_block` | `unsafe { ... }` |
| `await_expression` | `expr.await` |
| `attribute_item` | `#[derive(...)]`, `#[cfg(...)]` |
| `line_comment` | `// ...` |
| `block_comment` | `/* ... */` |

## Relational rules: always use stopBy: end

Without `stopBy: end`, ast-grep stops at the first non-matching child node. This almost never does what you want.

```yaml
rule:
  kind: function_item
  has:
    pattern: .unwrap()
    stopBy: end    # search the entire subtree
```

## Rewrite workflow

Always preview before applying:

```bash
# Step 1: preview
ast-grep run -p '$X.lock().unwrap()' -l rust .

# Step 2: apply
ast-grep run -p '$X.lock().unwrap()' -r '$X.lock().expect("lock poisoned")' -l rust -U .
```

## Cookbook: common Rust patterns

### Find all pub fn signatures in a file

```bash
ast-grep scan --inline-rules "id: pub-fns
language: rust
rule:
  all:
    - kind: function_item
    - has:
        kind: visibility_modifier
        stopBy: end" path/to/file.rs
```

### Find .unwrap() inside impl blocks

```bash
ast-grep scan --inline-rules 'id: unwrap-in-impl
language: rust
rule:
  pattern: $X.unwrap()
  inside:
    kind: impl_item
    stopBy: end' .
```

### Find async fn that never await

```bash
ast-grep scan --inline-rules 'id: async-no-await
language: rust
rule:
  all:
    - kind: function_item
    - has:
        kind: function_modifiers
        regex: "async"
        stopBy: end
    - not:
        has:
          kind: await_expression
          stopBy: end' .
```

### Replace println! with tracing::info!

```bash
ast-grep run -p 'println!($$$ARGS)' -r 'tracing::info!($$$ARGS)' -l rust -U .
```

### Find todo!() and unimplemented!()

```bash
ast-grep run -p 'todo!($$$)' -l rust .
ast-grep run -p 'unimplemented!($$$)' -l rust .
```

### Find functions returning Result without ?

```bash
ast-grep scan --inline-rules 'id: result-no-question
language: rust
rule:
  all:
    - kind: function_item
    - has:
        kind: generic_type
        has:
          kind: type_identifier
          regex: "^Result$"
        stopBy: end
    - not:
        has:
          kind: try_expression
          stopBy: end' .
```

## Escaping in inline rules

When using `--inline-rules` in a Bash command:

- **Double-quoted strings:** escape metavariables as `\$VAR`, `\$\$\$`
- **Single-quoted strings:** `$VAR` works as-is (shell doesn't expand inside single quotes)
- **In YAML values:** `$VAR` works directly

## Debugging when patterns don't match

1. `--debug-query=cst` — see the concrete syntax tree of your pattern
2. `--debug-query=pattern` — see how ast-grep interprets your pattern
3. Simplify: remove sub-rules and test each part in isolation
4. Verify node `kind` values against tree-sitter-rust grammar
5. Check that `stopBy: end` is on every relational rule
