---
name: rust-idioms
description: >-
  Write, review, and refactor Rust so it survives review: error paths that
  return Result instead of panicking on caller misuse, reuse of existing
  helpers, no dead API, typed values instead of strings and magic numbers,
  and iterator and allocation choices. Use when adding or changing .rs files,
  implementing a Rust feature or bug fix, writing a Rust library or module,
  or reviewing a Rust change. Not for Cargo or CI configuration, toolchain
  setup, or non-Rust code.
---

# Rust idioms

Generic Rust craft for any crate. Project rules (AGENTS.md, lint config) win
over this file. The compiler and `clippy::all` flag most mechanical
idioms; this covers what they mostly miss.

## Before you write

1. **Reuse first.** Before adding a helper, type, or constant, look for the
   concept in the crate: use a code index (Orbit, an LSP) for definitions and
   callers where you have one, and `rg` for literal text. Extend or call what
   exists. A second copy of a check is a smell; a third is a bug.
2. **No dead API.** Every new `pub` or `pub(crate)` item needs a caller in
   the same change (find callers with the code index or `rg`), or it does
   not ship. No "for later" methods, builders, or getters. Start private;
   widen only when something outside needs it.
3. **Parse, don't re-check.** Validate at the boundary and hand inner code a
   typed value (see [types](references/types.md)). Inner code never
   re-validates and never silently repairs bad input.
4. **Failures are values.** A caller can reach it, so it returns `Result` or
   `Option`; it does not panic. `assert!`, `expect`, `unwrap`, and slice
   indexing on caller-controlled input are bugs, including "use after
   shutdown/close", "empty input", and "index out of range". Panic only when
   your own invariant is broken and a message states it.
5. **Check the shape of the data.** Pick the collection and pass style for
   the access pattern, then see [iterators](references/iterators.md).

## Typed values (details: [types](references/types.md))

- Closed set of strings or integers: `enum` with `FromStr`/`TryFrom`,
  `match` without a `_ =>` arm so new variants break the build.
- Two values of the same primitive type that must not be mixed (ids,
  units, paths vs names): a newtype.
- Repeated literal: a named `const`, defined once.
- Booleans that select a mode: an enum. `Option<T>` for "may be absent",
  not sentinel values (`""`, `-1`, `usize::MAX`).
- Return owned types only when the callee creates the data; otherwise
  hand back borrows.

## Allocation and iterators (details: [iterators](references/iterators.md))

- Return `impl Iterator` (or take `impl IntoIterator`) instead of building a
  `Vec` that the caller immediately loops over or filters.
- One pass: chain adapters, `collect` once, at the end.
- Reserve capacity or use `collect` when the size is known; no `Vec`
  `contains` in a loop (quadratic), use a set or a map.
- `clone()` to satisfy the borrow checker is a design signal: restructure,
  borrow, or move before cloning. Clone where ownership is genuinely needed.
- `mem::take` or `Option::take` to move a field out instead of cloning it.

## Errors in short

- Library code: a small `enum` error implementing `Display` and `Error`
  with `source()`; a manual `From` impl (or `thiserror`'s
  `#[from]`) plus `?` to convert. Binary glue may
  use a boxed or context-carrying error.
- Never swallow: `.ok()`, `unwrap_or_default()`, `let _ =` on a `Result`
  hide failures. Decide: propagate, log and continue deliberately, or
  document why ignoring is correct.
- Keep the source error when mapping; do not replace it with a string.
- Channel disconnect and closed handles are normal runtime states of a
  public API: model them in the return type.
- Lock poisoning means another thread already panicked. Propagate the
  panic (`expect` with a message), recover with `into_inner` when the data
  stays valid, or return an error only when callers can act on it.

## Lints that are not in `clippy::all`

`clippy::unwrap_used`, `expect_used`, `indexing_slicing`, `panic`,
`let_underscore_must_use`, `unreachable`. If the project does not enable
them, still avoid the patterns in non-test code. Tests may unwrap. Where an
invariant really cannot fail, mark it with
`#[expect(clippy::expect_used, reason = "...")]` and say why.

## When done

Re-read the diff as a reviewer: duplicate of an existing helper? Unused
item? A string where an enum fits? An eager `collect` followed by a loop? A
panic reachable from a public function? Fix those, then run the project's
lint and test commands.

## Further reading

[Rust API Guidelines](https://rust-lang.github.io/api-guidelines/),
[*The Rust Programming Language*](https://doc.rust-lang.org/book/) ch. 6, 9,
and 13, and the [rust-unofficial patterns](https://rust-unofficial.github.io/patterns/)
catalogue (MPL-2.0, linked not copied). Attribution: [NOTICE](references/NOTICE.md).
