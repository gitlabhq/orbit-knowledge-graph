---
name: remove-llm-comments
description: >
  Remove and tighten unnecessary LLM-generated comments — including narration
  disguised as "why", restated constraints, and why-comments a named symbol or
  the code structure already conveys. Use while editing, and as the final
  narration-comment pass before opening an MR or pushing, to strip the LLM
  narration the model left behind.
allowed-tools: Read, Edit, Glob, Grep
---

# Remove LLM comments

The operational companion to the comment rule in `AGENTS.md` ("Code quality").
That rule is the law; this skill is the worked-example playbook. They must not
drift — if they disagree, `AGENTS.md` wins.

## When to run

Two moments:

- **While editing** — the ideal. Write clean as you go; don't narrate first.
- **Before opening an MR or pushing** — the backstop. Clean-as-you-go alone has
  proven insufficient (narration keeps landing in MRs), so run a final pass over
  the comments your change added or modified. This gate catches what slipped
  through; it is not permission to narrate first.

## The discriminator (from AGENTS.md)

> If a comment would survive deleting it without losing *why* information, delete it.

A comment earns its place only by explaining something the code cannot: a
non-obvious constraint, an invariant, a gotcha, intent a reader can't infer, or
an ADR/issue link. "What the next line does" is never a reason to keep it — and
neither is a *why* that a well-named symbol or the surrounding structure already
makes obvious (see "A true *why* can still go").

**Out of scope — never touch these regardless of the discriminator:** license
headers and links to issues, specs, or external docs. A license header carries
no *why* and a literal reading would delete it; don't. Leave both as-is.

The easy cases (pure narration) are below. The cases that actually leak through
review are the three after them: **tighten**, **de-duplicate**, and **redundant
with the code**. Learn those.

## The easy cases: pure narration

```rust
// Create a new vector to store results
let results = Vec::new();

// Iterate over items
for item in items {
```

The code already says what it does. Delete. Same for changelog comments
(`// Added error handling` — that's git history), decorative section banners
(`// ==== HELPERS ====` — if you need them, split the file), and signature
restatements (`// Takes a user ID and returns the user` above `fn get_user`).

## Don't only delete — tighten to the *why*

A comment can carry real *why* **and** narrate. Don't keep it whole and don't
nuke it: trim it down to the rationale, drop the rest. This is the most common
miss — the reviewer asked for the *why* to stay but smaller.

Before (a real *why*, but wordy and partly restating the code):

```rust
// `self` is the last live token; holding it across `finalize` keeps `inflight` non-zero
// (its Drop reclaims the slot) so `flush()` waits for the checkpoint, not just the writes.
```

After (same invariant, the mechanics the code already shows are dropped):

```rust
// Hold the last token across `finalize` so `inflight` stays non-zero until the checkpoint
// lands, making `flush()` wait for the checkpoint and not just for the writes.
```

The kept sentence is the one a reader could *not* reconstruct from the code: the
ordering guarantee `flush()` depends on. Everything restating the `Drop`/`Arc`
mechanics goes.

## De-duplicate — state a constraint once, at its best home

When the same constraint shows up on adjacent comments, it reads as emphasis but
it's just drift waiting to happen. Say it once, where it's most discoverable
(usually the type/function doc), and trim the neighbours to their *distinct*
point.

Before — two comments, both re-explaining the inflight/`Drop` reclaim:

```rust
/// `inflight` is reclaimed in [`Drop`], not in `finalize` ...        // (on the type doc)
...
// remaining starts at 1: a sentinel the pipeline releases after the parse finishes, so
// the commit can't finalize mid-stream even if every flushed part drains first. The
// matching `inflight` slot is reclaimed by `ProjectCommit::drop`, so a timeout that drops
// the run future before the sentinel release still frees it.
```

After — the second comment keeps only its own `remaining`-sentinel point; the
reclaim story lives once on the type doc:

```rust
// remaining starts at 1: a sentinel the pipeline releases after the parse finishes, so
// the commit can't finalize mid-stream even if every flushed part drains first.
```

These sections are sequential passes, not independent rules: a later pass may
delete even this surviving type-doc comment — see "A true *why* can still go" —
once a named symbol carries it.

## A true *why* can still go — when a named symbol carries it

The hardest call: a comment is genuine *why*, not narration, and you still
delete it because the code now says it. The discriminator is **redundancy with
the structure**: if a well-named symbol, type, or `impl` already names the
mechanism the comment describes, the comment is a second copy that will rot.

Before — an accurate, well-written doc-comment on the type:

```rust
/// `inflight` is reclaimed in [`Drop`], not in `finalize`, so every terminal path frees the
/// slot — including a job whose run future is dropped on timeout after submitting batches, where
/// the sentinel's `release()` never runs so `finalize` never fires. Reclaiming it in `finalize`
/// would leak the slot on that path and hang the `flush()` drain loop.
struct ProjectCommit { ... }
```

After — deleted, because a few lines below:

```rust
impl Drop for ProjectCommit {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}
```

The `Drop for ProjectCommit` impl **is** the statement "inflight is reclaimed in
Drop". The prose duplicated it. Keep the comment only if the *why* survives the
symbol — here it didn't.

> Be conservative. Delete on this ground only when the symbol genuinely carries
> the reasoning. A subtle invariant (e.g. *why* an order matters, what breaks if
> you change it) usually does **not** fit in a name and should stay — tightened.

## Rust traps in this codebase

- **`///` doc-comment narration.** A doc comment is still narration if it
  restates the signature or a visible `impl`. Doc comments are not exempt.
- **Test-setup / label comments.** `// Setup`, `// Clear env vars`, or a label
  over each `assert_eq!` restating the call. The test name and the assertion
  already say what's checked — delete. Keep only a comment that explains an
  invariant the test relies on (`// Insert the stale row second so argMax must
  resolve it`). A test comment may also need **tightening**, not deletion:

  ```rust
  // A timed-out job's submitted parts still drain (the writer owns them) while the run
  // future is dropped before it can release the sentinel. Dropping `commit` here without a
  // third `release()` mimics that: the slot must still be reclaimed and nothing checkpointed.
  ```

  →

  ```rust
  // Dropping `commit` without a third `release()` mimics a run future dropped on timeout:
  // the sentinel is never released, yet the slot must still be reclaimed.
  ```

- **Restating the adjacent line / config.** A comment that re-declares what the
  next line, a `const`, or a config key already declares (`// timeout is 30s`
  above `timeout: 30`). Delete; if the *number* needs a reason, give the reason,
  not the value.

## Scope discipline

As a cleanup pass, touch only the comments the current change adds or modifies.
Don't churn unrelated comments in files you happen to open — that bloats the
diff and buries the real change. The fix is scoped; the comment cleanup is too.

## Quick checklist

1. Pure narration / changelog / banner / signature restatement → **delete**.
2. Why **+** narration → **tighten** to just the why.
3. Same constraint on adjacent comments → **say it once**, trim the rest.
4. Genuine why that a named symbol/`impl` now carries → **delete** (conservatively).
5. Only touch comments this change introduced or modified.
