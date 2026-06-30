# comment-guard

Deterministic lint gates that catch mechanical review feedback (LLM narration
comments, bloated MR-description headlines) before it reaches a human reviewer.
Everything for these gates is self-contained in this folder so it can be removed
as a unit (see *Removing the gates* below). Task #2933.

## Contents

| File | Role |
|---|---|
| `narration_score.py` | Active narration-comment scorer (two high-precision detectors: `block_label`, `token_overlap`). Dependency-free Python. |
| `check-narration.sh` | Wrapper for the narration scorer: whole-tree, explicit-files (`{staged_files}`), and MR-diff-scoped (`--diff-base <sha>`) modes. |
| `score_description.py` | MR-description headline-section scorer (word / code-span / bare-identifier caps). |
| `check-mr-description.sh` | Reads the description from the predefined `CI_MERGE_REQUEST_DESCRIPTION` variable and runs the scorer. |
| `narration-comments.yml` | Lower-precision ast-grep-native fallback for the `block_label` half (see below). Committed for documentation / ast-grep-only setups; **not** the active gate. |

## Where the gates run

- **lefthook** `pre-commit` job `narration` (advisory; prints warnings, does not
  block — `|| true` in `lefthook.yml`).
- **CI** jobs `lint:narration` and `lint:mr-description`, defined in
  [`.gitlab/ci/comment-guard.yml`](../../.gitlab/ci/comment-guard.yml). Both use
  `allow_failure: true` (yellow/advisory). In merge-request pipelines the
  narration job scopes to lines the MR added (`--diff-base`).

The narration lint measured ~87% precision (~151 flags) over the current
`crates/` tree (task #2933).

## ast-grep fallback (`narration-comments.yml`)

ast-grep can express only the `block_label` half of the detector (a
`line_comment` opener denylist + `not` regexes for why-words and dividers). It
**cannot** express the two highest-precision parts:

- **`token_overlap`** — comparing a comment's tokens against the *next code
  line's* token set. ast-grep's relational rules (`precedes`/`follows`) cannot
  compare token sets, and the Rust `regex` crate ast-grep uses has no
  lookahead/lookbehind.
- **the multi-line-block exemption** — the single largest precision win, which
  needs adjacent-line context ast-grep does not model for `line_comment` nodes.

As a result the rule flags ~246 comments on the tree (vs 151 for the Python
scorer) at materially lower precision (~70%): the extra flags are continuation
lines of multi-line why-comments that the Python scorer correctly exempts.

```shell
mise exec -- ast-grep scan --rule scripts/comment-guard/narration-comments.yml crates/
```

## Promoting a gate to blocking

Blocking-ness lives in config, not the scripts (the scripts always exit non-zero
on findings):

- **CI:** remove `allow_failure: true` from the job in `.gitlab/ci/comment-guard.yml`.
- **lefthook:** remove the `|| true` suffix from the `narration` job's `run:` in
  `lefthook.yml`.

## Removing the gates

The kill-switch is three deletes plus two one-line reference removals (all
fail-loud, so nothing silently lingers):

```shell
rm -rf scripts/comment-guard .gitlab/ci/comment-guard.yml
# then remove:
#   - the `- local: .gitlab/ci/comment-guard.yml` line in .gitlab-ci.yml
#   - the `narration` pre-commit job in lefthook.yml
```
