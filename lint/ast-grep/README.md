# Narration-comment lint - ast-grep fallback

This directory holds the **lower-precision, ast-grep-native fallback** for the
narration-comment lint. It is committed for documentation and for ast-grep-only
setups, but it is **not** the active gate.

## What ships as the active lint

The active narration lint is the dependency-free Python scorer
[`scripts/narration_score.py`](../../scripts/narration_score.py), run in
warning-mode through [`scripts/check-narration.sh`](../../scripts/check-narration.sh)
from a lefthook `pre-commit` job and the CI `lint:narration` job. It measured
~87% precision (~151 flags) over the current `crates/` tree (task #2933).

## Why the Python scorer is primary, not this rule

ast-grep can express only the `block_label` half of the detector (a
`line_comment` opener denylist + `not` regexes for why-words and dividers). It
**cannot** express the two highest-precision parts:

- **`token_overlap`** — comparing a comment's tokens against the *next code
  line's* token set. ast-grep's relational rules (`precedes`/`follows`) cannot
  compare token sets, and the Rust `regex` crate ast-grep uses has no
  lookahead/lookbehind.
- **the multi-line-block exemption** — the single largest precision win, which
  needs adjacent-line context ast-grep does not model for `line_comment` nodes.

As a result this rule flags ~246 comments on the tree (vs 151 for the Python
scorer) at materially lower precision (~70%): the extra flags are continuation
lines of multi-line why-comments that the Python scorer correctly exempts.

## Running the fallback

```shell
mise exec -- ast-grep scan --rule lint/ast-grep/narration-comments.yml crates/
```
