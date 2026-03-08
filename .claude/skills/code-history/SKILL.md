---
name: code-history
description: Investigate the history, usage, and liveness of code using search and git blame/log. Use when determining if code is dead, understanding why something exists, finding all callers before refactoring, or deciding whether something is safe to remove. Also useful for answering "who added this and why" or "is anything still using this".
allowed-tools: Read, Grep, Glob, Bash(git *)
---

# Code History

Figure out if code is still used and whether it's safe to remove. Pick whichever techniques below are useful -- you don't need all of them.

## Techniques

Search for callers with `grep -rn 'symbol' --include='*.rs'` across the whole repo. Classify hits as definition, test, re-export, or real caller. Zero real callers usually means dead. Also check `pub use` re-exports -- if nothing imports the re-export, it's still dead.

Find the introducing commit with `git log --all --oneline --diff-filter=A -S 'symbol' -- '*.rs'`. Then `git show <commit> --stat` to see what else was part of that change.

Trace the lifecycle with `git log --all --oneline -S 'symbol' -- '*.rs'` to see every commit that added or removed lines with the symbol. Look for commits that deleted callers without deleting the code they called -- that's usually how dead code happens.

Blame with `git blame path/to/file.rs -L 100,120`. If a function hasn't been touched since the original commit but surrounding code has evolved, it's probably orphaned.

Check transitive usage -- a symbol with zero direct callers might still be reachable through `pub use` re-exports or trait impls (`impl Trait for`). If nothing outside the crate uses the re-export, it's dead.

## Decision framework

| Evidence | Verdict |
|---|---|
| Zero real callers, only definition + tests | Dead. Remove with its tests. |
| Re-exported but no external imports | Dead. Remove re-export too. |
| Caller was deleted in a later commit | Dead from incomplete refactor. |
| Only caller is itself dead | Transitively dead. Remove both. |
| One caller | Candidate for inlining. |
| 2+ callers | Alive. |

## Output format

1. What it does -- one sentence.
2. Who introduced it and why -- commit hash, date, original purpose.
3. What happened -- commit(s) that made it dead or reduced callers.
4. Current state -- real callers, test-only callers, re-exports.
5. Recommendation -- remove, inline, or keep. If remove, list all locations.

## Git commands reference

```bash
git log --all --oneline --diff-filter=A -S 'symbol' -- '*.rs'  # first appearance
git log --all --oneline -S 'symbol' -- '*.rs'                   # all touches
git log --all --oneline -20 -- path/to/file.rs                  # file history
git show <commit>:path/to/file.rs                               # file at commit
git show <commit> -- path/to/file.rs                            # diff for one file
git show <commit> --stat                                        # files changed
git blame path/to/file.rs -L 100,120                            # blame range
git blame -w path/to/file.rs -L 100,120                         # blame ignoring whitespace
git log -1 -S 'deleted_line' -- path/to/file.rs                 # find deletion
git log --all --merges --oneline -S 'symbol'                    # merge commits
```
