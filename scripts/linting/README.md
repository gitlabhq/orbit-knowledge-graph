# linting

Deterministic lint gates that catch mechanical review feedback (LLM narration
comments, bloated MR-description headlines, machine-sounding prose) before it
reaches a human reviewer. Everything for these gates is self-contained in this
folder so it can be removed as a unit (see *Removing the gates* below). Task #2933.

## Contents

| File | Role |
|---|---|
| `narration_score.py` | Active narration-comment scorer (two high-precision detectors: `block_label`, `token_overlap`). Dependency-free Python. |
| `check-narration.sh` | Wrapper for the narration scorer: whole-tree, explicit-files (`{staged_files}`), and MR-diff-scoped (`--diff-base <sha>`) modes. |
| `score_description.py` | MR-description headline-section scorer (word / code-span / bare-identifier caps). |
| `check-mr-description.sh` | Reads the description from the predefined `CI_MERGE_REQUEST_DESCRIPTION` variable and runs the scorer. |
| `prose_lint.py` | Prose linter for the text LLMs read. PEP 723 script (`uv run`, PyYAML only). Modes: `FILE...`, `--all`, `--diff-base <sha>`. |
| `prose_lint_test.py` | Unit tests for the prose linter (`mise run lint:prose:test`). |
| `yaml_lint.py` | Layout linter for the YAML test suites: Prettier plus a blank-line rule. PEP 723 script (`uv run`, PyYAML only). Modes: `FILE...`, no files for every suite, `--fix`. |
| `yaml_lint_test.py` | Unit tests for the YAML layout linter (`mise run lint:yaml:test`). |
| `comment_check.py` | Flags comments added or changed in a diff, found with tree-sitter. PEP 723 script with one pinned wheel per language, so it needs no download at run time. Modes: `--staged`, `--range BASE HEAD`. |
| `comment_check_hook.sh` | Lefthook entry point: maps the checker's exit code 3 to a blocked commit and lets every other failure through. |
| `comment_check_test.py` | Unit tests for the comment check (`mise run lint:comments:test`). |
| `narration-comments.yml` | Lower-precision ast-grep-native fallback for the `block_label` half (see below). Committed for documentation / ast-grep-only setups; **not** the active gate. |

## Prose linter

Scope (the `SCOPE` tuple in `prose_lint.py`): `config/prompts/**/*.yml`,
`config/setup/setup.yaml`, `skills/**/*.md` (minus the generated
`query_language.md`), `AGENTS.md`, `CONTEXT.md`, `crates/*/AGENTS.md`,
`docs/pages/engineering/development/**/*.md` (agent docs and runbooks), `docs/pages/engineering/architecture/**/*.md`,
`docs/design-documents/**/*.md`, and the MR and issue templates. `CLAUDE.md` is skipped
because CI already enforces that it equals `AGENTS.md`. Public docs under
`docs/source/` stay with Vale.

Before scoring, fenced code, tables, headings, blockquotes (quoted people keep
their own words), link targets, template placeholders, and column-aligned label
lines are dropped. Inline code counts as
one word. HTML comment markers are removed but the comment text is scored,
because template guidance is written for agents. In yaml, every string scalar
is scored except `name`, `version`, `variables`, `license`, `metadata`,
`allowed-tools`, and `compatibility`.

Every finding is `file:line: rule: message`. Rules:

| Rule | Fails when | Why |
|---|---|---|
| `sentence` | a sentence has more than 25 words | ASD-STE100 caps descriptive sentences at 25 words; long instructions lower LLM adherence (IFScale) |
| `average` | a unit of 3+ sentences averages more than 20 words | STE procedural cap |
| `dash` | an em or en dash appears | the most reliable machine-writing tell (Wikipedia "Signs of AI writing") |
| `tell` | a word or phrase from the machine-writing lexicon appears | Kobak et al. 2025 excess vocabulary, Liang et al. 2024, Wikipedia; includes negative parallelism and `, ensuring ...` tails |
| `prompt` | all-caps shouting (`IMPORTANT`, `MUST`, ...) or filler (`in order to`, `make sure to`, `please`, ...) appears | Anthropic skill guidance prefers a reason over emphasis; filler dilutes the instruction budget |

Not adopted, on purpose: readability grades (Flesch-Kincaid counts syllables
and misreads identifiers; Coleman-Liau flags ordinary technical vocabulary),
passive-voice regexes (too many false positives at lint severity), burstiness
(unreliable under 50 words), and any transformer detector (seconds of cold
start and documented false positives on formal technical English).

```shell
mise run lint:prose -- --all
mise run lint:prose -- skills/orbit/SKILL.md
mise run lint:prose:test
```

## Comment check

`CONTRIBUTING.md` ("Engineering conventions") says "Comments explain why. Do not narrate what the code
shows." The check lists every comment a diff adds or changes, so the author
confirms each one is worth keeping. It covers `.rs`, `.yaml`, `.yml`, `.toml`,
`.sh`, `.py`, `.js`, `.mjs`, `.ts`, `.rb`, and `.css`.

A comment counts as changed when its normalized text (markers, whitespace, and
case removed) is not already in the old version of the file. Moves and
re-indents pass. Renames are followed (`git diff -M`), so a renamed file keeps
its old comments. Adjacent line comments are one comment.

Skipped on purpose: rustdoc (`///`, `//!`, `/** */`), tool directives
(`#!`, PEP 723 headers, `shellcheck`, `clippy`, `yamllint`, and similar), files with an
`@generated` marker in the first 2 KiB, fixture and snapshot paths, the
`code-graph-incremental/langs` suites, and commits that conclude a merge.

The hook blocks the commit and prints the comments, the rule with its link, and
a digest. To proceed after you have deleted the comments that only narrate,
re-run the commit with the digest:

```shell
COMMENTS_OK=<digest> git commit ...
```

The digest hashes the flagged paths and comment text, so a different set of
comments needs a new digest. To skip the check, set `COMMENTS_CHECK=0`, which
also works for the CI script. For the hook alone, lefthook's own
`LEFTHOOK_EXCLUDE=comments` skips the job too.

Exit codes of `comment_check.py`: 0 clean, acknowledged, or skipped; 3 comments
flagged; 2 internal error or a file skipped on error in `--range` mode (CI shows the job as failed, not
green). The hook runs `comment_check_hook.sh`, which blocks the commit only on
exit 3. Any other failure, including `uv` or `mise` not starting (no network
with a cold cache, an untrusted worktree) or an internal error in the
checker, prints a note and lets the commit through.

Known gaps: a rewrapped comment counts as changed, and shell comments inside
YAML block scalars are not seen, because the YAML grammar treats them as text.

```shell
mise run lint:comments:test
uv run --quiet scripts/linting/comment_check.py --range <base> HEAD
```

## YAML layout linter

Scope (the `SUITES` tuple in `yaml_lint.py`): the integration test suites under
`crates/integration-tests/tests`, the code-graph fixtures, and the query corpus
under `fixtures/queries`. The goal is suite files that are easy to read.

Prettier, pinned in `package.json` and configured in `.prettierrc.yaml`, sets
the indentation, the quotes, and the spacing in a line. The print width is
large, so Prettier does not wrap long flow maps such as seed rows.

The script then adds one rule. A blank line separates two sibling entries when
one of them spans more than one line. Siblings are the top-level keys and the
items of each top-level list, for example each case of a suite. A comment
directly above an entry stays with that entry.

```shell
mise run lint:yaml
mise run lint:yaml:fix
mise run lint:yaml:test
```

## Where the gates run

- **lefthook** `pre-commit` job `narration` (advisory; prints warnings, does not
  block — `|| true` in `lefthook.yml`), `comments`, `prose`, and `yaml-suites`
  (these three block the commit).
- **CI** jobs `lint:narration`, `lint:comments`, `lint:mr-description`,
  `lint:prose`, and `lint:yaml`, defined in
  [`.gitlab/ci/linting.yml`](../../.gitlab/ci/linting.yml). The first three use
  `allow_failure: true` (yellow/advisory); `lint:prose` and `lint:yaml` block. In merge-request
  pipelines the narration and prose jobs scope to files the MR changed
  (`--diff-base`).

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
mise exec -- ast-grep scan --rule scripts/linting/narration-comments.yml crates/
```

## Promoting a gate to blocking

Blocking-ness lives in config, not the scripts (the scripts always exit non-zero
on findings):

- **CI:** remove `allow_failure: true` from the job in `.gitlab/ci/linting.yml`.
- **lefthook:** remove the `|| true` suffix from the `narration` or `prose` job's
  `run:` in `lefthook.yml`.

## Removing the gates

The kill-switch is two deletes plus a few one-line reference removals (all
fail-loud, so nothing silently lingers):

```shell
rm -rf scripts/linting .gitlab/ci/linting.yml
# then remove:
#   - the `- local: .gitlab/ci/linting.yml` line in .gitlab-ci.yml
#   - the `narration`, `comments`, `prose`, and `yaml-suites` pre-commit jobs in lefthook.yml
#   - the `lint:comments*`, `lint:prose*`, and `lint:yaml*` tasks in mise.toml
#   - the `prettier` dev dependency and `.prettierrc.yaml`
```
