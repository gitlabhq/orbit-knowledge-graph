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
| `narration-comments.yml` | Lower-precision ast-grep-native fallback for the `block_label` half (see below). Committed for documentation / ast-grep-only setups; **not** the active gate. |

## Prose linter

Scope (the `SCOPE` tuple in `prose_lint.py`): `config/prompts/**/*.yml`,
`config/setup/setup.yaml`, `skills/**/*.md` (minus the generated
`query_language.md`), `AGENTS.md`, `CONTEXT.md`, `crates/*/AGENTS.md`,
`docs/dev/agents-*.md`, and the MR and issue templates. `CLAUDE.md` is skipped
because CI already enforces that it equals `AGENTS.md`.

Before scoring, fenced code, tables, headings, link targets, template
placeholders, and column-aligned label lines are dropped. Inline code counts as
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

## Where the gates run

- **lefthook** `pre-commit` jobs `narration` and `prose` (advisory; print
  warnings, do not block — `|| true` in `lefthook.yml`).
- **CI** jobs `lint:narration`, `lint:mr-description`, and `lint:prose`, defined in
  [`.gitlab/ci/linting.yml`](../../.gitlab/ci/linting.yml). All three use
  `allow_failure: true` (yellow/advisory). In merge-request pipelines the
  narration and prose jobs scope to files the MR changed (`--diff-base`).

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
#   - the `narration` and `prose` pre-commit jobs in lefthook.yml
#   - the `lint:prose` and `lint:prose:test` tasks in mise.toml
```
