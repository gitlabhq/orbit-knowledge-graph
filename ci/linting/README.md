# Linting

These Python gates check narration comments, MR-description headlines, and prose.
They use the frozen dependencies in `ci/uv.lock`.

## Contents

| File | Role |
|---|---|
| `narration_score.py` | Active narration-comment scorer (two high-precision detectors: `block_label`, `token_overlap`). Dependency-free Python. |
| `check_narration.py` | Runs the narration scorer in-process: whole-tree, explicit-files (`{staged_files}`), and MR changed-line-only (`--diff-base <sha>`) modes. |
| `score_description.py` | MR-description headline-section scorer (word / code-span / bare-identifier caps). |
| `check_mr_description.py` | Reads `CI_MERGE_REQUEST_DESCRIPTION` and scores its headline in-process. |
| `prose_lint.py` | Prose linter for the text LLMs read. Uses PyYAML from the shared `ci` project. Modes: `FILE...`, `--all`, `--diff-base <sha>`. |
| `../tests/prose_test.py` | Prose linter tests. |
| `../tests/linting_test.py` | CLI tests for narration and MR descriptions, using a temporary Git repository. |
| `narration-comments.yml` | Lower-precision ast-grep fallback for `block_label`. |

## Commands

Run the Python entrypoints through the shared frozen `ci` project:

```shell
mise exec -- uv run --frozen --project ci python ci/linting/check_narration.py
mise exec -- uv run --frozen --project ci python ci/linting/check_narration.py --diff-base origin/main
mise exec -- uv run --frozen --project ci python ci/linting/check_mr_description.py
mise ci:test
```

`mise ci:test` runs the pytest suite in `ci/tests/`, configured in `ci/pyproject.toml`.

Narration file arguments are repository-relative paths. Quote paths that contain spaces.
Missing files and non-Rust paths are skipped. An unreachable diff base fails with exit 2.
Findings return exit 1; CI and lefthook control whether those findings block a change.
MR descriptions are skipped outside MR pipelines, when empty, or when truncated without a headline boundary.

## Prose linter

The `SCOPE` tuple in `prose_lint.py` covers prompts, setup YAML, skills, agent
guides, developer docs, design docs, and MR and issue templates.
It skips the generated skill reference `query_language.md` and superseded designs
under `previous_design/`. CI checks that `CLAUDE.md` equals `AGENTS.md`, so prose
lint skips the copy. Public docs under `docs/source/` use Vale.

Before scoring, fenced code, tables, headings, blockquotes (quoted people keep
their own words), link targets, template placeholders, and column-aligned label
lines are dropped. Inline code counts as
one word. HTML comment markers are removed but the comment text is scored,
because template guidance is written for agents. In YAML, every string scalar
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

```shell
mise run lint:prose -- --all
mise run lint:prose -- skills/orbit/SKILL.md
mise exec -- uv run --frozen --project ci pytest ci/tests/prose_test.py
```

## Where the gates run

- lefthook runs `narration` as advisory (`|| true`) and `prose` as blocking.
- CI runs narration and description in `advisory-checks` (`allow_failure: true`).
  Prose runs in `repository-checks` and blocks on failure.
  MR pipelines scope narration and prose to changed files with `--diff-base`.

See [`lefthook.yml`](../../lefthook.yml) and
[`.gitlab/ci/linting.yml`](../../.gitlab/ci/linting.yml) for gate settings.

## ast-grep fallback (`narration-comments.yml`)

The fallback checks comment openers. It cannot compare comment tokens with the
next code line or apply the Python scorer's multi-line-block exemption.
Use the Python gate for fewer false positives.

```shell
mise exec -- ast-grep scan --rule ci/linting/narration-comments.yml crates/
```
