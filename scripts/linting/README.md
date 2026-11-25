# linting

Deterministic lint gates that catch mechanical review feedback (code comments,
bloated MR-description headlines, machine-sounding prose) before it
reaches a human reviewer. Everything for these gates is self-contained in this
folder so it can be removed as a unit (see *Removing the gates* below). Task #2933.

## Contents

| File | Role |
|---|---|
| `score_description.py` | MR-description headline-section scorer (word / code-span / bare-identifier caps). |
| `check-mr-description.sh` | Reads the description from the predefined `CI_MERGE_REQUEST_DESCRIPTION` variable and runs the scorer. |
| `prose_lint.py` | Prose linter for the text LLMs read. PEP 723 script (`uv run`, PyYAML only). Modes: `FILE...`, `--all`, `--diff-base <sha>`. |
| `prose_lint_test.py` | Unit tests for the prose linter (`mise run lint:prose:test`). |
| `yaml_lint.py` | Layout linter for the YAML test suites: Prettier plus a blank-line rule. PEP 723 script (`uv run`, PyYAML only). Modes: `FILE...`, no files for every suite, `--fix`. |
| `yaml_lint_test.py` | Unit tests for the YAML layout linter (`mise run lint:yaml:test`). |
| `comments/` | Comment ban: one ast-grep rule for each language, the checker, and the ledger. |

## Prose linter

Scope (the `SCOPE` tuple in `prose_lint.py`): `config/prompts/**/*.yml`,
`config/setup/setup.yaml`, `skills/**/*.md`, `AGENTS.md`, `CONTEXT.md`, `crates/*/AGENTS.md`,
`docs/pages/engineering/development/**/*.md` (agent docs and runbooks), `docs/pages/engineering/architecture/**/*.md`,
`docs/design-documents/**/*.md`, and the MR and issue templates. Public docs under
`docs/docs-gitlab-com/` stay with Vale.

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

## Comment ban

Code comments are not allowed. Each rule in `comments/rules/` finds the comment
nodes of one language with ast-grep: Rust (also `///`, `//!`, and `#[doc]`),
YAML, shell, Python (also docstrings), TypeScript, TSX, JavaScript, Ruby, and
CSS. The check runs on every tracked file, also in hidden directories. A
shebang on line 1, a PEP 723 `# /// script` block at the top of a Python file,
and a tool directive pass. A directive passes only in its exact form, for
example `# shellcheck disable=SC2086`. A suppression may end with a reason:
`// ast-grep-ignore: <rule-id> -- <reason>`, because the log-safety lint asks
for one. The list is `DIRECTIVES` in `check.py`.

Not covered, because the check has no rule for the language: TOML, SQL,
protobuf, Jsonnet, Jinja templates, HCL, Dockerfiles, Robot Framework files,
shell scripts without a `.sh` extension, and shell code inside YAML block
scalars. The check finds comment nodes only, so prose in strings, constants, and
attribute arguments (for example a lint `reason`) is not checked.

`comments/check.py` runs the rules on the whole tree and applies the ledger in
`comments/ledger/`: one YAML file per code area (`crates/<crate>.yaml`,
`scripts.yaml`, `globs.yaml`, ...). Each file maps a path to its entries:

```yaml
crates/orbit-server/src/auth/validator.rs:
- category: security
  reason: Only HS256 is safe here; RSA keys hit RUSTSEC-2023-0071
  preview: "// SECURITY: Only HS256 (HMAC-SHA256) is supported…"
  hash: 0a6739661bf2be35
  lines: 3
```

- `category` is one of: safety, security, compliance, upstream, contract,
  invariant, magic-value, known-gap, tool-read, generated, vendored, test-data.
- `reason` says why no change to the code can replace the comment.
- `hash` and `lines` match one comment block of that many consecutive comments,
  only while every line is unchanged. An edit fails the check until the entry is
  updated in the same MR. `preview` is the first line, for readers.
- An entry without `hash` exempts every comment in the files that its path glob
  matches, for example generated protos and fixtures. `rules` limits an entry to
  some `comments-*` rules.
- An entry that exempts no comment fails the check. Code owners approve each
  change.

Print a ready entry for the comment block at a line:

```shell
mise run lint:comments -- --entry crates/example/src/lib.rs:42
```

Inline suppression of a `comments-*` rule fails the check. On failure the check
prints each comment with a hint for its root-cause fix, then the steps and the
ledger format.

```shell
mise run lint:comments
mise run lint:comments:test
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

- **lefthook** `pre-commit` jobs `comments`, `prose`, and `yaml-suites` block
  the commit.
- **CI** jobs `lint:comments`, `lint:mr-description`, `lint:prose`, and
  `lint:yaml`, defined in [`.gitlab/ci/linting.yml`](../../.gitlab/ci/linting.yml).
  `lint:mr-description` uses `allow_failure: true` (yellow/advisory); the
  other jobs block.

## Promoting a gate to blocking

Blocking-ness lives in config, not the scripts (the scripts always exit non-zero
on findings):

- **CI:** remove `allow_failure: true` from the job in `.gitlab/ci/linting.yml`.

## Removing the gates

The kill-switch is two deletes plus a few one-line reference removals (all
fail-loud, so nothing silently lingers):

```shell
rm -rf scripts/linting .gitlab/ci/linting.yml
# then remove:
#   - the `- local: .gitlab/ci/linting.yml` line in .gitlab-ci.yml
#   - the `comments`, `prose`, and `yaml-suites` pre-commit jobs in lefthook.yml
#   - the `lint:comments*`, `lint:prose*`, and `lint:yaml*` tasks in mise.toml
#   - the `prettier` dev dependency and `.prettierrc.yaml`
```
