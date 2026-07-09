### What does this MR do and why?

Adds an `orbit skill` command so the local CLI can print its own bundled skill
content — the manifest, reference docs, and the repo-map script — on demand.
This guarantees an installed binary always has a version-matched copy of the
skill, no matter how it was installed, without any extra packaging step.

### Related Issues

Related to gitlab-org/orbit/knowledge-graph#1000. (Deliberately not `Closes` —
the issue author will decide when to close.)

Chained on top of !2029 (`feat/orbit-local-skill`); GitLab will retarget this to
`main` once that merges.

### Testing

- `orbit skill`, `orbit skill references/sql.md`, and `orbit skill scripts/repo_map.py` print the expected content; an unknown/escaping path exits non-zero with a helpful list of valid files.
- New unit tests in the crate and two `mise test:cli` integration tests cover the happy paths, the no-arg == `SKILL.md` invariant, and rejection of unknown/traversal/absolute paths.
- `cargo fmt --check`, `cargo clippy -p orbit-local --all-targets` (warnings as errors), and the full `--test cli` suite (22 tests) pass locally.

### Performance Analysis

- [x] This merge request does not introduce any performance regression. Embedding a few small text files adds negligible binary size and no runtime cost on the hot paths.

<details>
<summary><b>Agent context</b> — long-form analysis, file-by-file walkthroughs, profiler output, alternatives considered</summary>

#### Design decision: compile-time embed (not archive-bundling)

The issue flagged an open question — serve the skill from files bundled next to
the binary, or embed at compile time with `include_str!`. I chose **compile-time
embed**.

**Why embed wins the stated goal** ("always version-matched and available
everywhere the binary is, regardless of install method"):

- The content lives *inside* the binary, so it is present and correct for all
  three install methods with zero packaging work: `glab orbit local` (managed
  binary), a direct release-tarball download, and a dev build. There is nothing
  to resolve at runtime and nothing that can be missing.
- It is version-matched by construction: the bytes are stamped in at the same
  build that stamps `ORBIT_VERSION`.
- It reuses the crate's existing pattern — `main.rs`/`list.rs` already do
  `include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"))`. I added a
  parallel `SKILLS_DIR` entry in `.cargo/config.toml`.

**Why archive-bundling loses here:** it would require correct binary-relative
path resolution across three *independent* code paths — the glab-managed cache
dir, a tarball unpacked by `install.sh`, and `target/debug` for dev builds —
plus changes to `scripts/upload-local-cli-release.sh`, the CI release job, and
`install.sh`. Any one of those getting the relative path wrong silently breaks
`orbit skill` for that install method. That is a lot of fragility for no benefit
over embedding.

**The `repo_map.py`-needs-a-filesystem-path concern** (the issue author's reason
for leaning toward bundling) is fully handled: `orbit skill scripts/repo_map.py`
prints to stdout, so a caller reconstitutes a path anywhere with
`orbit skill scripts/repo_map.py > /tmp/repo_map.py`, and the script already
honors `ORBIT_CMD=orbit`. The docs now show exactly this.

#### No manifest drift

Rather than hand-maintaining the list of embedded files (which could drift from
what is actually on disk), `build.rs` walks `skills/orbit-local/` at build time
and generates a `SKILL_FILES: &[(&str, &str)]` table of
`(relative_path, include_str!(abs_path))` into `OUT_DIR`. `orbit skill` reads
that table directly, so the set it can serve and the "available files" list it
prints on error are always exactly what the tree contains. `cargo:rerun-if-changed`
is emitted for the skill root and each file.

#### File-by-file

- `.cargo/config.toml` — add `SKILLS_DIR = "skills"` (workspace-relative), matching the existing `CONFIG_DIR` pattern.
- `crates/orbit-local/build.rs` — `generate_skill_files()` walks the skill dir and emits `OUT_DIR/skill_files.rs`; keeps the existing version/rpath logic untouched.
- `crates/orbit-local/src/skill.rs` — new module. `include!`s the generated table; `run(Option<String>)` prints `SKILL.md` by default or a validated relative path; `is_safe_relative()` rejects empty/absolute/backslash paths and any `.`/`..`/empty component before lookup, so absolute paths and traversal cannot escape the skill root. Unknown paths `bail!` with the sorted list of valid files (non-zero exit). Unit tests cover embedding, defaulting, rejection.
- `crates/orbit-local/src/main.rs` — register the `skill` module, add the `Skill { path: Option<String> }` clap variant with short + long help, dispatch to `skill::run`.
- `crates/orbit-local/src/descriptions.rs` — add `SKILL_SHORT`, consistent with the other `*_SHORT` constants.
- `crates/integration-tests/tests/cli.rs` — `skill_serves_bundled_content` and `skill_rejects_unknown_and_escaping_paths`.
- `skills/orbit-local/{SKILL.md,references/cli.md,references/repo_map.md}` — document `orbit skill [PATH]` as the version-matched access path and show the `> /tmp/repo_map.py` recipe with `ORBIT_CMD=orbit`. SKILL.md version bumped `0.1.0 → 0.2.0` to satisfy `skill-version-bump-check`.

#### What I did NOT do

- No archive-bundling / packaging changes (`install.sh`, release scripts) — that is the rejected alternative.
- No agent-discovery/`orbit skills install` mechanism — the issue explicitly scopes that out; `orbit skill [path]` is the access path, not the discovery mechanism.
- Did not run the full `mise test:fast` workspace suite locally (heavy and unrelated to this change); ran the `orbit-local` unit tests and the full `--test cli` suite instead. Relying on CI for the rest.

%{all_commits}

</details>

/label ~"devops::analytics" ~"section::analytics" ~"knowledge graph" ~"group::context-systems"
/label ~"type::feature"

/assign me
