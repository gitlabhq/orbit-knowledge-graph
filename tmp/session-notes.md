# Session notes — orbit skill [path] (knowledge-graph#1000)

## Deliverable
- Draft MR !2032: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2032
- Branch `feat/orbit-skill-subcommand` → target `feat/orbit-local-skill` (chained on !2029).
- Commit 01c4345e.

## Design decision: compile-time embed (include_str!), NOT archive-bundling
- Content embedded in the binary → present + version-matched for all 3 install
  methods (glab orbit local, tarball download, dev build) with zero packaging.
- build.rs walks skills/orbit-local/ and generates SKILL_FILES table into OUT_DIR
  → manifest cannot drift from the on-disk tree.
- repo_map.py path concern mitigated by `orbit skill scripts/repo_map.py > /tmp/x.py`
  (+ ORBIT_CMD=orbit, already parameterized). Docs updated to show this.
- Archive-bundling rejected: 3 independent runtime path-resolution paths + release
  script/install.sh changes = fragile, silently breaks per install method.

## Files
- .cargo/config.toml: + SKILLS_DIR
- crates/orbit-local/build.rs: generate_skill_files()
- crates/orbit-local/src/skill.rs (new): run(), is_safe_relative() traversal guard, unit tests
- crates/orbit-local/src/main.rs: Skill clap variant + dispatch
- crates/orbit-local/src/descriptions.rs: SKILL_SHORT
- crates/integration-tests/tests/cli.rs: 2 integration tests
- skills/orbit-local/{SKILL.md,references/cli.md,references/repo_map.md}: docs; version 0.1.0→0.2.0

## Verification (local)
- orbit-local unit tests: 23 pass (5 new skill tests)
- full --test cli suite: 22 pass (2 new)
- cargo fmt --check: clean; clippy -p orbit-local --all-targets -D warnings: clean
- skill-version-bump-check: pass
- markdownlint/lychee on skill docs: clean (3 MD044 in sql.md/SKILL.md are pre-existing from !2029, and skills/ isn't in CI's markdownlint glob)
- AGENTS.md == CLAUDE.md (untouched)

## Did NOT do
- No archive/packaging changes (rejected alternative).
- No agent-discovery / `orbit skills install` (issue scopes it out).
- Did not run full `mise test:fast` workspace suite locally (heavy, unrelated) — CI covers it.
- Local builds needed `eval "$(mise env)"` + ~/.cargo/bin on PATH because the mise
  rust shim install failed (core:rust postinstall sh exit 127); rustup toolchain 1.95.0
  is present and honored via rust-toolchain.toml. Non-bundled duckdb download works
  once full mise env is applied.

## Next
- Waiting for review feedback on this session. CI running on !2032.
