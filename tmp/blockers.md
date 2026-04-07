# Blockers

## Toolchain bootstrap timeout

Running Rust commands via `mise exec -- cargo ...` is currently dominated by first-time installation of Rust and several cargo tools configured by the repo environment. Commands are timing out before compile/test output is reached.

### Needed next
- Re-run:
  - `mise exec -- cargo fmt`
  - `mise exec -- cargo check -p migration-framework`
  - `mise exec -- cargo test -p migration-framework`
- Then fix compile/test failures, commit, push, create MR, and post issue note.
