//! Subcommand and MCP tool descriptions, compiled by `build.rs` from the
//! versioned YAML prompts under `config/prompts/local/`. Each module exposes
//! `SHORT` (CLI `--help`) and, for MCP tools, `DESCRIPTION` (the agent-facing
//! text, validated at build time to extend `SHORT`).

include!(concat!(env!("OUT_DIR"), "/local_prompts.rs"));
