//! Subcommand implementations kept out of the top-level module list.

pub(crate) mod config;
pub(crate) mod context;
pub(crate) mod fqn;
pub(crate) mod grep;
pub(crate) mod hook_guard;
pub(crate) mod repo_map;
pub(crate) mod setup;

pub(crate) fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
