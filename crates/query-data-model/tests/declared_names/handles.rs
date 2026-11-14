//! Engine code reads model facts by handle once the frontend has resolved names: no by-name
//! resolver, no test fixture, no storage kind decoding outside the one codec, no identity column
//! or connection name assumed by spelling, and no branching on a backend kind outside the code
//! that builds a connection's catalog or executor.

use std::path::Path;

use crate::lexer::Token;
use crate::modules::{Unit, units};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    Names,
    Testkit,
    KindCodec,
    IdentityColumn,
    ConnectionName,
    BackendKind,
}

pub struct Exempt {
    pub file: &'static str,
    pub rule: Rule,
    pub reason: &'static str,
}

const fn exempt(file: &'static str, rule: Rule, reason: &'static str) -> Exempt {
    Exempt { file, rule, reason }
}

pub const ENGINE: [&str; 4] = [
    "crates/query-engine/compiler",
    "crates/query-engine/shared",
    "crates/orbit-server",
    "crates/duckdb-client",
];

const BACKENDS: [&str; 4] = ["ClickHouse", "DuckDb", "OrbitCode", "Live"];
const BACKEND_KINDS: [&str; 4] = ["clickhouse", "duckdb", "orbit_code", "live"];

#[rustfmt::skip]
pub const EXEMPT: &[Exempt] = &[
    exempt("crates/query-engine/shared/src/hydration.rs", Rule::KindCodec, "the one storage codec: decode_kind turns stored kind values into entity handles"),
    exempt("crates/duckdb-client/src/search.rs", Rule::Names, "the CLI's local search resolves the entity the user names"),
    exempt("crates/duckdb-client/src/search.rs", Rule::IdentityColumn, "the CLI's local search reads the id column its own SELECT aliases"),
    exempt("crates/duckdb-client/src/search.rs", Rule::ConnectionName, "the CLI's local search opens the local deployment's catalog"),
    exempt("crates/query-engine/compiler/src/dag/connections.rs", Rule::BackendKind, "Peer::of builds each connection's catalog by its backend kind"),
    exempt("crates/query-engine/compiler/src/dag/mod.rs", Rule::BackendKind, "the wire names of the stage backend label, which plan output prints"),
    exempt("crates/query-engine/compiler/src/passes/scope_lookup.rs", Rule::IdentityColumn, "the scope lookup's key column; the placement rework replaces it"),
    exempt("crates/query-engine/compiler/src/constants.rs", Rule::IdentityColumn, "an internal column name of the compiler's own id-set relations, not an entity's identity"),
    exempt("crates/orbit-server/src/content/gitaly/mr_diff.rs", Rule::IdentityColumn, "the diff lookup reads the key properties its declaration lists"),
    exempt("crates/orbit-server/src/health_check.rs", Rule::BackendKind, "the health report's component name for the ClickHouse instance"),
    exempt("crates/orbit-server/src/health_client.rs", Rule::BackendKind, "the health report's component name for the ClickHouse instance"),
    exempt("crates/orbit-server/src/cluster_health.rs", Rule::BackendKind, "the health report's component name for the ClickHouse instance"),
    exempt("crates/query-engine/compiler/src/passes/plan/remote/code.rs", Rule::BackendKind, "names the backend in unsupported-shape errors"),
    exempt("crates/query-engine/compiler/src/passes/plan/remote/live.rs", Rule::BackendKind, "names the backend in unsupported-shape errors"),
];

/// The rules a file's non-test tokens break, once per rule.
fn broken(tokens: &[Token], connections: &[String]) -> Vec<Rule> {
    let ident = |at: usize, word: &str| matches!(tokens.get(at), Some(Token::Ident(found)) if found == word);
    let punct = |at: usize, c: char| tokens.get(at) == Some(&Token::Punct(c));
    let mut rules = Vec::new();
    for (at, token) in tokens.iter().enumerate() {
        let rule = match token {
            Token::Punct('.')
                if ident(at + 1, "names") && punct(at + 2, '(') && punct(at + 3, ')') =>
            {
                Some(Rule::Names)
            }
            Token::Ident(word) if word == "testkit" && punct(at + 1, ':') && punct(at + 2, ':') => {
                Some(Rule::Testkit)
            }
            Token::Ident(word) if word == "entity_kind" && punct(at + 1, '(') => {
                Some(Rule::KindCodec)
            }
            Token::Ident(word)
                if BACKENDS.contains(&word.as_str())
                    && punct(at + 1, ':')
                    && punct(at + 2, ':')
                    && ident(at + 3, "KIND") =>
            {
                Some(Rule::BackendKind)
            }
            Token::Str(text, _) if text == "id" => Some(Rule::IdentityColumn),
            Token::Str(text, _) if connections.contains(text) => Some(Rule::ConnectionName),
            Token::Str(text, _) if BACKEND_KINDS.contains(&text.as_str()) => {
                Some(Rule::BackendKind)
            }
            _ => None,
        };
        if let Some(rule) = rule
            && !rules.contains(&rule)
        {
            rules.push(rule);
        }
    }
    rules
}

/// Each `file: rule` the engine's non-test code breaks without an exemption, and each exemption
/// that covers nothing.
pub fn violations(repository: &Path, crates: &[&str], connections: &[String]) -> Vec<String> {
    let scanned: Vec<Unit> = crates
        .iter()
        .flat_map(|dir| units(&repository.join(dir)))
        .collect();
    let mut used = vec![false; EXEMPT.len()];
    let mut found = Vec::new();
    for unit in &scanned {
        let file = unit
            .file
            .strip_prefix(repository)
            .unwrap()
            .to_string_lossy();
        for rule in broken(&unit.tokens, connections) {
            match EXEMPT
                .iter()
                .position(|exempt| exempt.file == file && exempt.rule == rule)
            {
                Some(index) => used[index] = true,
                None => found.push(format!("{file}: {rule:?}")),
            }
        }
    }
    let stale = EXEMPT
        .iter()
        .zip(used)
        .filter(|(exempt, used)| {
            !used
                && crates
                    .iter()
                    .any(|dir| exempt.file.starts_with(&format!("{dir}/")))
        })
        .map(|(exempt, _)| {
            format!(
                "{}: {:?} exempt but clean ({})",
                exempt.file, exempt.rule, exempt.reason
            )
        });
    found.extend(stale);
    found
}
