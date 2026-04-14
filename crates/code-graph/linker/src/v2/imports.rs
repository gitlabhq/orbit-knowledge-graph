//! Import resolution strategies.
//!
//! Each strategy is a function that takes a name and tries to resolve it
//! to definitions via a different lookup method. The resolver calls them
//! in the order specified by `ResolutionRules.import_strategies`.

use code_graph_types::{CanonicalImport, CanonicalResult};

use super::context::DefRef;
use super::context::ResolutionContext;

/// Maximum number of candidates from bare-name lookup before
/// considering the result ambiguous.
const MAX_BARE_NAME_CANDIDATES: usize = 3;

/// Apply import strategies in order, returning the first non-empty result.
pub fn apply(
    strategies: &[super::rules::ImportStrategy],
    ctx: &ResolutionContext,
    file_idx: usize,
    name: &str,
    sep: &str,
) -> Vec<DefRef> {
    use super::rules::ImportStrategy;

    let result = &ctx.results[file_idx];

    for strategy in strategies {
        let candidates = match strategy {
            ImportStrategy::ScopeFqnWalk => scope_fqn_walk(ctx, result, name, sep),
            ImportStrategy::ExplicitImport => explicit_import(ctx, file_idx, name, sep),
            ImportStrategy::WildcardImport => wildcard_import(ctx, file_idx, name, sep),
            ImportStrategy::SamePackage => same_package(ctx, result, name, sep),
            ImportStrategy::SameFile => ctx
                .definitions
                .lookup_name(name)
                .iter()
                .filter(|r| r.file_idx == file_idx)
                .copied()
                .collect(),
            ImportStrategy::GlobalName { max_candidates } => {
                let candidates = ctx.definitions.lookup_name(name);
                if candidates.len() <= *max_candidates {
                    candidates.to_vec()
                } else {
                    vec![]
                }
            }
            ImportStrategy::FilePath => vec![],
        };

        if !candidates.is_empty() {
            return candidates;
        }
    }

    vec![]
}

/// Resolve an import to definitions by FQN matching.
pub fn resolve_import(
    ctx: &ResolutionContext,
    import: &CanonicalImport,
    sep: &str,
    bare_name_fallback: bool,
) -> Vec<DefRef> {
    let symbol_name = import
        .alias
        .as_deref()
        .or(import.name.as_deref())
        .unwrap_or("");

    if symbol_name.is_empty() || import.wildcard {
        return vec![];
    }

    let full_fqn = if import.path.is_empty() {
        symbol_name.to_string()
    } else {
        format!("{}{}{}", import.path, sep, symbol_name)
    };

    let by_fqn = ctx.definitions.lookup_fqn(&full_fqn);
    if !by_fqn.is_empty() {
        return by_fqn.to_vec();
    }

    if !import.path.is_empty() {
        let by_path = ctx.definitions.lookup_fqn(&import.path);
        if !by_path.is_empty() {
            return by_path.to_vec();
        }
    }

    if bare_name_fallback {
        let by_name = ctx.definitions.lookup_name(symbol_name);
        if by_name.len() <= MAX_BARE_NAME_CANDIDATES {
            return by_name.to_vec();
        }
    }

    vec![]
}

fn scope_fqn_walk(
    ctx: &ResolutionContext,
    result: &CanonicalResult,
    name: &str,
    sep: &str,
) -> Vec<DefRef> {
    for def in &result.definitions {
        if def.is_top_level {
            let candidate = format!("{}{}{}", def.fqn, sep, name);
            let matches = ctx.definitions.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }

    for def in &result.definitions {
        let fqn_str = def.fqn.to_string();
        let mut current = fqn_str.as_str();
        loop {
            let candidate = format!("{}{}{}", current, sep, name);
            let matches = ctx.definitions.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
            match current.rfind(sep) {
                Some(pos) => current = &current[..pos],
                None => break,
            }
        }
    }

    vec![]
}

fn explicit_import(ctx: &ResolutionContext, file_idx: usize, name: &str, sep: &str) -> Vec<DefRef> {
    let result = &ctx.results[file_idx];
    for imp in &result.imports {
        let imp_name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
        if imp_name == name {
            let defs = resolve_import(ctx, imp, sep, false);
            if !defs.is_empty() {
                return defs;
            }
        }
    }
    vec![]
}

fn wildcard_import(ctx: &ResolutionContext, file_idx: usize, name: &str, sep: &str) -> Vec<DefRef> {
    let result = &ctx.results[file_idx];
    for imp in &result.imports {
        if imp.wildcard {
            let candidate = format!("{}{}{}", imp.path, sep, name);
            let matches = ctx.definitions.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }
    vec![]
}

fn same_package(
    ctx: &ResolutionContext,
    result: &CanonicalResult,
    name: &str,
    sep: &str,
) -> Vec<DefRef> {
    for def in &result.definitions {
        if def.is_top_level {
            let fqn_str = def.fqn.to_string();
            if let Some(sep_pos) = fqn_str.rfind(sep) {
                let pkg = &fqn_str[..sep_pos];
                let candidate = format!("{}{}{}", pkg, sep, name);
                let matches = ctx.definitions.lookup_fqn(&candidate);
                if !matches.is_empty() {
                    return matches.to_vec();
                }
            }
        }
    }
    vec![]
}
