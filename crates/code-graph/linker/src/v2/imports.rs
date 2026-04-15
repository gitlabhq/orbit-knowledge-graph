//! Import resolution strategies.
//!
//! Each strategy is a function that takes a name and tries to resolve it
//! to definitions via a different lookup method. The resolver calls them
//! in the order specified by `ResolutionRules.import_strategies`.
//!
//! All strategies are deterministic: they construct candidate FQNs and
//! look them up in the FQN index. No bare-name guessing.

use code_graph_types::{CanonicalImport, CanonicalResult};

use super::context::DefRef;
use super::context::ResolutionContext;

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
            ImportStrategy::SameFile => same_file(ctx, file_idx, name),
            ImportStrategy::FilePath => vec![],
        };

        if !candidates.is_empty() {
            return candidates;
        }
    }

    vec![]
}

/// Resolve an import to definitions by FQN matching.
///
/// Constructs `"{path}{sep}{name}"` and looks up in the FQN index.
/// No bare-name fallback.
pub fn resolve_import(ctx: &ResolutionContext, import: &CanonicalImport, sep: &str) -> Vec<DefRef> {
    let symbol_name = import
        .alias
        .as_deref()
        .or(import.name.as_deref())
        .unwrap_or("");

    if symbol_name.is_empty() || import.wildcard {
        return vec![];
    }

    // Tier 1: construct full FQN from import path + symbol name.
    let full_fqn = if import.path.is_empty() {
        symbol_name.to_string()
    } else {
        format!("{}{}{}", import.path, sep, symbol_name)
    };

    let by_fqn = ctx.definitions.lookup_fqn(&full_fqn);
    if !by_fqn.is_empty() {
        return by_fqn.to_vec();
    }

    // Tier 2: the path itself might be the full FQN (e.g. `import com.example.Foo`).
    if !import.path.is_empty() {
        let by_path = ctx.definitions.lookup_fqn(&import.path);
        if !by_path.is_empty() {
            return by_path.to_vec();
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
    // Phase 1: try each top-level def's FQN as a prefix.
    for def in &result.definitions {
        if def.is_top_level {
            let candidate = format!("{}{}{}", def.fqn, sep, name);
            let matches = ctx.definitions.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }

    // Phase 2: walk up each def's FQN by stripping segments.
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
            let defs = resolve_import(ctx, imp, sep);
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

/// Same-file lookup by FQN: construct `"{file_def_fqn_prefix}.{name}"` for
/// each top-level definition in the file and check the FQN index.
fn same_file(ctx: &ResolutionContext, file_idx: usize, name: &str) -> Vec<DefRef> {
    // Direct FQN match: if `name` itself is a FQN of a def in this file.
    let by_fqn = ctx.definitions.lookup_fqn(name);
    let same_file: Vec<DefRef> = by_fqn
        .iter()
        .filter(|r| r.file_idx == file_idx)
        .copied()
        .collect();
    if !same_file.is_empty() {
        return same_file;
    }

    // Match by name among this file's definitions only.
    // This is deterministic: the name must match a definition's bare name
    // AND the definition must be in the same file.
    ctx.definitions
        .lookup_name(name)
        .iter()
        .filter(|r| r.file_idx == file_idx)
        .copied()
        .collect()
}
