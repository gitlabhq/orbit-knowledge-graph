use anyhow::Result;
use ontology::Ontology;

use super::Output;

/// `--kind` resolved against the ontology: definition kinds narrow matches, `File` and
/// `Directory` list where matches are, and hosted nodes such as `MergeRequest` are reported.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Kinds {
    pub(super) definitions: Vec<String>,
    /// How the header names the filter: `Definition` rather than every definition kind.
    pub(super) shown: Vec<String>,
    pub(super) output: Option<Output>,
    pub(super) hosted: Vec<String>,
    pub(super) unknown: Vec<String>,
}

const EVERYTHING: &[&str] = &["all", "any", "code", "everything", "*"];
const FILES: &[&str] = &["file", "files", "path", "paths"];
const DIRECTORIES: &[&str] = &[
    "dir",
    "dirs",
    "directory",
    "directories",
    "folder",
    "folders",
];
const DEFINITIONS: &[&str] = &[
    "def",
    "defs",
    "definition",
    "definitions",
    "symbol",
    "symbols",
];
const VALUE_KINDS: &[&str] = &["Variable", "Var", "Constant", "Const", "Static", "Local"];

/// Agent spellings for definition families, matched to kinds by `family_of`.
const FAMILIES: &[&[&str]] = &[
    &[
        "fn",
        "func",
        "function",
        "functions",
        "method",
        "methods",
        "callable",
    ],
    &[
        "type",
        "types",
        "class",
        "classes",
        "struct",
        "structs",
        "interface",
    ],
    &[
        "var",
        "vars",
        "variable",
        "variables",
        "field",
        "fields",
        "property",
    ],
];

/// The family of a definition kind: the shared repo-map lists first, then the name, so
/// labels such as `DecoratedClass` or a new language's kinds still land somewhere.
fn family_of(kind: &str) -> Option<usize> {
    use crate::commands::repo_map::{CALLABLE_KINDS, MEMBER_EXTRA_KINDS, TYPE_KINDS};
    let has = |parts: &[&str]| parts.iter().any(|part| kind.contains(part));
    if CALLABLE_KINDS.contains(&kind) || has(&["Function", "Method", "Constructor"]) {
        Some(0)
    } else if TYPE_KINDS.contains(&kind)
        || has(&[
            "Class",
            "Struct",
            "Interface",
            "Trait",
            "Record",
            "Object",
            "Type",
        ])
    {
        Some(1)
    } else if VALUE_KINDS.contains(&kind)
        || MEMBER_EXTRA_KINDS.contains(&kind)
        || has(&["Variable", "Field", "Property", "Constant"])
    {
        Some(2)
    } else {
        None
    }
}

pub(super) fn resolve(requested: &[String], known: &[String]) -> Result<Kinds> {
    let ontology = Ontology::load_embedded()?;
    let mut kinds = Kinds::default();
    let mut everything = false;
    for name in requested {
        let lower = name.to_lowercase();
        let singular = [lower.strip_suffix("es"), lower.strip_suffix('s')];
        let exact: Vec<String> = std::iter::once(lower.as_str())
            .chain(singular.into_iter().flatten())
            .find_map(|n| known.iter().find(|k| k.eq_ignore_ascii_case(n)))
            .cloned()
            .into_iter()
            .collect();
        let family: Vec<String> = FAMILIES
            .iter()
            .position(|aliases| aliases.contains(&lower.as_str()))
            .map(|family| {
                known
                    .iter()
                    .filter(|k| family_of(k) == Some(family))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let literal = known.iter().find(|k| *k == name);
        match lower.as_str() {
            _ if literal.is_some() => {
                kinds.shown.extend(literal.cloned());
                kinds.definitions.extend(literal.cloned());
            }
            n if EVERYTHING.contains(&n) => everything = true,
            n if FILES.contains(&n) => kinds.output = Some(Output::FileRows),
            n if DIRECTORIES.contains(&n) => kinds.output = Some(Output::Directories),
            n if DEFINITIONS.contains(&n) => {
                kinds.definitions.extend(known.iter().cloned());
                kinds.shown.push("Definition".into());
            }
            _ if !family.is_empty() => {
                kinds.shown.extend(family.clone());
                kinds.definitions.extend(family);
            }
            _ if !exact.is_empty() => {
                kinds.shown.extend(exact.clone());
                kinds.definitions.extend(exact);
            }
            _ => match ontology
                .node_names()
                .find(|node| node.eq_ignore_ascii_case(name))
            {
                Some(node) => kinds.hosted.push(node.to_string()),
                None => kinds.unknown.push(name.clone()),
            },
        }
    }
    if everything {
        kinds.definitions.clear();
        kinds.shown.clear();
    }
    for list in [&mut kinds.definitions, &mut kinds.shown] {
        list.sort();
        list.dedup();
    }
    Ok(kinds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_spellings_resolve_to_kinds_listings_and_hosted_nodes() {
        let known: Vec<String> = [
            "Class",
            "Method",
            "DecoratedFunction",
            "Type",
            "Var",
            "Field",
        ]
        .map(String::from)
        .to_vec();
        let resolved = |names: &[&str]| {
            let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
            resolve(&names, &known).unwrap()
        };
        assert_eq!(
            resolved(&["function"]).definitions,
            ["DecoratedFunction", "Method"]
        );
        assert_eq!(resolved(&["classes"]).definitions, ["Class", "Type"]);
        assert_eq!(resolved(&["variable"]).definitions, ["Field", "Var"]);
        assert_eq!(resolved(&["Method"]).definitions, ["Method"]);
        assert!(resolved(&["class", "all"]).definitions.is_empty());
        assert_eq!(resolved(&["file"]).output, Some(Output::FileRows));
        let kinds = resolved(&["MergeRequest", "call"]);
        assert_eq!(
            (kinds.hosted, kinds.unknown),
            (vec!["MergeRequest".into()], vec!["call".into()])
        );
    }
}
