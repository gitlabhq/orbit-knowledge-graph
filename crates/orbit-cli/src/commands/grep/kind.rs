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
const FAMILIES: &[(&[&str], &[&str])] = &[
    (
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
            "Function",
            "AsyncFunction",
            "AssociatedFunction",
            "Method",
            "Constructor",
        ],
    ),
    (
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
            "Class",
            "Struct",
            "Interface",
            "Trait",
            "Enum",
            "Record",
            "TypeAlias",
        ],
    ),
    (
        &[
            "var",
            "vars",
            "variable",
            "variables",
            "field",
            "fields",
            "property",
        ],
        &["Variable", "Field", "Property", "Constant", "Const"],
    ),
];

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
            .find(|(aliases, _)| aliases.contains(&lower.as_str()))
            .map(|(_, members)| {
                known
                    .iter()
                    .filter(|k| members.iter().any(|m| k.eq_ignore_ascii_case(m)))
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

    fn resolved(requested: &[&str]) -> Kinds {
        let known = ["Class", "Method", "Constructor", "Field", "Interface"].map(String::from);
        let requested: Vec<String> = requested.iter().map(|k| k.to_string()).collect();
        resolve(&requested, &known).unwrap()
    }

    #[test]
    fn agent_spellings_map_to_repository_kinds() {
        assert_eq!(
            resolved(&["function"]).definitions,
            ["Constructor", "Method"]
        );
        assert_eq!(resolved(&["Method"]).definitions, ["Method"]);
        assert_eq!(resolved(&["classes"]).definitions, ["Class", "Interface"]);
        assert_eq!(resolved(&["constructors"]).definitions, ["Constructor"]);
        assert_eq!(resolved(&["type"]).definitions, ["Class", "Interface"]);
        assert_eq!(resolved(&["def"]).definitions.len(), 5);
        assert!(resolved(&["class", "all"]).definitions.is_empty());
    }

    #[test]
    fn files_directories_and_hosted_nodes_are_not_definition_filters() {
        assert_eq!(resolved(&["file"]).output, Some(Output::FileRows));
        assert_eq!(resolved(&["Directory"]).output, Some(Output::Directories));
        let kinds = resolved(&["MergeRequest", "call"]);
        assert_eq!(kinds.hosted, ["MergeRequest"]);
        assert_eq!(kinds.unknown, ["call"]);
    }
}
