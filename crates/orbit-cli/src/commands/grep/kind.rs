use anyhow::Result;
use ontology::Ontology;

use super::Output;

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
pub(super) fn languages(requested: &mut Vec<String>) -> Vec<String> {
    use code_graph_incremental::treesitter::{SupportLang, all_languages};
    let mut builder = ignore::types::TypesBuilder::new();
    builder.add_defaults();
    let definitions = builder.definitions();
    let search_type = |name: &str| {
        if definitions.iter().any(|def| def.name() == name) {
            return Some(name.to_string());
        }
        let language = SupportLang::from_alias(name).or_else(|| SupportLang::from_family(name))?;
        let (_, entry) = all_languages().find(|(lang, _)| *lang == language)?;
        let glob = format!("*.{}", entry.extensions().first()?);
        definitions
            .iter()
            .find(|def| def.globs().contains(&glob))
            .map(|def| def.name().to_string())
    };
    let mut types = Vec::new();
    requested.retain(|name| {
        let lower = name.to_lowercase();
        let keyword = [EVERYTHING, FILES, DIRECTORIES, DEFINITIONS]
            .iter()
            .chain(FAMILIES.iter())
            .any(|list| list.contains(&lower.as_str()));
        match (keyword, search_type(&lower)) {
            (false, Some(ty)) => {
                types.push(ty);
                false
            }
            _ => true,
        }
    });
    types.sort();
    types.dedup();
    types
}

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
    &["class", "classes", "struct", "structs", "interface"],
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

pub(super) fn is_family(name: &str) -> bool {
    FAMILIES.iter().any(|aliases| aliases.contains(&name))
}

const TYPE_KINDS: &[&str] = &[
    "Class",
    "Struct",
    "Enum",
    "Trait",
    "Interface",
    "Module",
    "Type",
    "TypeAlias",
    "Record",
    "Object",
    "Namespace",
    "DataClass",
    "ValueClass",
    "AnnotationClass",
];

const CALLABLE_KINDS: &[&str] = &[
    "Method",
    "Function",
    "AssociatedFunction",
    "SingletonMethod",
    "StaticMethod",
    "Constructor",
    "ExtensionFunction",
    "AsyncFunction",
    "DecoratedFunction",
    "DecoratedMethod",
    "DecoratedAsyncFunction",
    "DecoratedAsyncMethod",
    "Lambda",
    "Macro",
];

const MEMBER_EXTRA_KINDS: &[&str] = &["Field", "Attribute", "Property", "ComputedProperty"];

/// The family of a definition kind: the kind lists first, then the name, so labels such as
/// `DecoratedClass` or a new language's kinds still land somewhere.
fn family_of(kind: &str) -> Option<usize> {
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
