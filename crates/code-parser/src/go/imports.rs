use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::go::types::{GoImportType, GoImportedSymbolInfo};
use crate::imports::ImportIdentifier;
use crate::utils::node_to_range;

/// Extract all import statements from a Go file.
///
/// Go `import_declaration` nodes only appear at the top level of a source file
/// (i.e. as direct children of the `source_file` root node).  There is no need
/// to recurse into function bodies, struct declarations, etc., so we iterate
/// only the direct children of the root node.
pub fn extract_imports(root: &Node<StrDoc<SupportLang>>, imports: &mut Vec<GoImportedSymbolInfo>) {
    for child in root.children() {
        if child.kind().as_ref() == "import_declaration" {
            extract_import_declaration(&child, imports);
        }
    }
}

fn extract_import_declaration(
    node: &Node<StrDoc<SupportLang>>,
    imports: &mut Vec<GoImportedSymbolInfo>,
) {
    for child in node.children() {
        let kind = child.kind();
        if kind.as_ref() == "import_spec" || kind.as_ref() == "import_spec_list" {
            extract_import_spec(&child, imports);
        }
    }
}

fn extract_import_spec(node: &Node<StrDoc<SupportLang>>, imports: &mut Vec<GoImportedSymbolInfo>) {
    if node.kind().as_ref() == "import_spec_list" {
        for child in node.children() {
            if child.kind().as_ref() == "import_spec" {
                extract_single_import_spec(&child, imports);
            }
        }
    } else {
        extract_single_import_spec(node, imports);
    }
}

fn extract_single_import_spec(
    node: &Node<StrDoc<SupportLang>>,
    imports: &mut Vec<GoImportedSymbolInfo>,
) {
    let path_node = node
        .children()
        .find(|n| n.kind().as_ref() == "interpreted_string_literal");

    if let Some(path_node) = path_node {
        let import_path = path_node.text().to_string();
        let import_path = import_path.trim_matches('"').to_string();

        let alias_node = node.children().find(|n| {
            let kind = n.kind();
            kind.as_ref() == "package_identifier"
                || kind.as_ref() == "blank_identifier"
                || kind.as_ref() == "dot"
                || (kind.as_ref() == "identifier" && n.range().start < path_node.range().start)
        });

        let alias = alias_node.map(|n| match n.kind().as_ref() {
            "blank_identifier" => "_".to_string(),
            "dot" => ".".to_string(),
            _ => n.text().to_string(),
        });

        let package_name = alias
            .clone()
            .or_else(|| extract_package_name_from_path(&import_path));

        let import_type = determine_import_type(&import_path);
        let range = node_to_range(node);

        let identifier = package_name.map(|name| ImportIdentifier {
            name,
            alias: alias.filter(|a| {
                let last = extract_package_name_from_path(&import_path);
                last.as_deref() != Some(a.as_str())
            }),
        });

        let import_info = GoImportedSymbolInfo {
            import_type,
            import_path: import_path.clone(),
            identifier,
            range,
            scope: None,
        };

        imports.push(import_info);
    }
}

/// Determine the type of import based on the import path.
///
/// * **Local** — path starts with `.` or `/` (relative / absolute on-disk path).
/// * **External** — the first path segment contains a dot (domain-like).
/// * **Standard** — everything else (no dots in the first segment).
fn determine_import_type(import_path: &str) -> GoImportType {
    if import_path.starts_with('.') || import_path.starts_with('/') {
        GoImportType::Local
    } else {
        let first_segment = import_path.split('/').next().unwrap_or(import_path);
        if first_segment.contains('.') {
            GoImportType::External
        } else {
            GoImportType::Standard
        }
    }
}

fn extract_package_name_from_path(import_path: &str) -> Option<String> {
    import_path.split('/').next_back().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determine_import_type() {
        assert_eq!(determine_import_type("fmt"), GoImportType::Standard);
        assert_eq!(determine_import_type("net/http"), GoImportType::Standard);
        assert_eq!(
            determine_import_type("github.com/user/repo"),
            GoImportType::External
        );
        assert_eq!(
            determine_import_type("golang.org/x/net"),
            GoImportType::External
        );
        assert_eq!(determine_import_type("./local"), GoImportType::Local);
        assert_eq!(determine_import_type("../parent"), GoImportType::Local);
    }

    #[test]
    fn test_extract_package_name_from_path() {
        assert_eq!(
            extract_package_name_from_path("fmt"),
            Some("fmt".to_string())
        );
        assert_eq!(
            extract_package_name_from_path("net/http"),
            Some("http".to_string())
        );
        assert_eq!(
            extract_package_name_from_path("github.com/user/repo/pkg"),
            Some("pkg".to_string())
        );
    }
}
