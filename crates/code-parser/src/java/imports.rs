use crate::{
    imports::ImportIdentifier,
    java::{
        types::{AstNode, JavaImportType, JavaImportedSymbolInfo},
        utils::{get_child_by_kind, node_types},
    },
    utils::node_to_range,
};

pub(in crate::java) fn extract_import_declaration(
    import_node: &AstNode,
) -> Option<JavaImportedSymbolInfo> {
    let import_text = import_node.text();
    let import_type = determine_import_type(&import_text, import_node);

    let (import_path, import_symbol) = extract_path_and_symbol(import_node, &import_type)?;

    let identifier = ImportIdentifier {
        name: import_symbol,
        alias: None,
    };

    Some(JavaImportedSymbolInfo {
        import_type,
        import_path,
        identifier: Some(identifier),
        range: node_to_range(import_node),
        scope: None,
    })
}

/// Determine the type of import (static, wildcard, or regular)
fn determine_import_type(import_text: &str, import_node: &AstNode) -> JavaImportType {
    let is_static = import_text.trim_start().starts_with("import static");
    let is_wildcard = get_child_by_kind(import_node, "asterisk").is_some();

    match (is_static, is_wildcard) {
        (true, _) => JavaImportType::StaticImport,
        (false, true) => JavaImportType::WildcardImport,
        (false, false) => JavaImportType::Import,
    }
}

/// Extract the import path and symbol name from the import node
fn extract_path_and_symbol(
    import_node: &AstNode,
    import_type: &JavaImportType,
) -> Option<(String, String)> {
    match import_type {
        JavaImportType::WildcardImport => extract_wildcard_import(import_node),
        _ => extract_regular_import(import_node),
    }
}

/// Extract path and symbol for wildcard imports (e.g., "java.util.*")
fn extract_wildcard_import(import_node: &AstNode) -> Option<(String, String)> {
    if let Some(scoped_identifier) = get_child_by_kind(import_node, node_types::SCOPED_IDENTIFIER) {
        Some((scoped_identifier.text().to_string(), "*".to_string()))
    } else {
        get_child_by_kind(import_node, node_types::IDENTIFIER)
            .map(|identifier| (identifier.text().to_string(), "*".to_string()))
    }
}

/// Extract path and symbol for regular imports (e.g., "java.util.List")
fn extract_regular_import(import_node: &AstNode) -> Option<(String, String)> {
    if let Some(scoped_identifier) = get_child_by_kind(import_node, node_types::SCOPED_IDENTIFIER) {
        let scope = scoped_identifier.field("scope")?.text().to_string();
        let name = scoped_identifier.field("name")?.text().to_string();
        Some((scope, name))
    } else {
        get_child_by_kind(import_node, node_types::IDENTIFIER)
            .map(|identifier| ("".to_string(), identifier.text().to_string()))
    }
}
