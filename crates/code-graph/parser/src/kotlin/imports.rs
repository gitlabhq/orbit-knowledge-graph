use crate::{
    imports::ImportIdentifier,
    kotlin::{
        types::{AstNode, KotlinImportType, KotlinImportedSymbolInfo, node_types},
        utils::get_child_by_kind,
    },
    utils::node_to_range,
};

/// Parses a Kotlin import AST node into a `KotlinImportedSymbolInfo`
pub(in crate::kotlin) fn parse_import_node(
    import_node: &AstNode,
) -> Option<KotlinImportedSymbolInfo> {
    let (import_path, symbol_name, alias) = parse_import_components(import_node)?;
    let import_type = determine_import_type(&symbol_name, &alias);

    let identifier = ImportIdentifier {
        name: symbol_name,
        alias,
    };

    Some(KotlinImportedSymbolInfo {
        import_type,
        import_path,
        identifier: Some(identifier),
        range: node_to_range(import_node),
        scope: None,
    })
}

fn determine_import_type(symbol_name: &str, alias: &Option<String>) -> KotlinImportType {
    if alias.is_some() {
        KotlinImportType::AliasedImport
    } else if symbol_name == "*" {
        KotlinImportType::WildcardImport
    } else {
        KotlinImportType::Import
    }
}

fn parse_import_components(import_node: &AstNode) -> Option<(String, String, Option<String>)> {
    let alias = extract_alias(import_node);

    if import_node.text().contains("*") {
        let path = get_child_by_kind(import_node, node_types::IDENTIFIER)
            .map(|node| node.text().to_string())
            .unwrap_or_default();
        return Some((path, "*".to_string(), alias));
    }

    let (path, symbol) = extract_path_and_symbol(import_node)?;
    Some((path, symbol, alias))
}

fn extract_alias(import_node: &AstNode) -> Option<String> {
    get_child_by_kind(import_node, node_types::IMPORT_ALIAS)
        .and_then(|alias_node| get_child_by_kind(&alias_node, node_types::TYPE_IDENTIFIER))
        .map(|type_id| type_id.text().to_string())
}

fn extract_path_and_symbol(import_node: &AstNode) -> Option<(String, String)> {
    let identifier_node = get_child_by_kind(import_node, node_types::IDENTIFIER)?;
    let children: Vec<_> = identifier_node.children().collect();

    if children.len() > 1 {
        let symbol = children.last()?.text().to_string();
        let path = children[..children.len() - 1]
            .iter()
            .filter(|child| child.kind() == node_types::SIMPLE_IDENTIFIER)
            .map(|child| child.text())
            .collect::<Vec<_>>()
            .join(".");

        Some((path, symbol))
    } else {
        let symbol = identifier_node.text().to_string();
        Some((String::new(), symbol))
    }
}
