use crate::analysis::types::ImportedSymbolNode;

/// Returns the name of the imported symbol and the full import path.
pub fn full_import_path(import: &ImportedSymbolNode) -> (String, String) {
    let name = match import.import_type.as_str() {
        "Import" | "StaticImport" => import
            .identifier
            .as_ref()
            .map(|i| i.name.clone())
            .unwrap_or_default(),
        _ => return (String::new(), String::new()),
    };

    (name.clone(), format!("{}.{}", import.import_path, name))
}
