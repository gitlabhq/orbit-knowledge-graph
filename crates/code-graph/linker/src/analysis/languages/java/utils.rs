use crate::analysis::types::{ImportType, ImportedSymbolNode};
use parser_core::java::types::JavaImportType;

// Imports

/// Returns the name of the imported symbol and the full import path.
pub fn full_import_path(import: &ImportedSymbolNode) -> (String, String) {
    let name = match import.import_type {
        ImportType::Java(JavaImportType::Import) => import
            .identifier
            .as_ref()
            .map(|i| i.name.clone())
            .unwrap_or_default(),
        ImportType::Java(JavaImportType::StaticImport) => import
            .identifier
            .as_ref()
            .map(|i| i.name.clone())
            .unwrap_or_default(),
        _ => return (String::new(), String::new()),
    };

    (name.clone(), format!("{}.{}", import.import_path, name))
}
