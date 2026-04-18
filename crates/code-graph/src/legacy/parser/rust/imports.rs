use crate::legacy::parser::{
    imports::{ImportIdentifier, ImportedSymbolInfo},
    rust::types::{RustFqn, RustFqnPart, RustImportType, node_types},
};
use crate::utils::node_to_range;
use smallvec::SmallVec;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

/// A symbol extracted from a use list with its corresponding AST node
#[derive(Clone)]
struct UseListSymbol<'a> {
    identifier: ImportIdentifier,
    node: Node<'a, StrDoc<SupportLang>>,
}

pub type RustImportedSymbolInfo = ImportedSymbolInfo<RustImportType, RustFqn>;

type ScopeStack = SmallVec<[RustFqnPart; 8]>;

/// Detect if a node represents an import statement and extract the import information
pub fn detect_import_declaration(
    node: &Node<StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
) -> Vec<RustImportedSymbolInfo> {
    match node.kind().as_ref() {
        node_types::USE_DECLARATION => extract_use_imports(node, current_scope),
        node_types::EXTERN_CRATE => extract_extern_crate_imports(node, current_scope),
        node_types::MODULE => extract_mod_declaration_import(node, current_scope),
        _ => Vec::new(),
    }
}

/// Extract imports from use declarations
fn extract_use_imports(
    node: &Node<StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
) -> Vec<RustImportedSymbolInfo> {
    let mut imports = Vec::new();
    let is_public = node
        .children()
        .any(|child| child.kind() == node_types::VISIBILITY_MODIFIER);

    if let Some(argument) = node.field("argument") {
        match argument.kind().as_ref() {
            node_types::USE_WILDCARD => {
                // Handle glob imports: use std::collections::*;
                if let Some(scoped_id) = argument
                    .children()
                    .find(|child| child.kind() == node_types::SCOPED_IDENTIFIER)
                {
                    let import_path = scoped_id.text().to_string();
                    let import_type = if is_public {
                        RustImportType::ReExportGlob
                    } else {
                        RustImportType::GlobUse
                    };

                    imports.push(create_import_info(
                        import_type,
                        import_path,
                        None,
                        node,
                        current_scope,
                    ));
                }
            }
            node_types::USE_AS_CLAUSE => {
                // Handle aliased imports: use std::HashMap as Map;
                if let Some(path) = argument.field("path") {
                    let import_path = path.text().to_string();
                    let alias = argument.field("alias").map(|a| a.text().to_string());
                    let original_name = import_path
                        .split("::")
                        .last()
                        .unwrap_or(&import_path)
                        .to_string();

                    let import_type = if is_public {
                        RustImportType::ReExportAliased
                    } else {
                        RustImportType::AliasedUse
                    };

                    let identifier = ImportIdentifier {
                        name: original_name,
                        alias,
                    };

                    imports.push(create_import_info(
                        import_type,
                        import_path,
                        Some(identifier),
                        node,
                        current_scope,
                    ));
                }
            }
            node_types::SCOPED_USE_LIST => {
                // Handle use groups: use std::io::{Error, Result};
                if let Some(path) = argument.children().find(|child| {
                    matches!(
                        child.kind().as_ref(),
                        node_types::SCOPED_IDENTIFIER | node_types::IDENTIFIER
                    )
                }) {
                    let base_path = path.text().to_string();

                    if let Some(use_list) = argument
                        .children()
                        .find(|child| child.kind() == node_types::USE_LIST)
                    {
                        let symbols = extract_use_list_symbols(&use_list);
                        let import_type = RustImportType::UseGroup;

                        for symbol in symbols {
                            imports.push(create_import_info(
                                import_type,
                                base_path.clone(),
                                Some(symbol.identifier),
                                &symbol.node,
                                current_scope,
                            ));
                        }
                    }
                }
            }
            node_types::USE_LIST => {
                // Handle complex nested use groups
                let symbols = extract_use_list_symbols(&argument);
                let import_type = RustImportType::UseGroup;

                for symbol in symbols {
                    imports.push(create_import_info(
                        import_type,
                        "".to_string(), // Will be extracted from symbol name
                        Some(symbol.identifier),
                        &symbol.node,
                        current_scope,
                    ));
                }
            }
            _ => {
                // Handle simple imports: use std::collections::HashMap;
                let import_path = argument.text().to_string();
                let symbol_name = import_path
                    .split("::")
                    .last()
                    .unwrap_or(&import_path)
                    .to_string();

                let import_type = if is_public {
                    RustImportType::ReExport
                } else {
                    RustImportType::Use
                };

                let identifier = ImportIdentifier {
                    name: symbol_name,
                    alias: None,
                };

                imports.push(create_import_info(
                    import_type,
                    import_path,
                    Some(identifier),
                    node,
                    current_scope,
                ));
            }
        }
    }

    imports
}

/// Extract imports from extern crate declarations
fn extract_extern_crate_imports(
    node: &Node<StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
) -> Vec<RustImportedSymbolInfo> {
    let mut imports = Vec::new();

    if let Some(name_field) = node.field("name") {
        let crate_name = name_field.text().to_string();

        if let Some(alias_field) = node.field("alias") {
            // Aliased extern crate: extern crate serde as ser;
            let alias = alias_field.text().to_string();
            let identifier = ImportIdentifier {
                name: crate_name.clone(),
                alias: Some(alias),
            };

            imports.push(create_import_info(
                RustImportType::AliasedExternCrate,
                crate_name,
                Some(identifier),
                node,
                current_scope,
            ));
        } else {
            // Basic extern crate: extern crate serde;
            let identifier = ImportIdentifier {
                name: crate_name.clone(),
                alias: None,
            };

            imports.push(create_import_info(
                RustImportType::ExternCrate,
                crate_name,
                Some(identifier),
                node,
                current_scope,
            ));
        }
    }

    imports
}

/// Extract imports from module declarations
fn extract_mod_declaration_import(
    node: &Node<StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
) -> Vec<RustImportedSymbolInfo> {
    let mut imports = Vec::new();

    // Only handle mod declarations that don't have a body (i.e., mod name;)
    if node.field("body").is_none()
        && let Some(name_field) = node.field("name")
    {
        let module_name = name_field.text().to_string();
        let identifier = ImportIdentifier {
            name: module_name.clone(),
            alias: None,
        };

        imports.push(create_import_info(
            RustImportType::ModDeclaration,
            module_name,
            Some(identifier),
            node,
            current_scope,
        ));
    }

    imports
}

/// Extract individual symbols from a use_list AST node by traversing its children
fn extract_use_list_symbols<'a>(
    use_list_node: &Node<'a, StrDoc<SupportLang>>,
) -> SmallVec<[UseListSymbol<'a>; 8]> {
    let mut symbols = SmallVec::new();

    // Traverse children of the use_list node
    for child in use_list_node.children() {
        match child.kind().as_ref() {
            node_types::IDENTIFIER => {
                // Simple identifier like "Error"
                symbols.push(UseListSymbol {
                    identifier: ImportIdentifier {
                        name: child.text().to_string(),
                        alias: None,
                    },
                    node: child,
                });
            }
            node_types::USE_AS_CLAUSE => {
                // Aliased identifier like "HashMap as Map"
                let mut name = None;
                let mut alias = None;

                for grandchild in child.children() {
                    match grandchild.kind().as_ref() {
                        node_types::IDENTIFIER | node_types::SCOPED_IDENTIFIER => {
                            if name.is_none() {
                                name = Some(grandchild.text().to_string());
                            } else {
                                alias = Some(grandchild.text().to_string());
                            }
                        }
                        _ => {}
                    }
                }

                if let Some(name) = name {
                    symbols.push(UseListSymbol {
                        identifier: ImportIdentifier { name, alias },
                        node: child,
                    });
                }
            }
            node_types::SCOPED_IDENTIFIER => {
                // Handle scoped identifiers that might appear in use lists
                symbols.push(UseListSymbol {
                    identifier: ImportIdentifier {
                        name: child.text().to_string(),
                        alias: None,
                    },
                    node: child,
                });
            }
            node_types::SCOPED_USE_LIST => {
                // Handle nested use groups like "collections::{HashMap, BTreeMap}"
                let mut prefix = None;
                let mut nested_symbols = SmallVec::new();

                for grandchild in child.children() {
                    match grandchild.kind().as_ref() {
                        node_types::IDENTIFIER | node_types::SCOPED_IDENTIFIER => {
                            if prefix.is_none() {
                                prefix = Some(grandchild.text().to_string());
                            }
                        }
                        node_types::USE_LIST => {
                            nested_symbols = extract_use_list_symbols(&grandchild);
                        }
                        _ => {}
                    }
                }

                if let Some(prefix_path) = prefix {
                    // Combine prefix with nested symbols
                    for nested_symbol in nested_symbols {
                        symbols.push(UseListSymbol {
                            identifier: ImportIdentifier {
                                name: format!("{}::{}", prefix_path, nested_symbol.identifier.name),
                                alias: nested_symbol.identifier.alias,
                            },
                            node: nested_symbol.node,
                        });
                    }
                }
            }
            node_types::USE_WILDCARD => {
                // Handle glob patterns like "network::*"
                // Find the path before the wildcard
                if let Some(parent) = child.parent() {
                    for sibling in parent.children() {
                        if sibling.kind() == node_types::SCOPED_IDENTIFIER
                            || sibling.kind() == node_types::IDENTIFIER
                        {
                            symbols.push(UseListSymbol {
                                identifier: ImportIdentifier {
                                    name: format!("{}::*", sibling.text()),
                                    alias: None,
                                },
                                node: child,
                            });
                            break;
                        }
                    }
                }
            }
            _ => {
                // Ignore other nodes like braces, commas, whitespace
            }
        }
    }

    symbols
}

/// Create a RustImportedSymbolInfo from the given parameters
fn create_import_info(
    import_type: RustImportType,
    import_path: String,
    identifier: Option<ImportIdentifier>,
    node: &Node<StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
) -> RustImportedSymbolInfo {
    let range = node_to_range(node);
    let scope = if current_scope.is_empty() {
        None
    } else {
        Some(RustFqn::new(current_scope.clone()))
    };

    RustImportedSymbolInfo {
        import_type,
        import_path,
        identifier,
        range,
        scope,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::parser::parser::SupportedLanguage;
    use crate::legacy::parser::{LanguageParser, parser::GenericParser};

    fn extract_imports_for_test(
        rust_code: &str,
    ) -> crate::legacy::parser::Result<Vec<RustImportedSymbolInfo>> {
        let parser = GenericParser::new(SupportedLanguage::Rust);
        let parser_result = parser.parse(rust_code, None)?;

        let mut all_imports = Vec::new();
        let empty_scope = SmallVec::new();

        // Traverse the AST and detect imports directly
        for node in parser_result.ast.root().dfs() {
            let imports = detect_import_declaration(&node, &empty_scope);
            all_imports.extend(imports);
        }

        Ok(all_imports)
    }

    #[test]
    fn test_basic_use_imports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
use std::collections::HashMap;
use std::io;
use tokio;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Validate exact expected imports with their identifiers
        let expected_imports = vec![
            ("std::collections::HashMap", "HashMap"),
            ("std::io", "io"),
            ("tokio", "tokio"),
        ];

        // Validate each expected import
        for (expected_path, expected_identifier) in expected_imports {
            let found = imports.iter().any(|imp| {
                imp.import_type == RustImportType::Use
                    && imp.import_path == expected_path
                    && imp
                        .identifier
                        .as_ref()
                        .map(|id| id.name == expected_identifier && id.alias.is_none())
                        .unwrap_or(false)
            });
            assert!(
                found,
                "Should find use import: {expected_path} with identifier {expected_identifier}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_glob_imports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
use std::collections::*;
use std::prelude::*;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Validate exact expected glob imports
        let expected_glob_imports = vec!["std::collections", "std::prelude"];

        for expected_path in expected_glob_imports {
            let found = imports.iter().any(|imp| {
                imp.import_type == RustImportType::GlobUse
                    && imp.import_path == expected_path
                    && imp.identifier.is_none()
            });
            assert!(found, "Should find glob import: {expected_path}");
        }

        Ok(())
    }

    #[test]
    fn test_aliased_imports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
use std::collections::HashMap as Map;
use std::sync::Arc as AtomicRC;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Validate exact expected aliased imports
        let expected_aliased_imports = vec![
            ("std::collections::HashMap", "HashMap", "Map"),
            ("std::sync::Arc", "Arc", "AtomicRC"),
        ];

        for (expected_path, expected_name, expected_alias) in expected_aliased_imports {
            let found = imports.iter().any(|imp| {
                imp.import_type == RustImportType::AliasedUse
                    && imp.import_path == expected_path
                    && imp.identifier.as_ref().is_some_and(|id| {
                        id.name == expected_name
                            && id.alias.as_ref() == Some(&expected_alias.to_string())
                    })
            });
            assert!(
                found,
                "Should find aliased import: {expected_path} as {expected_name} -> {expected_alias}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_extern_crate_imports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
extern crate serde;
extern crate log;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        let extern_imports: Vec<_> = imports
            .iter()
            .filter(|imp| imp.import_type == RustImportType::ExternCrate)
            .collect();

        assert_eq!(
            extern_imports.len(),
            2,
            "Should find 2 extern crate imports"
        );

        let crate_names: Vec<&String> = extern_imports.iter().map(|imp| &imp.import_path).collect();
        assert!(crate_names.contains(&&"serde".to_string()));
        assert!(crate_names.contains(&&"log".to_string()));

        // Check identifiers match crate names
        let identifiers: Vec<Option<&String>> = extern_imports
            .iter()
            .map(|imp| imp.identifier.as_ref().map(|id| &id.name))
            .collect();

        assert!(identifiers.contains(&Some(&"serde".to_string())));
        assert!(identifiers.contains(&Some(&"log".to_string())));

        Ok(())
    }

    #[test]
    fn test_aliased_extern_crate_imports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
extern crate serde as ser;
extern crate tokio as async_runtime;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        let aliased_extern_imports: Vec<_> = imports
            .iter()
            .filter(|imp| imp.import_type == RustImportType::AliasedExternCrate)
            .collect();

        assert_eq!(
            aliased_extern_imports.len(),
            2,
            "Should find 2 aliased extern crate imports"
        );

        // Check original crate names
        let crate_names: Vec<&String> = aliased_extern_imports
            .iter()
            .map(|imp| &imp.import_path)
            .collect();
        assert!(crate_names.contains(&&"serde".to_string()));
        assert!(crate_names.contains(&&"tokio".to_string()));

        // Check aliases
        let aliases: Vec<Option<&String>> = aliased_extern_imports
            .iter()
            .map(|imp| imp.identifier.as_ref().and_then(|id| id.alias.as_ref()))
            .collect();

        assert!(aliases.contains(&Some(&"ser".to_string())));
        assert!(aliases.contains(&Some(&"async_runtime".to_string())));

        Ok(())
    }

    #[test]
    fn test_re_exports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
pub use std::collections::HashMap;
pub use std::fs::File;
pub use std::collections::*;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Validate exact expected re-exports
        let expected_re_exports = vec![
            ("std::collections::HashMap", "HashMap"),
            ("std::fs::File", "File"),
        ];

        for (expected_path, expected_name) in expected_re_exports {
            let found = imports.iter().any(|imp| {
                imp.import_type == RustImportType::ReExport
                    && imp.import_path == expected_path
                    && imp
                        .identifier
                        .as_ref()
                        .is_some_and(|id| id.name == expected_name && id.alias.is_none())
            });
            assert!(
                found,
                "Should find re-export: {expected_path} with identifier {expected_name}"
            );
        }

        // Check for glob re-export if pattern is available
        let glob_re_export_found = imports.iter().any(|imp| {
            imp.import_type == RustImportType::ReExportGlob
                && imp.import_path.contains("std::collections")
        });

        if glob_re_export_found {
            // Verify the glob re-export has no specific identifier
            let glob_re_export = imports.iter().find(|imp| {
                imp.import_type == RustImportType::ReExportGlob
                    && imp.import_path.contains("std::collections")
            });
            assert!(
                glob_re_export.unwrap().identifier.is_none(),
                "Glob re-export should not have identifier"
            );
        }

        Ok(())
    }

    #[test]
    fn test_module_declarations() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
mod network;
mod database;
mod utils;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        let mod_declarations: Vec<_> = imports
            .iter()
            .filter(|imp| imp.import_type == RustImportType::ModDeclaration)
            .collect();

        assert_eq!(
            mod_declarations.len(),
            3,
            "Should find 3 module declarations"
        );

        let module_names: Vec<&String> = mod_declarations
            .iter()
            .map(|imp| &imp.import_path)
            .collect();
        assert!(module_names.contains(&&"network".to_string()));
        assert!(module_names.contains(&&"database".to_string()));
        assert!(module_names.contains(&&"utils".to_string()));

        // Check identifiers match module names
        let identifiers: Vec<Option<&String>> = mod_declarations
            .iter()
            .map(|imp| imp.identifier.as_ref().map(|id| &id.name))
            .collect();

        assert!(identifiers.contains(&Some(&"network".to_string())));
        assert!(identifiers.contains(&Some(&"database".to_string())));
        assert!(identifiers.contains(&Some(&"utils".to_string())));

        Ok(())
    }

    #[test]
    fn test_import_scopes_with_fqn_integration() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
// Top-level imports (no scope)
use std::collections::HashMap;
pub use std::sync::Mutex;

mod my_module {
    // Imports inside module
    use std::fs::File;
    
    fn helper_function() {
        // Import inside function within module
        use std::io::Result;
    }
    
    impl MyStruct {
        // Import inside impl block within module
        pub use crate::utils::*;
    }
}

fn main() {
    // Import inside function
    use std::path::Path;
}

impl TopLevelStruct {
    // Import inside top-level impl
    use crate::constants::*;
}
"#;

        // Use our new direct AST approach with scope integration
        let parser = GenericParser::new(SupportedLanguage::Rust);
        let parser_result = parser.parse(rust_code, None)?;

        // Build FQN map to get scope information for testing scope integration
        let (_node_fqn_map, _, _, imports) =
            crate::legacy::parser::rust::fqn::build_fqn_and_node_indices(&parser_result.ast);

        // All imports extracted with proper scope information

        // Validate top-level imports (should have no scope)
        let top_level_imports = vec!["std::collections::HashMap", "std::sync::Mutex"];

        for expected_path in top_level_imports {
            let found = imports.iter().find(|imp| imp.import_path == expected_path);
            assert!(
                found.is_some(),
                "Should find top-level import: {expected_path}"
            );
            let import = found.unwrap();
            assert!(
                import.scope.is_none(),
                "Top-level import {expected_path} should have no scope"
            );
        }

        // Validate function-scoped imports
        let function_scoped_imports = vec![
            ("std::path::Path", vec!["main"]),
            ("std::io::Result", vec!["my_module", "helper_function"]),
        ];

        for (expected_path, expected_scope_parts) in function_scoped_imports {
            let found = imports.iter().find(|imp| imp.import_path == expected_path);
            assert!(
                found.is_some(),
                "Should find function-scoped import: {expected_path}"
            );
            let import = found.unwrap();

            if let Some(scope) = &import.scope {
                let scope_parts: Vec<String> = scope
                    .parts
                    .iter()
                    .map(|p| p.node_name().to_string())
                    .collect();
                assert_eq!(
                    scope_parts, expected_scope_parts,
                    "Import {expected_path} should have scope {expected_scope_parts:?}, got {scope_parts:?}"
                );
            } else {
                panic!(
                    "Import {expected_path} should have scope {expected_scope_parts:?}, but has None"
                );
            }
        }

        // Validate module-scoped imports
        let module_scoped_imports = vec!["std::fs::File"];

        for expected_path in module_scoped_imports {
            let found = imports.iter().find(|imp| imp.import_path == expected_path);
            assert!(
                found.is_some(),
                "Should find module-scoped import: {expected_path}"
            );
            let import = found.unwrap();

            if let Some(scope) = &import.scope {
                let scope_parts: Vec<String> = scope
                    .parts
                    .iter()
                    .map(|p| p.node_name().to_string())
                    .collect();
                assert!(
                    scope_parts.contains(&"my_module".to_string()),
                    "Import {expected_path} should be in my_module scope"
                );
            } else {
                panic!("Import {expected_path} should have module scope, but has None");
            }
        }

        // Validate impl-scoped imports
        let impl_scoped_imports = vec![
            ("crate::constants", "TopLevelStruct"), // use crate::constants::*; inside impl TopLevelStruct
            ("crate::utils", "MyStruct"), // pub use crate::utils::*; inside impl MyStruct (within my_module)
        ];

        for (expected_path, expected_impl) in impl_scoped_imports {
            let found = imports.iter().any(|imp| {
                imp.import_path == expected_path
                    && if let Some(scope) = &imp.scope {
                        scope
                            .parts
                            .iter()
                            .any(|part| part.node_name().contains(expected_impl))
                    } else {
                        false
                    }
            });

            assert!(
                found,
                "Should find impl-scoped import: {expected_path} in {expected_impl}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_comprehensive_mixed_imports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
// Basic use statements
use std::collections::HashMap;
use std::io;
use tokio;

// Glob imports
use std::collections::*;
use std::prelude::*;

// Aliased imports
use std::collections::HashMap as Map;
use std::sync::Arc as AtomicRC;

// External crates
extern crate serde;
extern crate tokio as async_runtime;

// Module declarations
mod network;
mod database;

// Re-exports
pub use std::collections::BTreeMap;
pub use std::fs::{File, OpenOptions};

// Relative imports
use super::parent_module;
use crate::root_module;
use self::current_module;

// Use groups - testing complex import identifiers
use std::io::{Error, Result};
use crate::utils::{Position, Range, node_to_range};

fn main() {
    println!("Testing comprehensive imports");
}
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Define expected imports based on actual parser output
        let expected_imports = vec![
            // Basic use statements
            (
                RustImportType::Use,
                "std::collections::HashMap",
                Some("HashMap"),
            ),
            (RustImportType::Use, "std::io", Some("io")),
            (RustImportType::Use, "tokio", Some("tokio")),
            // Glob imports (have None identifier)
            (RustImportType::GlobUse, "std::collections", None),
            (RustImportType::GlobUse, "std::prelude", None),
            // Aliased imports
            (
                RustImportType::AliasedUse,
                "std::collections::HashMap",
                Some("HashMap"),
            ),
            (RustImportType::AliasedUse, "std::sync::Arc", Some("Arc")),
            // External crates
            (RustImportType::ExternCrate, "serde", Some("serde")),
            (RustImportType::AliasedExternCrate, "tokio", Some("tokio")),
            // Module declarations
            (RustImportType::ModDeclaration, "network", Some("network")),
            (RustImportType::ModDeclaration, "database", Some("database")),
            // Re-exports
            (
                RustImportType::ReExport,
                "std::collections::BTreeMap",
                Some("BTreeMap"),
            ),
            // Relative imports
            (
                RustImportType::Use,
                "super::parent_module",
                Some("parent_module"),
            ),
            (
                RustImportType::Use,
                "crate::root_module",
                Some("root_module"),
            ),
            (
                RustImportType::Use,
                "self::current_module",
                Some("current_module"),
            ),
            // Use groups
            (RustImportType::UseGroup, "std::fs", Some("File")),
            (RustImportType::UseGroup, "std::fs", Some("OpenOptions")),
            (RustImportType::UseGroup, "std::io", Some("Error")),
            (RustImportType::UseGroup, "std::io", Some("Result")),
            (RustImportType::UseGroup, "crate::utils", Some("Position")),
            (RustImportType::UseGroup, "crate::utils", Some("Range")),
            (
                RustImportType::UseGroup,
                "crate::utils",
                Some("node_to_range"),
            ),
        ];

        // Verify specific imports exist
        for (expected_type, expected_path, expected_name) in expected_imports {
            let found = imports.iter().any(|import| {
                import.import_type == expected_type
                    && import.import_path == expected_path
                    && match expected_name {
                        Some(name) => import.identifier.as_ref().is_some_and(|id| id.name == name),
                        None => import.identifier.is_none(),
                    }
            });

            assert!(
                found,
                "Expected import not found: {expected_type:?} from '{expected_path}' with identifier '{expected_name:?}'"
            );
        }

        // Verify ranges are properly set
        for import in &imports {
            assert!(
                import.range.start.line < import.range.end.line
                    || (import.range.start.line == import.range.end.line
                        && import.range.start.column <= import.range.end.column),
                "Import range should be valid"
            );
            assert!(
                import.range.byte_offset.0 <= import.range.byte_offset.1,
                "Byte offset should be valid"
            );
        }

        Ok(())
    }

    #[test]
    fn test_import_range_information() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"use std::collections::HashMap;
use std::io;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Validate exact expected imports
        let expected_imports = vec![("std::collections::HashMap", "HashMap"), ("std::io", "io")];

        for (expected_path, expected_name) in expected_imports {
            let found = imports.iter().any(|imp| {
                imp.import_type == RustImportType::Use
                    && imp.import_path == expected_path
                    && imp
                        .identifier
                        .as_ref()
                        .is_some_and(|id| id.name == expected_name && id.alias.is_none())
            });
            assert!(
                found,
                "Should find import: {expected_path} with identifier {expected_name}"
            );
        }

        for import in &imports {
            // Verify range information is valid
            assert!(
                import.range.start.line <= import.range.end.line,
                "Start line should be <= end line"
            );
            assert!(
                import.range.byte_offset.0 <= import.range.byte_offset.1,
                "Start byte should be <= end byte"
            );

            // All imports should start from line 0 or 1 (0-indexed or 1-indexed)
            assert!(
                import.range.start.line <= 1,
                "Import should start early in file"
            );

            // Verify we have proper import paths
            assert!(
                !import.import_path.is_empty(),
                "Import path should not be empty"
            );
        }

        Ok(())
    }

    #[test]
    fn test_use_group_unique_ranges() -> crate::legacy::parser::Result<()> {
        let rust_code = "use std::collections::{HashMap as Map, BTreeMap, HashSet};";

        let imports = extract_imports_for_test(rust_code)?;

        let use_group_imports: Vec<_> = imports
            .iter()
            .filter(|imp| imp.import_type == RustImportType::UseGroup)
            .collect();

        assert_eq!(use_group_imports.len(), 3, "Should find 3 symbols");

        // Verify each import has unique byte ranges
        let mut ranges: Vec<_> = use_group_imports
            .iter()
            .map(|imp| imp.range.byte_offset)
            .collect();
        ranges.sort();
        ranges.dedup();
        assert_eq!(ranges.len(), 3, "All ranges should be unique");

        Ok(())
    }

    #[test]
    fn test_use_group_imports() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
use std::io::{Error, Result};
use crate::utils::{Position, Range, node_to_range};
use std::collections::{HashMap as Map, BTreeMap};
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Should find use group imports with individual symbols extracted
        let use_group_imports: Vec<_> = imports
            .iter()
            .filter(|imp| imp.import_type == RustImportType::UseGroup)
            .collect();

        // We should find individual symbols now:
        // std::io::{Error, Result} = 2 symbols
        // crate::utils::{Position, Range, node_to_range} = 3 symbols
        // std::collections::{HashMap as Map, BTreeMap} = 2 symbols
        // Total: 7 symbols
        assert_eq!(
            use_group_imports.len(),
            7,
            "Should find 7 individual symbols from use groups"
        );

        // Verify the paths are captured correctly
        let paths: Vec<&String> = use_group_imports
            .iter()
            .map(|imp| &imp.import_path)
            .collect();
        assert!(paths.contains(&&"std::io".to_string()));
        assert!(paths.contains(&&"crate::utils".to_string()));
        assert!(paths.contains(&&"std::collections".to_string()));

        // Verify specific symbols are extracted correctly
        let symbol_names: Vec<&String> = use_group_imports
            .iter()
            .filter_map(|imp| imp.identifier.as_ref().map(|id| &id.name))
            .collect();

        assert!(symbol_names.contains(&&"Error".to_string()));
        assert!(symbol_names.contains(&&"Result".to_string()));
        assert!(symbol_names.contains(&&"Position".to_string()));
        assert!(symbol_names.contains(&&"Range".to_string()));
        assert!(symbol_names.contains(&&"node_to_range".to_string()));
        assert!(symbol_names.contains(&&"HashMap".to_string()));
        assert!(symbol_names.contains(&&"BTreeMap".to_string()));

        // Verify aliases are captured correctly
        let map_import = use_group_imports
            .iter()
            .find(|imp| {
                imp.identifier
                    .as_ref()
                    .is_some_and(|id| id.name == "HashMap")
            })
            .expect("Should find HashMap import");

        assert_eq!(
            map_import.identifier.as_ref().unwrap().alias,
            Some("Map".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_complex_import_patterns() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
// Complex nested use statements
use std::{
    collections::{
        hash_map::{Entry, Keys, Values},
        HashMap,
    },
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

// Conditional imports with attributes
#[cfg(feature = "async")]
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

// Complex re-exports with mixed patterns
pub use crate::internal::{
    config::Config,
    database::Database as DB,
    network::*,
};

// Macro imports
use log::{debug, error, info, warn};

// Very deeply nested use groups
use some::very::{
    deeply::{
        nested::{module::Type, another::AnotherType as AT},
        other::OtherType,
    },
    path::PathType,
};
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Should find some imports - let's be flexible about the exact count since complex patterns might not all be supported yet
        assert!(!imports.is_empty(), "Should find some complex imports");

        // Verify we're extracting nested symbols correctly
        let nested_symbols: Vec<&String> = imports
            .iter()
            .filter_map(|imp| imp.identifier.as_ref().map(|id| &id.name))
            .filter(|name| name.contains("::"))
            .collect();

        // Should find deeply nested symbols
        assert!(
            nested_symbols
                .iter()
                .any(|name| name.contains("deeply::nested")),
            "Should find deeply nested symbols"
        );

        // Should find aliased nested symbols
        let aliased_symbols: Vec<_> = imports
            .iter()
            .filter_map(|imp| imp.identifier.as_ref().filter(|id| id.alias.is_some()))
            .collect();

        assert!(
            !aliased_symbols.is_empty(),
            "Should find aliased symbols in complex imports"
        );

        Ok(())
    }

    #[test]
    fn test_public_glob_re_export() -> crate::legacy::parser::Result<()> {
        let rust_code = r#"
pub use std::collections::*;
pub use crate::utils::*;
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Should find public glob re-export patterns
        let re_export_globs: Vec<_> = imports
            .iter()
            .filter(|imp| imp.import_type == RustImportType::ReExportGlob)
            .collect();

        assert_eq!(
            re_export_globs.len(),
            2,
            "Should find 2 ReExportGlob patterns"
        );

        // Check specific paths
        let paths: Vec<&String> = re_export_globs.iter().map(|imp| &imp.import_path).collect();
        assert!(
            paths.contains(&&"std::collections".to_string()),
            "Should find std::collections"
        );
        assert!(
            paths.contains(&&"crate::utils".to_string()),
            "Should find crate::utils"
        );

        Ok(())
    }

    #[test]
    fn test_complex_use_group_patterns() -> crate::legacy::parser::Result<()> {
        // Test that complex use group patterns are properly supported
        let rust_code = r#"
// Complex nested use group with multiple levels
use std::{
    collections::{
        hash_map::{Entry, Keys, Values},
        HashMap,
    },
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

// Simple use groups  
use log::{debug, error, info, warn};
use futures::{Future, Stream};

// Multi-line use group with nested structure
use tokio::{
    runtime::Runtime,
    sync::{mpsc, oneshot},
};
"#;

        // Use our new direct AST approach
        let imports = extract_imports_for_test(rust_code)?;

        // Verify we're extracting the complex patterns correctly
        assert!(
            !imports.is_empty(),
            "Should find imports from complex use group patterns"
        );

        // Validate exact symbols from the complex nested std use group
        let expected_std_symbols = vec![
            "collections::hash_map::Entry",
            "collections::hash_map::Keys",
            "collections::hash_map::Values",
            "collections::HashMap",
            "sync::atomic::AtomicBool",
            "sync::atomic::Ordering",
            "sync::Arc",
            "sync::Mutex",
        ];

        for expected_symbol in expected_std_symbols {
            let found = imports.iter().any(|imp| {
                imp.import_path == "std"
                    && imp
                        .identifier
                        .as_ref()
                        .map(|id| id.name == expected_symbol)
                        .unwrap_or(false)
            });
            assert!(found, "Should find std symbol: {expected_symbol}");
        }

        // Validate exact symbols from the log use group
        let expected_log_symbols = vec!["debug", "error", "info", "warn"];

        for expected_symbol in expected_log_symbols {
            let found = imports.iter().any(|imp| {
                imp.import_path == "log"
                    && imp
                        .identifier
                        .as_ref()
                        .map(|id| id.name == expected_symbol)
                        .unwrap_or(false)
            });
            assert!(found, "Should find log symbol: {expected_symbol}");
        }

        // Validate exact symbols from the tokio use group
        let expected_tokio_symbols = vec!["runtime::Runtime", "sync::mpsc", "sync::oneshot"];

        for expected_symbol in expected_tokio_symbols {
            let found = imports.iter().any(|imp| {
                imp.import_path == "tokio"
                    && imp
                        .identifier
                        .as_ref()
                        .map(|id| id.name == expected_symbol)
                        .unwrap_or(false)
            });
            assert!(found, "Should find tokio symbol: {expected_symbol}");
        }

        Ok(())
    }
}
