use crate::imports::ImportIdentifier;
use crate::python::types::{
    PythonFqn, PythonImportType, PythonImportedSymbolInfo, PythonNodeFqnMap,
};
use crate::utils::node_to_range;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

/// Extracts imports from AST using tree-sitter, looking up scopes from the pre-built FQN map
pub fn find_imports(
    ast: &Root<StrDoc<SupportLang>>,
    node_fqn_map: &PythonNodeFqnMap,
) -> Vec<PythonImportedSymbolInfo> {
    let mut imports = Vec::with_capacity(32);

    // Stack-based traversal
    let mut stack: Vec<Node<StrDoc<SupportLang>>> = vec![ast.root()];

    while let Some(node) = stack.pop() {
        let node_kind = node.kind();

        match node_kind.as_ref() {
            "import_statement" => {
                extract_import_statement(&node, node_fqn_map, &mut imports);
            }
            "import_from_statement" => {
                extract_import_from_statement(&node, node_fqn_map, &mut imports);
            }
            "future_import_statement" => {
                extract_future_import_statement(&node, node_fqn_map, &mut imports);
            }
            _ => {}
        }

        // Add children to stack in reverse order for depth-first traversal
        let children: Vec<_> = node.children().collect();
        for child in children.into_iter().rev() {
            stack.push(child);
        }
    }

    imports
}

/// Extract imports from `import x` or `import x, y` statements
fn extract_import_statement(
    node: &Node<StrDoc<SupportLang>>,
    node_fqn_map: &PythonNodeFqnMap,
    imports: &mut Vec<PythonImportedSymbolInfo>,
) {
    let scope = get_scope_for_node(node, node_fqn_map);

    for child in node.children() {
        let child_kind = child.kind();

        if child_kind == "dotted_name" {
            // Regular import: import module
            let import_path = child.text().to_string();
            imports.push(PythonImportedSymbolInfo {
                import_type: PythonImportType::Import,
                import_path: import_path.clone(),
                identifier: Some(ImportIdentifier {
                    name: import_path,
                    alias: None,
                }),
                range: node_to_range(&child),
                scope: scope.clone(),
            });
        } else if child_kind == "aliased_import" {
            // Aliased import: import module as alias
            if let (Some(name_node), Some(alias_node)) = (child.field("name"), child.field("alias"))
            {
                let import_path = name_node.text().to_string();
                let alias = alias_node.text().to_string();
                imports.push(PythonImportedSymbolInfo {
                    import_type: PythonImportType::AliasedImport,
                    import_path: import_path.clone(),
                    identifier: Some(ImportIdentifier {
                        name: import_path,
                        alias: Some(alias),
                    }),
                    range: node_to_range(&name_node),
                    scope: scope.clone(),
                });
            }
        }
    }
}

/// Extract imports from `from x import y` statements
fn extract_import_from_statement(
    node: &Node<StrDoc<SupportLang>>,
    node_fqn_map: &PythonNodeFqnMap,
    imports: &mut Vec<PythonImportedSymbolInfo>,
) {
    let scope = get_scope_for_node(node, node_fqn_map);

    // Get the module path (could be dotted_name or relative_import)
    let module_node = node.field("module_name");
    let (import_path, is_relative) = if let Some(module) = &module_node {
        let module_kind = module.kind();
        if module_kind == "relative_import" {
            (module.text().to_string(), true)
        } else {
            (module.text().to_string(), false)
        }
    } else {
        return; // No module name, skip
    };

    // Check for wildcard import first
    let has_wildcard = node.children().any(|c| c.kind() == "wildcard_import");
    if has_wildcard {
        let import_type = if is_relative {
            PythonImportType::RelativeWildcardImport
        } else {
            PythonImportType::WildcardImport
        };
        imports.push(PythonImportedSymbolInfo {
            import_type,
            import_path: import_path.clone(),
            identifier: Some(ImportIdentifier {
                name: "*".to_string(),
                alias: None,
            }),
            range: node_to_range(module_node.as_ref().unwrap()),
            scope: scope.clone(),
        });
        return;
    }

    // Process imported symbols
    for child in node.children() {
        let child_kind = child.kind();

        if child_kind == "dotted_name" {
            // Skip the module_name field - we only want imported symbols
            if module_node.as_ref().map(|m| m.range()) == Some(child.range()) {
                continue;
            }

            // From import: from module import symbol
            // The dotted_name may contain multiple identifiers
            for identifier in child.children() {
                if identifier.kind() == "identifier" {
                    let symbol_name = identifier.text().to_string();
                    let import_type = if is_relative {
                        PythonImportType::RelativeImport
                    } else {
                        PythonImportType::FromImport
                    };
                    imports.push(PythonImportedSymbolInfo {
                        import_type,
                        import_path: import_path.clone(),
                        identifier: Some(ImportIdentifier {
                            name: symbol_name,
                            alias: None,
                        }),
                        range: node_to_range(&identifier),
                        scope: scope.clone(),
                    });
                }
            }
        } else if child_kind == "aliased_import" {
            // Aliased from import: from module import symbol as alias
            if let (Some(name_node), Some(alias_node)) = (child.field("name"), child.field("alias"))
            {
                let symbol_name = name_node.text().to_string();
                let alias = alias_node.text().to_string();
                let import_type = if is_relative {
                    PythonImportType::AliasedRelativeImport
                } else {
                    PythonImportType::AliasedFromImport
                };
                imports.push(PythonImportedSymbolInfo {
                    import_type,
                    import_path: import_path.clone(),
                    identifier: Some(ImportIdentifier {
                        name: symbol_name,
                        alias: Some(alias),
                    }),
                    range: node_to_range(&name_node),
                    scope: scope.clone(),
                });
            }
        }
    }
}

/// Extract imports from `from __future__ import x` statements
fn extract_future_import_statement(
    node: &Node<StrDoc<SupportLang>>,
    node_fqn_map: &PythonNodeFqnMap,
    imports: &mut Vec<PythonImportedSymbolInfo>,
) {
    let scope = get_scope_for_node(node, node_fqn_map);
    let import_path = "__future__".to_string();

    for child in node.children() {
        let child_kind = child.kind();

        if child_kind == "dotted_name" {
            // Future import: from __future__ import symbol
            let symbol_name = child.text().to_string();
            imports.push(PythonImportedSymbolInfo {
                import_type: PythonImportType::FutureImport,
                import_path: import_path.clone(),
                identifier: Some(ImportIdentifier {
                    name: symbol_name,
                    alias: None,
                }),
                range: node_to_range(&child),
                scope: scope.clone(),
            });
        } else if child_kind == "aliased_import" {
            // Aliased future import: from __future__ import symbol as alias
            if let (Some(name_node), Some(alias_node)) = (child.field("name"), child.field("alias"))
            {
                let symbol_name = name_node.text().to_string();
                let alias = alias_node.text().to_string();
                imports.push(PythonImportedSymbolInfo {
                    import_type: PythonImportType::AliasedFutureImport,
                    import_path: import_path.clone(),
                    identifier: Some(ImportIdentifier {
                        name: symbol_name,
                        alias: Some(alias),
                    }),
                    range: node_to_range(&name_node),
                    scope: scope.clone(),
                });
            }
        }
    }
}

/// Get the scope (FQN) for an import node by looking it up in the FQN map
fn get_scope_for_node(
    node: &Node<StrDoc<SupportLang>>,
    node_fqn_map: &PythonNodeFqnMap,
) -> Option<PythonFqn> {
    let range = node_to_range(node);
    node_fqn_map.get(&range).map(|(_, fqn)| fqn.clone())
}

#[cfg(test)]
mod import_tests {
    use super::*;
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};
    use crate::python::fqn::build_fqn_index;

    fn test_import_extraction(
        code: &str,
        expected_imported_symbols: Vec<(PythonImportType, &str, ImportIdentifier)>,
        description: &str,
    ) {
        println!("\n=== Testing: {description} ===");
        println!("Code snippet:\n{code}");

        let parser = GenericParser::default_for_language(SupportedLanguage::Python);
        let parse_result = parser.parse(code, Some("test.py")).unwrap();
        let (node_fqn_map, _definitions) = build_fqn_index(&parse_result.ast);
        let imported_symbols = find_imports(&parse_result.ast, &node_fqn_map);

        assert_eq!(
            imported_symbols.len(),
            expected_imported_symbols.len(),
            "Expected {} imported symbols, found {}",
            expected_imported_symbols.len(),
            imported_symbols.len()
        );

        println!("Found {} imported symbols:", imported_symbols.len());
        for (expected_type, expected_path, expected_identifier) in expected_imported_symbols {
            let _matching_symbol = imported_symbols
                .iter()
                .find(|i| {
                    i.import_type == expected_type
                        && i.import_path == expected_path
                        && i.identifier == Some(expected_identifier.clone())
                })
                .unwrap_or_else(|| {
                    panic!(
                        "Could not find: type={:?}, path={}, name={:?}, alias={:?}",
                        expected_type,
                        expected_path,
                        expected_identifier.name,
                        expected_identifier.alias
                    )
                });

            println!(
                "Found: type={:?}, path={}, name={:?}, alias={:?}",
                expected_type, expected_path, expected_identifier.name, expected_identifier.alias
            );
        }
        println!("✅ All assertions passed for: {description}\n");
    }

    #[test]
    fn test_regular_imports() {
        let code = r#"
import this.is.deeply.nested
import numpy, torch.nn
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::Import,
                "this.is.deeply.nested",
                ImportIdentifier {
                    name: "this.is.deeply.nested".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::Import,
                "numpy",
                ImportIdentifier {
                    name: "numpy".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::Import,
                "torch.nn",
                ImportIdentifier {
                    name: "torch.nn".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Regular imports");
    }

    #[test]
    fn test_aliased_regular_imports() {
        let code = r#"
import xml.etree.ElementTree as ET
import numpy as np, torch as pytorch
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::AliasedImport,
                "xml.etree.ElementTree",
                ImportIdentifier {
                    name: "xml.etree.ElementTree".to_string(),
                    alias: Some("ET".to_string()),
                },
            ),
            (
                PythonImportType::AliasedImport,
                "numpy",
                ImportIdentifier {
                    name: "numpy".to_string(),
                    alias: Some("np".to_string()),
                },
            ),
            (
                PythonImportType::AliasedImport,
                "torch",
                ImportIdentifier {
                    name: "torch".to_string(),
                    alias: Some("pytorch".to_string()),
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Aliased imports");
    }

    #[test]
    fn test_from_imports() {
        let code = r#"
from os.path import join
from numpy import array, matrix
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::FromImport,
                "os.path",
                ImportIdentifier {
                    name: "join".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::FromImport,
                "numpy",
                ImportIdentifier {
                    name: "array".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::FromImport,
                "numpy",
                ImportIdentifier {
                    name: "matrix".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "From imports");
    }

    #[test]
    fn test_aliased_from_imports() {
        let code = r#"
from urllib.parse import quote as url_quote
from typing import List as ListType, Dict as DictType
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::AliasedFromImport,
                "urllib.parse",
                ImportIdentifier {
                    name: "quote".to_string(),
                    alias: Some("url_quote".to_string()),
                },
            ),
            (
                PythonImportType::AliasedFromImport,
                "typing",
                ImportIdentifier {
                    name: "List".to_string(),
                    alias: Some("ListType".to_string()),
                },
            ),
            (
                PythonImportType::AliasedFromImport,
                "typing",
                ImportIdentifier {
                    name: "Dict".to_string(),
                    alias: Some("DictType".to_string()),
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Aliased from imports");
    }

    #[test]
    fn test_mixed_from_imports() {
        let code = r#"
from collections import namedtuple, defaultdict as dd, other_stuff
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::FromImport,
                "collections",
                ImportIdentifier {
                    name: "namedtuple".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::AliasedFromImport,
                "collections",
                ImportIdentifier {
                    name: "defaultdict".to_string(),
                    alias: Some("dd".to_string()),
                },
            ),
            (
                PythonImportType::FromImport,
                "collections",
                ImportIdentifier {
                    name: "other_stuff".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(
            code,
            expected_imported_symbols,
            "Mixed from imports (aliased and non-aliased)",
        );
    }

    #[test]
    fn test_wildcard_imports() {
        let code = r#"
from tkinter import *
from this.is.nested import *
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::WildcardImport,
                "tkinter",
                ImportIdentifier {
                    name: "*".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::WildcardImport,
                "this.is.nested",
                ImportIdentifier {
                    name: "*".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Wildcard imports");
    }

    #[test]
    fn test_relative_imports() {
        let code = r#"
from .. import rel_symbol
from .. import rel_symbol1, rel_symbol2
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::RelativeImport,
                "..",
                ImportIdentifier {
                    name: "rel_symbol".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::RelativeImport,
                "..",
                ImportIdentifier {
                    name: "rel_symbol1".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::RelativeImport,
                "..",
                ImportIdentifier {
                    name: "rel_symbol2".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Relative imports");
    }

    #[test]
    fn test_aliased_relative_imports() {
        let code = r#"
from .subpackage import something as sth
from .subpackage import one_thing as oth, another_thing as ath
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::AliasedRelativeImport,
                ".subpackage",
                ImportIdentifier {
                    name: "something".to_string(),
                    alias: Some("sth".to_string()),
                },
            ),
            (
                PythonImportType::AliasedRelativeImport,
                ".subpackage",
                ImportIdentifier {
                    name: "one_thing".to_string(),
                    alias: Some("oth".to_string()),
                },
            ),
            (
                PythonImportType::AliasedRelativeImport,
                ".subpackage",
                ImportIdentifier {
                    name: "another_thing".to_string(),
                    alias: Some("ath".to_string()),
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Aliased relative imports");
    }

    #[test]
    fn test_relative_wildcard_imports() {
        let code = r#"
from ..parent_module import *
        "#;
        let expected_imported_symbols = vec![(
            PythonImportType::RelativeWildcardImport,
            "..parent_module",
            ImportIdentifier {
                name: "*".to_string(),
                alias: None,
            },
        )];
        test_import_extraction(code, expected_imported_symbols, "Relative wildcard imports");
    }

    #[test]
    fn test_future_imports() {
        let code = r#"
from __future__ import annotations
from __future__ import print_function, division
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::FutureImport,
                "__future__",
                ImportIdentifier {
                    name: "annotations".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::FutureImport,
                "__future__",
                ImportIdentifier {
                    name: "print_function".to_string(),
                    alias: None,
                },
            ),
            (
                PythonImportType::FutureImport,
                "__future__",
                ImportIdentifier {
                    name: "division".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Future imports");
    }

    #[test]
    fn test_aliased_future_imports() {
        let code = r#"
from __future__ import annotations as annot
from __future__ import print_function as pf, division as div
        "#;
        let expected_imported_symbols = vec![
            (
                PythonImportType::AliasedFutureImport,
                "__future__",
                ImportIdentifier {
                    name: "annotations".to_string(),
                    alias: Some("annot".to_string()),
                },
            ),
            (
                PythonImportType::AliasedFutureImport,
                "__future__",
                ImportIdentifier {
                    name: "print_function".to_string(),
                    alias: Some("pf".to_string()),
                },
            ),
            (
                PythonImportType::AliasedFutureImport,
                "__future__",
                ImportIdentifier {
                    name: "division".to_string(),
                    alias: Some("div".to_string()),
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Aliased future imports");
    }
}
