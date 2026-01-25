use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use crate::analysis::types::{DefinitionNode, FqnType, ImportedSymbolNode, OptimizedFileTree};

/// Given 'from foo.bar import symbol', returns possible files:
/// - foo/bar.py
/// - foo/bar/__init__.py
///
/// For relative imports like 'from .foo import symbol' from 'pkg/module.py':
/// - pkg/foo.py
/// - pkg/foo/__init__.py
///
/// Note that Python resolution order prioritizes packages over modules,
/// so the files returned are ordered by priority (highest first).
pub fn get_possible_symbol_locations(
    imported_symbol_node: &ImportedSymbolNode,
    file_tree: &OptimizedFileTree,
    _definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
) -> Vec<String> {
    let mut possible_files = Vec::new();

    // Pre-compute lowercase paths to avoid repeated conversions
    let file_path = imported_symbol_node.location.file_path.to_lowercase();
    let import_path = imported_symbol_node.import_path.to_lowercase();
    let is_relative = import_path.starts_with('.');

    if is_relative {
        // Handle relative imports
        possible_files.extend(resolve_relative_import(&import_path, &file_path));
    } else {
        // Handle absolute imports
        possible_files.extend(resolve_absolute_import(&import_path, &file_path, file_tree));
    }

    // Filter to only files that exist in the tree and deduplicate
    let mut seen = HashSet::new();
    possible_files
        .into_iter()
        .filter_map(|f| file_tree.get_denormalized_file(&f).cloned())
        .filter(|f| seen.insert(f.clone()))
        .collect()
}

/// Optimized version of relative import resolution
fn resolve_relative_import(module_path: &str, current_file: &str) -> Vec<String> {
    let mut possible_files = Vec::new();
    let current_path = Path::new(current_file);

    // Count leading dots
    let level = module_path.chars().take_while(|&c| c == '.').count();

    // Remove leading dots to get the actual module path
    let relative_module = if level < module_path.len() {
        &module_path[level..]
    } else {
        ""
    };

    // Navigate up the directory tree based on dot count
    let base_dir = if current_path.file_name() == Some(std::ffi::OsStr::new("__init__.py")) {
        // If we're in __init__.py, start from the package directory (parent of __init__.py)
        // For level 0 (no dots), stay in the same package
        // For level 1 (one dot), go up one level from the package directory
        let mut dir = current_path.parent().unwrap().to_path_buf();
        for _ in 1..level {
            if let Some(parent) = dir.parent() {
                dir = parent.to_path_buf();
            }
        }
        dir
    } else {
        // If we're in a regular module, start from parent
        let mut dir = current_path.to_path_buf();
        for _ in 0..level {
            if let Some(parent) = dir.parent() {
                dir = parent.to_path_buf();
            }
        }
        dir
    };

    if !relative_module.is_empty() {
        // Convert module path to file paths
        let module_parts: Vec<&str> = relative_module.split('.').collect();
        possible_files.extend(get_possible_paths(&base_dir, &module_parts));
    } else {
        // Just dots (like 'from .. import something')
        // This imports from the __init__.py of the target package
        let init_file = base_dir.join("__init__.py");
        possible_files.push(init_file.to_string_lossy().to_string());
    }
    possible_files
}

/// Optimized version of absolute import resolution
fn resolve_absolute_import(
    module_path: &str,
    current_file: &str,
    optimized_tree: &OptimizedFileTree,
) -> Vec<String> {
    let mut possible_files = Vec::new();
    let module_parts: Vec<&str> = module_path.split('.').collect();

    if !module_parts.is_empty() {
        // Use precomputed root directories
        let search_paths = optimized_tree.get_root_dirs();

        // Try to resolve from each search path
        for search_path in search_paths {
            possible_files.extend(get_possible_paths(search_path, &module_parts));
        }

        // Also try from the directory of the importing file
        let importing_path = Path::new(current_file);
        if let Some(importing_dir) = importing_path.parent() {
            possible_files.extend(get_possible_paths(importing_dir, &module_parts));
        }
    }

    possible_files
}

/// Get possible paths for a given module parts
/// - base_path: The base path to start from (e.g. /path/to/project)
/// - module_parts: The module parts to resolve (e.g. ['foo', 'bar'])
fn get_possible_paths(base_path: &Path, module_parts: &[&str]) -> Vec<String> {
    let mut possible_files = Vec::new();

    if module_parts.is_empty() {
        return possible_files;
    }

    // Build the path from parts
    let mut path = base_path.to_path_buf();
    for part in &module_parts[..module_parts.len() - 1] {
        path = path.join(part);
    }

    let last_part = module_parts[module_parts.len() - 1];

    // Option 1: It's a Python file
    let file_path = path.join(format!("{last_part}.py"));
    possible_files.push(file_path.to_string_lossy().to_string());

    // Option 2: It's a package with __init__.py
    let package_init = path.join(last_part).join("__init__.py");
    possible_files.push(package_init.to_string_lossy().to_string());

    // // Option 3: For single module, could also be directly in base_path
    // if module_parts.len() == 1 {
    //     let direct_file = base_path.join(format!("{}.py", last_part));
    //     let direct_file_str = direct_file.to_string_lossy().to_string();
    //     if !possible_files.contains(&direct_file_str) {
    //         possible_files.push(direct_file_str);
    //     }
    // }

    possible_files
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::types::{
        ImportIdentifier, ImportType, ImportedSymbolLocation, ImportedSymbolNode,
    };
    use parser_core::python::types::PythonImportType;
    use std::collections::HashMap;

    /// Helper function to create a test ImportedSymbolNode
    fn create_imported_symbol_node(
        import_path: &str,
        file_path: &str,
        import_type: PythonImportType,
    ) -> ImportedSymbolNode {
        ImportedSymbolNode::new(
            ImportType::Python(import_type),
            import_path.to_string(),
            Some(ImportIdentifier {
                name: "test_symbol".to_string(),
                alias: None,
            }),
            ImportedSymbolLocation {
                file_path: file_path.to_string(),
                start_byte: 0,
                end_byte: 10,
                start_line: 1,
                end_line: 1,
                start_col: 0,
                end_col: 10,
            },
        )
    }

    /// Helper function to create a test file tree
    fn create_file_tree() -> OptimizedFileTree {
        let file_paths = vec![
            "src/main.py".to_string(),
            "src/utils.py".to_string(),
            "src/package/__init__.py".to_string(),
            "src/package/module.py".to_string(),
            "src/package/subpackage/__init__.py".to_string(),
            "src/package/subpackage/utils.py".to_string(),
            "src/package/subpackage/helpers.py".to_string(),
            "tests/test_main.py".to_string(),
            "tests/test_utils.py".to_string(),
            "tests/package/__init__.py".to_string(),
            "tests/package/test_module.py".to_string(),
            "root_module.py".to_string(),
            "root_package/__init__.py".to_string(),
            "root_package/module.py".to_string(),
        ];
        OptimizedFileTree::new(file_paths.iter())
    }

    #[test]
    fn test_absolute_import_simple_module() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol =
            create_imported_symbol_node("utils", "src/main.py", PythonImportType::FromImport);

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should find utils.py in the same directory as main.py
        assert!(result.contains(&"src/utils.py".to_string()));
        // The function should return at least one result
        assert!(!result.is_empty());
    }

    #[test]
    fn test_absolute_import_package_module() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol = create_imported_symbol_node(
            "package.module",
            "src/main.py",
            PythonImportType::FromImport,
        );

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        println!("Result for package.module: {result:?}");

        // Should find package/module.py
        assert!(result.contains(&"src/package/module.py".to_string()));
        // The function should return at least one result
        assert!(!result.is_empty());
    }

    #[test]
    fn test_relative_import_same_directory() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol =
            create_imported_symbol_node(".utils", "src/main.py", PythonImportType::RelativeImport);

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should find utils.py in the same directory
        assert!(result.contains(&"src/utils.py".to_string()));
    }

    #[test]
    fn test_relative_import_parent_directory() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol = create_imported_symbol_node(
            "..module",
            "src/package/subpackage/helpers.py",
            PythonImportType::RelativeImport,
        );

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should find package/module.py (going up one level from subpackage)
        assert!(result.contains(&"src/package/module.py".to_string()));
    }

    #[test]
    fn test_relative_import_from_init_py() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol = create_imported_symbol_node(
            ".module",
            "src/package/__init__.py",
            PythonImportType::RelativeImport,
        );

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should find module.py in the same package
        assert!(result.contains(&"src/package/module.py".to_string()));
    }

    #[test]
    fn test_relative_import_just_dots() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol = create_imported_symbol_node(
            "..",
            "src/package/subpackage/helpers.py",
            PythonImportType::RelativeImport,
        );

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should find __init__.py in parent package
        assert!(result.contains(&"src/package/__init__.py".to_string()));
    }

    #[test]
    fn test_import_nonexistent_module() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol = create_imported_symbol_node(
            "nonexistent.module",
            "src/main.py",
            PythonImportType::FromImport,
        );

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should return empty vector since the module doesn't exist
        assert!(result.is_empty());
    }

    #[test]
    fn test_relative_import_nonexistent_module() {
        let file_tree = create_file_tree();
        let definition_map = HashMap::new();

        let imported_symbol = create_imported_symbol_node(
            ".nonexistent",
            "src/main.py",
            PythonImportType::RelativeImport,
        );

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should return empty vector since the module doesn't exist
        assert!(result.is_empty());
    }

    #[test]
    fn test_case_insensitive_matching() {
        let file_paths = [
            "src/Utils.py".to_string(),
            "src/Package/Module.py".to_string(),
        ];
        let file_tree = OptimizedFileTree::new(file_paths.iter());
        let definition_map = HashMap::new();

        // Test with lowercase import path
        let imported_symbol =
            create_imported_symbol_node("utils", "src/main.py", PythonImportType::FromImport);

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should find Utils.py due to case-insensitive matching
        assert!(result.contains(&"src/Utils.py".to_string()));
    }

    #[test]
    fn test_duplicate_removal() {
        let file_paths = [
            "src/utils.py".to_string(),
            "src/utils.py".to_string(), // Duplicate
        ];
        let file_tree = OptimizedFileTree::new(file_paths.iter());
        let definition_map = HashMap::new();

        let imported_symbol =
            create_imported_symbol_node("utils", "src/main.py", PythonImportType::FromImport);

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should only have one entry despite duplicate in file_tree
        assert_eq!(result.len(), 1);
        assert!(result.contains(&"src/utils.py".to_string()));
    }

    #[test]
    fn test_empty_file_tree() {
        let file_paths = Vec::new();
        let file_tree = OptimizedFileTree::new(file_paths.iter());
        let definition_map = HashMap::new();

        let imported_symbol =
            create_imported_symbol_node("utils", "src/main.py", PythonImportType::FromImport);

        let result = get_possible_symbol_locations(&imported_symbol, &file_tree, &definition_map);

        // Should return empty vector
        assert!(result.is_empty());
    }
}
