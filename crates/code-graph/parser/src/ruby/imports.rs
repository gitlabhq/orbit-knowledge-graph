use crate::imports::ImportedSymbolInfo;
use crate::ruby::types::{RubyFqn, RubyImportType};

pub type RubyImportedSymbolInfo = ImportedSymbolInfo<RubyImportType, RubyFqn>;

#[cfg(test)]
mod import_tests {
    use super::*;
    use crate::imports::ImportIdentifier;
    use crate::parser::{Language, ParserType, UnifiedParseResult};
    use crate::ruby::analyzer::RubyAnalyzer;
    use crate::ruby::types::RubyImportType;
    // use crate::ruby::visit::extract_definitions_and_references_from_prism;
    fn test_import_extraction(
        code: &str,
        expected_imported_symbols: Vec<(RubyImportType, &str, ImportIdentifier)>, // (import_type, import_path, identifier)
    ) {
        let parser = ParserType::for_language(Language::Ruby);
        let parse_result = parser.parse(code, None).unwrap();
        let ruby_analyzer = RubyAnalyzer::new();

        let imported_symbols = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let analysis_result = ruby_analyzer
                .analyze_with_prism(code, &prism_parse_result.ast)
                .unwrap();
            analysis_result.imports
        } else {
            panic!("Expected Ruby parse result");
        };

        assert_eq!(
            imported_symbols.len(),
            expected_imported_symbols.len(),
            "Expected {} imported symbols, found {}",
            expected_imported_symbols.len(),
            imported_symbols.len()
        );
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
        }
    }

    #[test]
    fn test_require_imports() {
        let code = r#"
require 'json'
require 'net/http'
require 'activerecord'
        "#;
        let expected_imported_symbols = vec![
            (
                RubyImportType::Require,
                "'json'",
                ImportIdentifier {
                    name: "'json'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Require,
                "'net/http'",
                ImportIdentifier {
                    name: "'net/http'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Require,
                "'activerecord'",
                ImportIdentifier {
                    name: "'activerecord'".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols);
    }

    #[test]
    fn test_require_relative_imports() {
        let code = r#"
require_relative '../models/user'
require_relative './config/database'
require_relative 'helpers'
        "#;
        let expected_imported_symbols = vec![
            (
                RubyImportType::RequireRelative,
                "'../models/user'",
                ImportIdentifier {
                    name: "'../models/user'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::RequireRelative,
                "'./config/database'",
                ImportIdentifier {
                    name: "'./config/database'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::RequireRelative,
                "'helpers'",
                ImportIdentifier {
                    name: "'helpers'".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols);
    }

    #[test]
    fn test_load_imports() {
        let code = r#"
load 'script.rb'
load './config/settings.rb'
        "#;
        let expected_imported_symbols = vec![
            (
                RubyImportType::Load,
                "'script.rb'",
                ImportIdentifier {
                    name: "'script.rb'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Load,
                "'./config/settings.rb'",
                ImportIdentifier {
                    name: "'./config/settings.rb'".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols);
    }

    #[test]
    fn test_autoload_imports() {
        let code = r#"
autoload :MyClass, 'my_class'
autoload :SomeModule, 'lib/some_module'
        "#;
        let expected_imported_symbols = vec![
            (
                RubyImportType::Autoload,
                "'my_class'",
                ImportIdentifier {
                    name: ":MyClass".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Autoload,
                "'lib/some_module'",
                ImportIdentifier {
                    name: ":SomeModule".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols);
    }

    #[test]
    fn test_mixed_imports() {
        let code = r#"
require 'json'
require_relative '../models/user'
load 'script.rb'
autoload :MyClass, 'my_class'
        "#;
        let expected_imported_symbols = vec![
            (
                RubyImportType::Require,
                "'json'",
                ImportIdentifier {
                    name: "'json'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::RequireRelative,
                "'../models/user'",
                ImportIdentifier {
                    name: "'../models/user'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Load,
                "'script.rb'",
                ImportIdentifier {
                    name: "'script.rb'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Autoload,
                "'my_class'",
                ImportIdentifier {
                    name: ":MyClass".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols);
    }

    #[test]
    fn test_comprehensive_imports() {
        let code = r#"
# Basic patterns
require 'json'
require_relative '../models/user'
load 'script.rb'
autoload :MyClass, 'my_class'

# With parentheses
require('net/http')
require_relative('./config/database')
load('settings.rb')
autoload(:SomeModule, 'lib/some_module')

# Kernel explicit calls
Kernel.require 'openssl'
Kernel.load 'kernel_script.rb'

# Load with force reload
load 'config.rb', true

# Nested paths
require 'active_record/base'
require_relative '../../../shared/utils'
        "#;
        let expected_imported_symbols = vec![
            // Basic patterns
            (
                RubyImportType::Require,
                "'json'",
                ImportIdentifier {
                    name: "'json'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::RequireRelative,
                "'../models/user'",
                ImportIdentifier {
                    name: "'../models/user'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Load,
                "'script.rb'",
                ImportIdentifier {
                    name: "'script.rb'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Autoload,
                "'my_class'",
                ImportIdentifier {
                    name: ":MyClass".to_string(),
                    alias: None,
                },
            ),
            // With parentheses (clean paths without parentheses)
            (
                RubyImportType::Require,
                "'net/http'",
                ImportIdentifier {
                    name: "'net/http'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::RequireRelative,
                "'./config/database'",
                ImportIdentifier {
                    name: "'./config/database'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Load,
                "'settings.rb'",
                ImportIdentifier {
                    name: "'settings.rb'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Autoload,
                "'lib/some_module'",
                ImportIdentifier {
                    name: ":SomeModule".to_string(),
                    alias: None,
                },
            ),
            // Kernel explicit calls
            (
                RubyImportType::Require,
                "'openssl'",
                ImportIdentifier {
                    name: "'openssl'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::Load,
                "'kernel_script.rb'",
                ImportIdentifier {
                    name: "'kernel_script.rb'".to_string(),
                    alias: None,
                },
            ),
            // Load with force reload (only the path, not the extra argument)
            (
                RubyImportType::Load,
                "'config.rb'",
                ImportIdentifier {
                    name: "'config.rb'".to_string(),
                    alias: None,
                },
            ),
            // Nested paths
            (
                RubyImportType::Require,
                "'active_record/base'",
                ImportIdentifier {
                    name: "'active_record/base'".to_string(),
                    alias: None,
                },
            ),
            (
                RubyImportType::RequireRelative,
                "'../../../shared/utils'",
                ImportIdentifier {
                    name: "'../../../shared/utils'".to_string(),
                    alias: None,
                },
            ),
        ];
        test_import_extraction(code, expected_imported_symbols);
    }

    #[test]
    fn test_import_scopes_with_fqn_integration() {
        let ruby_code = r#"
# Top-level imports (no scope)
require 'json'
require_relative '../config/database'

class MyClass
  # Imports inside class
  require 'activerecord'
  load 'class_helper.rb'
  
  def initialize
    # Import inside method within class
    require 'debug'
  end
  
  class << self
    # Import inside singleton class
    autoload :Validator, 'validators/my_validator'
  end
end

module MyModule
  # Import inside module
  require_relative './module_helper'
  
  def self.setup
    # Import inside module method
    load 'setup.rb'
  end
  
  class NestedClass
    # Import inside nested class within module
    require 'nested_dependency'
  end
end

def top_level_method
  # Import inside top-level method
  require 'method_dependency'
end
        "#;

        let parser = ParserType::for_language(Language::Ruby);
        let parse_result = parser.parse(ruby_code, None).unwrap();
        let ruby_analyzer = RubyAnalyzer::new();

        let imported_symbols = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let analysis_result = ruby_analyzer
                .analyze_with_prism(ruby_code, &prism_parse_result.ast)
                .unwrap();
            analysis_result.imports
        } else {
            panic!("Expected Ruby parse result");
        };

        let find_import_by_path = |path: &str| -> &RubyImportedSymbolInfo {
            imported_symbols
                .iter()
                .find(|symbol| symbol.import_path == path)
                .unwrap_or_else(|| panic!("Should find import: {path}"))
        };

        let get_scope_parts = |import: &RubyImportedSymbolInfo| -> Vec<String> {
            match &import.scope {
                Some(scope) => scope
                    .parts
                    .iter()
                    .map(|part| part.node_name().to_string())
                    .collect(),
                None => vec![],
            }
        };

        let top_level_imports = ["'json'", "'../config/database'"];
        for expected_path in &top_level_imports {
            let import = find_import_by_path(expected_path);
            assert!(
                import.scope.is_none(),
                "Top-level import {} should have no scope, got {:?}",
                expected_path,
                get_scope_parts(import)
            );
        }

        let assert_import_has_scope = |path: &str, expected_scope: &[&str]| {
            let import = find_import_by_path(path);
            let actual_scope = get_scope_parts(import);
            let expected_scope_strings: Vec<String> =
                expected_scope.iter().map(|s| s.to_string()).collect();

            assert_eq!(
                actual_scope, expected_scope_strings,
                "Import {path} should have scope {expected_scope_strings:?}, got {actual_scope:?}"
            );
        };

        assert_import_has_scope("'activerecord'", &["MyClass"]);

        assert_import_has_scope("'debug'", &["MyClass", "initialize"]);
        assert_import_has_scope("'method_dependency'", &["top_level_method"]);
        assert_import_has_scope("'./module_helper'", &["MyModule"]);
        assert_import_has_scope("'setup.rb'", &["MyModule", "setup"]);
        assert_import_has_scope("'nested_dependency'", &["MyModule", "NestedClass"]);
    }

    #[test]
    fn test_fqn_based_import_detection() {
        let ruby_code = r#"
require 'json'
require_relative '../models/user'
load 'script.rb'
autoload :MyClass, 'my_class'

class MyClass
  require 'activerecord'
  autoload :Validator, 'validators/my_validator'
end
        "#;

        let parser = ParserType::for_language(Language::Ruby);
        let parse_result = parser.parse(ruby_code, None).unwrap();
        let ruby_analyzer = RubyAnalyzer::new();

        let fqn_imports = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let analysis_result = ruby_analyzer
                .analyze_with_prism(ruby_code, &prism_parse_result.ast)
                .unwrap();
            analysis_result.imports
        } else {
            panic!("Expected Ruby parse result");
        };

        // Helper to find exact import match
        let find_exact_import = |expected_type: RubyImportType, expected_path: &str| {
            fqn_imports
                .iter()
                .find(|import| {
                    import.import_type == expected_type && import.import_path == expected_path
                })
                .unwrap_or_else(|| {
                    panic!("Should find exact match for {expected_type:?} '{expected_path}'")
                })
        };

        // Verify scoping works correctly by checking specific expected imports
        // Top-level imports (should have no scope)
        let expected_top_level = [
            (RubyImportType::Require, "'json'"),
            (RubyImportType::RequireRelative, "'../models/user'"),
            (RubyImportType::Load, "'script.rb'"),
            (RubyImportType::Autoload, "'my_class'"),
        ];

        for (import_type, path) in expected_top_level {
            let import = find_exact_import(import_type, path);
            assert!(
                import.scope.is_none(),
                "Import {path} should be top-level (no scope)"
            );
        }

        // Scoped imports (should have MyClass scope)
        let expected_scoped = [
            (RubyImportType::Require, "'activerecord'"),
            (RubyImportType::Autoload, "'validators/my_validator'"),
        ];

        for (import_type, path) in expected_scoped {
            let import = find_exact_import(import_type, path);
            assert!(import.scope.is_some(), "Import {path} should be scoped");
        }
    }
}
