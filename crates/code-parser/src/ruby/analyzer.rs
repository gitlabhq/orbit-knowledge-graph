use ruby_prism::{ParseResult, parse};

use crate::analyzer::{AnalysisResult, Analyzer};
use crate::references::ReferenceInfo;
use crate::ruby::fqn::ruby_fqn_to_string;
use crate::ruby::references::types::{
    RubyExpressionMetadata, RubyReferenceType, RubyTargetResolution,
};
use crate::ruby::types::RubyFqn;
use crate::ruby::types::{RubyDefinitionType, RubyImportType};
use crate::ruby::visit::extract_definitions_and_references_from_prism;

/// Type alias for Ruby-specific analyzer
pub type RubyAnalyzer = Analyzer<RubyFqn, RubyDefinitionType, RubyImportType>;

/// Type alias for Ruby-specific analysis result with references
pub type RubyAnalysisResult = AnalysisResult<
    RubyFqn,
    RubyDefinitionType,
    RubyImportType,
    ReferenceInfo<RubyTargetResolution, RubyReferenceType, RubyExpressionMetadata, RubyFqn>,
>;

impl RubyAnalyzer {
    /// Analyze Ruby code using ruby-prism parser for fast definition and expression extraction
    pub fn analyze_with_prism(
        &self,
        code: &str,
        parse_result: &ParseResult<'_>,
    ) -> crate::Result<RubyAnalysisResult> {
        // Extract definitions, references, and imports using the prism-based visitor
        // References are now created directly during AST traversal for better memory efficiency
        let (definitions, imports, references) =
            extract_definitions_and_references_from_prism(code, parse_result);

        // No conversion needed - references are already in the correct format
        Ok(AnalysisResult::new(definitions, imports, references))
    }

    /// Convenience method to parse and analyze Ruby code in one step
    pub fn parse_and_analyze(&self, code: &str) -> crate::Result<RubyAnalysisResult> {
        let parse_result = parse(code.as_bytes());
        self.analyze_with_prism(code, &parse_result)
    }
}

impl RubyAnalysisResult {
    /// Get FQN strings for all definitions that have them (prism-specific)
    pub fn ruby_definition_fqn_strings(&self) -> Vec<String> {
        self.definition_fqn_strings(ruby_fqn_to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definitions::DefinitionLookup;
    use crate::ruby::types::RubyDefinitionType;

    #[test]
    fn test_analyzer_creation() {
        let _analyzer = RubyAnalyzer::new();
    }

    #[test]
    fn test_analyze_simple_ruby_code() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let ruby_code = r#"
class Calculator
  attr_reader :value

  def initialize
    @value = 0
  end

  def add(number)
    @value += number
    self
  end

  def self.create
    new
  end
end

module MathUtils
  PI = 3.14159
  
  def self.square(n)
    n * n
  end
end
"#;

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(ruby_code, &parse_result)?;

        // Check that we found definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");

        // Check specific types
        let classes = result.definitions_of_type(&RubyDefinitionType::Class);
        let modules = result.definitions_of_type(&RubyDefinitionType::Module);
        let methods = result.definitions_of_type(&RubyDefinitionType::Method);
        let singleton_methods = result.definitions_of_type(&RubyDefinitionType::SingletonMethod);

        assert!(!classes.is_empty(), "Should find classes");
        assert!(!modules.is_empty(), "Should find modules");
        assert!(!methods.is_empty(), "Should find methods");
        assert!(
            !singleton_methods.is_empty(),
            "Should find singleton methods"
        );

        // Check specific names
        let calculator_class = result.definitions_by_name("Calculator");
        assert!(!calculator_class.is_empty(), "Should find Calculator class");

        let math_utils_module = result.definitions_by_name("MathUtils");
        assert!(
            !math_utils_module.is_empty(),
            "Should find MathUtils module"
        );

        // Check FQN functionality
        let fqn_strings = result.ruby_definition_fqn_strings();
        assert!(!fqn_strings.is_empty(), "Should have FQN strings");

        println!("Found FQN strings: {fqn_strings:?}");

        // Print results for debugging
        println!("Found {} definitions:", result.definitions.len());

        let counts = result.count_definitions_by_type();
        println!("Counts by type: {counts:?}");

        Ok(())
    }

    #[test]
    fn test_analyzer_with_complex_ruby_code() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let ruby_code = r#"
module Authentication
  class User
    attr_accessor :name, :email
    
    ROLES = %w[admin user guest].freeze
    
    def initialize(name, email)
      @name = name
      @email = email
    end
    
    def admin?
      @role == :admin
    end
    
    def self.find_by_email(email)
      # Implementation
    end
    
    class << self
      def all_users
        # Implementation
      end
    end
  end
  
  module Validators
    EMAIL_REGEX = /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i
    
    def self.valid_email?(email)
      EMAIL_REGEX.match?(email)
    end
  end
end
"#;

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(ruby_code, &parse_result)?;

        // Verify we found various types of definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");

        let counts = result.count_definitions_by_type();
        println!("Complex code counts: {counts:?}");

        // Should find nested structures - updated for callable-only design
        assert!(counts.get(&RubyDefinitionType::Module).unwrap_or(&0) >= &2);
        assert!(counts.get(&RubyDefinitionType::Class).unwrap_or(&0) >= &1);
        assert!(counts.get(&RubyDefinitionType::Method).unwrap_or(&0) >= &3);

        // Test FQN functionality with nested structures
        let definitions = result.all_definitions();
        assert!(!definitions.is_empty(), "Should have definitions with FQNs");

        for def in &definitions {
            let fqn_string = ruby_fqn_to_string(&def.fqn);
            println!("Definition with FQN: {} -> {}", def.name, fqn_string);
        }

        Ok(())
    }

    #[test]
    fn test_analysis_result_methods() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let ruby_code = r#"
class Test
  def method1; end
  def method2; end
end

class Test2
  def method1; end
end
"#;

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(ruby_code, &parse_result)?;

        // Test definitions_by_name
        let method1_defs = result.definitions_by_name("method1");
        assert_eq!(method1_defs.len(), 2, "Should find 2 method1 definitions");

        let test_defs = result.definitions_by_name("Test");
        assert_eq!(test_defs.len(), 1, "Should find 1 Test class");

        // Test definition_names
        let names = result.definition_names();
        assert!(names.contains(&"Test"));
        assert!(names.contains(&"Test2"));
        assert!(names.contains(&"method1"));
        assert!(names.contains(&"method2"));

        // Test definitions
        let definitions = result.all_definitions();
        assert!(!definitions.is_empty(), "Should have definitions");

        // Test fqn_strings
        let fqn_strings = result.ruby_definition_fqn_strings();
        assert!(!fqn_strings.is_empty(), "Should have FQN strings");

        for fqn_string in &fqn_strings {
            assert!(!fqn_string.is_empty(), "FQN strings should not be empty");
        }

        Ok(())
    }

    #[test]
    fn test_error_handling() {
        // These imports are left in case they're needed for tests
        // use crate::parser::{GenericParser, LanguageParser};

        let analyzer = RubyAnalyzer::new();

        // Create a valid Ruby parse result
        let parse_result = parse("class Test; end".as_bytes());
        let result = analyzer.analyze_with_prism("class Test; end", &parse_result);
        assert!(result.is_ok(), "Should work with Ruby language");
    }

    #[test]
    fn test_analyzer_with_sample_rb_fixture() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let fixture_path = "src/ruby/fixtures/sample.rb";
        let ruby_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read sample.rb fixture");

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(&ruby_code, &parse_result)?;

        // Verify we found definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find definitions in sample.rb"
        );

        // Count by type
        let counts = result.count_definitions_by_type();
        println!("sample.rb analyzer counts: {counts:?}");

        // Test specific filtering
        let modules = result.definitions_of_type(&RubyDefinitionType::Module);
        let classes = result.definitions_of_type(&RubyDefinitionType::Class);
        let methods = result.definitions_of_type(&RubyDefinitionType::Method);
        let singleton_methods = result.definitions_of_type(&RubyDefinitionType::SingletonMethod);

        assert!(modules.len() >= 2, "Should find at least 2 modules");
        assert!(classes.len() >= 2, "Should find at least 2 classes");
        assert!(methods.len() >= 2, "Should find at least 2 methods");
        assert!(
            singleton_methods.len() >= 2,
            "Should find at least 2 singleton methods"
        );

        // Test name searching
        let auth_service_defs = result.definitions_by_name("AuthenticationService");
        assert!(
            !auth_service_defs.is_empty(),
            "Should find AuthenticationService"
        );

        let credentials_checker_defs = result.definitions_by_name("CredentialsChecker");
        assert!(
            !credentials_checker_defs.is_empty(),
            "Should find CredentialsChecker"
        );

        // Test definition names
        let names = result.definition_names();
        assert!(names.contains(&"AuthenticationService"));
        assert!(names.contains(&"CredentialsChecker"));
        assert!(names.contains(&"initialize"));
        assert!(names.contains(&"valid_password?"));

        // Test FQN functionality
        let definitions = result.all_definitions();
        assert!(!definitions.is_empty(), "Should have definitions with FQNs");

        let fqn_strings = result.ruby_definition_fqn_strings();
        assert!(!fqn_strings.is_empty(), "Should have FQN strings");

        println!(
            "Found {} definitions in sample.rb",
            result.definitions.len()
        );

        Ok(())
    }

    #[test]
    fn test_analyzer_with_monolith_sample_1_rb_fixture() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let fixture_path = "src/ruby/fixtures/monolith_sample_1.rb";
        let ruby_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read monolith_sample_1.rb fixture");

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(&ruby_code, &parse_result)?;

        // Verify we found definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find definitions in monolith_sample_1.rb"
        );

        // Count by type
        let counts = result.count_definitions_by_type();
        println!("monolith_sample_1.rb analyzer counts: {counts:?}");

        assert_eq!(
            counts.get(&RubyDefinitionType::Class).unwrap_or(&0),
            &1,
            "Should find exactly 1 class"
        );
        assert!(
            counts.get(&RubyDefinitionType::Method).unwrap_or(&0) >= &9,
            "Should find at least 9 methods"
        );

        // Test name searching
        let jwt_controller_defs = result.definitions_by_name("JwtController");
        assert_eq!(
            jwt_controller_defs.len(),
            1,
            "Should find exactly 1 JwtController"
        );

        // Verify specific method names
        let names = result.definition_names();
        let expected_methods = [
            "auth",
            "authenticate_project_or_user",
            "log_authentication_failed",
            "render_access_denied",
            "auth_params",
            "additional_params",
            "scopes_param",
            "auth_user",
            "bypass_admin_mode!",
        ];

        for method_name in &expected_methods {
            assert!(
                names.contains(method_name),
                "Should find {method_name} method"
            );
        }

        println!(
            "Found {} definitions in monolith_sample_1.rb",
            result.definitions.len()
        );

        Ok(())
    }

    #[test]
    fn test_analyzer_with_references_test_rails_rb_fixture() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let fixture_path = "src/ruby/fixtures/references_test_rails.rb";
        let ruby_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read references_test_rails.rb fixture");

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(&ruby_code, &parse_result)?;

        // Verify we found definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find definitions in references_test_rails.rb"
        );

        // Count by type
        let counts = result.count_definitions_by_type();
        println!("references_test_rails.rb analyzer counts: {counts:?}");

        // Should find multiple classes, modules, and methods
        assert!(
            counts.get(&RubyDefinitionType::Class).unwrap_or(&0) >= &3,
            "Should find at least 3 classes"
        );
        assert!(
            counts.get(&RubyDefinitionType::Module).unwrap_or(&0) >= &3,
            "Should find at least 3 modules"
        );
        assert!(
            counts.get(&RubyDefinitionType::Method).unwrap_or(&0) >= &4,
            "Should find at least 4 methods"
        );

        // Test specific definitions
        let names = result.definition_names();
        assert!(names.contains(&"ApplicationController"));
        assert!(names.contains(&"UsersController"));
        assert!(names.contains(&"User"));
        assert!(names.contains(&"ActionController"));
        assert!(names.contains(&"set_user"));
        assert!(names.contains(&"show"));
        assert!(names.contains(&"create"));

        println!(
            "Found {} definitions in references_test_rails.rb",
            result.definitions.len()
        );

        Ok(())
    }

    #[test]
    fn test_analyzer_with_references_test_tracing_rb_fixture() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let fixture_path = "src/ruby/fixtures/references_test_tracing.rb";
        let ruby_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read references_test_tracing.rb fixture");

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(&ruby_code, &parse_result)?;

        // Verify we found definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find definitions in references_test_tracing.rb"
        );

        // Count by type
        let counts = result.count_definitions_by_type();
        println!("references_test_tracing.rb analyzer counts: {counts:?}");

        assert!(
            counts.get(&RubyDefinitionType::Module).unwrap_or(&0) >= &2,
            "Should find at least 2 modules"
        );
        assert!(
            counts.get(&RubyDefinitionType::Class).unwrap_or(&0) >= &1,
            "Should find at least 1 class"
        );
        assert!(
            counts.get(&RubyDefinitionType::Method).unwrap_or(&0) >= &1,
            "Should find at least 1 method"
        );
        assert!(
            counts
                .get(&RubyDefinitionType::SingletonMethod)
                .unwrap_or(&0)
                >= &1,
            "Should find at least 1 singleton method"
        );

        // Test specific definitions
        let names = result.definition_names();
        assert!(names.contains(&"Service"));
        assert!(names.contains(&"Client"));
        assert!(names.contains(&"Runner"));
        assert!(names.contains(&"execute"));
        assert!(names.contains(&"build"));

        println!(
            "Found {} definitions in references_test_tracing.rb",
            result.definitions.len()
        );

        Ok(())
    }

    #[test]
    fn test_analyzer_comprehensive_fixture_coverage() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let fixtures = [
            "src/ruby/fixtures/sample.rb",
            "src/ruby/fixtures/monolith_sample_1.rb",
            "src/ruby/fixtures/references_test_rails.rb",
            "src/ruby/fixtures/references_test_tracing.rb",
        ];

        let mut total_definitions = 0;
        let mut total_counts = std::collections::HashMap::new();
        let mut all_definition_names = std::collections::HashSet::new();
        let mut all_fqn_strings = std::collections::HashSet::new();

        for fixture_path in &fixtures {
            let ruby_code = std::fs::read_to_string(fixture_path)
                .unwrap_or_else(|_| panic!("Should be able to read {fixture_path}"));

            let parse_result = parse(ruby_code.as_bytes());
            let result = analyzer.analyze_with_prism(&ruby_code, &parse_result)?;

            assert!(
                !result.definitions.is_empty(),
                "Should find definitions in {fixture_path}"
            );

            total_definitions += result.definitions.len();

            // Accumulate counts
            let counts = result.count_definitions_by_type();
            for (def_type, count) in counts {
                *total_counts.entry(def_type).or_insert(0) += count;
            }

            // Collect all definition names
            for name in result.definition_names() {
                all_definition_names.insert(name.to_string());
            }

            // Collect all FQN strings
            for fqn_string in result.ruby_definition_fqn_strings() {
                all_fqn_strings.insert(fqn_string);
            }

            println!("{}: {} definitions", fixture_path, result.definitions.len());
        }

        println!("Total definitions across all fixtures: {total_definitions}");
        println!("Total counts by type: {total_counts:?}");
        println!(
            "Total unique definition names: {}",
            all_definition_names.len()
        );
        println!("Total unique FQN strings: {}", all_fqn_strings.len());

        // Comprehensive assertions - updated for callable-only design
        assert!(
            total_definitions >= 30,
            "Should find at least 30 definitions across all fixtures"
        );
        assert!(
            total_counts.get(&RubyDefinitionType::Class).unwrap_or(&0) >= &5,
            "Should find at least 5 classes"
        );
        assert!(
            total_counts.get(&RubyDefinitionType::Module).unwrap_or(&0) >= &5,
            "Should find at least 5 modules"
        );
        assert!(
            total_counts.get(&RubyDefinitionType::Method).unwrap_or(&0) >= &10,
            "Should find at least 10 methods"
        );
        assert!(
            total_counts
                .get(&RubyDefinitionType::SingletonMethod)
                .unwrap_or(&0)
                >= &3,
            "Should find at least 3 singleton methods"
        );

        // Verify we found a good variety of definition names
        assert!(
            all_definition_names.len() >= 25,
            "Should find at least 25 unique definition names"
        );

        // Verify we have FQN strings
        assert!(!all_fqn_strings.is_empty(), "Should have FQN strings");

        // Test that key definitions are found across fixtures
        let key_definitions = [
            "AuthenticationService",
            "CredentialsChecker",
            "JwtController",
            "ApplicationController",
            "UsersController",
            "Service",
            "Client",
            "Runner",
        ];

        for key_def in &key_definitions {
            assert!(
                all_definition_names.contains(*key_def),
                "Should find key definition: {key_def}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_analyzer_definition_filtering_and_grouping() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let fixture_path = "src/ruby/fixtures/sample.rb";
        let ruby_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read sample.rb fixture");

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(&ruby_code, &parse_result)?;

        // Test definitions_of_type filtering
        let all_modules = result.definitions_of_type(&RubyDefinitionType::Module);
        let all_classes = result.definitions_of_type(&RubyDefinitionType::Class);
        let all_methods = result.definitions_of_type(&RubyDefinitionType::Method);

        // Each filtered group should only contain the specified type
        for module_def in &all_modules {
            assert_eq!(module_def.definition_type, RubyDefinitionType::Module);
        }
        for class_def in &all_classes {
            assert_eq!(class_def.definition_type, RubyDefinitionType::Class);
        }
        for method_def in &all_methods {
            assert_eq!(method_def.definition_type, RubyDefinitionType::Method);
        }

        // Test definitions_by_name filtering
        let auth_service_defs = result.definitions_by_name("AuthenticationService");
        for def in &auth_service_defs {
            assert_eq!(def.name, "AuthenticationService");
        }

        // Test count_definitions_by_type
        let counts = result.count_definitions_by_type();
        let manual_module_count = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == RubyDefinitionType::Module)
            .count();
        assert_eq!(
            counts.get(&RubyDefinitionType::Module).unwrap_or(&0),
            &manual_module_count
        );

        // Test definition_names
        let names = result.definition_names();
        let manual_names: std::collections::HashSet<_> =
            result.definitions.iter().map(|d| d.name.as_str()).collect();
        let names_set: std::collections::HashSet<_> = names.into_iter().collect();
        assert_eq!(names_set, manual_names);

        // Test FQN functionality
        let definitions = result.all_definitions();
        let fqn_strings = result.ruby_definition_fqn_strings();

        assert_eq!(
            definitions.len(),
            fqn_strings.len(),
            "Number of definitions with FQN should match number of FQN strings"
        );

        println!("Filtering and grouping tests passed for sample.rb");

        Ok(())
    }

    #[test]
    fn test_analyzer_with_comprehensive_definitions_fixture() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let fixture_path = "src/ruby/fixtures/comprehensive_definitions.rb";
        let ruby_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read comprehensive_definitions.rb fixture");

        let parse_result = parse(ruby_code.as_bytes());
        let result = analyzer.analyze_with_prism(&ruby_code, &parse_result)?;

        // Verify we found definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find definitions in comprehensive_definitions.rb"
        );

        // Count by type
        let counts = result.count_definitions_by_type();
        println!("comprehensive_definitions.rb analyzer counts: {counts:?}");

        // Verify all supported definition types are present
        assert!(
            counts.get(&RubyDefinitionType::Module).unwrap_or(&0) >= &2,
            "Should find at least 2 modules"
        );
        assert!(
            counts.get(&RubyDefinitionType::Class).unwrap_or(&0) >= &3,
            "Should find at least 3 classes"
        );
        assert!(
            counts.get(&RubyDefinitionType::Method).unwrap_or(&0) >= &8,
            "Should find at least 8 methods"
        );
        assert!(
            counts
                .get(&RubyDefinitionType::SingletonMethod)
                .unwrap_or(&0)
                >= &4,
            "Should find at least 4 singleton methods"
        );
        assert!(
            counts.get(&RubyDefinitionType::Lambda).unwrap_or(&0) >= &4,
            "Should find at least 4 lambdas"
        );
        assert!(
            counts.get(&RubyDefinitionType::Proc).unwrap_or(&0) >= &3,
            "Should find at least 3 procs"
        );

        // Test specific definitions we expect - updated for callable-only design
        let names = result.definition_names();
        let expected_definitions = [
            "DataProcessing",
            "Processor",
            "ConfigurationManager",
            "Utilities",
            "Cache",
            "VALIDATOR",
            "TRANSFORMER",
            "CONFIG_VALIDATOR",
            "initialize",
            "process",
            "create_default",
        ];

        for expected_def in &expected_definitions {
            assert!(
                names.contains(expected_def),
                "Should find definition: {expected_def}"
            );
        }

        // Test FQN functionality
        let definitions = result.all_definitions();
        assert!(!definitions.is_empty(), "Should have definitions with FQNs");

        let fqn_strings = result.ruby_definition_fqn_strings();
        assert!(!fqn_strings.is_empty(), "Should have FQN strings");

        // Verify we have good representation of each definition type
        let lambda_defs = result.definitions_of_type(&RubyDefinitionType::Lambda);
        let proc_defs = result.definitions_of_type(&RubyDefinitionType::Proc);

        assert!(!lambda_defs.is_empty(), "Should find lambda definitions");
        assert!(!proc_defs.is_empty(), "Should find proc definitions");

        // Log some examples for verification
        println!("Found {} total definitions", result.definitions.len());
        println!("Lambda definitions found:");
        for def in lambda_defs {
            let fqn_str = ruby_fqn_to_string(&def.fqn);
            println!("  {} -> {}", def.name, fqn_str);
        }

        println!("Proc definitions found:");
        for def in proc_defs {
            let fqn_str = ruby_fqn_to_string(&def.fqn);
            println!("  {} -> {}", def.name, fqn_str);
        }

        Ok(())
    }

    #[test]
    fn test_analyzer_v2_creation() {
        let _analyzer = RubyAnalyzer::new();
    }

    #[test]
    fn test_parse_and_analyze_simple() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let code = r#"
class User
  def initialize(name)
    @name = name
  end
  
  def self.find_by_name(name)
    # Implementation
  end
end

module Authentication
  def self.authenticate(user)
    # Implementation
  end
end

# This will create expressions/references
user = User.new("test")
found_user = User.find_by_name("test")
Authentication.authenticate(user)
"#;

        let result = analyzer.parse_and_analyze(code)?;

        // Now definition extraction is working, so we should find definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find definitions with prism parser v2"
        );

        println!("Found {} definitions:", result.definitions.len());
        for def in &result.definitions {
            println!("  {:?}: {}", def.definition_type, def.name);
        }

        // Should also have references from the expressions we added
        if !result.references.is_empty() {
            println!("Found {} references:", result.references.len());
            for reference in &result.references {
                println!("  {}: {:?}", reference.name, reference.reference_type);
            }
        } else {
            println!(
                "No references found (this may be expected if expressions aren't extracted yet)"
            );
        }

        Ok(())
    }

    #[test]
    fn test_analyzer_v2_with_expressions() -> crate::Result<()> {
        let analyzer = RubyAnalyzer::new();
        let code = r#"
class Profile
  def settings
    # ...
  end
end

class User
  def profile
    some_var = Profile.new
    some_var.settings()
  end
end
"#;

        let result = analyzer.parse_and_analyze(code)?;

        // Should find both definitions and references
        assert!(!result.definitions.is_empty(), "Should find definitions");
        assert!(!result.references.is_empty(), "Should find references");

        // Should find assignment and call references
        let has_assignment = result.references.iter().any(|r| {
            r.reference_type == crate::ruby::references::types::RubyReferenceType::Assignment
        });
        let has_call = result
            .references
            .iter()
            .any(|r| r.reference_type == crate::ruby::references::types::RubyReferenceType::Call);

        assert!(has_assignment, "Should find assignment references");
        assert!(has_call, "Should find call references");

        println!(
            "Found {} definitions and {} references",
            result.definitions.len(),
            result.references.len()
        );

        Ok(())
    }
}
