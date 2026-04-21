use crate::legacy::parser::definitions::DefinitionInfo;
use crate::legacy::parser::ruby::types::{RubyDefinitionType, RubyFqn, RubyFqnPartType};
use crate::utils::Range;
use rustc_hash::FxHashMap;

/// Represents a Ruby definition found in the code
/// This is now a type alias using the generic DefinitionInfo with Ruby-specific types
pub type RubyDefinitionInfo = DefinitionInfo<RubyDefinitionType, RubyFqn>;
/// Map that stores definitions by their node ranges
/// This is populated during FQN traversal and used by the analyzer
pub type RubyDefinitionsMap = FxHashMap<Range, RubyDefinitionInfo>;

/// Create definition info from FQN data
/// This is called during FQN traversal when we encounter nodes that represent definitions
pub fn create_definition_from_fqn(
    fqn_part_type: RubyFqnPartType,
    name: &str,
    fqn: RubyFqn,
    range: Range,
) -> Option<RubyDefinitionInfo> {
    if let Ok(definition_type) = RubyDefinitionType::try_from(fqn_part_type) {
        Some(RubyDefinitionInfo::new(
            definition_type,
            name.to_string(),
            fqn,
            range,
        ))
    } else {
        None
    }
}

/// Extract definitions from the definitions map
/// This is called by the analyzer to get all definitions found during FQN traversal
pub fn extract_definitions_from_map(
    definitions_map: &RubyDefinitionsMap,
) -> Vec<RubyDefinitionInfo> {
    definitions_map.values().cloned().collect()
}

#[cfg(test)]
mod definition_tests {

    use super::*;
    use crate::legacy::parser::parser::{ParserType, SupportedLanguage, UnifiedParseResult};
    use crate::legacy::parser::ruby::fqn::ruby_fqn_to_string;
    use crate::legacy::parser::ruby::visit::extract_definitions_and_references_from_prism;

    /// Helper function to test a definition type with a code snippet
    fn test_definition_extraction(
        code: &str,
        expected_definitions: Vec<(&str, RubyDefinitionType, &str)>, // (name, type, expected_fqn)
        description: &str,
    ) {
        let parser = ParserType::for_language(SupportedLanguage::Ruby);
        let parse_result = parser.parse(code, None).unwrap();
        let definitions = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let (definitions, _imports, _references) =
                extract_definitions_and_references_from_prism(code, &prism_parse_result.ast);
            definitions
        } else {
            panic!("Expected Ruby parse result");
        };

        assert_eq!(
            definitions.len(),
            expected_definitions.len(),
            "Expected {} definitions, found {}",
            expected_definitions.len(),
            definitions.len()
        );

        for (expected_name, expected_type, expected_fqn) in expected_definitions {
            let matching_def = definitions
                .iter()
                .find(|d| d.name == expected_name && d.definition_type == expected_type)
                .unwrap_or_else(|| {
                    panic!("Could not find definition: {expected_name} of type {expected_type:?}")
                });

            let actual_fqn = &matching_def.fqn;
            let actual_fqn_str = ruby_fqn_to_string(actual_fqn);
            assert_eq!(
                actual_fqn_str, expected_fqn,
                "FQN mismatch for {expected_name}: expected '{expected_fqn}', got '{actual_fqn_str}'"
            );
        }
        println!("✅ All assertions passed for: {description}\n");
    }

    #[test]
    fn test_class_definitions_comprehensive() {
        // Basic class
        test_definition_extraction(
            r#"
class User
end
"#,
            vec![("User", RubyDefinitionType::Class, "User")],
            "Basic class definition",
        );

        // Class with inheritance
        test_definition_extraction(
            r#"
class AdminUser < User
end
"#,
            vec![("AdminUser", RubyDefinitionType::Class, "AdminUser")],
            "Class with inheritance",
        );

        // Nested classes
        test_definition_extraction(
            r#"
module Authentication
  class User
    class Profile
    end
  end
end
"#,
            vec![
                (
                    "Authentication",
                    RubyDefinitionType::Module,
                    "Authentication",
                ),
                ("User", RubyDefinitionType::Class, "Authentication::User"),
                (
                    "Profile",
                    RubyDefinitionType::Class,
                    "Authentication::User::Profile",
                ),
            ],
            "Nested classes with modules",
        );

        // Class with complex inheritance and mixins
        test_definition_extraction(
            r#"
module Trackable
end

class ApplicationRecord
end

class User < ApplicationRecord
  include Trackable
end
"#,
            vec![
                ("Trackable", RubyDefinitionType::Module, "Trackable"),
                (
                    "ApplicationRecord",
                    RubyDefinitionType::Class,
                    "ApplicationRecord",
                ),
                ("User", RubyDefinitionType::Class, "User"),
            ],
            "Class with inheritance and module inclusion",
        );
    }

    #[test]
    fn test_module_definitions_comprehensive() {
        // Basic module
        test_definition_extraction(
            r#"
module Utilities
end
"#,
            vec![("Utilities", RubyDefinitionType::Module, "Utilities")],
            "Basic module definition",
        );

        // Nested modules
        test_definition_extraction(
            r#"
module Authentication
  module Providers
    module OAuth
    end
  end
end
"#,
            vec![
                (
                    "Authentication",
                    RubyDefinitionType::Module,
                    "Authentication",
                ),
                (
                    "Providers",
                    RubyDefinitionType::Module,
                    "Authentication::Providers",
                ),
                (
                    "OAuth",
                    RubyDefinitionType::Module,
                    "Authentication::Providers::OAuth",
                ),
            ],
            "Deeply nested modules",
        );

        // Module with classes and methods
        test_definition_extraction(
            r#"
module ServiceLayer
  class BaseService
    def call
    end
  end
  
  def self.configure
  end
end
"#,
            vec![
                ("ServiceLayer", RubyDefinitionType::Module, "ServiceLayer"),
                (
                    "BaseService",
                    RubyDefinitionType::Class,
                    "ServiceLayer::BaseService",
                ),
                (
                    "call",
                    RubyDefinitionType::Method,
                    "ServiceLayer::BaseService#call",
                ),
                (
                    "configure",
                    RubyDefinitionType::SingletonMethod,
                    "ServiceLayer::configure",
                ),
            ],
            "Module with mixed definitions",
        );
    }

    #[test]
    fn test_method_definitions_comprehensive() {
        // Basic instance methods
        test_definition_extraction(
            r#"
class Calculator
  def add(a, b)
    a + b
  end
  
  def subtract(a, b)
    a - b
  end
end
"#,
            vec![
                ("Calculator", RubyDefinitionType::Class, "Calculator"),
                ("add", RubyDefinitionType::Method, "Calculator#add"),
                (
                    "subtract",
                    RubyDefinitionType::Method,
                    "Calculator#subtract",
                ),
            ],
            "Basic instance methods",
        );

        // Methods with complex parameters
        test_definition_extraction(
            r#"
class User
  def initialize(name, email, age: 18, **options)
    @name = name
  end
  
  def update_profile(name: nil, email: nil, &block)
    # Update logic
  end
end
"#,
            vec![
                ("User", RubyDefinitionType::Class, "User"),
                ("initialize", RubyDefinitionType::Method, "User#initialize"),
                (
                    "update_profile",
                    RubyDefinitionType::Method,
                    "User#update_profile",
                ),
            ],
            "Methods with complex parameter signatures",
        );

        // Private and protected methods
        test_definition_extraction(
            r#"
class SecureService
  def public_method
  end
  
  private
  
  def private_method
  end
  
  protected
  
  def protected_method
  end
end
"#,
            vec![
                ("SecureService", RubyDefinitionType::Class, "SecureService"),
                (
                    "public_method",
                    RubyDefinitionType::Method,
                    "SecureService#public_method",
                ),
                (
                    "private_method",
                    RubyDefinitionType::Method,
                    "SecureService#private_method",
                ),
                (
                    "protected_method",
                    RubyDefinitionType::Method,
                    "SecureService#protected_method",
                ),
            ],
            "Methods with visibility modifiers",
        );
    }

    #[test]
    fn test_singleton_method_definitions_comprehensive() {
        // Class methods with self
        test_definition_extraction(
            r#"
class User
  def self.find_by_email(email)
    # Implementation
  end
  
  def self.create_with_defaults(name)
    # Implementation
  end
end
"#,
            vec![
                ("User", RubyDefinitionType::Class, "User"),
                (
                    "find_by_email",
                    RubyDefinitionType::SingletonMethod,
                    "User::find_by_email",
                ),
                (
                    "create_with_defaults",
                    RubyDefinitionType::SingletonMethod,
                    "User::create_with_defaults",
                ),
            ],
            "Class methods with self keyword",
        );

        // Singleton methods on objects
        test_definition_extraction(
            r#"
user = User.new

def user.special_method
  # Implementation
end

module Service
  def self.call
    # Implementation
  end
end
"#,
            vec![
                (
                    "special_method",
                    RubyDefinitionType::SingletonMethod,
                    "user::special_method",
                ),
                ("Service", RubyDefinitionType::Module, "Service"),
                ("call", RubyDefinitionType::SingletonMethod, "Service::call"),
            ],
            "Singleton methods on objects and modules",
        );

        // Class << self syntax
        test_definition_extraction(
            r#"
class DatabaseConnection
  class << self
    def establish_connection
      # Implementation
    end
    
    def close_connection
      # Implementation
    end
  end
end
"#,
            vec![
                (
                    "DatabaseConnection",
                    RubyDefinitionType::Class,
                    "DatabaseConnection",
                ),
                (
                    "establish_connection",
                    RubyDefinitionType::Method,
                    "DatabaseConnection#establish_connection",
                ),
                (
                    "close_connection",
                    RubyDefinitionType::Method,
                    "DatabaseConnection#close_connection",
                ),
            ],
            "Class methods using class << self syntax",
        );
    }

    #[test]
    fn test_lambda_definitions_comprehensive() {
        // Lambda assigned to constant - this creates a named, reusable entity
        test_definition_extraction(
            r#"
class MathOperations
  VALIDATOR = lambda do |input|
    input.is_a?(Numeric) && input > 0
  end
  
  TRANSFORMER = lambda { |x| x * 2 }
end
"#,
            vec![
                (
                    "MathOperations",
                    RubyDefinitionType::Class,
                    "MathOperations",
                ),
                (
                    "VALIDATOR",
                    RubyDefinitionType::Lambda,
                    "MathOperations::VALIDATOR",
                ),
                (
                    "TRANSFORMER",
                    RubyDefinitionType::Lambda,
                    "MathOperations::TRANSFORMER",
                ),
            ],
            "Lambda assigned to constants",
        );

        // Lambda assigned to variables
        test_definition_extraction(
            r#"
module Processors
  processor = lambda { |data| data.process }
  @instance_processor = lambda { |item| item.transform }
  @@class_processor = lambda { |batch| batch.validate }
end
"#,
            vec![
                ("Processors", RubyDefinitionType::Module, "Processors"),
                (
                    "processor",
                    RubyDefinitionType::Lambda,
                    "Processors::processor",
                ),
                (
                    "@instance_processor",
                    RubyDefinitionType::Lambda,
                    "Processors::@instance_processor",
                ),
                (
                    "@@class_processor",
                    RubyDefinitionType::Lambda,
                    "Processors::@@class_processor",
                ),
            ],
            "Lambda assigned to different variable types",
        );
    }

    #[test]
    fn test_proc_definitions_comprehensive() {
        // Basic Proc.new assigned to variable
        test_definition_extraction(
            r#"
increment = Proc.new { |x| x + 1 }
"#,
            vec![("increment", RubyDefinitionType::Proc, "increment")],
            "Basic Proc.new assignment to variable",
        );

        test_definition_extraction(
            r#"
class Calculator
  OPERATIONS = {
    add: Proc.new { |a, b| a + b },
    multiply: Proc.new { |a, b| a * b }
  }
end
"#,
            vec![("Calculator", RubyDefinitionType::Class, "Calculator")],
            "Proc expressions in hash definitions - non-callable constant not captured",
        );

        // Multiple Proc assignments - only capture callable assignments, not constants assigned to Proc.new
        test_definition_extraction(
            r#"
module EventHandlers
  ON_SUCCESS = Proc.new do |result|
    puts "Success: #{result}"
  end
  
  ON_ERROR = Proc.new do |error|
    puts "Error: #{error.message}"
  end
end
"#,
            vec![
                ("EventHandlers", RubyDefinitionType::Module, "EventHandlers"),
                (
                    "ON_SUCCESS",
                    RubyDefinitionType::Proc,
                    "EventHandlers::ON_SUCCESS",
                ),
                (
                    "ON_ERROR",
                    RubyDefinitionType::Proc,
                    "EventHandlers::ON_ERROR",
                ),
            ],
            "Multiple Proc assignments - callable definitions captured",
        );
    }

    #[test]
    fn test_complex_mixed_definitions() {
        test_definition_extraction(
            r#"
module Api
  module V1
    class UsersController < ApplicationController
      attr_reader :current_user
      
      CACHE_DURATION = 1.hour
      
      def index
        users = User.all
        render json: users
      end
      
      def show
        user = find_user
        render json: user
      end
      
      def self.authenticate
        # Authentication logic
      end
      
      private
      
      def find_user
        User.find(params[:id])
      end
      
      SERIALIZER = lambda do |user|
        { id: user.id, name: user.name }
      end
      
      FORMATTER = Proc.new do |data|
        data.to_json
      end
    end
  end
end
"#,
            vec![
                ("Api", RubyDefinitionType::Module, "Api"),
                ("V1", RubyDefinitionType::Module, "Api::V1"),
                (
                    "UsersController",
                    RubyDefinitionType::Class,
                    "Api::V1::UsersController",
                ),
                (
                    "index",
                    RubyDefinitionType::Method,
                    "Api::V1::UsersController#index",
                ),
                (
                    "show",
                    RubyDefinitionType::Method,
                    "Api::V1::UsersController#show",
                ),
                (
                    "authenticate",
                    RubyDefinitionType::SingletonMethod,
                    "Api::V1::UsersController::authenticate",
                ),
                (
                    "find_user",
                    RubyDefinitionType::Method,
                    "Api::V1::UsersController#find_user",
                ),
                (
                    "SERIALIZER",
                    RubyDefinitionType::Lambda,
                    "Api::V1::UsersController::SERIALIZER",
                ),
                (
                    "FORMATTER",
                    RubyDefinitionType::Proc,
                    "Api::V1::UsersController::FORMATTER",
                ),
                // Anonymous blocks are no longer captured
            ],
            "Complex Rails controller with only callable definitions captured",
        );
    }

    #[test]
    fn test_edge_cases_and_error_conditions() {
        // Empty definitions
        test_definition_extraction(
            r#"
class EmptyClass
end

module EmptyModule
end
"#,
            vec![
                ("EmptyClass", RubyDefinitionType::Class, "EmptyClass"),
                ("EmptyModule", RubyDefinitionType::Module, "EmptyModule"),
            ],
            "Empty class and module definitions",
        );

        // Single-line definitions
        test_definition_extraction(
            r#"
class User; end
module Auth; end
def quick_method; end
"#,
            vec![
                ("User", RubyDefinitionType::Class, "User"),
                ("Auth", RubyDefinitionType::Module, "Auth"),
                ("quick_method", RubyDefinitionType::Method, "quick_method"),
            ],
            "Single-line definitions",
        );

        // Complex nesting levels
        test_definition_extraction(
            r#"
module A
  module B
    module C
      class D
        class E
          def f
            puts "method body"
          end
        end
      end
    end
  end
end
"#,
            vec![
                ("A", RubyDefinitionType::Module, "A"),
                ("B", RubyDefinitionType::Module, "A::B"),
                ("C", RubyDefinitionType::Module, "A::B::C"),
                ("D", RubyDefinitionType::Class, "A::B::C::D"),
                ("E", RubyDefinitionType::Class, "A::B::C::D::E"),
                ("f", RubyDefinitionType::Method, "A::B::C::D::E#f"),
            ],
            "Deep nesting of definitions",
        );
    }

    #[test]
    fn ensure_callable_only() {
        let code = r#"
# Issue 1: Non-callable constants that should NOT be captured as definitions
MY_CONSTANT = "some_value"
SERVICE = SomeClass.new
CONFIG = { key: "value" }
NUMBERS = [1, 2, 3]

# Issue 2: Callable constants that SHOULD be captured as definitions
MY_LAMBDA = -> { puts "hello" }
PROC_CONSTANT = Proc.new { puts "world" }

# Issue 3: Anonymous blocks that should NOT be captured (no stable FQN possible)
[1, 2, 3].each do |item|
  puts item
end

# Issue 4: Blocks assigned to variables (should be captured as lambda/proc definitions)
my_block = proc { |x| x * 2 }

# Issue 5: Complex cases
module TestModule
  # Non-callable constant (should NOT be captured)
  API_URL = "https://example.com"
  
  # Callable constant (should be captured)
  VALIDATOR = lambda { |x| x.valid? }
  
  def test_method
    # Anonymous block (should NOT be captured)
    items.each do |item|
      process(item)
    end
    
    # Block assigned to variable (should be captured)
    processor = proc { |data| data.transform }
  end
end
"#;

        let parser = ParserType::for_language(SupportedLanguage::Ruby);
        let parse_result = parser.parse(code, None).unwrap();
        let definitions = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let (definitions, _imports, _references) =
                extract_definitions_and_references_from_prism(code, &prism_parse_result.ast);
            definitions
        } else {
            panic!("Expected Ruby parse result");
        };

        let problematic_constants = definitions
            .iter()
            .filter(|d| {
                matches!(
                    d.name.as_ref(),
                    "MY_CONSTANT" | "SERVICE" | "CONFIG" | "NUMBERS" | "API_URL"
                )
            })
            .collect::<Vec<_>>();

        let anonymous_blocks = definitions
            .iter()
            .filter(|d| d.name == "block")
            .collect::<Vec<_>>();

        assert_eq!(
            problematic_constants.len(),
            0,
            "Non-callable constants should not be captured"
        );
        assert_eq!(
            anonymous_blocks.len(),
            0,
            "Anonymous blocks should not be captured"
        );
    }

    #[test]
    fn test_definition_ranges_capture_entire_definitions() {
        let code = r#"
class User
  def initialize(name)
    @name = name
  end

  def self.find_by_email(email)
    # Implementation
  end
end

module Authentication
  TIMEOUT = 30.minutes
  
  def self.authenticate(credentials)
    # Authentication logic
  end
end

LAMBDA_CONSTANT = lambda { |x| x * 2 }
proc_variable = Proc.new { |data| data.process }
"#;

        let parser = ParserType::for_language(SupportedLanguage::Ruby);
        let parse_result = parser.parse(code, None).unwrap();
        let definitions = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let (definitions, _imports, _references) =
                extract_definitions_and_references_from_prism(code, &prism_parse_result.ast);
            definitions
        } else {
            panic!("Expected Ruby parse result");
        };

        for def in &definitions {
            let start_byte = def.range.byte_offset.0;
            let end_byte = def.range.byte_offset.1;
            let definition_text = &code[start_byte..end_byte];

            match def.definition_type {
                RubyDefinitionType::Class => {
                    assert!(
                        definition_text.trim_start().starts_with("class"),
                        "Class definition should start with 'class', but got: '{definition_text}'"
                    );
                    assert!(
                        definition_text.trim_end().ends_with("end"),
                        "Class definition should end with 'end', but got: '{definition_text}'"
                    );
                }
                RubyDefinitionType::Module => {
                    assert!(
                        definition_text.trim_start().starts_with("module"),
                        "Module definition should start with 'module', but got: '{definition_text}'"
                    );
                    assert!(
                        definition_text.trim_end().ends_with("end"),
                        "Module definition should end with 'end', but got: '{definition_text}'"
                    );
                }
                RubyDefinitionType::Method => {
                    assert!(
                        definition_text.trim_start().starts_with("def"),
                        "Method definition should start with 'def', but got: '{definition_text}'"
                    );
                    assert!(
                        definition_text.trim_end().ends_with("end"),
                        "Method definition should end with 'end', but got: '{definition_text}'"
                    );
                }
                RubyDefinitionType::SingletonMethod => {
                    assert!(
                        definition_text.trim_start().starts_with("def"),
                        "Singleton method definition should start with 'def', but got: '{definition_text}'"
                    );
                    assert!(
                        definition_text.trim_end().ends_with("end"),
                        "Singleton method definition should end with 'end', but got: '{definition_text}'"
                    );
                }
                RubyDefinitionType::Lambda => {
                    assert!(
                        definition_text.starts_with("LAMBDA_CONSTANT"),
                        "Lambda definition should start with 'LAMBDA_CONSTANT', but got: '{definition_text}'"
                    );
                    assert!(
                        definition_text.ends_with("}"),
                        "Lambda definition should end with '}}', but got: '{definition_text}'"
                    );
                }
                RubyDefinitionType::Proc => {
                    assert!(
                        definition_text.starts_with("proc_variable"),
                        "Proc definition should start with 'proc_variable', but got: '{definition_text}'"
                    );
                    assert!(
                        definition_text.ends_with("}"),
                        "Proc definition should end with '}}', but got: '{definition_text}'"
                    );
                }
            }
        }

        let user_class = definitions
            .iter()
            .find(|d| d.name == "User" && d.definition_type == RubyDefinitionType::Class);
        assert!(user_class.is_some(), "Should find User class");

        let auth_module = definitions.iter().find(|d| {
            d.name == "Authentication" && d.definition_type == RubyDefinitionType::Module
        });
        assert!(auth_module.is_some(), "Should find Authentication module");
    }

    #[test]
    fn test_nested_definition_ranges() {
        let code = r#"
module Outer
  class Inner
    def method_one
      puts "hello"
    end

    def self.class_method
      puts "class method"
    end

    CONSTANT = lambda do |x|
      x.process
    end
  end
end
"#;

        let parser = ParserType::for_language(SupportedLanguage::Ruby);
        let parse_result = parser.parse(code, None).unwrap();
        let definitions = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let (definitions, _imports, _references) =
                extract_definitions_and_references_from_prism(code, &prism_parse_result.ast);
            definitions
        } else {
            panic!("Expected Ruby parse result");
        };

        // Verify that nested definitions don't overlap incorrectly
        for (i, def1) in definitions.iter().enumerate() {
            for (j, def2) in definitions.iter().enumerate() {
                if i == j {
                    continue;
                }

                let range1 = &def1.range.byte_offset;
                let range2 = &def2.range.byte_offset;

                // Check if ranges overlap incorrectly
                let overlaps = !(range1.1 <= range2.0 || range2.1 <= range1.0);

                if overlaps {
                    // If they overlap, one should be completely contained within the other
                    let def1_contains_def2 = range1.0 <= range2.0 && range2.1 <= range1.1;
                    let def2_contains_def1 = range2.0 <= range1.0 && range1.1 <= range2.1;

                    assert!(
                        def1_contains_def2 || def2_contains_def1,
                        "Definitions {} ({:?}) and {} ({:?}) have invalid overlapping ranges: {}-{} vs {}-{}",
                        def1.name,
                        def1.definition_type,
                        def2.name,
                        def2.definition_type,
                        range1.0,
                        range1.1,
                        range2.0,
                        range2.1
                    );
                }
            }
        }
    }

    #[test]
    fn test_single_line_definition_ranges() {
        let code = r#"
class SingleLine; end
module SingleMod; end
def single_method; end
"#;

        let parser = ParserType::for_language(SupportedLanguage::Ruby);
        let parse_result = parser.parse(code, None).unwrap();
        let definitions = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let (definitions, _imports, _references) =
                extract_definitions_and_references_from_prism(code, &prism_parse_result.ast);
            definitions
        } else {
            panic!("Expected Ruby parse result");
        };

        for def in &definitions {
            let start_byte = def.range.byte_offset.0;
            let end_byte = def.range.byte_offset.1;
            let definition_text = &code[start_byte..end_byte];

            println!("Single-line definition {}: '{}'", def.name, definition_text);

            match def.definition_type {
                RubyDefinitionType::Class => {
                    assert!(
                        definition_text.contains("class") && definition_text.contains("end"),
                        "Single-line class should contain both 'class' and 'end': '{definition_text}'"
                    );
                }
                RubyDefinitionType::Module => {
                    assert!(
                        definition_text.contains("module") && definition_text.contains("end"),
                        "Single-line module should contain both 'module' and 'end': '{definition_text}'"
                    );
                }
                RubyDefinitionType::Method => {
                    assert!(
                        definition_text.contains("def") && definition_text.contains("end"),
                        "Single-line method should contain both 'def' and 'end': '{definition_text}'"
                    );
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_fqn_map_uses_definition_node_ranges_not_name_ranges() {
        let ruby_code = r#"module TestModule
  class Calculator
    def initialize(value)
      @value = value
    end
    
    def self.create(value)
      new(value)
    end
    
    LAMBDA_PROC = lambda do |x|
      x * 2
    end
  end
end
"#;

        let parser = ParserType::for_language(SupportedLanguage::Ruby);
        let parse_result = parser.parse(ruby_code, None).unwrap();
        let definitions = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let (definitions, _imports, _references) =
                extract_definitions_and_references_from_prism(ruby_code, &prism_parse_result.ast);
            definitions
        } else {
            panic!("Expected Ruby parse result");
        };

        assert!(!definitions.is_empty(), "Should find definitions");

        let module_def = definitions
            .iter()
            .find(|def| {
                def.name == "TestModule"
                    && matches!(def.definition_type, RubyDefinitionType::Module)
            })
            .expect("Should find TestModule");
        assert_eq!(
            module_def.range.start.line, 0,
            "Module should start at line 0"
        );
        assert_eq!(
            module_def.range.end.line, 14,
            "Module should end at line 14 (entire definition)"
        );

        let class_def = definitions
            .iter()
            .find(|def| {
                def.name == "Calculator" && matches!(def.definition_type, RubyDefinitionType::Class)
            })
            .expect("Should find Calculator");
        assert_eq!(
            class_def.range.start.line, 1,
            "Class should start at line 1"
        );
        assert_eq!(
            class_def.range.end.line, 13,
            "Class should end at line 13 (entire definition)"
        );

        let method_def = definitions
            .iter()
            .find(|def| {
                def.name == "initialize"
                    && matches!(def.definition_type, RubyDefinitionType::Method)
            })
            .expect("Should find initialize");
        assert_eq!(
            method_def.range.start.line, 2,
            "Method should start at line 2"
        );
        assert_eq!(
            method_def.range.end.line, 4,
            "Method should end at line 4 (entire definition)"
        );

        let singleton_def = definitions
            .iter()
            .find(|def| {
                def.name == "create"
                    && matches!(def.definition_type, RubyDefinitionType::SingletonMethod)
            })
            .expect("Should find create singleton method");
        assert_eq!(
            singleton_def.range.start.line, 6,
            "Singleton method should start at line 6"
        );
        assert_eq!(
            singleton_def.range.end.line, 8,
            "Singleton method should end at line 8 (entire definition)"
        );

        let lambda_def = definitions
            .iter()
            .find(|def| {
                def.name == "LAMBDA_PROC"
                    && matches!(def.definition_type, RubyDefinitionType::Lambda)
            })
            .expect("Should find LAMBDA_PROC");
        assert_eq!(
            lambda_def.range.start.line, 10,
            "Lambda should start at line 10"
        );
        assert_eq!(
            lambda_def.range.end.line, 12,
            "Lambda should end at line 12 (entire definition)"
        );
    }
}
