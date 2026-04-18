use crate::legacy::parser::fqn::Fqn;
use crate::legacy::parser::ruby::types::{RubyFqn, RubyFqnPartType};

/// Create an FQN from a Ruby-style string like "Module::Class::method"
pub fn fqn_from_ruby_string(fqn_str: &str) -> Fqn<String> {
    let parts = fqn_str.split("::").map(|s| s.to_string()).collect();
    Fqn::new(parts)
}

/// Ruby-specific helper functions for FQN operations
/// Returns the FQN as a string, joined by '::' (Ruby-specific)
pub fn ruby_fqn_to_string(fqn: &RubyFqn) -> String {
    if fqn.parts.is_empty() {
        return String::new();
    }

    if fqn.parts.len() == 1 {
        let part = &fqn.parts[0];
        // Single part methods should just be the method name
        part.node_name().to_string()
    } else {
        let mut total_capacity = 0;
        let mut singleton_methods = 0;

        for part in fqn.parts.iter() {
            total_capacity += part.node_name().len();
            if part.node_type == RubyFqnPartType::SingletonMethod {
                singleton_methods += 1;
            }
        }

        total_capacity += (fqn.parts.len() - 1) * 2; // "::" separators
        total_capacity += singleton_methods; // "#" prefixes

        let mut result = String::with_capacity(total_capacity);

        for (i, part) in fqn.parts.iter().enumerate() {
            if i > 0 {
                // For the last part, use method separator based on method type
                if i == fqn.parts.len() - 1 {
                    match part.node_type {
                        RubyFqnPartType::Method => result.push('#'), // Instance methods: Class#method
                        RubyFqnPartType::SingletonMethod => result.push_str("::"), // Singleton methods: Class::method
                        _ => result.push_str("::"), // All other parts: Class::Module
                    }
                } else {
                    result.push_str("::");
                }
            }

            // SingletonMethod nodes get a '#' prefix when they're singleton method receivers
            // e.g., "Class::receiver_obj::#method" -> "Class::receiver_obj.method"
            if part.node_type == RubyFqnPartType::SingletonMethod && i < fqn.parts.len() - 1 {
                result.push('#');
            }
            result.push_str(part.node_name());
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::parser::parser::{ParserType, SupportedLanguage, UnifiedParseResult};
    use crate::legacy::parser::ruby::types::RubyFqnPart;
    use crate::legacy::parser::ruby::types::RubyFqnPartType;
    use crate::legacy::parser::ruby::visit::extract_definitions_and_references_from_prism;
    use crate::utils::Position;
    use crate::utils::Range;

    #[test]
    fn test_ruby_fqn_string_operations() {
        let fqn = fqn_from_ruby_string("AuthenticationService::CredentialsChecker::initialize");

        assert_eq!(fqn.len(), 3);
        assert_eq!(fqn.parts[0], "AuthenticationService");
        assert_eq!(fqn.parts[1], "CredentialsChecker");
        assert_eq!(fqn.parts[2], "initialize");
    }

    #[test]
    fn test_ruby_fqn_metadata_types() {
        // Test that the metadata types are properly defined and work
        let metadata = RubyFqnPart::new(
            RubyFqnPartType::Class,
            "MyClass".to_string(),
            Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
        );
        assert_eq!(metadata.node_type, RubyFqnPartType::Class);
        assert_eq!(
            metadata.range,
            Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20))
        );

        let method_metadata = RubyFqnPart::new(
            RubyFqnPartType::Method,
            "MyMethod".to_string(),
            Range::new(Position::new(30, 40), Position::new(30, 40), (30, 40)),
        );
        assert_eq!(method_metadata.node_type, RubyFqnPartType::Method);

        // Test RubyFqnPart creation
        let part = RubyFqnPart::new(
            RubyFqnPartType::Class,
            "MyClass".to_string(),
            Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
        );
        assert_eq!(part.node_type, RubyFqnPartType::Class);
        assert_eq!(part.node_name, "MyClass");
    }

    #[test]
    fn test_ruby_fqn_with_metadata() {
        // Test creating a Ruby FQN
        let parts = vec![
            RubyFqnPart::new(
                RubyFqnPartType::Class,
                "MyClass".to_string(),
                Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
            ),
            RubyFqnPart::new(
                RubyFqnPartType::Method,
                "my_method".to_string(),
                Range::new(Position::new(30, 40), Position::new(30, 40), (30, 40)),
            ),
        ];

        let ruby_fqn = RubyFqn::new(parts.into_iter().collect());
        assert_eq!(ruby_fqn.len(), 2);
        assert_eq!(ruby_fqn_to_string(&ruby_fqn), "MyClass#my_method");

        // Test accessing node types
        assert_eq!(ruby_fqn.parts[0].node_type, RubyFqnPartType::Class);
        assert_eq!(ruby_fqn.parts[1].node_type, RubyFqnPartType::Method);
    }

    #[test]
    fn test_fqn_captures_whole_definition_range() {
        let ruby_code = r#"
module TestModule
  class TestClass
    def test_method(param)
      puts "hello"
    end
    
    def self.class_method
      puts "class method"
    end
    
    def receiver_obj.receiver_method
      puts "receiver method"
    end
  end
  
  # Constant assignments
  MY_CONSTANT = "regular constant"
  MY_PROC = proc do |x|
    x * 2
  end
  MY_LAMBDA = lambda do |x|
    x * 3
  end
  
  # Variable assignments (lambda/proc)
  lambda_var = lambda do |x|
    x * 2
  end
  
  proc_var = proc do |x|
    x * 4
  end
  
  # Instance variable assignments
  @lambda_instance_var = lambda do |x|
    x * 5
  end
  
  @proc_instance_var = proc do |x|
    x * 6
  end
  
  # Class variable assignments
  @@lambda_class_var = lambda do |x|
    x * 7
  end
  
  @@proc_class_var = proc do |x|
    x * 8
  end
  
  # Standalone Proc.new call
  Proc.new do |x|
    x * 9
  end
  
  # Block with parameters
  [1, 2, 3].each do |item|
    puts item
  end
end
"#;

        let parser = ParserType::for_language(SupportedLanguage::Ruby);
        let parse_result = parser.parse(ruby_code, Some("test.rb")).unwrap();

        let definitions = if let UnifiedParseResult::Ruby(prism_parse_result) = parse_result {
            let (definitions, _imports, _references) =
                extract_definitions_and_references_from_prism(ruby_code, &prism_parse_result.ast);
            definitions
        } else {
            panic!("Expected Ruby parse result");
        };

        // Convert definitions to found_entries format for compatibility with existing test logic
        let found_entries: Vec<(String, Range, &Range)> = definitions
            .iter()
            .map(|def| {
                let fqn_string = ruby_fqn_to_string(&def.fqn);
                (fqn_string, def.range, &def.range)
            })
            .collect();

        // Test that the ranges are correct (span multiple lines)
        let module_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule")
            .expect("Should find TestModule");

        assert!(
            module_entry.1.line_span() > 1,
            "Module range should span multiple lines, got {} lines. Range: {:?}",
            module_entry.1.line_span(),
            module_entry.1
        );

        let class_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::TestClass")
            .expect("Should find TestClass");

        assert!(
            class_entry.1.line_span() > 1,
            "Class range should span multiple lines, got {} lines. Range: {:?}",
            class_entry.1.line_span(),
            class_entry.1
        );

        let method_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::TestClass#test_method")
            .expect("Should find test_method");

        assert!(
            method_entry.1.line_span() > 1,
            "Method range should span multiple lines, got {} lines. Range: {:?}",
            method_entry.1.line_span(),
            method_entry.1
        );

        let singleton_method_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::TestClass::class_method")
            .expect("Should find class_method");

        assert!(
            singleton_method_entry.1.line_span() > 1,
            "Singleton method range should span multiple lines, got {} lines. Range: {:?}",
            singleton_method_entry.1.line_span(),
            singleton_method_entry.1
        );

        let receiver_method_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::TestClass::receiver_obj::receiver_method")
            .expect("Should find receiver_method");

        assert!(
            receiver_method_entry.1.line_span() > 1,
            "Receiver method range should span multiple lines, got {} lines. Range: {:?}",
            receiver_method_entry.1.line_span(),
            receiver_method_entry.1
        );

        let proc_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::MY_PROC")
            .expect("Should find MY_PROC");

        assert!(
            proc_entry.1.line_span() > 1,
            "Proc constant range should span multiple lines, got {} lines. Range: {:?}",
            proc_entry.1.line_span(),
            proc_entry.1
        );

        let lambda_constant_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::MY_LAMBDA")
            .expect("Should find MY_LAMBDA");

        assert!(
            lambda_constant_entry.1.line_span() > 1,
            "Lambda constant range should span multiple lines, got {} lines. Range: {:?}",
            lambda_constant_entry.1.line_span(),
            lambda_constant_entry.1
        );

        let lambda_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::lambda_var")
            .expect("Should find lambda_var");

        assert!(
            lambda_entry.1.line_span() > 1,
            "Lambda variable range should span multiple lines, got {} lines. Range: {:?}",
            lambda_entry.1.line_span(),
            lambda_entry.1
        );

        let proc_var_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::proc_var")
            .expect("Should find proc_var");

        assert!(
            proc_var_entry.1.line_span() > 1,
            "Proc variable range should span multiple lines, got {} lines. Range: {:?}",
            proc_var_entry.1.line_span(),
            proc_var_entry.1
        );

        let lambda_instance_var_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::@lambda_instance_var")
            .expect("Should find @lambda_instance_var");

        assert!(
            lambda_instance_var_entry.1.line_span() > 1,
            "Lambda instance variable range should span multiple lines, got {} lines. Range: {:?}",
            lambda_instance_var_entry.1.line_span(),
            lambda_instance_var_entry.1
        );

        let proc_instance_var_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::@proc_instance_var")
            .expect("Should find @proc_instance_var");

        assert!(
            proc_instance_var_entry.1.line_span() > 1,
            "Proc instance variable range should span multiple lines, got {} lines. Range: {:?}",
            proc_instance_var_entry.1.line_span(),
            proc_instance_var_entry.1
        );

        let lambda_class_var_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::@@lambda_class_var")
            .expect("Should find @@lambda_class_var");

        assert!(
            lambda_class_var_entry.1.line_span() > 1,
            "Lambda class variable range should span multiple lines, got {} lines. Range: {:?}",
            lambda_class_var_entry.1.line_span(),
            lambda_class_var_entry.1
        );

        let proc_class_var_entry = found_entries
            .iter()
            .find(|(fqn, _, _)| fqn == "TestModule::@@proc_class_var")
            .expect("Should find @@proc_class_var");

        assert!(
            proc_class_var_entry.1.line_span() > 1,
            "Proc class variable range should span multiple lines, got {} lines. Range: {:?}",
            proc_class_var_entry.1.line_span(),
            proc_class_var_entry.1
        );
    }
}
