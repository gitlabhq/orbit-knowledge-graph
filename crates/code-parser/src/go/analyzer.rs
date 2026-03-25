use crate::ParseResult;
use crate::analyzer::Analyzer;
use crate::go::ast::parse_ast;
use crate::go::types::{
    GoDefinitionInfo, GoDefinitionType, GoFqn, GoImportType, GoImportedSymbolInfo, GoReferenceInfo,
};

#[cfg(test)]
use std::collections::HashMap;

/// Type alias for Go analyzer
pub type GoAnalyzer = Analyzer<GoFqn, GoDefinitionType, GoImportType>;

/// Result of analyzing Go source code
pub struct GoAnalyzerResult {
    /// All definitions found in the code
    pub definitions: Vec<GoDefinitionInfo>,
    /// All imported symbols found in the code
    pub imports: Vec<GoImportedSymbolInfo>,
    /// All references found in the code
    pub references: Vec<GoReferenceInfo>,
}

impl GoAnalyzer {
    /// Analyze a Go source file and extract definitions, imports, and references.
    pub fn analyze(&self, parser_result: &ParseResult) -> crate::Result<GoAnalyzerResult> {
        Ok(parse_ast(&parser_result.ast))
    }
}

impl GoAnalyzerResult {
    /// Get FQN strings for all definitions
    pub fn go_definition_fqn_strings(&self) -> Vec<String> {
        use crate::go::ast::go_fqn_to_string;
        self.definitions
            .iter()
            .map(|def| go_fqn_to_string(&def.fqn))
            .collect()
    }

    /// Get total number of definitions
    pub fn total_definitions(&self) -> usize {
        self.definitions.len()
    }

    /// Get total number of imports
    pub fn total_imports(&self) -> usize {
        self.imports.len()
    }

    /// Get total number of references
    pub fn total_references(&self) -> usize {
        self.references.len()
    }
}

#[cfg(test)]
impl GoAnalyzerResult {
    /// Filter definitions by type
    pub fn definitions_of_type(&self, def_type: &GoDefinitionType) -> Vec<&GoDefinitionInfo> {
        self.definitions
            .iter()
            .filter(|def| def.definition_type == *def_type)
            .collect()
    }

    /// Filter imports by type
    pub fn imports_of_type(&self, import_type: &GoImportType) -> Vec<&GoImportedSymbolInfo> {
        self.imports
            .iter()
            .filter(|import| import.import_type == *import_type)
            .collect()
    }

    /// Count definitions by type
    pub fn count_definitions_by_type(&self) -> HashMap<GoDefinitionType, usize> {
        let mut counts = HashMap::new();
        for def in &self.definitions {
            *counts.entry(def.definition_type).or_insert(0) += 1;
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::types::GoReferenceType;
    use crate::{LanguageParser, SupportedLanguage, parser::GenericParser};

    fn make_result(code: &str, file: &str) -> GoAnalyzerResult {
        let analyzer = GoAnalyzer::new();
        let parser = GenericParser::default_for_language(SupportedLanguage::Go);
        let parse_result = parser.parse(code, Some(file)).unwrap();
        analyzer.analyze(&parse_result).unwrap()
    }

    #[test]
    fn test_go_basic_function() {
        let result = make_result(
            r#"
package main

import "fmt"

func Hello() string {
    return "hello"
}
"#,
            "test.go",
        );

        assert!(
            !result
                .definitions_of_type(&GoDefinitionType::Function)
                .is_empty()
        );
        assert!(!result.imports.is_empty());
    }

    #[test]
    fn test_go_struct_and_method() {
        let result = make_result(
            r#"
package models

type Person struct {
    Name string
    Age  int
}

func (p *Person) GetName() string {
    return p.Name
}
"#,
            "person.go",
        );

        let structs = result.definitions_of_type(&GoDefinitionType::Struct);
        assert!(!structs.is_empty());
        assert_eq!(structs[0].name, "Person");

        let methods = result.definitions_of_type(&GoDefinitionType::Method);
        assert!(!methods.is_empty());
        assert_eq!(methods[0].name, "GetName");
    }

    #[test]
    fn test_go_interface() {
        let result = make_result(
            r#"
package services

type UserService interface {
    GetUser(id int) (*User, error)
}
"#,
            "service.go",
        );

        let interfaces = result.definitions_of_type(&GoDefinitionType::Interface);
        assert!(!interfaces.is_empty());
        assert_eq!(interfaces[0].name, "UserService");
    }

    #[test]
    fn test_go_fqn_strings() {
        let result = make_result(
            r#"
package services

type UserService struct {}

func (s *UserService) GetUser() string {
    return "user"
}

func NewUserService() *UserService {
    return &UserService{}
}
"#,
            "service.go",
        );

        let fqns = result.go_definition_fqn_strings();
        assert!(fqns.contains(&"services.UserService".to_string()));
        assert!(fqns.contains(&"services.UserService.GetUser".to_string()));
        assert!(fqns.contains(&"services.NewUserService".to_string()));
    }

    #[test]
    fn test_go_import_types() {
        let result = make_result(
            r#"
package main

import (
    "fmt"
    "net/http"
    "github.com/user/repo"
)

func main() {}
"#,
            "main.go",
        );

        let std_imports = result.imports_of_type(&GoImportType::Standard);
        let ext_imports = result.imports_of_type(&GoImportType::External);
        assert!(!std_imports.is_empty());
        assert!(!ext_imports.is_empty());
    }

    #[test]
    fn test_go_local_var_does_not_leak() {
        let result = make_result(
            r#"
package mypkg

var GlobalVar = "global"

func DoSomething() string {
    var localVar = "local"
    _ = localVar
    return localVar
}
"#,
            "leak.go",
        );

        let var_defs = result.definitions_of_type(&GoDefinitionType::Variable);
        let names: Vec<&str> = var_defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"GlobalVar"));
        assert!(!names.contains(&"localVar"));
    }

    #[test]
    fn test_go_function_call_classification() {
        let result = make_result(
            r#"
package main

import "fmt"

func main() {
    fmt.Println("hello")
}
"#,
            "refs.go",
        );

        let function_calls: Vec<&str> = result
            .references
            .iter()
            .filter(|r| r.reference_type == GoReferenceType::FunctionCall)
            .map(|r| r.name.as_str())
            .collect();

        assert!(function_calls.contains(&"Println"));
    }
}
