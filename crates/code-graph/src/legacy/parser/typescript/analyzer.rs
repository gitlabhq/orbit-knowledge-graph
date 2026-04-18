use crate::legacy::parser::analyzer::{AnalysisResult, Analyzer};
use crate::legacy::parser::parser::ParseResult;
use crate::legacy::parser::typescript::swc::definitions::extract_swc_definitions;
use crate::legacy::parser::typescript::swc::expressions::extract_swc_expressions;
use crate::legacy::parser::typescript::swc::imports::extract_swc_imports;
use crate::legacy::parser::typescript::swc::references::resolve::resolve_references;
use crate::legacy::parser::typescript::swc::references::types::TypeScriptReferenceInfo;
use crate::legacy::parser::typescript::types::{
    TypeScriptDefinitionType, TypeScriptFqn, TypeScriptImportType,
};

/// Type aliases for Typescript-specific analyzer and analysis result
pub type TypeScriptAnalyzer =
    Analyzer<TypeScriptFqn, TypeScriptDefinitionType, TypeScriptImportType>;
pub type TypeScriptAnalysisResult = AnalysisResult<
    TypeScriptFqn,
    TypeScriptDefinitionType,
    TypeScriptImportType,
    TypeScriptReferenceInfo,
>;

impl TypeScriptAnalyzer {
    /// Analyze Typescript code and extract definitions with FQN computation
    pub fn analyze_swc(
        &self,
        parser_result: &ParseResult<crate::legacy::parser::typescript::types::TypeScriptSwcAst>,
    ) -> crate::legacy::parser::Result<TypeScriptAnalysisResult> {
        let definitions = extract_swc_definitions(&parser_result.ast);
        let imports = extract_swc_imports(&parser_result.ast);
        let expressions = extract_swc_expressions(&parser_result.ast);
        let references = resolve_references(&definitions, &imports, &expressions);
        Ok(TypeScriptAnalysisResult {
            definitions,
            imports,
            references,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::parser::parser::create_typescript_parser;
    use crate::legacy::parser::typescript::ast::typescript_fqn_to_string;
    use std::fs;

    fn get_analysis_result(
        analyzer: &TypeScriptAnalyzer,
        test_path: &str,
    ) -> crate::legacy::parser::Result<TypeScriptAnalysisResult> {
        let parser = create_typescript_parser();
        let code = fs::read_to_string(test_path).expect("Error in reading JS file");
        let parse_result = parser.parse(&code, Some(test_path))?;
        analyzer.analyze_swc(&parse_result)
    }

    #[test]
    fn test_analyzer_e2e() -> crate::legacy::parser::Result<()> {
        let analyzer = TypeScriptAnalyzer::new();
        let test_path = "src/legacy/parser/typescript/fixtures/javascript/sample.js";
        let result = get_analysis_result(&analyzer, test_path)?;

        // Check that we found definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");
        println!("Total # of Definitions: {:?}", result.definitions.len());
        let mut definitions = result.definitions.clone();
        definitions.sort_by_key(|def| def.range.start.line);
        for def in definitions {
            println!("Definition: {:?}", typescript_fqn_to_string(&def.fqn));
        }

        // Check that we found imports
        assert!(!result.imports.is_empty(), "Should find imports");
        println!("Total # of Imports: {:?}", result.imports.len());
        let mut imports = result.imports.clone();
        imports.sort_by_key(|import| import.range.start.line);
        for import in imports {
            println!("Import: {:?}", import.identifier);
        }

        // Check that we found references
        assert!(!result.references.is_empty(), "Should find references");
        println!("Total # of References: {:?}", result.references.len());
        let mut references = result.references.clone();
        references.sort_by_key(|reference| reference.range.start.line);
        for reference in references {
            println!("Reference: {:?}", reference.name);
        }

        Ok(())
    }
}
