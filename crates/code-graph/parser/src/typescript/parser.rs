use super::types::TypeScriptSwcAst;
use crate::parser::{ParseResult, SupportedLanguage};
use crate::{Error, Result};
use std::path::Path;
use swc_common::{FileName, SourceMap, sync::Lrc};
use swc_ecma_parser::{EsSyntax, Parser, StringInput, Syntax, TsSyntax, lexer::Lexer};

// NOTE: Tsx/Jsx will be supported in the future, but not yet
#[derive(Debug, PartialEq, Eq)]
pub enum EcmaDialect {
    JavaScript,
    TypeScript,
    Tsx,
    Jsx,
    Unknown,
}

impl EcmaDialect {
    pub fn from_path(path_str: Option<&str>) -> Self {
        let extension = path_str.and_then(|path| Path::new(path).extension());
        match extension {
            Some(ext) => match ext.to_str() {
                Some(ext) => match ext {
                    "js" => Self::JavaScript,
                    "ts" => Self::TypeScript,
                    "tsx" => Self::Tsx,
                    "jsx" => Self::Jsx,
                    _ => Self::Unknown,
                },
                _ => Self::Unknown,
            },
            None => Self::Unknown,
        }
    }

    pub fn to_syntax(&self) -> Option<Syntax> {
        match self {
            Self::JavaScript => Some(Syntax::Es(EsSyntax {
                decorators: true,
                decorators_before_export: false,
                ..Default::default()
            })),
            Self::TypeScript => Some(Syntax::Typescript(TsSyntax::default())),
            Self::Tsx => Some(Syntax::Typescript(TsSyntax::default())),
            Self::Jsx => Some(Syntax::Typescript(TsSyntax::default())),
            Self::Unknown => None,
        }
    }
}

pub struct TypeScriptParser;

impl Default for TypeScriptParser {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeScriptParser {
    pub fn new() -> Self {
        Self
    }

    pub fn language(&self) -> SupportedLanguage {
        SupportedLanguage::TypeScript
    }

    pub fn parse<'a>(
        &self,
        code: &'a str,
        file_path: Option<&'a str>,
    ) -> Result<ParseResult<'a, TypeScriptSwcAst>> {
        let dialect = EcmaDialect::from_path(file_path);
        if dialect == EcmaDialect::Unknown {
            return Err(Error::Parse("Unsupported language".to_string()));
        }
        match file_path {
            Some(path) => match parse_ast(dialect, path, code) {
                Ok(module_and_sm) => {
                    Ok(ParseResult::new(self.language(), Some(path), module_and_sm))
                }
                Err(e) => Err(e),
            },
            None => Err(Error::Parse("File path is required".to_string())),
        }
    }
}

pub fn parse_ast(dialect: EcmaDialect, file_path: &str, code: &str) -> Result<TypeScriptSwcAst> {
    let src_map: Lrc<SourceMap> = Lrc::<SourceMap>::default();
    let filename = Lrc::new(FileName::Custom(file_path.into()));
    let source_file = src_map.new_source_file(filename.clone(), code.to_string());
    let syntax = dialect.to_syntax();

    if syntax.is_none() {
        return Err(Error::Parse("Unsupported language".to_string()));
    }

    let lexer = Lexer::new(
        syntax.unwrap(),
        Default::default(),
        StringInput::from(&*source_file),
        None,
    );

    let mut parser = Parser::new_from(lexer);
    match parser.parse_module() {
        Ok(module) => Ok(TypeScriptSwcAst::new(module, src_map.clone())),
        Err(e) => {
            tracing::error!("Error: {:?} for dialect: {:?}", e, dialect);
            Err(Error::Parse("Failed to parse module".to_string()))
        }
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;
    use swc_ecma_ast::{ModuleDecl, ModuleItem, Stmt};

    fn parse_module(dialect: EcmaDialect, code: &str) -> Result<(Vec<ModuleDecl>, Vec<Stmt>)> {
        let file_path = "test.ts";
        let ast = parse_ast(dialect, file_path, code);
        if ast.is_err() {
            println!("Error: {:?}", ast.err());
            return Err(Error::Parse("Failed to parse module".to_string()));
        }
        let ast = ast.unwrap();

        let mut declarations: Vec<ModuleDecl> = Vec::new();
        let mut statements: Vec<Stmt> = Vec::new();

        for module_item in ast.module.body {
            match module_item {
                ModuleItem::ModuleDecl(decl) => {
                    declarations.push(decl);
                }
                ModuleItem::Stmt(stmt) => {
                    statements.push(stmt);
                }
            }
        }

        for decl in &declarations {
            println!("Declaration: {:?}", decl);
        }
        for stmt in &statements {
            println!("Statement: {:?}", stmt);
        }

        Ok((declarations, statements))
    }

    #[test]
    fn test_parse_module() {
        let code = r#"
        import { a } from "b";
        const b = 1;
        function c() {
            return b;
        }
        export { c };
        "#;
        match parse_module(EcmaDialect::TypeScript, code) {
            Ok((declarations, statements)) => {
                assert_eq!(declarations.len(), 2);
                assert_eq!(statements.len(), 2);
            }
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    #[test]
    fn parse_bad_code() {
        let code = r#"
        let let = 3;
        "#;
        match parse_module(EcmaDialect::TypeScript, code) {
            Ok(_) => {
                panic!("Should have failed to parse bad code");
            }
            Err(e) => {
                println!("Error: {:?}", e);
                assert!(matches!(e, Error::Parse(_)));
            }
        }
    }
}
