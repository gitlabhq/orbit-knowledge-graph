use crate::definitions::DefinitionTypeInfo;
use crate::{
    csharp::types::{CSharpDefinitionInfo, CSharpImportedSymbolInfo},
    definitions::DefinitionInfo,
    java::types::{JavaDefinitionInfo, JavaImportedSymbolInfo, JavaReferenceInfo},
    kotlin::types::{KotlinDefinitionInfo, KotlinImportedSymbolInfo, KotlinReferenceInfo},
    parser::Language,
    python::types::{PythonDefinitionInfo, PythonImportedSymbolInfo, PythonReferenceInfo},
    references::ReferenceInfo,
    ruby::{
        definitions::RubyDefinitionInfo,
        imports::RubyImportedSymbolInfo,
        references::types::{RubyExpressionMetadata, RubyReferenceType, RubyTargetResolution},
    },
    rust::{imports::RustImportedSymbolInfo, types::RustDefinitionInfo},
    typescript::{
        swc::references::types::TypeScriptReferenceInfo,
        types::{TypeScriptDefinitionInfo, TypeScriptImportedSymbolInfo},
    },
};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SkippedFile {
    pub file_path: String,
    pub reason: String,
    pub file_size: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ErroredFile {
    pub file_path: String,
    pub language: Option<Language>,
    pub error_message: String,
    pub error_stage: ProcessingStage,
}

#[derive(Debug, Clone)]
pub enum ProcessingStage {
    FileSystem,
    Parsing,
    Unknown,
}

#[derive(Debug)]
pub enum ProcessingResult {
    Success(FileProcessingResult),
    Skipped(SkippedFile),
    Error(ErroredFile),
}

impl ProcessingResult {
    pub fn is_success(&self) -> bool {
        matches!(self, ProcessingResult::Success(_))
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self, ProcessingResult::Skipped(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, ProcessingResult::Error(_))
    }

    pub fn file_path(&self) -> &str {
        match self {
            ProcessingResult::Success(result) => &result.file_path,
            ProcessingResult::Skipped(skipped) => &skipped.file_path,
            ProcessingResult::Error(errored) => &errored.file_path,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Definitions {
    Ruby(Vec<RubyDefinitionInfo>),
    Python(Vec<PythonDefinitionInfo>),
    Kotlin(Vec<KotlinDefinitionInfo>),
    Java(Vec<JavaDefinitionInfo>),
    CSharp(Vec<CSharpDefinitionInfo>),
    TypeScript(Vec<TypeScriptDefinitionInfo>),
    Rust(Vec<RustDefinitionInfo>),
    Unknown(Vec<DefinitionInfo<(), ()>>),
}

impl Definitions {
    pub fn count(&self) -> usize {
        match self {
            Definitions::Ruby(defs) => defs.len(),
            Definitions::Python(defs) => defs.len(),
            Definitions::Kotlin(defs) => defs.len(),
            Definitions::Java(defs) => defs.len(),
            Definitions::CSharp(defs) => defs.len(),
            Definitions::TypeScript(defs) => defs.len(),
            Definitions::Rust(defs) => defs.len(),
            Definitions::Unknown(defs) => defs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn iter_definition_types(&self) -> Box<dyn Iterator<Item = String> + '_> {
        match self {
            Definitions::Ruby(defs) => Box::new(
                defs.iter()
                    .map(|def| def.definition_type.as_str().to_string()),
            ),
            Definitions::Python(defs) => Box::new(
                defs.iter()
                    .map(|def| def.definition_type.as_str().to_string()),
            ),
            Definitions::Kotlin(defs) => Box::new(
                defs.iter()
                    .map(|def| def.definition_type.as_str().to_string()),
            ),
            Definitions::Java(defs) => Box::new(
                defs.iter()
                    .map(|def| def.definition_type.as_str().to_string()),
            ),
            Definitions::CSharp(defs) => Box::new(
                defs.iter()
                    .map(|def| def.definition_type.as_str().to_string()),
            ),
            Definitions::TypeScript(defs) => Box::new(
                defs.iter()
                    .map(|def| def.definition_type.as_str().to_string()),
            ),
            Definitions::Rust(defs) => Box::new(
                defs.iter()
                    .map(|def| def.definition_type.as_str().to_string()),
            ),
            Definitions::Unknown(_) => Box::new(std::iter::empty()),
        }
    }

    pub fn iter_python(&self) -> Option<impl Iterator<Item = &PythonDefinitionInfo>> {
        match self {
            Definitions::Python(defs) => Some(defs.iter()),
            _ => None,
        }
    }

    pub fn iter_ruby(&self) -> Option<impl Iterator<Item = &RubyDefinitionInfo>> {
        match self {
            Definitions::Ruby(defs) => Some(defs.iter()),
            _ => None,
        }
    }

    pub fn iter_kotlin(&self) -> Option<impl Iterator<Item = &KotlinDefinitionInfo>> {
        match self {
            Definitions::Kotlin(defs) => Some(defs.iter()),
            _ => None,
        }
    }

    pub fn iter_java(&self) -> Option<impl Iterator<Item = &JavaDefinitionInfo>> {
        match self {
            Definitions::Java(defs) => Some(defs.iter()),
            _ => None,
        }
    }

    pub fn iter_csharp(&self) -> Option<impl Iterator<Item = &CSharpDefinitionInfo>> {
        match self {
            Definitions::CSharp(defs) => Some(defs.iter()),
            _ => None,
        }
    }

    pub fn iter_typescript(&self) -> Option<impl Iterator<Item = &TypeScriptDefinitionInfo>> {
        match self {
            Definitions::TypeScript(defs) => Some(defs.iter()),
            _ => None,
        }
    }

    pub fn iter_rust(&self) -> Option<impl Iterator<Item = &RustDefinitionInfo>> {
        match self {
            Definitions::Rust(defs) => Some(defs.iter()),
            _ => None,
        }
    }

    pub fn iter_unknown(&self) -> Option<impl Iterator<Item = &DefinitionInfo<(), ()>>> {
        match self {
            Definitions::Unknown(defs) => Some(defs.iter()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ImportedSymbols {
    Ruby(Vec<RubyImportedSymbolInfo>),
    Python(Vec<PythonImportedSymbolInfo>),
    Kotlin(Vec<KotlinImportedSymbolInfo>),
    Java(Vec<JavaImportedSymbolInfo>),
    CSharp(Vec<CSharpImportedSymbolInfo>),
    TypeScript(Vec<TypeScriptImportedSymbolInfo>),
    Rust(Vec<RustImportedSymbolInfo>),
}

impl ImportedSymbols {
    pub fn count(&self) -> usize {
        match self {
            ImportedSymbols::Ruby(imports) => imports.len(),
            ImportedSymbols::Python(imports) => imports.len(),
            ImportedSymbols::Kotlin(imports) => imports.len(),
            ImportedSymbols::Java(imports) => imports.len(),
            ImportedSymbols::CSharp(imports) => imports.len(),
            ImportedSymbols::TypeScript(imports) => imports.len(),
            ImportedSymbols::Rust(imports) => imports.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn iter_kotlin(&self) -> Option<impl Iterator<Item = &KotlinImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Kotlin(imports) => Some(imports.iter()),
            _ => None,
        }
    }

    pub fn iter_java(&self) -> Option<impl Iterator<Item = &JavaImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Java(imports) => Some(imports.iter()),
            _ => None,
        }
    }

    pub fn iter_csharp(&self) -> Option<impl Iterator<Item = &CSharpImportedSymbolInfo>> {
        match self {
            ImportedSymbols::CSharp(imports) => Some(imports.iter()),
            _ => None,
        }
    }

    pub fn iter_python(&self) -> Option<impl Iterator<Item = &PythonImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Python(imports) => Some(imports.iter()),
            _ => None,
        }
    }

    pub fn iter_ruby(&self) -> Option<impl Iterator<Item = &RubyImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Ruby(imports) => Some(imports.iter()),
            _ => None,
        }
    }

    pub fn iter_typescript(&self) -> Option<impl Iterator<Item = &TypeScriptImportedSymbolInfo>> {
        match self {
            ImportedSymbols::TypeScript(imports) => Some(imports.iter()),
            _ => None,
        }
    }

    pub fn iter_rust(&self) -> Option<impl Iterator<Item = &RustImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Rust(imports) => Some(imports.iter()),
            _ => None,
        }
    }
}

pub type RubyReference = ReferenceInfo<
    RubyTargetResolution,
    RubyReferenceType,
    RubyExpressionMetadata,
    crate::ruby::types::RubyFqn,
>;

#[derive(Debug, Clone)]
pub enum References {
    Ruby(Vec<RubyReference>),
    Kotlin(Vec<KotlinReferenceInfo>),
    TypeScript(Vec<TypeScriptReferenceInfo>),
    Java(Vec<JavaReferenceInfo>),
    Python(Vec<PythonReferenceInfo>),
}

impl References {
    pub fn count(&self) -> usize {
        match self {
            References::Ruby(references) => references.len(),
            References::Kotlin(references) => references.len(),
            References::TypeScript(references) => references.len(),
            References::Java(references) => references.len(),
            References::Python(references) => references.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn iter_ruby(&self) -> Option<impl Iterator<Item = &RubyReference>> {
        match self {
            References::Ruby(references) => Some(references.iter()),
            _ => None,
        }
    }

    pub fn iter_kotlin(&self) -> Option<impl Iterator<Item = &KotlinReferenceInfo>> {
        match self {
            References::Kotlin(references) => Some(references.iter()),
            _ => None,
        }
    }

    pub fn iter_typescript(&self) -> Option<impl Iterator<Item = &TypeScriptReferenceInfo>> {
        match self {
            References::TypeScript(references) => Some(references.iter()),
            _ => None,
        }
    }

    pub fn iter_java(&self) -> Option<impl Iterator<Item = &JavaReferenceInfo>> {
        match self {
            References::Java(references) => Some(references.iter()),
            _ => None,
        }
    }

    pub fn iter_python(&self) -> Option<impl Iterator<Item = &PythonReferenceInfo>> {
        match self {
            References::Python(references) => Some(references.iter()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileProcessingResult {
    pub file_path: String,
    pub extension: String,
    pub file_size: u64,
    pub language: Language,
    pub definitions: Definitions,
    pub imported_symbols: Option<ImportedSymbols>,
    pub references: Option<References>,
    pub stats: ProcessingStats,
    pub is_supported: bool,
}

#[derive(Debug, Clone)]
pub struct ProcessingStats {
    pub total_time: Duration,
    pub parse_time: Duration,
    pub rules_time: Duration,
    pub analysis_time: Duration,
    pub rule_matches: usize,
    pub definitions_count: usize,
    pub imported_symbols_count: usize,
}
