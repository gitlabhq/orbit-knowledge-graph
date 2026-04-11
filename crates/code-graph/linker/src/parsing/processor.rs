use crate::analysis::languages::js::JsAnalyzer;
use crate::analysis::languages::js_sfc;
use crate::loading::FileInfo;
use log::debug;
use parser_core::definitions::DefinitionInfo;
use parser_core::{
    csharp::{
        analyzer::CSharpAnalyzer,
        types::{CSharpDefinitionInfo, CSharpImportedSymbolInfo},
    },
    definitions::DefinitionTypeInfo,
    java::{
        analyzer::JavaAnalyzer,
        types::{JavaDefinitionInfo, JavaImportedSymbolInfo, JavaReferenceInfo},
    },
    kotlin::{
        analyzer::KotlinAnalyzer,
        types::{KotlinDefinitionInfo, KotlinImportedSymbolInfo, KotlinReferenceInfo},
    },
    parser::{ParserType, SupportedLanguage, UnifiedParseResult, detect_language_from_extension},
    python::{
        analyzer::PythonAnalyzer,
        types::{PythonDefinitionInfo, PythonImportedSymbolInfo, PythonReferenceInfo},
    },
    references::ReferenceInfo,
    ruby::{
        analyzer::RubyAnalyzer,
        definitions::RubyDefinitionInfo,
        imports::RubyImportedSymbolInfo,
        references::types::{RubyExpressionMetadata, RubyReferenceType, RubyTargetResolution},
    },
    rust::{analyzer::RustAnalyzer, imports::RustImportedSymbolInfo, types::RustDefinitionInfo},
    typescript::{
        analyzer::TypeScriptAnalyzer,
        swc::references::types::TypeScriptReferenceInfo,
        types::{TypeScriptDefinitionInfo, TypeScriptImportedSymbolInfo},
    },
};
use std::time::{Duration, Instant};

/// Represents a file that was skipped during processing
#[derive(Debug, Clone)]
pub struct SkippedFile {
    pub file_path: String,
    pub reason: String,
    pub file_size: Option<u64>,
}

/// Represents a file that encountered an error during processing
#[derive(Debug, Clone)]
pub struct ErroredFile {
    pub file_path: String,
    pub language: Option<SupportedLanguage>,
    pub error_message: String,
    pub error_stage: ProcessingStage,
}

/// Represents the stage where processing failed
#[derive(Debug, Clone)]
pub enum ProcessingStage {
    FileSystem, // Failed to read file metadata or content
    Parsing,    // Failed during parsing/analysis
    Unknown,    // Unknown stage
}

/// Result of processing a file that can be success, skipped, or error
#[derive(Debug)]
pub enum ProcessingResult {
    Success(FileProcessingResult),
    Skipped(SkippedFile),
    Error(ErroredFile),
}

impl ProcessingResult {
    /// Check if the result is a success
    pub fn is_success(&self) -> bool {
        matches!(self, ProcessingResult::Success(_))
    }

    /// Check if the result is skipped
    pub fn is_skipped(&self) -> bool {
        matches!(self, ProcessingResult::Skipped(_))
    }

    /// Check if the result is an error
    pub fn is_error(&self) -> bool {
        matches!(self, ProcessingResult::Error(_))
    }

    /// Get the file path regardless of result type
    pub fn file_path(&self) -> &str {
        match self {
            ProcessingResult::Success(result) => &result.file_path,
            ProcessingResult::Skipped(skipped) => &skipped.file_path,
            ProcessingResult::Error(errored) => &errored.file_path,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileProcessor<'a> {
    pub path: String,
    pub content: &'a str,
    /// Pre-computed file extension to avoid duplicate parsing
    pub extension: String,
}

impl<'a> FileProcessor<'a> {
    /// Create a new File with the given path and content
    pub fn new(path: String, content: &'a str) -> Self {
        let extension = std::path::Path::new(&path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("unknown")
            .to_string();

        Self {
            path,
            content,
            extension,
        }
    }

    /// Create a new File from FileInfo with pre-computed metadata
    pub fn from_file_info(file_info: FileInfo, content: &'a str) -> Self {
        Self {
            path: file_info.path.to_string_lossy().to_string(),
            content,
            extension: file_info
                .path
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        }
    }

    /// Create a new File with empty content (for lazy loading)
    pub fn new_empty(path: String) -> Self {
        let extension = std::path::Path::new(&path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("unknown")
            .to_string();

        Self {
            path,
            content: "",
            extension,
        }
    }

    /// Get the file path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the file content
    pub fn content(&self) -> &str {
        self.content
    }

    /// Get the file extension
    pub fn extension(&self) -> &str {
        &self.extension
    }

    /// Set the file content
    pub fn set_content(&mut self, content: &'a str) {
        self.content = content;
    }

    pub fn size(&self) -> u64 {
        self.content.len() as u64
    }

    /// Process the file and extract definitions using a language parser
    pub fn process(&self) -> ProcessingResult {
        let start_time = Instant::now();

        // 1. Detect language using pre-computed extension (avoids duplicate parsing)
        let language = match detect_language_from_extension(&self.extension) {
            Ok(lang) => lang,
            Err(e) => {
                return ProcessingResult::Error(ErroredFile {
                    file_path: self.path.clone(),
                    language: None,
                    error_message: format!("Failed to detect language: {e}"),
                    error_stage: ProcessingStage::Parsing,
                });
            }
        };

        // Check if language is supported
        let is_supported = matches!(
            language,
            SupportedLanguage::Ruby
                | SupportedLanguage::Python
                | SupportedLanguage::Kotlin
                | SupportedLanguage::Java
                | SupportedLanguage::CSharp
                | SupportedLanguage::TypeScript
                | SupportedLanguage::Rust
                | SupportedLanguage::Js
                | SupportedLanguage::Vue
                | SupportedLanguage::Svelte
        );
        if !is_supported {
            return ProcessingResult::Skipped(SkippedFile {
                file_path: self.path.clone(),
                reason: format!("Unsupported language: {language:?}"),
                file_size: Some(self.size()),
            });
        }

        // Check if file is excluded
        if language
            .exclude_extensions()
            .iter()
            .any(|suffix| self.path.ends_with(suffix))
        {
            return ProcessingResult::Skipped(SkippedFile {
                file_path: self.path.clone(),
                reason: format!("File is excluded due to exclude_extensions match: {language:?}"),
                file_size: Some(self.size()),
            });
        }

        // JS/Vue/Svelte: use OXC directly, bypassing parser-core
        if matches!(
            language,
            SupportedLanguage::Js | SupportedLanguage::Vue | SupportedLanguage::Svelte
        ) {
            return self.process_js(language, start_time);
        }

        // Use unified pipeline for all languages (ParserType + rules + analyzer)
        {
            // 2. Parse the file using language-specific parser (prism for Ruby, swc for TypeScript, ast-grep for others)
            let parse_start = Instant::now();
            let parser = ParserType::for_language(language);
            let parse_result = match parser.parse(self.content, Some(&self.path)) {
                Ok(result) => result,
                Err(e) => {
                    return ProcessingResult::Error(ErroredFile {
                        file_path: self.path.clone(),
                        language: Some(language),
                        error_message: format!("Failed to parse: {e}"),
                        error_stage: ProcessingStage::Parsing,
                    });
                }
            };
            let parse_time = parse_start.elapsed();

            // 3. Use language-specific analyzer to extract constructs
            let analysis_start = Instant::now();
            let (definitions, imports, references) =
                match self.analyze_file(language, &parse_result) {
                    Ok(result) => result,
                    Err(e) => {
                        return ProcessingResult::Error(ErroredFile {
                            file_path: self.path.clone(),
                            language: Some(language),
                            error_message: format!("Failed to analyze: {e}"),
                            error_stage: ProcessingStage::Parsing,
                        });
                    }
                };
            let analysis_time = analysis_start.elapsed();

            let definitions_count = definitions.count();
            let imported_symbols_count = imports.as_ref().map_or(0, |i| i.count());

            ProcessingResult::Success(FileProcessingResult {
                file_path: self.path.clone(),
                extension: self.extension.clone(),
                file_size: self.size(),
                language,
                definitions,
                imported_symbols: imports,
                references,
                stats: ProcessingStats {
                    total_time: start_time.elapsed(),
                    parse_time,
                    rules_time: Duration::ZERO,
                    analysis_time,
                    rule_matches: 0,
                    definitions_count,
                    imported_symbols_count,
                },
                is_supported: true,
            })
        }
    }

    /// Process JS/TS/Vue/Svelte files using OXC directly.
    fn process_js(&self, language: SupportedLanguage, start_time: Instant) -> ProcessingResult {
        let analysis_start = Instant::now();

        // For Vue/Svelte, extract script blocks first
        let sources: Vec<(String, String)> = match language {
            SupportedLanguage::Vue | SupportedLanguage::Svelte => {
                let ext = if language == SupportedLanguage::Vue {
                    "vue"
                } else {
                    "svelte"
                };
                let blocks = js_sfc::extract_scripts(self.content, ext);
                if blocks.is_empty() {
                    return ProcessingResult::Skipped(SkippedFile {
                        file_path: self.path.clone(),
                        reason: "No <script> blocks found".to_string(),
                        file_size: Some(self.size()),
                    });
                }
                blocks
                    .into_iter()
                    .map(|b| {
                        let ext = if b.is_typescript { "ts" } else { "js" };
                        let virtual_path = format!("{}.{ext}", self.path);
                        (virtual_path, b.source_text)
                    })
                    .collect()
            }
            _ => vec![(self.path.clone(), self.content.to_string())],
        };

        let mut all_definitions = Vec::new();
        let mut all_imported_symbols = Vec::new();
        let mut all_relationships = Vec::new();

        for (file_path, source) in &sources {
            match JsAnalyzer::analyze_file(source, file_path, &self.path) {
                Ok(result) => {
                    all_definitions.extend(result.definitions);
                    all_imported_symbols.extend(result.imported_symbols);
                    all_relationships.extend(result.relationships);
                }
                Err(e) => {
                    return ProcessingResult::Error(ErroredFile {
                        file_path: self.path.clone(),
                        language: Some(language),
                        error_message: format!("OXC analysis failed: {e}"),
                        error_stage: ProcessingStage::Parsing,
                    });
                }
            }
        }

        let analysis_time = analysis_start.elapsed();
        let definitions_count = all_definitions.len();
        let imported_symbols_count = all_imported_symbols.len();

        ProcessingResult::Success(FileProcessingResult {
            file_path: self.path.clone(),
            extension: self.extension.clone(),
            file_size: self.size(),
            language,
            definitions: Definitions::JsOxc,
            imported_symbols: None,
            references: None,
            stats: ProcessingStats {
                total_time: start_time.elapsed(),
                parse_time: Duration::ZERO,
                rules_time: Duration::ZERO,
                analysis_time,
                rule_matches: 0,
                definitions_count,
                imported_symbols_count,
            },
            is_supported: true,
        })
    }

    fn analyze_file(
        &self,
        language: SupportedLanguage,
        parse_result: &UnifiedParseResult,
    ) -> Result<(Definitions, Option<ImportedSymbols>, Option<References>), anyhow::Error> {
        debug!("Starting to analyze file {}.", self.path);
        let result = match language {
            SupportedLanguage::Ruby => {
                if let UnifiedParseResult::Ruby(ruby_result) = parse_result {
                    let analyzer = RubyAnalyzer::new();
                    match analyzer.analyze_with_prism(self.content, &ruby_result.ast) {
                        Ok(analysis_result) => {
                            // Return references directly instead of converting to expressions
                            let references = if analysis_result.references.is_empty() {
                                None
                            } else {
                                Some(References::Ruby(analysis_result.references))
                            };

                            Ok((
                                Definitions::Ruby(analysis_result.definitions),
                                if analysis_result.imports.is_empty() {
                                    None
                                } else {
                                    Some(ImportedSymbols::Ruby(analysis_result.imports))
                                },
                                references,
                            ))
                        }
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to analyze Ruby file '{}': {}",
                            self.path,
                            e
                        )),
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Expected Ruby parse result for Ruby file '{}'",
                        self.path
                    ))
                }
            }
            SupportedLanguage::Python => {
                if let UnifiedParseResult::TreeSitter(ast_result) = parse_result {
                    let analyzer = PythonAnalyzer::new();
                    match analyzer.analyze(ast_result) {
                        Ok(analysis_result) => Ok((
                            Definitions::Python(analysis_result.definitions),
                            Some(ImportedSymbols::Python(analysis_result.imports)),
                            Some(References::Python(analysis_result.references)),
                        )),
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to analyze Python file '{}': {}",
                            self.path,
                            e
                        )),
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Expected TreeSitter parse result for Python file '{}'",
                        self.path
                    ))
                }
            }
            SupportedLanguage::Kotlin => {
                if let UnifiedParseResult::TreeSitter(ast_result) = parse_result {
                    let analyzer = KotlinAnalyzer::new();
                    match analyzer.analyze(ast_result) {
                        Ok(analysis_result) => Ok((
                            Definitions::Kotlin(analysis_result.definitions),
                            Some(ImportedSymbols::Kotlin(analysis_result.imports)),
                            Some(References::Kotlin(analysis_result.references)),
                        )),
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to analyze Kotlin file '{}': {}",
                            self.path,
                            e
                        )),
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Expected TreeSitter parse result for Kotlin file '{}'",
                        self.path
                    ))
                }
            }
            SupportedLanguage::Java => {
                if let UnifiedParseResult::TreeSitter(ast_result) = parse_result {
                    let analyzer = JavaAnalyzer::new();
                    match analyzer.analyze(ast_result) {
                        Ok(analysis_result) => Ok((
                            Definitions::Java(analysis_result.definitions),
                            Some(ImportedSymbols::Java(analysis_result.imports)),
                            Some(References::Java(analysis_result.references)),
                        )),
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to analyze Java file '{}': {}",
                            self.path,
                            e
                        )),
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Expected TreeSitter parse result for Java file '{}'",
                        self.path
                    ))
                }
            }
            SupportedLanguage::CSharp => {
                if let UnifiedParseResult::TreeSitter(ast_result) = parse_result {
                    let analyzer = CSharpAnalyzer::new();
                    match analyzer.analyze(ast_result) {
                        Ok(analysis_result) => Ok((
                            Definitions::CSharp(analysis_result.definitions),
                            Some(ImportedSymbols::CSharp(analysis_result.imports)),
                            None, // CSharp doesn't extract references currently
                        )),
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to analyze CSharp file '{}': {}",
                            self.path,
                            e
                        )),
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Expected TreeSitter parse result for CSharp file '{}'",
                        self.path
                    ))
                }
            }
            SupportedLanguage::TypeScript => {
                if let UnifiedParseResult::TypeScript(typescript_result) = parse_result {
                    let analyzer = TypeScriptAnalyzer::new();
                    match analyzer.analyze_swc(typescript_result) {
                        Ok(analysis_result) => Ok((
                            Definitions::TypeScript(analysis_result.definitions),
                            Some(ImportedSymbols::TypeScript(analysis_result.imports)),
                            Some(References::TypeScript(analysis_result.references)),
                        )),
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to analyze TypeScript file '{}': {}",
                            self.path,
                            e
                        )),
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Expected TypeScript parse result for TypeScript file '{}'",
                        self.path
                    ))
                }
            }
            SupportedLanguage::Rust => {
                if let UnifiedParseResult::TreeSitter(ast_result) = parse_result {
                    let analyzer = RustAnalyzer::new();
                    match analyzer.analyze(ast_result) {
                        Ok(analysis_result) => Ok((
                            Definitions::Rust(analysis_result.definitions),
                            Some(ImportedSymbols::Rust(analysis_result.imports)),
                            None, // Rust doesn't extract references currently
                        )),
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to analyze Rust file '{}': {}",
                            self.path,
                            e
                        )),
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Expected TreeSitter parse result for Rust file '{}'",
                        self.path
                    ))
                }
            }
            SupportedLanguage::Js | SupportedLanguage::Vue | SupportedLanguage::Svelte => {
                // OXC-based analysis handled directly in the linker, not through parser-core.
                // TODO: Wire JsAnalyzer::analyze_file() here
                Ok((Definitions::JsOxc, None, None))
            }
        };
        debug!("Finished analyzing file {}.", self.path);
        result
    }
}

/// Enum to hold definitions based on language
#[derive(Clone, Debug)]
pub enum Definitions {
    Ruby(Vec<RubyDefinitionInfo>),
    Python(Vec<PythonDefinitionInfo>),
    Kotlin(Vec<KotlinDefinitionInfo>),
    Java(Vec<JavaDefinitionInfo>),
    CSharp(Vec<CSharpDefinitionInfo>),
    TypeScript(Vec<TypeScriptDefinitionInfo>),
    Rust(Vec<RustDefinitionInfo>),
    /// Placeholder for JS/TS files analyzed by OXC directly in the linker.
    JsOxc,
    Unknown(Vec<DefinitionInfo<(), ()>>),
}

impl Definitions {
    /// Get the count of definitions regardless of type
    pub fn count(&self) -> usize {
        match self {
            Definitions::Ruby(defs) => defs.len(),
            Definitions::Python(defs) => defs.len(),
            Definitions::Kotlin(defs) => defs.len(),
            Definitions::Java(defs) => defs.len(),
            Definitions::CSharp(defs) => defs.len(),
            Definitions::TypeScript(defs) => defs.len(),
            Definitions::Rust(defs) => defs.len(),
            Definitions::JsOxc => 0,
            Definitions::Unknown(defs) => defs.len(),
        }
    }

    /// Check if there are any definitions
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Get an iterator over definition type strings using the proper DefinitionTypeInfo trait
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
            Definitions::JsOxc => Box::new(std::iter::empty()),
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

/// Enum to hold imported symbols based on language
#[derive(Clone, Debug)]
pub enum ImportedSymbols {
    Java(Vec<JavaImportedSymbolInfo>),
    Kotlin(Vec<KotlinImportedSymbolInfo>),
    Python(Vec<PythonImportedSymbolInfo>),
    CSharp(Vec<CSharpImportedSymbolInfo>),
    Ruby(Vec<RubyImportedSymbolInfo>),
    TypeScript(Vec<TypeScriptImportedSymbolInfo>),
    Rust(Vec<RustImportedSymbolInfo>),
}

impl ImportedSymbols {
    /// Get the count of imported symbols regardless of type
    pub fn count(&self) -> usize {
        match self {
            ImportedSymbols::Java(imported_symbols) => imported_symbols.len(),
            ImportedSymbols::Kotlin(imported_symbols) => imported_symbols.len(),
            ImportedSymbols::Python(imported_symbols) => imported_symbols.len(),
            ImportedSymbols::CSharp(imported_symbols) => imported_symbols.len(),
            ImportedSymbols::Ruby(imported_symbols) => imported_symbols.len(),
            ImportedSymbols::TypeScript(imported_symbols) => imported_symbols.len(),
            ImportedSymbols::Rust(imported_symbols) => imported_symbols.len(),
        }
    }

    /// Check if there are any imported symbols
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn iter_kotlin(&self) -> Option<impl Iterator<Item = &KotlinImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Kotlin(imported_symbols) => Some(imported_symbols.iter()),
            _ => None,
        }
    }

    pub fn iter_java(&self) -> Option<impl Iterator<Item = &JavaImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Java(imported_symbols) => Some(imported_symbols.iter()),
            _ => None,
        }
    }

    pub fn iter_csharp(
        &self,
    ) -> Option<
        impl Iterator<
            Item = &parser_core::imports::ImportedSymbolInfo<
                parser_core::csharp::types::CSharpImportType,
                parser_core::csharp::types::CSharpFqn,
            >,
        >,
    > {
        match self {
            ImportedSymbols::CSharp(imported_symbols) => Some(imported_symbols.iter()),
            _ => None,
        }
    }

    pub fn iter_python(&self) -> Option<impl Iterator<Item = &PythonImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Python(imported_symbols) => Some(imported_symbols.iter()),
            _ => None,
        }
    }

    pub fn iter_ruby(&self) -> Option<impl Iterator<Item = &RubyImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Ruby(imported_symbols) => Some(imported_symbols.iter()),
            _ => None,
        }
    }

    pub fn iter_typescript(&self) -> Option<impl Iterator<Item = &TypeScriptImportedSymbolInfo>> {
        match self {
            ImportedSymbols::TypeScript(imported_symbols) => Some(imported_symbols.iter()),
            _ => None,
        }
    }

    pub fn iter_rust(&self) -> Option<impl Iterator<Item = &RustImportedSymbolInfo>> {
        match self {
            ImportedSymbols::Rust(imported_symbols) => Some(imported_symbols.iter()),
            _ => None,
        }
    }
}

/// Type alias for Ruby references
pub type RubyReference = ReferenceInfo<
    RubyTargetResolution,
    RubyReferenceType,
    RubyExpressionMetadata,
    parser_core::ruby::types::RubyFqn,
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
    /// Get the count of references regardless of type
    pub fn count(&self) -> usize {
        match self {
            References::Ruby(references) => references.len(),
            References::Kotlin(references) => references.len(),
            References::TypeScript(references) => references.len(),
            References::Java(references) => references.len(),
            References::Python(references) => references.len(),
        }
    }

    /// Check if there are any references
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

/// Result of processing a single file using Ruby analyzer
#[derive(Clone, Debug)]
pub struct FileProcessingResult {
    /// File path
    pub file_path: String,
    /// Extension of the file
    pub extension: String,
    /// File size in bytes
    pub file_size: u64,
    /// Detected language
    pub language: SupportedLanguage,
    /// Extracted definitions
    pub definitions: Definitions,
    /// Extracted imported symbols
    pub imported_symbols: Option<ImportedSymbols>,
    /// Extracted references for Ruby (used for reference resolution)
    pub references: Option<References>,
    /// Processing statistics
    pub stats: ProcessingStats,
    /// Whether this language is supported for analysis
    pub is_supported: bool,
}

/// Processing statistics
#[derive(Debug, Clone)]
pub struct ProcessingStats {
    /// Total processing time
    pub total_time: Duration,
    /// Time spent parsing
    pub parse_time: Duration,
    /// Time spent running rules
    pub rules_time: Duration,
    /// Time spent in Ruby analysis
    pub analysis_time: Duration,
    /// Number of rule matches found
    pub rule_matches: usize,
    /// Number of definitions extracted
    pub definitions_count: usize,
    /// Number of imported symbols extracted
    pub imported_symbols_count: usize,
}
