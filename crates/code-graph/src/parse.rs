pub use parser_core::parse_types::*;

use log::debug;
use parser_core::{
    csharp::analyzer::CSharpAnalyzer,
    java::analyzer::JavaAnalyzer,
    kotlin::analyzer::KotlinAnalyzer,
    parser::{Language, ParserType, UnifiedParseResult, detect_language_from_extension},
    python::analyzer::PythonAnalyzer,
    ruby::analyzer::RubyAnalyzer,
    rust::analyzer::RustAnalyzer,
    typescript::analyzer::TypeScriptAnalyzer,
};
use std::time::{Duration, Instant};

/// Parse a source file by path and content.
///
/// Detects language from extension, selects the right parser backend,
/// runs the language-specific analyzer, and returns a `ProcessingResult`.
///
/// Pure function — no I/O, no threading.
pub fn parse(path: &str, content: &str) -> ProcessingResult {
    let start_time = Instant::now();

    let extension = std::path::Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("unknown");

    let language = match detect_language_from_extension(extension) {
        Ok(lang) => lang,
        Err(e) => {
            return ProcessingResult::Error(ErroredFile {
                file_path: path.to_string(),
                language: None,
                error_message: format!("Failed to detect language: {e}"),
                error_stage: ProcessingStage::Parsing,
            });
        }
    };

    let is_supported = matches!(
        language,
        Language::Ruby
            | Language::Python
            | Language::Kotlin
            | Language::Java
            | Language::CSharp
            | Language::TypeScript
            | Language::Rust
    );

    let file_size = content.len() as u64;

    if !is_supported {
        return ProcessingResult::Skipped(SkippedFile {
            file_path: path.to_string(),
            reason: format!("Unsupported language: {language:?}"),
            file_size: Some(file_size),
        });
    }

    if language
        .exclude_extensions()
        .iter()
        .any(|suffix| path.ends_with(suffix))
    {
        return ProcessingResult::Skipped(SkippedFile {
            file_path: path.to_string(),
            reason: format!("File is excluded due to exclude_extensions match: {language:?}"),
            file_size: Some(file_size),
        });
    }

    let parse_start = Instant::now();
    let parser = ParserType::for_language(language);
    let parse_result = match parser.parse(content, Some(path)) {
        Ok(result) => result,
        Err(e) => {
            return ProcessingResult::Error(ErroredFile {
                file_path: path.to_string(),
                language: Some(language),
                error_message: format!("Failed to parse: {e}"),
                error_stage: ProcessingStage::Parsing,
            });
        }
    };
    let parse_time = parse_start.elapsed();

    let analysis_start = Instant::now();
    let (definitions, imports, references) = match analyze(path, language, &parse_result, content) {
        Ok(result) => result,
        Err(e) => {
            return ProcessingResult::Error(ErroredFile {
                file_path: path.to_string(),
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
        file_path: path.to_string(),
        extension: extension.to_string(),
        file_size,
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

fn analyze(
    path: &str,
    language: Language,
    parse_result: &UnifiedParseResult,
    content: &str,
) -> Result<(Definitions, Option<ImportedSymbols>, Option<References>), anyhow::Error> {
    debug!("Starting to analyze file {}.", path);
    let result = match language {
        Language::Ruby => {
            if let UnifiedParseResult::Ruby(ruby_result) = parse_result {
                let analyzer = RubyAnalyzer::new();
                match analyzer.analyze_with_prism(content, &ruby_result.ast) {
                    Ok(r) => {
                        let references = if r.references.is_empty() {
                            None
                        } else {
                            Some(References::Ruby(r.references))
                        };
                        Ok((
                            Definitions::Ruby(r.definitions),
                            if r.imports.is_empty() {
                                None
                            } else {
                                Some(ImportedSymbols::Ruby(r.imports))
                            },
                            references,
                        ))
                    }
                    Err(e) => Err(anyhow::anyhow!("Failed to analyze Ruby file '{path}': {e}")),
                }
            } else {
                Err(anyhow::anyhow!("Expected Ruby parse result for '{path}'"))
            }
        }
        Language::Python => {
            if let UnifiedParseResult::TreeSitter(ast) = parse_result {
                let analyzer = PythonAnalyzer::new();
                match analyzer.analyze(ast) {
                    Ok(r) => Ok((
                        Definitions::Python(r.definitions),
                        Some(ImportedSymbols::Python(r.imports)),
                        Some(References::Python(r.references)),
                    )),
                    Err(e) => Err(anyhow::anyhow!(
                        "Failed to analyze Python file '{path}': {e}"
                    )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "Expected TreeSitter parse result for '{path}'"
                ))
            }
        }
        Language::Kotlin => {
            if let UnifiedParseResult::TreeSitter(ast) = parse_result {
                let analyzer = KotlinAnalyzer::new();
                match analyzer.analyze(ast) {
                    Ok(r) => Ok((
                        Definitions::Kotlin(r.definitions),
                        Some(ImportedSymbols::Kotlin(r.imports)),
                        Some(References::Kotlin(r.references)),
                    )),
                    Err(e) => Err(anyhow::anyhow!(
                        "Failed to analyze Kotlin file '{path}': {e}"
                    )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "Expected TreeSitter parse result for '{path}'"
                ))
            }
        }
        Language::Java => {
            if let UnifiedParseResult::TreeSitter(ast) = parse_result {
                let analyzer = JavaAnalyzer::new();
                match analyzer.analyze(ast) {
                    Ok(r) => Ok((
                        Definitions::Java(r.definitions),
                        Some(ImportedSymbols::Java(r.imports)),
                        Some(References::Java(r.references)),
                    )),
                    Err(e) => Err(anyhow::anyhow!("Failed to analyze Java file '{path}': {e}")),
                }
            } else {
                Err(anyhow::anyhow!(
                    "Expected TreeSitter parse result for '{path}'"
                ))
            }
        }
        Language::CSharp => {
            if let UnifiedParseResult::TreeSitter(ast) = parse_result {
                let analyzer = CSharpAnalyzer::new();
                match analyzer.analyze(ast) {
                    Ok(r) => Ok((
                        Definitions::CSharp(r.definitions),
                        Some(ImportedSymbols::CSharp(r.imports)),
                        None,
                    )),
                    Err(e) => Err(anyhow::anyhow!(
                        "Failed to analyze CSharp file '{path}': {e}"
                    )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "Expected TreeSitter parse result for '{path}'"
                ))
            }
        }
        Language::TypeScript => {
            if let UnifiedParseResult::TypeScript(ts) = parse_result {
                let analyzer = TypeScriptAnalyzer::new();
                match analyzer.analyze_swc(ts) {
                    Ok(r) => Ok((
                        Definitions::TypeScript(r.definitions),
                        Some(ImportedSymbols::TypeScript(r.imports)),
                        Some(References::TypeScript(r.references)),
                    )),
                    Err(e) => Err(anyhow::anyhow!(
                        "Failed to analyze TypeScript file '{path}': {e}"
                    )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "Expected TypeScript parse result for '{path}'"
                ))
            }
        }
        Language::Rust => {
            if let UnifiedParseResult::TreeSitter(ast) = parse_result {
                let analyzer = RustAnalyzer::new();
                match analyzer.analyze(ast) {
                    Ok(r) => Ok((
                        Definitions::Rust(r.definitions),
                        Some(ImportedSymbols::Rust(r.imports)),
                        None,
                    )),
                    Err(e) => Err(anyhow::anyhow!("Failed to analyze Rust file '{path}': {e}")),
                }
            } else {
                Err(anyhow::anyhow!(
                    "Expected TreeSitter parse result for '{path}'"
                ))
            }
        }
    };
    debug!("Finished analyzing file {}.", path);
    result
}
