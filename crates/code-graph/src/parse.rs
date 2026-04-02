use crate::fs::SourceFile;
use code_graph_linker::parsing::processor::FileProcessor;
pub use code_graph_linker::parsing::processor::{
    Definitions, ErroredFile, FileProcessingResult, ImportedSymbols, ProcessingResult,
    ProcessingStage, ProcessingStats, References, SkippedFile,
};

/// Parse a source file, extracting definitions, imports, and references.
///
/// Pure function — no I/O, no threading. The caller controls concurrency.
pub fn parse(file: &SourceFile) -> ProcessingResult {
    let processor = FileProcessor::new(file.path.to_string_lossy().to_string(), &file.content);
    processor.process()
}
