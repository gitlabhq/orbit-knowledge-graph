use code_graph_types::CanonicalResult;

/// Primary entry point for languages that own their full parse pipeline
/// (e.g. Ruby with ruby-prism, or any language with deeply custom logic).
///
/// The language module handles everything — parsing, scope resolution,
/// reference resolution — and produces a `CanonicalResult` directly.
/// The v2 linker doesn't care how you got there.
pub trait CanonicalParser {
    fn parse_file(&self, source: &[u8], file_path: &str) -> crate::Result<CanonicalResult>;
}
