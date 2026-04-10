use bolero::check;
use parser_core::LanguageParser;
use parser_core::parser::{GenericParser, SupportedLanguage};
use parser_core::rust::analyzer::RustAnalyzer;
use std::str::from_utf8;

fn main() {
    let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
    let analyzer = RustAnalyzer::new();

    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Ok(result) = parser.parse(s, Some("fuzz.rs"))
        {
            let _ = analyzer.analyze(&result);
        }
    });
}
