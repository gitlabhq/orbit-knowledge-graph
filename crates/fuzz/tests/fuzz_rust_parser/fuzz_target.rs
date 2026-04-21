use bolero::check;
use code_graph::legacy::parser::LanguageParser;
use code_graph::legacy::parser::parser::{GenericParser, SupportedLanguage};
use code_graph::legacy::parser::rust::analyzer::RustAnalyzer;
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
