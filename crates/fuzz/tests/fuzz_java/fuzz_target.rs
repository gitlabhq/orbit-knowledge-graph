use bolero::check;
use parser_core::LanguageParser;
use parser_core::java::analyzer::JavaAnalyzer;
use parser_core::parser::{GenericParser, SupportedLanguage};
use std::str::from_utf8;

fn main() {
    let parser = GenericParser::default_for_language(SupportedLanguage::Java);
    let analyzer = JavaAnalyzer::new();

    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Ok(result) = parser.parse(s, Some("fuzz.java"))
        {
            let _ = analyzer.analyze(&result);
        }
    });
}
