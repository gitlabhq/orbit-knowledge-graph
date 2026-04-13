use bolero::check;
use parser_core::LanguageParser;
use parser_core::parser::{GenericParser, SupportedLanguage};
use parser_core::python::analyzer::PythonAnalyzer;
use std::str::from_utf8;

fn main() {
    let parser = GenericParser::default_for_language(SupportedLanguage::Python);
    let analyzer = PythonAnalyzer::new();

    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Ok(result) = parser.parse(s, Some("fuzz.py"))
        {
            let _ = analyzer.analyze(&result);
        }
    });
}
