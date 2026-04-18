use bolero::check;
use code_graph::legacy::parser::LanguageParser;
use code_graph::legacy::parser::kotlin::analyzer::KotlinAnalyzer;
use code_graph::legacy::parser::parser::{GenericParser, SupportedLanguage};
use std::str::from_utf8;

fn main() {
    let parser = GenericParser::default_for_language(SupportedLanguage::Kotlin);
    let analyzer = KotlinAnalyzer::new();

    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Ok(result) = parser.parse(s, Some("fuzz.kt"))
        {
            let _ = analyzer.analyze(&result);
        }
    });
}
