use bolero::check;
use parser_core::LanguageParser;
use parser_core::csharp::analyzer::CSharpAnalyzer;
use parser_core::parser::{GenericParser, SupportedLanguage};
use std::str::from_utf8;

fn main() {
    let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
    let analyzer = CSharpAnalyzer::new();

    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Ok(result) = parser.parse(s, Some("fuzz.cs"))
        {
            let _ = analyzer.analyze(&result);
        }
    });
}
