use bolero::check;
use parser_core::typescript::analyzer::TypeScriptAnalyzer;
use parser_core::typescript::parser::TypeScriptParser;
use std::str::from_utf8;

fn main() {
    let parser = TypeScriptParser::new();
    let analyzer = TypeScriptAnalyzer::new();

    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input) {
            // Test both .ts and .js dialects
            for ext in &["fuzz.ts", "fuzz.js"] {
                if let Ok(result) = parser.parse(s, Some(ext)) {
                    let _ = analyzer.analyze_swc(&result);
                }
            }
        }
    });
}
