use bolero::check;
use parser_core::ruby::analyzer::RubyAnalyzer;
use std::str::from_utf8;

fn main() {
    let analyzer = RubyAnalyzer::new();

    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input) {
            let _ = analyzer.parse_and_analyze(s);
        }
    });
}
