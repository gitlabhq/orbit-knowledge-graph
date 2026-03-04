#![no_main]

use libfuzzer_sys::fuzz_target;
use parser_core::ruby::analyzer::RubyAnalyzer;

fuzz_target!(|data: &str| {
    let analyzer = RubyAnalyzer::new();
    let _ = analyzer.parse_and_analyze(data);
});
