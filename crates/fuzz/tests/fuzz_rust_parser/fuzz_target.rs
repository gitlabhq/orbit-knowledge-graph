use bolero::check;
use code_graph::v2::config::Language;
use std::str::from_utf8;

fn main() {
    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input) {
            let _ = Language::Rust.parse_ast(s);
        }
    });
}
