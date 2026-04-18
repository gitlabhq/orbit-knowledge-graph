use oxc_linter::loader::{JavaScriptSource, PartialLoader};

pub fn extract_scripts<'a>(source: &'a str, extension: &str) -> Vec<JavaScriptSource<'a>> {
    PartialLoader::parse(extension, source).unwrap_or_default()
}
