use oxc_linter::loader::{JavaScriptSource, PartialLoader};

pub fn extract_scripts<'a>(
    source: &'a str,
    extension: &str,
) -> Result<Vec<JavaScriptSource<'a>>, String> {
    PartialLoader::parse(extension, source)
        .ok_or_else(|| format!("failed to parse embedded scripts for .{extension} file"))
}
