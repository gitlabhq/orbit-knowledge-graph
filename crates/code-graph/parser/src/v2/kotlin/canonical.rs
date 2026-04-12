use crate::v2::CanonicalParser;
use code_graph_types::CanonicalResult;

pub struct KotlinCanonicalParser;

impl CanonicalParser for KotlinCanonicalParser {
    fn parse_file(&self, _source: &[u8], _file_path: &str) -> crate::Result<CanonicalResult> {
        todo!()
    }
}
