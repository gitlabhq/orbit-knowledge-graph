use crate::utils::Position;

/// Line offset cache for efficient position calculations
pub struct LineOffsetCache {
    line_starts: Vec<usize>,
}

impl LineOffsetCache {
    pub fn new(source: &str) -> Self {
        let mut line_starts = Vec::with_capacity(source.len() / 50); // Estimate ~50 chars per line
        line_starts.push(0);

        for (i, byte) in source.bytes().enumerate() {
            if byte == b'\n' {
                line_starts.push(i + 1);
            }
        }

        Self { line_starts }
    }

    pub fn offset_to_position(&self, offset: usize) -> Position {
        // Binary search is O(log n) instead of O(n)
        let line = match self.line_starts.binary_search(&offset) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        };
        let column = offset.saturating_sub(self.line_starts[line]);
        Position { line, column }
    }
}
