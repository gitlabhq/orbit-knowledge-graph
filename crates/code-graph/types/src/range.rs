use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Position {
    pub line: usize,
    pub column: usize,
}

impl Position {
    pub const fn new(line: usize, column: usize) -> Self {
        Self { line, column }
    }

    const fn as_tuple(&self) -> (usize, usize) {
        (self.line, self.column)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Range {
    pub start: Position,
    pub end: Position,
    pub byte_offset: (usize, usize),
}

impl Range {
    pub const fn new(start: Position, end: Position, byte_offset: (usize, usize)) -> Self {
        Self {
            start,
            end,
            byte_offset,
        }
    }

    pub const fn empty() -> Self {
        Self {
            start: Position::new(0, 0),
            end: Position::new(0, 0),
            byte_offset: (0, 0),
        }
    }

    pub fn contains(&self, pos: &Position) -> bool {
        let p = pos.as_tuple();
        self.start.as_tuple() <= p && p <= self.end.as_tuple()
    }

    pub const fn line_span(&self) -> usize {
        self.end.line.saturating_sub(self.start.line) + 1
    }

    pub const fn byte_length(&self) -> usize {
        self.byte_offset.1.saturating_sub(self.byte_offset.0)
    }

    pub fn is_contained_within(&self, other: Range) -> bool {
        self.byte_offset.0 >= other.byte_offset.0 && self.byte_offset.1 <= other.byte_offset.1
    }
}

impl std::fmt::Display for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}-{}:{} (bytes {}..{})",
            self.start.line,
            self.start.column,
            self.end.line,
            self.end.column,
            self.byte_offset.0,
            self.byte_offset.1
        )
    }
}
