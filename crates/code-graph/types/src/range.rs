use serde::{Deserialize, Serialize};

/// Represents a position in source code (line, column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Position {
    pub line: usize,
    pub column: usize,
}

impl Position {
    pub const fn new(line: usize, column: usize) -> Self {
        Self { line, column }
    }
}

/// Represents a range in source code (start and end positions).
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

    pub fn empty() -> Self {
        Self {
            start: Position::new(0, 0),
            end: Position::new(0, 0),
            byte_offset: (0, 0),
        }
    }

    pub fn contains(&self, pos: &Position) -> bool {
        use std::cmp::Ordering;

        let starts_before_or_eq = match self.start.line.cmp(&pos.line) {
            Ordering::Less => true,
            Ordering::Equal => self.start.column <= pos.column,
            Ordering::Greater => false,
        };

        let ends_after_or_eq = match self.end.line.cmp(&pos.line) {
            Ordering::Greater => true,
            Ordering::Equal => self.end.column >= pos.column,
            Ordering::Less => false,
        };

        starts_before_or_eq && ends_after_or_eq
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
