use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedError {
    pub code: Option<u32>,
    pub category: ErrorCategory,
    pub message: String,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCategory {
    MemoryLimit,
    UnknownFunction,
    TypeMismatch,
    SyntaxError,
    UnknownColumn,
    UnknownTable,
    Timeout,
    NetworkError,
    ParameterError,
    CompilationError,
    Other,
}

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCategory::MemoryLimit => write!(f, "MEMORY_LIMIT"),
            ErrorCategory::UnknownFunction => write!(f, "UNKNOWN_FUNCTION"),
            ErrorCategory::TypeMismatch => write!(f, "TYPE_MISMATCH"),
            ErrorCategory::SyntaxError => write!(f, "SYNTAX_ERROR"),
            ErrorCategory::UnknownColumn => write!(f, "UNKNOWN_COLUMN"),
            ErrorCategory::UnknownTable => write!(f, "UNKNOWN_TABLE"),
            ErrorCategory::Timeout => write!(f, "TIMEOUT"),
            ErrorCategory::NetworkError => write!(f, "NETWORK_ERROR"),
            ErrorCategory::ParameterError => write!(f, "PARAMETER_ERROR"),
            ErrorCategory::CompilationError => write!(f, "COMPILATION_ERROR"),
            ErrorCategory::Other => write!(f, "OTHER"),
        }
    }
}

impl ParsedError {
    pub fn parse(error: &str) -> Self {
        let code = extract_error_code(error);
        let category = categorize_error(code, error);
        let summary = create_summary(&category, error);

        Self {
            code,
            category,
            message: error.to_string(),
            summary,
        }
    }

    pub fn is_transient(&self) -> bool {
        matches!(
            self.category,
            ErrorCategory::MemoryLimit | ErrorCategory::Timeout | ErrorCategory::NetworkError
        )
    }

    pub fn needs_query_fix(&self) -> bool {
        matches!(
            self.category,
            ErrorCategory::UnknownFunction
                | ErrorCategory::TypeMismatch
                | ErrorCategory::SyntaxError
                | ErrorCategory::UnknownColumn
                | ErrorCategory::UnknownTable
        )
    }
}

fn extract_error_code(error: &str) -> Option<u32> {
    if let Some(start) = error.find("Code: ") {
        let after_code = &error[start + 6..];
        if let Some(end) = after_code.find('.')
            && let Ok(code) = after_code[..end].trim().parse::<u32>()
        {
            return Some(code);
        }
    }
    None
}

fn categorize_error(code: Option<u32>, error: &str) -> ErrorCategory {
    if let Some(c) = code {
        match c {
            241 => return ErrorCategory::MemoryLimit,
            46 => return ErrorCategory::UnknownFunction,
            53 => return ErrorCategory::TypeMismatch,
            62 => return ErrorCategory::SyntaxError,
            47 => return ErrorCategory::UnknownColumn,
            60 => return ErrorCategory::UnknownTable,
            159 | 160 => return ErrorCategory::Timeout,
            _ => {}
        }
    }

    let lower = error.to_lowercase();

    if lower.contains("memory limit") || lower.contains("memory_limit") {
        ErrorCategory::MemoryLimit
    } else if lower.contains("unknown function") || lower.contains("does not exist") {
        ErrorCategory::UnknownFunction
    } else if lower.contains("type mismatch") || lower.contains("cannot convert") {
        ErrorCategory::TypeMismatch
    } else if lower.contains("syntax error") || lower.contains("parse error") {
        ErrorCategory::SyntaxError
    } else if lower.contains("unknown column") {
        ErrorCategory::UnknownColumn
    } else if lower.contains("unknown table") || lower.contains("doesn't exist") {
        ErrorCategory::UnknownTable
    } else if lower.contains("timeout") || lower.contains("execution time") {
        ErrorCategory::Timeout
    } else if lower.contains("network") || lower.contains("connection") {
        ErrorCategory::NetworkError
    } else if lower.contains("parameter substitution") {
        ErrorCategory::ParameterError
    } else if lower.contains("compilation failed") {
        ErrorCategory::CompilationError
    } else {
        ErrorCategory::Other
    }
}

fn create_summary(category: &ErrorCategory, error: &str) -> String {
    match category {
        ErrorCategory::MemoryLimit => {
            if let Some(summary) = extract_memory_details(error) {
                summary
            } else {
                "Query exceeded memory limit".to_string()
            }
        }
        ErrorCategory::UnknownFunction => {
            if let Some(name) = extract_quoted_value(error, "Function with name '", "'") {
                format!("Unknown function: {}", name)
            } else {
                "Unknown function".to_string()
            }
        }
        ErrorCategory::TypeMismatch => {
            if error.contains("Cannot convert string") {
                if let Some(val) = extract_quoted_value(error, "Cannot convert string '", "'") {
                    format!("Type mismatch: '{}' is not a number", val)
                } else {
                    "Type mismatch: string to number conversion failed".to_string()
                }
            } else {
                "Type mismatch".to_string()
            }
        }
        ErrorCategory::SyntaxError => "SQL syntax error".to_string(),
        ErrorCategory::UnknownColumn => {
            if let Some(name) = extract_quoted_value(error, "Unknown column '", "'") {
                format!("Unknown column: {}", name)
            } else {
                "Unknown column".to_string()
            }
        }
        ErrorCategory::UnknownTable => {
            if let Some(name) = extract_quoted_value(error, "Table ", " doesn't exist") {
                format!("Unknown table: {}", name)
            } else {
                "Unknown table".to_string()
            }
        }
        ErrorCategory::Timeout => "Query execution timeout".to_string(),
        ErrorCategory::NetworkError => "Network/connection error".to_string(),
        ErrorCategory::ParameterError => "Parameter substitution failed".to_string(),
        ErrorCategory::CompilationError => "Query compilation failed".to_string(),
        ErrorCategory::Other => {
            if error.len() > 60 {
                format!("{}...", &error[..60])
            } else {
                error.to_string()
            }
        }
    }
}

fn extract_memory_details(error: &str) -> Option<String> {
    let would_use = extract_memory_value(error, "would use ")?;
    let maximum = extract_memory_value(error, "maximum: ")?;
    Some(format!(
        "Memory limit: needed {} MiB, limit {} MiB",
        would_use, maximum
    ))
}

fn extract_memory_value(error: &str, prefix: &str) -> Option<String> {
    let start = error.find(prefix)?;
    let after = &error[start + prefix.len()..];
    let end = after.find([',', ':', '('])?;
    Some(after[..end].trim().to_string())
}

fn extract_quoted_value(error: &str, prefix: &str, suffix: &str) -> Option<String> {
    let start = error.find(prefix)?;
    let after = &error[start + prefix.len()..];
    let end = after.find(suffix)?;
    Some(after[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_error() {
        let error = "bad response: Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 583.66 MiB (attempt to allocate chunk of 138245016 bytes), maximum: 476.84 MiB.: While executing FillingRightJoinSide. (MEMORY_LIMIT_EXCEEDED)";
        let parsed = ParsedError::parse(error);

        assert_eq!(parsed.code, Some(241));
        assert_eq!(parsed.category, ErrorCategory::MemoryLimit);
        assert!(parsed.summary.contains("583.66 MiB"));
        assert!(parsed.is_transient());
    }

    #[test]
    fn test_parse_unknown_function() {
        let error =
            "bad response: Code: 46. DB::Exception: Function with name 'ARRAY' does not exist.";
        let parsed = ParsedError::parse(error);

        assert_eq!(parsed.code, Some(46));
        assert_eq!(parsed.category, ErrorCategory::UnknownFunction);
        assert!(parsed.summary.contains("ARRAY"));
        assert!(parsed.needs_query_fix());
    }

    #[test]
    fn test_parse_type_mismatch() {
        let error = "Code: 53. DB::Exception: Cannot convert string '2026-01-01' to type Int64.";
        let parsed = ParsedError::parse(error);

        assert_eq!(parsed.code, Some(53));
        assert_eq!(parsed.category, ErrorCategory::TypeMismatch);
        assert!(parsed.summary.contains("2026-01-01"));
    }

    #[test]
    fn test_extract_error_code() {
        assert_eq!(extract_error_code("Code: 241. Error"), Some(241));
        assert_eq!(extract_error_code("Code: 46. Error"), Some(46));
        assert_eq!(extract_error_code("No code here"), None);
    }
}
