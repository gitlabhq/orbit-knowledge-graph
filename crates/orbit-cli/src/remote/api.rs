use std::sync::OnceLock;

use cynic::GraphQlError;
use serde::Deserialize;

static SELECTED: OnceLock<Api> = OnceLock::new();

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Api {
    Graphql,
    Rest,
}

impl Api {
    pub(crate) fn selected() -> Option<Api> {
        SELECTED.get().copied()
    }

    pub(crate) fn remember_from(errors: &[GraphQlError<ErrorExtensions>]) -> Api {
        *SELECTED.get_or_init(|| {
            let missing_schema = errors
                .iter()
                .filter_map(|error| error.extensions.as_ref())
                .any(ErrorExtensions::is_missing_schema);
            if missing_schema {
                Api::Rest
            } else {
                Api::Graphql
            }
        })
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorExtensions {
    code: Option<String>,
}

impl ErrorExtensions {
    fn is_missing_schema(&self) -> bool {
        matches!(
            self.code.as_deref(),
            Some("undefinedField" | "undefinedType" | "argumentNotAccepted" | "undefinedDirective")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_unknown_schema_elements_count_as_a_missing_schema() {
        for (code, missing) in [
            ("undefinedField", true),
            ("undefinedType", true),
            ("argumentNotAccepted", true),
            ("undefinedDirective", true),
            ("missingRequiredArguments", false),
        ] {
            let extensions = ErrorExtensions {
                code: Some(code.to_string()),
            };
            assert_eq!(extensions.is_missing_schema(), missing, "{code}");
        }
    }
}
