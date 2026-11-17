use std::sync::OnceLock;

use cynic::GraphQlError;
use orbit_cli_graphql::ORBIT_SINCE;
use serde::Deserialize;
use tokio::sync::OnceCell;

static SELECTED: OnceLock<Api> = OnceLock::new();
static INSTANCE: OnceCell<Option<Instance>> = OnceCell::const_new();

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Api {
    Graphql,
    Rest,
}

impl Api {
    pub(crate) async fn selected(instance: impl Future<Output = Option<Instance>>) -> Option<Api> {
        if let Some(api) = SELECTED.get() {
            return Some(*api);
        }
        let instance = INSTANCE.get_or_init(|| instance).await.as_ref();
        (instance.and_then(Instance::serves_orbit_graphql) == Some(false))
            .then(|| *SELECTED.get_or_init(|| Api::Rest))
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

#[derive(Deserialize)]
pub(crate) struct Instance {
    version: String,
    #[serde(default)]
    enterprise: bool,
}

impl Instance {
    fn serves_orbit_graphql(&self) -> Option<bool> {
        let version = semver::Version::parse(&self.version).ok()?;
        let line = (version.major, version.minor);
        let floor = ORBIT_SINCE
            .iter()
            .rev()
            .find(|(major, minor, _)| (*major, *minor) <= line);
        Some(
            self.enterprise
                && floor.is_some_and(|&(major, minor, patch)| {
                    line > (major, minor) || version.patch >= patch
                }),
        )
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

    #[test]
    fn orbit_graphql_starts_at_the_first_patch_of_each_release_line() {
        for (version, enterprise, serves) in [
            ("19.3.9-ee", true, Some(false)),
            ("19.4.0-ee", true, Some(false)),
            ("19.4.6-ee", true, Some(false)),
            ("19.4.7-ee", true, Some(true)),
            ("19.5.0-ee", true, Some(false)),
            ("19.5.5-ee", true, Some(true)),
            ("19.6.0-pre", true, Some(true)),
            ("19.7.2-ee", true, Some(true)),
            ("20.0.0", true, Some(true)),
            ("19.6.0", false, Some(false)),
            ("19.6", true, None),
            ("unknown", true, None),
        ] {
            let instance = Instance {
                version: version.to_string(),
                enterprise,
            };
            assert_eq!(instance.serves_orbit_graphql(), serves, "{version}");
        }
    }
}
