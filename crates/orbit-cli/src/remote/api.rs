use std::sync::OnceLock;

use cynic::GraphQlError;
use orbit_cli_graphql::ORBIT_SINCE;
use serde::Deserialize;
use tokio::sync::OnceCell;

static SELECTED: OnceLock<Api> = OnceLock::new();
static LACKS_ORBIT_GRAPHQL: OnceCell<bool> = OnceCell::const_new();

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
        let lacks = LACKS_ORBIT_GRAPHQL
            .get_or_init(|| async { instance.await.is_some_and(|i| i.lacks_orbit_graphql()) })
            .await;
        lacks.then(|| *SELECTED.get_or_init(|| Api::Rest))
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
    enterprise: bool,
}

impl Instance {
    fn lacks_orbit_graphql(&self) -> bool {
        let Ok(version) = semver::Version::parse(&self.version) else {
            return false;
        };
        let version = (version.major, version.minor, version.patch);
        !self.enterprise
            || !ORBIT_SINCE
                .iter()
                .rfind(|floor| (floor.0, floor.1) <= (version.0, version.1))
                .is_some_and(|floor| *floor <= version)
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
    fn orbit_graphql_needs_ee_from_the_first_patch_of_each_release_line() {
        for (version, enterprise, lacks) in [
            ("19.3.9-ee", true, true),
            ("19.4.0-ee", true, true),
            ("19.4.6-ee", true, true),
            ("19.4.7-ee", true, false),
            ("19.5.0-ee", true, true),
            ("19.5.0-rc42-ee", true, true),
            ("19.5.5-ee", true, false),
            ("19.6.0-rc42-ee", true, false),
            ("19.6.0-pre", true, false),
            ("19.7.2-ee", true, false),
            ("20.0.0", true, false),
            ("19.6.0", false, true),
            ("19.6", true, false),
            ("unknown", true, false),
        ] {
            let instance = Instance {
                version: version.to_string(),
                enterprise,
            };
            assert_eq!(instance.lacks_orbit_graphql(), lacks, "{version}");
        }
    }
}
