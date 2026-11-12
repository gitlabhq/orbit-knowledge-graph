use std::path::Path;

use anyhow::{Context, Result};
use globset::{Glob, GlobMatcher};
use serde::Deserialize;

pub(crate) const CONFIG_PATH: &str = ".gitlab/orbit-quality.yml";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    #[serde(default)]
    layers: Vec<RuleSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleSpec {
    name: String,
    from: Vec<String>,
    deny: Option<Vec<String>>,
    allow_only: Option<Vec<String>>,
    #[serde(default)]
    except: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Rule {
    pub name: String,
    from: Vec<GlobMatcher>,
    deny: Vec<GlobMatcher>,
    allow_only: Option<Vec<GlobMatcher>>,
    except: Vec<GlobMatcher>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Violation {
    pub rule: String,
    pub from: String,
    pub to: String,
    pub refs: usize,
    pub example: String,
}

fn matchers(field: &str, rule: &str, patterns: &[String]) -> Result<Vec<GlobMatcher>> {
    patterns
        .iter()
        .map(|pattern| {
            Glob::new(pattern)
                .map(|glob| glob.compile_matcher())
                .with_context(|| format!("layer `{rule}`: invalid {field} pattern `{pattern}`"))
        })
        .collect()
}

fn any(matchers: &[GlobMatcher], path: &str) -> bool {
    matchers.iter().any(|m| m.is_match(path))
}

impl Rule {
    fn from_spec(spec: RuleSpec) -> Result<Self> {
        anyhow::ensure!(
            !spec.from.is_empty(),
            "layer `{}`: `from` needs at least one path pattern",
            spec.name
        );
        let (deny, allow_only) = match (&spec.deny, &spec.allow_only) {
            (Some(deny), None) => (deny.as_slice(), None),
            (None, Some(allow)) => (&[][..], Some(allow.as_slice())),
            _ => anyhow::bail!(
                "layer `{}`: set exactly one of `deny` or `allow_only`",
                spec.name
            ),
        };
        Ok(Self {
            from: matchers("from", &spec.name, &spec.from)?,
            deny: matchers("deny", &spec.name, deny)?,
            allow_only: allow_only
                .map(|allow| matchers("allow_only", &spec.name, allow))
                .transpose()?,
            except: matchers("except", &spec.name, &spec.except)?,
            name: spec.name,
        })
    }

    pub(crate) fn forbids(&self, from: &str, to: &str) -> bool {
        if !any(&self.from, from) || any(&self.from, to) || any(&self.except, to) {
            return false;
        }
        match &self.allow_only {
            None => any(&self.deny, to),
            Some(allow) => !any(allow, to),
        }
    }
}

pub(crate) fn parse(yaml: &str) -> Result<Vec<Rule>> {
    let config: ConfigFile = orbit_utils::yaml::from_str(yaml)
        .map_err(|e| anyhow::anyhow!("{CONFIG_PATH} is invalid: {e}"))?;
    config.layers.into_iter().map(Rule::from_spec).collect()
}

pub(crate) fn load(repo: &Path) -> Result<Option<Vec<Rule>>> {
    let path = repo.join(CONFIG_PATH);
    match std::fs::read_to_string(&path) {
        Ok(yaml) => parse(&yaml).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
    }
}

pub(crate) fn check<'a>(
    rules: &[Rule],
    edges: impl IntoIterator<Item = (&'a str, &'a str, usize, &'a str)>,
) -> Vec<Violation> {
    let mut out: Vec<Violation> = Vec::new();
    for (from, to, refs, example) in edges {
        for rule in rules.iter().filter(|rule| rule.forbids(from, to)) {
            out.push(Violation {
                rule: rule.name.clone(),
                from: from.to_string(),
                to: to.to_string(),
                refs,
                example: example.to_string(),
            });
        }
    }
    out.sort_by(|a, b| {
        a.rule
            .cmp(&b.rule)
            .then(a.from.cmp(&b.from))
            .then(a.to.cmp(&b.to))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"
layers:
  - name: cli stays out of the server
    from: ["crates/orbit-cli/**"]
    deny: ["crates/orbit-server/**", "crates/indexer/**"]
  - name: compiler is pure
    from: ["crates/query-engine/compiler/**"]
    allow_only: ["crates/query-engine/**", "crates/ontology/**", "crates/utils/**"]
    except: ["crates/query-engine/compiler/build.rs"]
"#;

    fn edges(
        pairs: &[(&'static str, &'static str)],
    ) -> Vec<(&'static str, &'static str, usize, &'static str)> {
        pairs.iter().map(|(a, b)| (*a, *b, 1, "")).collect()
    }

    #[test]
    fn deny_flags_only_listed_targets() {
        let rules = parse(CONFIG).unwrap();
        let found = check(
            &rules,
            edges(&[
                ("crates/orbit-cli/src/main.rs", "crates/indexer/src/lib.rs"),
                ("crates/orbit-cli/src/main.rs", "crates/ontology/src/lib.rs"),
                (
                    "crates/orbit-server/src/main.rs",
                    "crates/indexer/src/lib.rs",
                ),
            ]),
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].rule, "cli stays out of the server");
        assert_eq!(found[0].to, "crates/indexer/src/lib.rs");
    }

    #[test]
    fn allow_only_flags_everything_else_but_not_itself_or_exceptions() {
        let rules = parse(CONFIG).unwrap();
        let found = check(
            &rules,
            edges(&[
                (
                    "crates/query-engine/compiler/src/a.rs",
                    "crates/query-engine/types/src/b.rs",
                ),
                (
                    "crates/query-engine/compiler/src/a.rs",
                    "crates/query-engine/compiler/src/c.rs",
                ),
                (
                    "crates/query-engine/compiler/src/a.rs",
                    "crates/clickhouse-client/src/lib.rs",
                ),
                (
                    "crates/query-engine/compiler/src/a.rs",
                    "crates/query-engine/compiler/build.rs",
                ),
            ]),
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].to, "crates/clickhouse-client/src/lib.rs");
    }

    #[test]
    fn a_rule_needs_exactly_one_of_deny_or_allow_only() {
        let both =
            "layers:\n  - name: x\n    from: [a/**]\n    deny: [b/**]\n    allow_only: [c/**]\n";
        let neither = "layers:\n  - name: x\n    from: [a/**]\n";
        assert!(parse(both).unwrap_err().to_string().contains("exactly one"));
        assert!(
            parse(neither)
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
        let empty_allow_and_deny =
            "layers:\n  - name: x\n    from: [a/**]\n    deny: [b/**]\n    allow_only: []\n";
        assert!(parse(empty_allow_and_deny).is_err());
    }

    #[test]
    fn an_empty_allow_only_forbids_everything_outside_the_layer() {
        let rules =
            parse("layers:\n  - name: leaf\n    from: [u/**]\n    allow_only: []\n").unwrap();
        let found = check(&rules, edges(&[("u/a.rs", "u/b.rs"), ("u/a.rs", "v/c.rs")]));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].to, "v/c.rs");
    }

    #[test]
    fn bad_globs_and_unknown_keys_are_errors() {
        assert!(
            parse("layers:\n  - name: x\n    from: ['a/[']\n    deny: [b]\n")
                .unwrap_err()
                .to_string()
                .contains("invalid from pattern")
        );
        assert!(
            parse("layers:\n  - name: x\n    from: [a]\n    deny: [b]\n    typo: 1\n").is_err()
        );
        assert!(
            parse("").map(|r| r.is_empty()).unwrap_or(false)
                || parse("layers: []\n").unwrap().is_empty()
        );
    }
}
