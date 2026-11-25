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
    #[serde(default)]
    forbid: Vec<ForbidSpec>,
    #[serde(default)]
    dead: DeadSpec,
    #[serde(default)]
    test_paths: Vec<String>,
    #[serde(default)]
    test_symbols: Vec<String>,
    #[serde(default)]
    shallow: ShallowSpec,
}

/// When a caller module uses too much of another module's surface: the
/// module has at least `min_surface` functions and types, and one caller
/// uses at least `max_used_share` of them.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShallowSpec {
    #[serde(default = "default_min_surface")]
    min_surface: usize,
    #[serde(default = "default_max_used_share")]
    max_used_share: f64,
    #[serde(default)]
    ignore_modules: Vec<String>,
}

impl Default for ShallowSpec {
    fn default() -> Self {
        Self {
            min_surface: default_min_surface(),
            max_used_share: default_max_used_share(),
            ignore_modules: Vec::new(),
        }
    }
}

fn default_min_surface() -> usize {
    6
}

fn default_max_used_share() -> f64 {
    0.5
}

#[derive(Debug)]
pub(crate) struct ShallowConfig {
    pub min_surface: usize,
    pub max_used_share: f64,
    ignore: Vec<GlobMatcher>,
}

impl Default for ShallowConfig {
    fn default() -> Self {
        Self {
            min_surface: default_min_surface(),
            max_used_share: default_max_used_share(),
            ignore: Vec::new(),
        }
    }
}

impl ShallowConfig {
    pub(crate) fn ignores(&self, module: &str) -> bool {
        any(&self.ignore, module)
    }
}

/// Names and paths the dead-code check leaves out, such as entry points and
/// test helpers. Names match whole function names; `*` is a wildcard.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeadSpec {
    #[serde(default)]
    ignore_names: Vec<String>,
    #[serde(default)]
    ignore_paths: Vec<String>,
}

#[derive(Debug, Default)]
pub(crate) struct DeadConfig {
    names: Vec<GlobMatcher>,
    paths: Vec<GlobMatcher>,
}

impl DeadConfig {
    pub(crate) fn ignores(&self, name: &str, path: &str) -> bool {
        any(&self.names, name) || any(&self.paths, path)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForbidSpec {
    name: String,
    symbols: Vec<String>,
    #[serde(default)]
    only_in: Vec<String>,
    #[serde(default)]
    paths: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Forbid {
    pub name: String,
    names: Vec<String>,
    symbols: Vec<regex::Regex>,
    only_in: Vec<GlobMatcher>,
    paths: Vec<GlobMatcher>,
}

#[derive(Debug, Default)]
pub(crate) struct Config {
    pub layers: Vec<Rule>,
    pub forbid: Vec<Forbid>,
    pub dead: DeadConfig,
    pub test_paths: Vec<String>,
    pub test_symbols: Vec<String>,
    pub shallow: ShallowConfig,
}

impl Config {
    pub(crate) fn rule_count(&self) -> usize {
        self.layers.len() + self.forbid.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SymbolUse {
    pub rule: String,
    pub file: String,
    pub symbol: String,
    pub matched: String,
    pub line: i64,
}

fn symbol_pattern(rule: &str, pattern: &str) -> Result<regex::Regex> {
    let body = regex::escape(pattern.trim_start_matches("::")).replace(r"\*", r"[A-Za-z0-9_:.]*");
    regex::Regex::new(&format!(r"^(::)?{body}((::|\.).*)?$"))
        .with_context(|| format!("forbid `{rule}`: invalid symbol `{pattern}`"))
}

impl Forbid {
    fn from_spec(spec: ForbidSpec) -> Result<Self> {
        anyhow::ensure!(
            !spec.symbols.is_empty(),
            "forbid `{}`: `symbols` needs at least one entry",
            spec.name
        );
        Ok(Self {
            names: spec
                .symbols
                .iter()
                .map(|symbol| symbol.trim_start_matches("::").to_string())
                .collect(),
            symbols: spec
                .symbols
                .iter()
                .map(|symbol| symbol_pattern(&spec.name, symbol))
                .collect::<Result<_>>()?,
            only_in: matchers("only_in", &spec.name, &spec.only_in)?,
            paths: matchers("paths", &spec.name, &spec.paths)?,
            name: spec.name,
        })
    }

    /// The configured symbol this use matches, when the rule applies to the file.
    pub(crate) fn forbids(&self, file: &str, symbol: &str) -> Option<&str> {
        if !(self.paths.is_empty() || any(&self.paths, file)) || any(&self.only_in, file) {
            return None;
        }
        self.symbols
            .iter()
            .position(|pattern| pattern.is_match(symbol))
            .map(|i| self.names[i].as_str())
    }
}

pub(crate) fn check_symbols<'a>(
    rules: &[Forbid],
    uses: impl IntoIterator<Item = (&'a str, &'a str, i64)>,
) -> Vec<SymbolUse> {
    let mut out: Vec<SymbolUse> = Vec::new();
    for (file, symbol, line) in uses {
        for rule in rules {
            if let Some(matched) = rule.forbids(file, symbol) {
                out.push(SymbolUse {
                    rule: rule.name.clone(),
                    file: file.to_string(),
                    symbol: symbol.trim_start_matches("::").to_string(),
                    matched: matched.to_string(),
                    line,
                });
            }
        }
    }
    out.sort_by(|a, b| {
        a.rule
            .cmp(&b.rule)
            .then(a.file.cmp(&b.file))
            .then(a.line.cmp(&b.line))
    });
    out.dedup_by(|a, b| a.rule == b.rule && a.file == b.file);
    out
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

pub(crate) fn parse_config(yaml: &str) -> Result<Config> {
    let config: ConfigFile = orbit_utils::yaml::from_str(yaml)
        .map_err(|e| anyhow::anyhow!("{CONFIG_PATH} is invalid: {e}"))?;
    Ok(Config {
        layers: config
            .layers
            .into_iter()
            .map(Rule::from_spec)
            .collect::<Result<_>>()?,
        forbid: config
            .forbid
            .into_iter()
            .map(Forbid::from_spec)
            .collect::<Result<_>>()?,
        dead: DeadConfig {
            names: matchers("ignore_names", "dead", &config.dead.ignore_names)?,
            paths: matchers("ignore_paths", "dead", &config.dead.ignore_paths)?,
        },
        test_paths: config.test_paths,
        test_symbols: config.test_symbols,
        shallow: {
            anyhow::ensure!(
                (0.0..=1.0).contains(&config.shallow.max_used_share),
                "shallow: max_used_share must be between 0 and 1"
            );
            ShallowConfig {
                min_surface: config.shallow.min_surface.max(1),
                max_used_share: config.shallow.max_used_share,
                ignore: matchers("ignore_modules", "shallow", &config.shallow.ignore_modules)?,
            }
        },
    })
}

#[cfg(test)]
pub(crate) fn parse(yaml: &str) -> Result<Vec<Rule>> {
    Ok(parse_config(yaml)?.layers)
}

pub(crate) fn load(repo: &Path) -> Result<Option<Config>> {
    let path = repo.join(CONFIG_PATH);
    match std::fs::read_to_string(&path) {
        Ok(yaml) => parse_config(&yaml).map(Some),
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

    const FORBID: &str = r#"
forbid:
  - name: outbound HTTP goes through Gitlab::HTTP
    symbols: ["Net::HTTP", "Faraday", "HTTParty"]
    only_in: ["lib/gitlab/http*.rb", "gems/gitlab-http/**"]
    paths: ["app/**", "lib/**", "ee/**"]
  - name: no Marshal
    symbols: ["Marshal"]
"#;

    #[test]
    fn forbid_matches_symbols_and_their_children_but_not_prefixes() {
        let config = parse_config(FORBID).unwrap();
        let found = check_symbols(
            &config.forbid,
            [
                ("app/models/a.rb", "Net::HTTP::Post", 3),
                ("app/models/a.rb", "::Net::HTTP", 4),
                ("app/models/b.rb", "Net::HTTPS", 1),
                ("app/models/c.rb", "FaradayMiddleware", 1),
                ("lib/gitlab/http.rb", "Net::HTTP", 9),
                ("spec/a_spec.rb", "Net::HTTP", 1),
                ("spec/a_spec.rb", "Marshal", 2),
            ],
        );
        let got: Vec<(&str, &str)> = found
            .iter()
            .map(|u| (u.rule.as_str(), u.file.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("no Marshal", "spec/a_spec.rb"),
                ("outbound HTTP goes through Gitlab::HTTP", "app/models/a.rb"),
            ]
        );
        assert_eq!(found[1].symbol, "Net::HTTP::Post");
        assert_eq!(found[1].matched, "Net::HTTP");
    }

    #[test]
    fn forbid_supports_wildcards() {
        let config = parse_config(
            "forbid:\n  - name: raw sql\n    symbols: [\"ActiveRecord::Base.connection*\"]\n",
        )
        .unwrap();
        let found = check_symbols(
            &config.forbid,
            [
                ("a.rb", "ActiveRecord::Base.connection.execute", 1),
                ("b.rb", "ActiveRecord::Base", 1),
            ],
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].file, "a.rb");
    }

    #[test]
    fn shallow_defaults_and_bounds() {
        let config = parse_config("layers: []\n").unwrap();
        assert_eq!(config.shallow.min_surface, 6);
        assert!((config.shallow.max_used_share - 0.5).abs() < f64::EPSILON);
        assert!(parse_config("shallow:\n  max_used_share: 1.5\n").is_err());
        let tuned =
            parse_config("shallow:\n  min_surface: 10\n  ignore_modules: [\"crates/utils/**\"]\n")
                .unwrap();
        assert_eq!(tuned.shallow.min_surface, 10);
        assert!(tuned.shallow.ignores("crates/utils/src"));
        assert!(!tuned.shallow.ignores("crates/ontology/src"));
    }

    #[test]
    fn dead_ignores_match_whole_names_and_paths() {
        let config = parse_config(
            "dead:\n  ignore_names: [main, \"test_*\"]\n  ignore_paths: [\"benches/**\"]\n",
        )
        .unwrap();
        assert!(config.dead.ignores("main", "src/a.rs"));
        assert!(config.dead.ignores("test_parse", "src/a.rs"));
        assert!(config.dead.ignores("run", "benches/x.rs"));
        assert!(!config.dead.ignores("domain", "src/a.rs"));
        assert!(!config.dead.ignores("attest_parse", "src/a.rs"));
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
