//! YAML rule loader. Compiles declarative rewrite rules into `Rewrite` structs.
//!
//! Format:
//! ```yaml
//! stages:
//!   - name: normalize
//!     rules:
//!       - match: '(aliased_import name: $N)'
//!         set_text: '$N'
//!       - match: '(aliased_import alias: $A)'
//!         append: '(__alias @$A)'
//!       - match: '(attribute object: (identifier "self") attribute: $A)'
//!         replace: '(__ivar @$A)'
//!   - name: retag
//!     rules:
//!       - match: '(attribute object: $O attribute: $M)'
//!         retag:
//!           kind: __member
//!           fields: { O: object, M: member }
//! ```

use crate::intern::Lang;
use crate::pattern::{Out, Rewrite};

pub enum ResolveStage {
    Rules(Vec<Rewrite>),
    Climb { while_kind: u16, mark_kind: u16 },
}

#[derive(Clone)]
pub struct ParseFileSpec {
    pub name: String,
    pub format: ParseFormat,
}

#[derive(Clone, Copy)]
pub enum ParseFormat {
    Json,
    Toml,
}

pub struct ResolveConfig {
    pub stages: Vec<ResolveStage>,
    pub parse_files: Vec<ParseFileSpec>,
    pub lookup_from: Vec<u16>,
    pub external: Vec<String>,
    pub display_source: DisplaySource,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum DisplaySource {
    #[default]
    Original,
    Resolved,
}

impl Default for ResolveConfig {
    fn default() -> Self {
        Self {
            stages: vec![],
            parse_files: vec![],
            lookup_from: vec![],
            external: vec![],
            display_source: DisplaySource::Original,
        }
    }
}

#[derive(serde::Deserialize)]
struct RuleFile {
    stages: Vec<Stage>,
    #[serde(default)]
    resolve: Option<ResolveSection>,
    #[serde(default)]
    display: Option<Vec<Stage>>,
}

#[derive(serde::Deserialize)]
struct ResolveSection {
    #[serde(default)]
    parse_files: Vec<ParseFileEntry>,
    #[serde(default)]
    lookup_from: Vec<String>,
    #[serde(default)]
    external: Vec<String>,
    #[serde(default)]
    display_source: Option<String>,
    stages: Vec<ResolveStageSpec>,
}

#[derive(serde::Deserialize)]
struct ParseFileEntry {
    name: String,
    format: String,
}

#[derive(serde::Deserialize)]
struct ResolveStageSpec {
    #[allow(dead_code)]
    name: Option<String>,
    #[serde(default)]
    rules: Option<Vec<Rule>>,
    #[serde(default)]
    climb: Option<ClimbSpec>,
}

#[derive(serde::Deserialize)]
struct ClimbSpec {
    r#while: String,
    mark: String,
}

#[derive(serde::Deserialize)]
struct Stage {
    #[allow(dead_code)]
    name: Option<String>,
    rules: Vec<Rule>,
}

#[derive(serde::Deserialize)]
struct Rule {
    #[serde(rename = "match")]
    pattern: String,
    #[serde(default)]
    replace: Option<String>,
    #[serde(default)]
    append: Option<Vec<String>>,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
}

/// Compile a YAML rule file into stages of rewrites.
pub fn load_rules(yaml: &str, lang: &Lang) -> Vec<Vec<Rewrite>> {
    let file: RuleFile = serde_yaml::from_str(yaml).expect("failed to parse rule YAML");
    file.stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect()
}

pub struct LangConfig {
    pub rewrite_stages: Vec<Vec<Rewrite>>,
    pub resolve: ResolveConfig,
    pub display_rules: Vec<Rewrite>,
}

/// Load both rewrite stages and resolve config from a language YAML file.
pub fn load_lang(yaml: &str, lang: &Lang) -> (Vec<Vec<Rewrite>>, ResolveConfig) {
    let file: RuleFile = serde_yaml::from_str(yaml).expect("failed to parse rule YAML");
    let rewrites = file
        .stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect();
    let resolve = match file.resolve {
        Some(section) => compile_resolve(&section, lang),
        None => ResolveConfig::default(),
    };
    (rewrites, resolve)
}

pub fn load_lang_full(yaml: &str, lang: &Lang) -> LangConfig {
    let file: RuleFile = serde_yaml::from_str(yaml).expect("failed to parse rule YAML");
    let rewrite_stages = file
        .stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect();
    let resolve = match file.resolve {
        Some(section) => compile_resolve(&section, lang),
        None => ResolveConfig::default(),
    };
    let display_rules = file
        .display
        .unwrap_or_default()
        .iter()
        .flat_map(|stage| compile_stage(stage, lang))
        .collect();
    LangConfig {
        rewrite_stages,
        resolve,
        display_rules,
    }
}

fn compile_resolve(section: &ResolveSection, lang: &Lang) -> ResolveConfig {
    let parse_files = section
        .parse_files
        .iter()
        .map(|pf| ParseFileSpec {
            name: pf.name.clone(),
            format: match pf.format.as_str() {
                "json" => ParseFormat::Json,
                "toml" => ParseFormat::Toml,
                other => panic!("unknown parse_files format: {other}"),
            },
        })
        .collect();
    let stages = section
        .stages
        .iter()
        .map(|spec| {
            if let Some(climb) = &spec.climb {
                let while_kind = lang.intern_kind(&climb.r#while);
                let mark_kind = lang.intern_kind(&climb.mark);
                ResolveStage::Climb {
                    while_kind,
                    mark_kind,
                }
            } else if let Some(rules) = &spec.rules {
                let compiled = rules
                    .iter()
                    .flat_map(|rule| compile_rule(rule, lang))
                    .collect();
                ResolveStage::Rules(compiled)
            } else {
                panic!("resolve stage must have either `rules` or `climb`");
            }
        })
        .collect();
    let lookup_from = section
        .lookup_from
        .iter()
        .map(|name| lang.intern_kind(name))
        .collect();
    ResolveConfig {
        stages,
        parse_files,
        lookup_from,
        external: section.external.clone(),
        display_source: match section.display_source.as_deref() {
            Some("resolved") => DisplaySource::Resolved,
            _ => DisplaySource::Original,
        },
    }
}

fn compile_stage(stage: &Stage, lang: &Lang) -> Vec<Rewrite> {
    stage
        .rules
        .iter()
        .flat_map(|rule| compile_rule(rule, lang))
        .collect()
}

fn compile_rule(rule: &Rule, lang: &Lang) -> Vec<Rewrite> {
    let pat = &rule.pattern;

    if let Some(ref tpl) = rule.replace {
        let tpl = tpl.clone();
        let mut rw = Rewrite::new(lang, pat, move |c| Out::Replace(c.template(&tpl)));
        if let Some(ref wc) = rule.where_clause {
            rw.guards = parse_where_clause(wc, &rw.slots);
        }
        return vec![rw];
    }

    if let Some(ref appends) = rule.append {
        let appends = appends.clone();
        let mut rw = Rewrite::new(lang, pat, move |c| {
            Out::Append(appends.iter().map(|tpl| c.template(tpl)).collect())
        });
        if let Some(ref wc) = rule.where_clause {
            rw.guards = parse_where_clause(wc, &rw.slots);
        }
        return vec![rw];
    }

    panic!("rule has no action: {:?}", pat);
}

fn parse_where_clause(
    clause: &str,
    slots: &std::collections::HashMap<Box<str>, u16>,
) -> Vec<(u16, u16, bool)> {
    clause
        .split("&&")
        .map(|part| {
            let part = part.trim();
            let (a, b, eq) = if let Some((l, r)) = part.split_once("==") {
                (l.trim(), r.trim(), true)
            } else if let Some((l, r)) = part.split_once("!=") {
                (l.trim(), r.trim(), false)
            } else {
                panic!("invalid where clause: {part}");
            };
            let sa = slots
                .get(a.trim_start_matches('$'))
                .unwrap_or_else(|| panic!("unknown capture in where: {a}"));
            let sb = slots
                .get(b.trim_start_matches('$'))
                .unwrap_or_else(|| panic!("unknown capture in where: {b}"));
            (*sa, *sb, eq)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_replace_rule() {
        let yaml = r#"
stages:
  - name: test
    rules:
      - match: '(attribute object: (identifier "self") attribute: $A)'
        replace: '(__ivar @$A)'
"#;
        let mut lang = Lang::new();
        let stages = load_rules(yaml, &lang);
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].len(), 1);
    }

    #[test]
    fn load_multi_stage() {
        let yaml = r#"
stages:
  - name: normalize
    rules:
      - match: '(aliased_import name: $N alias: $A)'
        replace: '(aliased_import @$N (__alias @$A))'
      - match: '(attribute object: (identifier "self") attribute: $A)'
        replace: '(__ivar @$A)'
      - match: '(class_definition superclasses: (argument_list $$$SUPERS:identifier|attribute|call) $$$REST)'
        replace: '(class_definition $$$REST $$$SUPERS=>__supertype)'

  - name: retag-refs
    rules:
      - match: '(attribute object: $O attribute: $M)'
        replace: '(__member @$M (__object @$O))'

  - name: classify
    rules:
      - match: '(class_definition name: $N body: $B)'
        replace: '(__def (__defname @$N) (__deftype "Class") (__scope) $B)'
"#;
        let mut lang = Lang::new();
        let stages = load_rules(yaml, &lang);
        assert_eq!(stages.len(), 3);
    }
}
