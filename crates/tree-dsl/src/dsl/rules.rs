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

use crate::lang::Lang;
use crate::pattern::Rewrite;

#[derive(serde::Deserialize)]
struct RuleFile {
    stages: Vec<Stage>,
    #[serde(default)]
    resolve: Option<ResolveSection>,
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
    #[serde(rename = "name")]
    _name: Option<String>,
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
    #[serde(rename = "name")]
    _name: Option<String>,
    rules: Vec<Rule>,
}

#[derive(serde::Deserialize)]
struct Rule {
    #[serde(rename = "match")]
    pattern: String,
    #[serde(default)]
    replace: Option<String>,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
}

/// Load both rewrite stages and resolve config from a language YAML file.
pub fn load_lang(
    yaml: &str,
    lang: &mut Lang,
) -> (Vec<Vec<Rewrite>>, crate::file_tree::ResolveConfig) {
    let file: RuleFile = serde_yaml::from_str(yaml).expect("failed to parse rule YAML");
    let rewrites = file
        .stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect();
    let resolve = match file.resolve {
        Some(section) => compile_resolve(&section, lang),
        None => crate::file_tree::ResolveConfig::default(),
    };
    (rewrites, resolve)
}

fn compile_resolve(section: &ResolveSection, lang: &mut Lang) -> crate::file_tree::ResolveConfig {
    use crate::file_tree::{ParseFileSpec, ParseFormat, ResolveStage};

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
    crate::file_tree::ResolveConfig {
        stages,
        parse_files,
        lookup_from,
        external: section.external.clone(),
        display_source: match section.display_source.as_deref() {
            Some("resolved") => crate::file_tree::DisplaySource::Resolved,
            _ => crate::file_tree::DisplaySource::Original,
        },
    }
}

fn compile_stage(stage: &Stage, lang: &mut Lang) -> Vec<Rewrite> {
    stage
        .rules
        .iter()
        .flat_map(|rule| compile_rule(rule, lang))
        .collect()
}

fn compile_rule(rule: &Rule, lang: &mut Lang) -> Vec<Rewrite> {
    let pat = &rule.pattern;

    if let Some(ref tpl) = rule.replace {
        return vec![Rewrite::compile(
            lang,
            pat,
            tpl,
            rule.where_clause.as_deref(),
        )];
    }

    panic!("rule has no action: {:?}", pat);
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
        let stages = load_lang(yaml, &mut lang).0;
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
        let stages = load_lang(yaml, &mut lang).0;
        assert_eq!(stages.len(), 3);
    }
}
