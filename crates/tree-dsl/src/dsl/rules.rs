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

use std::collections::HashMap;

use crate::intern::Lang;
use crate::pattern::{Out, Rewrite, TagEntry, Tf};

use super::parser::parse;
use super::types::{Ctx, Pat};

pub enum ResolveStage {
    Rules(Vec<Rewrite>),
    Climb { while_kind: u16, mark_kind: u16 },
}

#[derive(Clone)]
pub struct ParseFileSpec {
    pub name: String,
    pub format: ParseFormat,
}

#[derive(Clone)]
pub enum ParseFormat {
    Json,
    Toml,
    Raw(regex::Regex),
}

/// Whole-language settings from the YAML `config:` section, grouped by the
/// phase that reads them. Settings read by more than one phase go in `global`.
#[derive(Default)]
pub struct Config {
    pub link: LinkConfig,
    pub resolve: ResolveConfig,
}

pub struct LinkConfig {
    /// Names the language defines everywhere without an import. An unresolved
    /// call to one does not fall back to wildcard imports.
    pub builtins: rustc_hash::FxHashSet<u32>,
    /// Whether an import may rebind a name already defined in the same scope.
    /// Ruby autoloads must not; a local class always wins.
    pub imports_shadow_locals: bool,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            builtins: Default::default(),
            imports_shadow_locals: true,
        }
    }
}

#[derive(Default)]
pub struct ResolveConfig {
    /// Module roots that never resolve to project files (a stdlib list).
    pub external: Vec<String>,
    /// Directory-tree marker kinds that import paths are looked up from.
    pub lookup_from: Vec<u16>,
    /// Manifest files parsed into the directory tree before resolve stages run.
    pub parse_files: Vec<ParseFileSpec>,
}

#[derive(serde::Deserialize, Default)]
struct ConfigSection {
    #[serde(default)]
    link: Option<LinkSection>,
    #[serde(default)]
    resolve: Option<ResolveSettingsSection>,
}

#[derive(serde::Deserialize)]
struct LinkSection {
    #[serde(default)]
    builtins: Vec<String>,
    #[serde(default = "default_true")]
    imports_shadow_locals: bool,
}

#[derive(serde::Deserialize)]
struct ResolveSettingsSection {
    #[serde(default)]
    external: Vec<String>,
    #[serde(default)]
    lookup_from: Vec<String>,
    #[serde(default)]
    parse_files: Vec<ParseFileEntry>,
}

fn default_true() -> bool {
    true
}

fn compile_config(section: Option<&ConfigSection>, lang: &Lang) -> Config {
    let Some(section) = section else {
        return Config::default();
    };
    let link = section
        .link
        .as_ref()
        .map_or_else(LinkConfig::default, |l| LinkConfig {
            builtins: l.builtins.iter().map(|b| lang.syms.intern(b)).collect(),
            imports_shadow_locals: l.imports_shadow_locals,
        });
    let resolve = section
        .resolve
        .as_ref()
        .map_or_else(ResolveConfig::default, |r| ResolveConfig {
            external: r.external.clone(),
            lookup_from: r
                .lookup_from
                .iter()
                .map(|name| lang.intern_kind(name))
                .collect(),
            parse_files: r
                .parse_files
                .iter()
                .map(|pf| ParseFileSpec {
                    name: pf.name.clone(),
                    format: match pf.format.as_str() {
                        "json" => ParseFormat::Json,
                        "toml" => ParseFormat::Toml,
                        "raw" => {
                            let pattern = pf
                                .extract
                                .as_deref()
                                .expect("raw format requires extract pattern");
                            ParseFormat::Raw(
                                regex::Regex::new(pattern).expect("invalid extract regex"),
                            )
                        }
                        other => panic!("unknown parse_files format: {other}"),
                    },
                })
                .collect(),
        });
    Config { link, resolve }
}

#[derive(serde::Deserialize)]
struct RuleFile {
    #[serde(default)]
    config: Option<ConfigSection>,
    stages: Vec<Stage>,
    #[serde(default)]
    resolve: Option<ResolveSection>,
    #[serde(default)]
    display: Option<Vec<Stage>>,
}

#[derive(serde::Deserialize)]
struct ResolveSection {
    stages: Vec<ResolveStageSpec>,
}

#[derive(serde::Deserialize)]
struct ParseFileEntry {
    name: String,
    format: String,
    #[serde(default)]
    extract: Option<String>,
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
    #[serde(default)]
    tag: Option<HashMap<String, String>>,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
    #[serde(default)]
    unique: Option<String>,
    #[serde(default)]
    tag_on: Option<String>,
}

fn unique_guard(lang: &Lang, spec: &str) -> (Pat, u16, usize) {
    let pattern = spec.starts_with('(');
    let kind = lang.intern_kind(if pattern { "__defname" } else { spec });
    let mut ctx = Ctx::new(lang);
    ctx.slot("ROOT");
    (
        parse(&mut ctx, if pattern { spec } else { "(__def)" }),
        kind,
        ctx.slots.len(),
    )
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
    pub resolve_stages: Vec<ResolveStage>,
    pub config: Config,
    pub display_rules: Vec<Rewrite>,
}

/// Load rewrite stages, resolve stages, and whole-language config from a language YAML file.
pub fn load_lang(yaml: &str, lang: &Lang) -> (Vec<Vec<Rewrite>>, Vec<ResolveStage>, Config) {
    let file: RuleFile = serde_yaml::from_str(yaml).expect("failed to parse rule YAML");
    let rewrites = file
        .stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect();
    let resolve = file
        .resolve
        .as_ref()
        .map_or_else(Vec::new, |section| compile_resolve(section, lang));
    (
        rewrites,
        resolve,
        compile_config(file.config.as_ref(), lang),
    )
}

pub fn load_lang_full(yaml: &str, lang: &Lang) -> LangConfig {
    let file: RuleFile = serde_yaml::from_str(yaml).expect("failed to parse rule YAML");
    let rewrite_stages = file
        .stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect();
    let resolve_stages = file
        .resolve
        .as_ref()
        .map_or_else(Vec::new, |section| compile_resolve(section, lang));
    let display_rules = file
        .display
        .unwrap_or_default()
        .iter()
        .flat_map(|stage| compile_stage(stage, lang))
        .collect();
    LangConfig {
        rewrite_stages,
        resolve_stages,
        config: compile_config(file.config.as_ref(), lang),
        display_rules,
    }
}

fn compile_resolve(section: &ResolveSection, lang: &Lang) -> Vec<ResolveStage> {
    section
        .stages
        .iter()
        .map(|spec| {
            if let Some(climb) = &spec.climb {
                ResolveStage::Climb {
                    while_kind: lang.intern_kind(&climb.r#while),
                    mark_kind: lang.intern_kind(&climb.mark),
                }
            } else if let Some(rules) = &spec.rules {
                ResolveStage::Rules(
                    rules
                        .iter()
                        .flat_map(|rule| compile_rule(rule, lang))
                        .collect(),
                )
            } else {
                panic!("resolve stage must have either `rules` or `climb`");
            }
        })
        .collect()
}

fn compile_stage(stage: &Stage, lang: &Lang) -> Vec<Rewrite> {
    stage
        .rules
        .iter()
        .flat_map(|rule| compile_rule(rule, lang))
        .collect()
}

fn compile_tags(tag_map: &HashMap<String, String>, ctx: &mut crate::pattern::Ctx) -> Vec<TagEntry> {
    tag_map
        .iter()
        .map(|(k, v)| {
            let key = ctx.lang.syms.intern(k);
            let (slot, val) = compile_tag_value(v, ctx);
            TagEntry { key, slot, val }
        })
        .collect()
}

fn compile_rule(rule: &Rule, lang: &Lang) -> Vec<Rewrite> {
    let pat = &rule.pattern;

    if let Some(ref tpl) = rule.replace {
        let tpl = tpl.clone();
        let tags = rule.tag.clone();
        let tag_on = rule.tag_on.as_deref().map(|k| lang.intern_kind(k));
        let mut rw = Rewrite::new(lang, pat, move |c| {
            let replace = c.template(&tpl);
            let tag_entries = tags.as_ref().map(|t| compile_tags(t, c));
            Out::Replace(replace, tag_entries, tag_on)
        });
        if let Some(ref wc) = rule.where_clause {
            rw.guards = parse_where_clause(wc, &rw.slots);
        }
        rw.unique = rule.unique.as_deref().map(|u| unique_guard(lang, u));
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

    if let Some(ref tag_map) = rule.tag {
        let tag_map = tag_map.clone();
        let mut rw = Rewrite::new(lang, pat, move |c| Out::Tag(compile_tags(&tag_map, c)));
        if let Some(ref wc) = rule.where_clause {
            rw.guards = parse_where_clause(wc, &rw.slots);
        }
        return vec![rw];
    }

    panic!("rule has no action: {:?}", pat);
}

fn compile_tag_value(val: &str, ctx: &mut crate::pattern::Ctx) -> (u16, Tf) {
    if let Some(rest) = val.strip_prefix("@$") {
        let (slot_name, pipeline) = match rest.find('|') {
            Some(i) => (&rest[..i], Some(&rest[i + 1..])),
            None => (rest, None),
        };
        let slot = ctx.slot(slot_name);
        match pipeline {
            Some(pipe) => (slot, crate::dsl::parser::parse_pipeline(ctx, pipe)),
            None => (slot, Tf::Id),
        }
    } else {
        (0, Tf::LitSym(ctx.lang.syms.intern(val)))
    }
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
        let lang = Lang::new();
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
        let lang = Lang::new();
        let stages = load_rules(yaml, &lang);
        assert_eq!(stages.len(), 3);
    }
}
