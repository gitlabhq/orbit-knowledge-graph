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

use crate::error::LoadError;
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
    /// Same-named class defs in one package are parts of one type, keyed by
    /// generic arity. True for C#, where a non-partial duplicate cannot compile.
    pub merge_same_named_types: bool,
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
    #[serde(default)]
    merge_same_named_types: bool,
}

fn default_true() -> bool {
    true
}

fn compile_config(section: Option<&ConfigSection>, lang: &Lang) -> Result<Config, LoadError> {
    let Some(section) = section else {
        return Ok(Config::default());
    };
    let link = section
        .link
        .as_ref()
        .map_or_else(LinkConfig::default, |l| LinkConfig {
            builtins: l.builtins.iter().map(|b| lang.syms.intern(b)).collect(),
            imports_shadow_locals: l.imports_shadow_locals,
        });
    let Some(r) = section.resolve.as_ref() else {
        return Ok(Config {
            link,
            resolve: ResolveConfig::default(),
        });
    };
    let parse_file = |pf: &ParseFileEntry| -> Result<ParseFileSpec, LoadError> {
        let format = match pf.format.as_str() {
            "json" => ParseFormat::Json,
            "toml" => ParseFormat::Toml,
            "raw" => {
                let pattern = pf.extract.as_deref().ok_or_else(|| {
                    LoadError(format!("parse_files {}: raw format needs extract", pf.name))
                })?;
                ParseFormat::Raw(regex::Regex::new(pattern)?)
            }
            other => {
                return Err(LoadError(format!(
                    "parse_files {}: unknown format {other}",
                    pf.name
                )));
            }
        };
        Ok(ParseFileSpec {
            name: pf.name.clone(),
            format,
        })
    };
    let resolve = ResolveConfig {
        external: r.external.clone(),
        lookup_from: r
            .lookup_from
            .iter()
            .map(|name| lang.intern_kind(name))
            .collect(),
        parse_files: r
            .parse_files
            .iter()
            .map(parse_file)
            .collect::<Result<_, _>>()?,
        merge_same_named_types: r.merge_same_named_types,
    };
    Ok(Config { link, resolve })
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

fn unique_guard(lang: &Lang, spec: &str) -> Result<(Pat, u16, usize), LoadError> {
    let pattern = spec.starts_with('(');
    let kind = lang.intern_kind(if pattern { "__defname" } else { spec });
    let mut ctx = Ctx::new(lang);
    ctx.slot("ROOT")?;
    let pat = parse(&mut ctx, if pattern { spec } else { "(__def)" })?;
    Ok((pat, kind, ctx.slots.len()))
}

fn read(yaml: &str) -> Result<RuleFile, LoadError> {
    Ok(orbit_utils::yaml::from_str(yaml)?)
}

fn compile_stages(stages: &[Stage], lang: &Lang) -> Result<Vec<Vec<Rewrite>>, LoadError> {
    stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect()
}

/// Compile a YAML rule file into stages of rewrites.
pub fn load_rules(yaml: &str, lang: &Lang) -> Result<Vec<Vec<Rewrite>>, LoadError> {
    compile_stages(&read(yaml)?.stages, lang)
}

pub struct LangConfig {
    pub rewrite_stages: Vec<Vec<Rewrite>>,
    pub resolve_stages: Vec<ResolveStage>,
    pub config: Config,
    pub display_rules: Vec<Rewrite>,
}

/// Load rewrite stages, resolve stages, and whole-language config from a language YAML file.
pub fn load_lang(
    yaml: &str,
    lang: &Lang,
) -> Result<(Vec<Vec<Rewrite>>, Vec<ResolveStage>, Config), LoadError> {
    let file = read(yaml)?;
    let rewrites = compile_stages(&file.stages, lang)?;
    let resolve = match &file.resolve {
        Some(section) => compile_resolve(section, lang)?,
        None => vec![],
    };
    Ok((
        rewrites,
        resolve,
        compile_config(file.config.as_ref(), lang)?,
    ))
}

pub fn load_lang_full(yaml: &str, lang: &Lang) -> Result<LangConfig, LoadError> {
    let file = read(yaml)?;
    let rewrite_stages = compile_stages(&file.stages, lang)?;
    let resolve_stages = match &file.resolve {
        Some(section) => compile_resolve(section, lang)?,
        None => vec![],
    };
    let display_rules = compile_stages(file.display.as_deref().unwrap_or_default(), lang)?
        .into_iter()
        .flatten()
        .collect();
    Ok(LangConfig {
        rewrite_stages,
        resolve_stages,
        config: compile_config(file.config.as_ref(), lang)?,
        display_rules,
    })
}

fn compile_resolve(section: &ResolveSection, lang: &Lang) -> Result<Vec<ResolveStage>, LoadError> {
    section
        .stages
        .iter()
        .map(|spec| {
            if let Some(climb) = &spec.climb {
                Ok(ResolveStage::Climb {
                    while_kind: lang.intern_kind(&climb.r#while),
                    mark_kind: lang.intern_kind(&climb.mark),
                })
            } else if let Some(rules) = &spec.rules {
                let rules = rules
                    .iter()
                    .map(|rule| compile_rule(rule, lang))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ResolveStage::Rules(rules))
            } else {
                Err(LoadError(format!(
                    "resolve stage {:?} needs rules or climb",
                    spec.name
                )))
            }
        })
        .collect()
}

fn compile_stage(stage: &Stage, lang: &Lang) -> Result<Vec<Rewrite>, LoadError> {
    stage
        .rules
        .iter()
        .map(|rule| compile_rule(rule, lang))
        .collect()
}

fn compile_tags(
    tag_map: &HashMap<String, String>,
    ctx: &mut crate::pattern::Ctx,
) -> Result<Vec<TagEntry>, LoadError> {
    tag_map
        .iter()
        .map(|(k, v)| {
            let key = ctx.lang.syms.intern(k);
            let (slot, val) = compile_tag_value(v, ctx)?;
            Ok(TagEntry { key, slot, val })
        })
        .collect()
}

fn compile_rule(rule: &Rule, lang: &Lang) -> Result<Rewrite, LoadError> {
    let pat = &rule.pattern;
    let mut rw = if let Some(ref tpl) = rule.replace {
        let tpl = tpl.clone();
        let tags = rule.tag.clone();
        let tag_on = rule.tag_on.as_deref().map(|k| lang.intern_kind(k));
        let mut rw = Rewrite::new(lang, pat, move |c| {
            let replace = c.template(&tpl)?;
            let tag_entries = tags.as_ref().map(|t| compile_tags(t, c)).transpose()?;
            Ok(Out::Replace(replace, tag_entries, tag_on))
        })?;
        rw.unique = rule
            .unique
            .as_deref()
            .map(|u| unique_guard(lang, u))
            .transpose()?;
        rw
    } else if let Some(ref appends) = rule.append {
        let appends = appends.clone();
        Rewrite::new(lang, pat, move |c| {
            let pats = appends
                .iter()
                .map(|tpl| c.template(tpl))
                .collect::<Result<_, _>>()?;
            Ok(Out::Append(pats))
        })?
    } else if let Some(ref tag_map) = rule.tag {
        let tag_map = tag_map.clone();
        let tag_on = rule.tag_on.as_deref().map(|k| lang.intern_kind(k));
        Rewrite::new(lang, pat, move |c| {
            Ok(Out::Tag(compile_tags(&tag_map, c)?, tag_on))
        })?
    } else {
        return Err(LoadError(format!(
            "rule {pat:?} has no replace, append, or tag"
        )));
    };
    if let Some(ref wc) = rule.where_clause {
        rw.guards = parse_where_clause(wc, &rw.slots)?;
    }
    Ok(rw)
}

fn compile_tag_value(val: &str, ctx: &mut crate::pattern::Ctx) -> Result<(u16, Tf), LoadError> {
    let Some(rest) = val.strip_prefix("@$") else {
        return Ok((0, Tf::LitSym(ctx.lang.syms.intern(val))));
    };
    let (slot_name, pipeline) = match rest.find('|') {
        Some(i) => (&rest[..i], Some(&rest[i + 1..])),
        None => (rest, None),
    };
    let slot = ctx.slot(slot_name)?;
    Ok(match pipeline {
        Some(pipe) => (slot, crate::dsl::parser::parse_pipeline(ctx, pipe)?),
        None => (slot, Tf::Id),
    })
}

fn parse_where_clause(
    clause: &str,
    slots: &std::collections::HashMap<Box<str>, u16>,
) -> Result<Vec<(u16, u16, bool)>, LoadError> {
    let slot = |name: &str| {
        slots
            .get(name.trim_start_matches('$'))
            .copied()
            .ok_or_else(|| LoadError(format!("where clause names unknown capture {name}")))
    };
    clause
        .split("&&")
        .map(|part| {
            let part = part.trim();
            let (a, b, eq) = if let Some((l, r)) = part.split_once("==") {
                (l.trim(), r.trim(), true)
            } else if let Some((l, r)) = part.split_once("!=") {
                (l.trim(), r.trim(), false)
            } else {
                return Err(LoadError(format!("where clause {part:?} needs == or !=")));
            };
            Ok((slot(a)?, slot(b)?, eq))
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
        let stages = load_rules(yaml, &lang).unwrap();
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
        let stages = load_rules(yaml, &lang).unwrap();
        assert_eq!(stages.len(), 3);
    }
}

#[cfg(test)]
mod load_tests {
    use super::*;
    use crate::treesitter::SupportLang;

    /// Every compiled-in rule file goes through the same path a user file
    /// would, so a broken one fails here rather than at index time.
    #[test]
    fn every_embedded_rule_file_compiles() {
        for (lang_id, _) in crate::treesitter::all_languages() {
            let Some(yaml) = crate::treesitter::lang_yaml(lang_id) else {
                continue;
            };
            let lang = Lang::new();
            load_lang_full(yaml, &lang).unwrap_or_else(|e| panic!("{lang_id:?}: {e}"));
            let _ = lang_id.ts_language();
        }
        let _ = SupportLang::from_extension("py");
    }

    #[test]
    fn malformed_rule_files_are_errors_not_panics() {
        let lang = Lang::new();
        let bad = |yaml: &str| match load_lang(yaml, &lang) {
            Err(e) => e.0,
            Ok(_) => panic!("{yaml:?} loaded"),
        };
        assert!(bad("stages: [").contains("yaml"));
        assert!(
            bad("stages:\n  - rules:\n      - match: '(a'\n        tag: {x: y}")
                .contains("pattern")
        );
        assert!(
            bad("stages:\n  - rules:\n      - match: '(a)'\n        replace: '(b @$Z)'")
                .contains("unknown capture $Z")
        );
        assert!(
            bad("stages:\n  - rules:\n      - match: '(a)'\n        replace: '(b @$ROOT|nope)'")
                .contains("unknown transform")
        );
        assert!(bad("stages:\n  - rules:\n      - match: '(a)'\n        replace: '(b @$ROOT|regex_replace(\"(\", \"\"))'").contains("regex"));
        assert!(
            bad("stages:\n  - rules:\n      - match: '(a)'").contains("no replace, append, or tag")
        );
        assert!(bad("stages:\n  - rules:\n      - match: '(a $X)'\n        tag: {k: v}\n        where: '$X == $Q'").contains("unknown capture $Q"));
    }
}
