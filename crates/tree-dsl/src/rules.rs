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
use crate::pattern::{Out, Rewrite, Tf};

#[derive(serde::Deserialize)]
struct RuleFile {
    stages: Vec<Stage>,
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
    // Exactly one action:
    #[serde(default)]
    replace: Option<String>,
    #[serde(default)]
    append: Option<StringOrList>,
    #[serde(default)]
    append_under: Option<AppendUnder>,
    #[serde(default)]
    set_text: Option<String>,
    #[serde(default)]
    set_kind: Option<String>,
    #[serde(default)]
    retag: Option<RetagSpec>,
    #[serde(default)]
    remove: Option<bool>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum StringOrList {
    Single(String),
    List(Vec<String>),
}

#[derive(serde::Deserialize)]
struct RetagSpec {
    kind: String,
    fields: std::collections::HashMap<String, String>,
}

#[derive(serde::Deserialize)]
struct AppendUnder {
    target: String,
    each: String,
    kind: String,
    #[serde(default)]
    tf: Option<String>,
}

/// Compile a YAML rule file into stages of rewrites.
pub fn load_rules(yaml: &str, lang: &mut Lang) -> Vec<Vec<Rewrite>> {
    let file: RuleFile = serde_yaml::from_str(yaml).expect("failed to parse rule YAML");
    file.stages
        .iter()
        .map(|stage| compile_stage(stage, lang))
        .collect()
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

    if rule.remove == Some(true) {
        return vec![Rewrite::new(lang, pat, |_| Out::Remove)];
    }

    if let Some(ref text) = rule.set_text {
        let slot_name = text.trim_start_matches('$').to_string();
        return vec![Rewrite::new(lang, pat, move |c| Out::SetText {
            target: 0,
            from: c.slot(&slot_name),
            tf: Tf::Id,
        })];
    }

    if let Some(ref kind_str) = rule.set_kind {
        let k = kind_str.clone();
        return vec![Rewrite::new(lang, pat, move |c| Out::SetKind(c.kind(&k)))];
    }

    if let Some(ref tpl) = rule.replace {
        let tpl = tpl.clone();
        return vec![Rewrite::new(lang, pat, move |c| {
            Out::Replace(c.template(&tpl))
        })];
    }

    if let Some(ref spec) = rule.retag {
        let kind_str = spec.kind.clone();
        let field_pairs: Vec<(String, String)> = spec
            .fields
            .iter()
            .map(|(slot, field)| (slot.clone(), field.clone()))
            .collect();
        return vec![Rewrite::new(lang, pat, move |c| {
            let kind = c.kind(&kind_str);
            let fields = field_pairs
                .iter()
                .map(|(s, f)| (c.slot(s), c.field(f)))
                .collect();
            Out::Retag { kind, fields }
        })];
    }

    if let Some(ref au) = rule.append_under {
        let target = au.target.clone();
        let each = au.each.clone();
        let kind = au.kind.clone();
        let tf_spec = au.tf.clone();
        return vec![Rewrite::new(lang, pat, move |c| {
            let tf = match tf_spec.as_deref() {
                None | Some("id") => Tf::Id,
                Some(s) if s.starts_with("strip=") => Tf::Strip(s[6..].into()),
                Some(s) if s.starts_with("field=") => Tf::Field(c.field(&s[6..])),
                Some(s) => panic!("unknown tf: {s}"),
            };
            Out::Append {
                under: c.slot(&target),
                each: c.slot(&each),
                kind: c.kind(&kind),
                tf,
            }
        })];
    }

    if let Some(ref append) = rule.append {
        let specs: Vec<String> = match append {
            StringOrList::Single(s) => parse_append_specs(s),
            StringOrList::List(v) => v.clone(),
        };
        let mut rewrites = Vec::new();
        for spec in specs {
            let parsed = parse_append_node(spec.trim());
            let pat = pat.clone();
            match parsed {
                AppendNode::Literal { kind, sym } => {
                    // Leak the string to get a &'static str for Tf::Const.
                    // These are compiled once at startup, so the leak is bounded.
                    let sym_static: &'static str = Box::leak(sym.into_boxed_str());
                    rewrites.push(Rewrite::new(lang, &pat, move |c| Out::Append {
                        under: 0,
                        each: 0,
                        kind: c.kind(&kind),
                        tf: Tf::Const(sym_static),
                    }));
                }
                AppendNode::FromCapture { kind, capture, tf } => {
                    rewrites.push(Rewrite::new(lang, &pat, move |c| Out::Append {
                        under: 0,
                        each: c.slot(&capture),
                        kind: c.kind(&kind),
                        tf: tf.clone(),
                    }));
                }
                AppendNode::Bare { kind } => {
                    rewrites.push(Rewrite::new(lang, &pat, move |c| Out::Append {
                        under: 0,
                        each: 0,
                        kind: c.kind(&kind),
                        tf: Tf::Const(""),
                    }));
                }
            }
        }
        return rewrites;
    }

    panic!("rule has no action: {:?}", pat);
}

// ── Tf parsing ──

fn parse_tf_spec(spec: Option<&str>) -> Tf {
    match spec {
        None | Some("id") => Tf::Id,
        Some(s) if s.starts_with("strip=") => Tf::Strip(s[6..].into()),
        Some(s) if s.starts_with("field=") => {
            panic!("field tf needs lang context, use append_under with explicit field")
        }
        Some(s) => panic!("unknown tf: {s}"),
    }
}

// ── Append parsing ──

enum AppendNode {
    Literal {
        kind: String,
        sym: String,
    },
    FromCapture {
        kind: String,
        capture: String,
        tf: Tf,
    },
    Bare {
        kind: String,
    },
}

/// Split `(__deftype "Class") (__scope)` into individual s-expression specs.
fn parse_append_specs(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => {
                if depth == 0 {
                    start = i;
                }
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    result.push(s[start..=i].to_string());
                }
            }
            _ => {}
        }
    }
    if result.is_empty() && !s.trim().is_empty() {
        result.push(s.trim().to_string());
    }
    result
}

/// Parse `(__deftype "Class")` or `(__alias @$A)` or `(__scope)` or `(__decorator @$D|strip=@)`.
fn parse_append_node(s: &str) -> AppendNode {
    let inner = s
        .strip_prefix('(')
        .unwrap_or(s)
        .strip_suffix(')')
        .unwrap_or(s)
        .trim();
    let (kind, rest) = match inner.split_once(' ') {
        Some((k, r)) => (k.to_string(), Some(r.trim())),
        None => (inner.to_string(), None),
    };

    let Some(val) = rest else {
        return AppendNode::Bare { kind };
    };

    // Quoted literal: (__deftype "Class")
    if val.starts_with('"') && val.ends_with('"') {
        return AppendNode::Literal {
            kind,
            sym: val[1..val.len() - 1].to_string(),
        };
    }

    // Capture reference: (__alias @$A) or (__decorator @$D|strip=@)
    if val.starts_with("@$") {
        let rest = &val[2..];
        let (capture, tf) = if let Some((cap, tf_str)) = rest.split_once('|') {
            (cap.to_string(), parse_tf_spec(Some(tf_str)))
        } else {
            (rest.to_string(), Tf::Id)
        };
        return AppendNode::FromCapture { kind, capture, tf };
    }

    // Bare literal without quotes
    AppendNode::Literal {
        kind,
        sym: val.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_specs_single() {
        let specs = parse_append_specs(r#"(__deftype "Class")"#);
        assert_eq!(specs, vec![r#"(__deftype "Class")"#]);
    }

    #[test]
    fn parse_specs_multiple() {
        let specs = parse_append_specs(r#"(__deftype "Class") (__scope)"#);
        assert_eq!(specs, vec![r#"(__deftype "Class")"#, "(__scope)"]);
    }

    #[test]
    fn load_simple_rules() {
        let yaml = r#"
stages:
  - name: test
    rules:
      - match: '(identifier "self")'
        remove: true
      - match: '(class_definition)'
        append: '(__deftype "Class") (__scope)'
"#;
        let mut lang = Lang::new();
        let stages = load_rules(yaml, &mut lang);
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].len(), 3); // remove + 2 appends
    }

    #[test]
    fn load_retag_rule() {
        let yaml = r#"
stages:
  - name: retag
    rules:
      - match: '(attribute object: $O attribute: $M)'
        retag:
          kind: __member
          fields: { O: object, M: member }
"#;
        let mut lang = Lang::new();
        let stages = load_rules(yaml, &mut lang);
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].len(), 1);
    }

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
        let stages = load_rules(yaml, &mut lang);
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].len(), 1);
    }

    #[test]
    fn load_full_python_stages() {
        let yaml = r#"
stages:
  - name: normalize
    rules:
      - match: '(aliased_import name: $N)'
        set_text: '$N'
      - match: '(aliased_import alias: $A)'
        append: '(__alias @$A)'
      - match: '(attribute object: (identifier "self") attribute: $A)'
        replace: '(__ivar @$A)'
      - match: '(attribute object: (identifier "cls") attribute: $A)'
        replace: '(__ivar @$A)'
      - match: '(class_definition superclasses: (argument_list $$$SUPERS:identifier|attribute|call))'
        append_under:
          target: ROOT
          each: SUPERS
          kind: __supertype
          tf: 'field=function'

  - name: retag
    rules:
      - match: '(attribute object: $O attribute: $M)'
        retag:
          kind: __member
          fields: { O: object, M: member }
      - match: '(call function: $F arguments: $A)'
        retag:
          kind: __call
          fields: { F: callee, A: args }

  - name: classify
    rules:
      - match: '(class_definition)'
        append: '(__deftype "Class") (__scope)'
      - match: '(function_definition)'
        append: '(__deftype "Function") (__scope)'
"#;
        let mut lang = Lang::new();
        let stages = load_rules(yaml, &mut lang);
        assert_eq!(stages.len(), 3);
    }
}
