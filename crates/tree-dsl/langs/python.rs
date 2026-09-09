use crate::lang::Lang;
use crate::run::LangDef;

const PYTHON_RULES_YAML: &str = include_str!("python.yaml");

pub fn lang_def(lang: &mut Lang) -> LangDef {
    let rewrite_stages = crate::rules::load_rules(PYTHON_RULES_YAML, lang);
    LangDef {
        rewrites: rewrite_stages,
        colorings: vec![],
        resolver_spec: None,
    }
}
