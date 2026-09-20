use rustc_hash::FxHashMap;

use crate::constants::{PATH_SEP, RELATIVE_SELF, RELATIVE_PARENT, RELATIVE_DOTDOT, RELATIVE_DOT};
use crate::treesitter::SupportLang;

pub fn build_file_index(
    labels: &[String],
    support_lang: SupportLang,
    index_names: &[String],
) -> FxHashMap<String, usize> {
    let mut idx: FxHashMap<String, usize> =
        FxHashMap::with_capacity_and_hasher(labels.len() * 3, Default::default());
    for (fi, path) in labels.iter().enumerate() {
        let file_lang = SupportLang::from_path(path).unwrap_or(support_lang);
        let stem = file_lang.strip_extension(path);
        idx.insert(path.clone(), fi);
        idx.insert(stem.to_string(), fi);
        for name in index_names {
            let suffix = format!("{PATH_SEP}{name}");
            if stem.ends_with(&suffix) {
                let pkg = &stem[..stem.len() - suffix.len()];
                if !pkg.is_empty() {
                    idx.insert(pkg.to_string(), fi);
                }
            } else if stem == name.as_str() {
                idx.insert(String::new(), fi);
            }
        }
    }
    idx
}

pub fn is_index_file(path: &str, support_lang: SupportLang, index_names: &[String]) -> bool {
    let file_lang = SupportLang::from_path(path).unwrap_or(support_lang);
    let stem = file_lang.strip_extension(path);
    index_names
        .iter()
        .any(|idx| stem.ends_with(&format!("{PATH_SEP}{idx}")) || stem == idx.as_str())
}

pub fn resolve_import_source(source_str: &str, current_file: &str) -> String {
    if source_str.starts_with(RELATIVE_SELF)
        || source_str.starts_with(RELATIVE_PARENT)
    {
        resolve_relative(current_file, source_str)
    } else {
        source_str.to_string()
    }
}

pub fn is_external(source_str: &str, external: &[String]) -> bool {
    external
        .iter()
        .any(|e| e == source_str.split(PATH_SEP).next().unwrap_or(source_str))
}

pub fn join(base: &str, name: &str) -> String {
    format!("{base}{PATH_SEP}{name}")
}

pub fn resolve_submodule(
    target_path: &str,
    name: &str,
    support_lang: SupportLang,
    index_names: &[String],
    file_index: &FxHashMap<String, usize>,
) -> Option<usize> {
    let stem = support_lang.strip_extension(target_path);
    let dir = index_names
        .iter()
        .find_map(|idx| stem.strip_suffix(&format!("{PATH_SEP}{idx}")))?;
    file_index.get(&format!("{dir}{PATH_SEP}{name}")).copied()
}

pub fn resolve_path(
    target: &str,
    file_index: &FxHashMap<String, usize>,
    prefixes: &[String],
) -> Option<usize> {
    file_index.get(target).copied().or_else(|| {
        prefixes.iter().find_map(|p| {
            let c = if p.is_empty() {
                target.to_string()
            } else {
                format!("{p}{PATH_SEP}{target}")
            };
            file_index.get(&c).copied()
        })
    })
}

pub fn resolve_relative(current_file: &str, source: &str) -> String {
    let dir = current_file
        .rsplit_once(PATH_SEP)
        .map(|(d, _)| d)
        .unwrap_or("");
    let mut parts: Vec<&str> = if dir.is_empty() {
        Vec::new()
    } else {
        dir.split(PATH_SEP).collect()
    };
    let mut rest = source;
    loop {
        if let Some(r) = rest.strip_prefix(RELATIVE_PARENT) {
            parts.pop();
            rest = r;
        } else if let Some(r) = rest.strip_prefix(RELATIVE_SELF) {
            rest = r;
        } else {
            break;
        }
    }
    if rest == RELATIVE_DOTDOT {
        parts.pop();
        rest = "";
    } else if rest == RELATIVE_DOT {
        rest = "";
    }
    if rest.is_empty() {
        parts.join(PATH_SEP)
    } else if parts.is_empty() {
        rest.to_string()
    } else {
        format!("{}{PATH_SEP}{rest}", parts.join(PATH_SEP))
    }
}
