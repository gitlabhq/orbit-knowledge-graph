use rustc_hash::FxHashMap;

use crate::constants::PATH_SEP;
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
