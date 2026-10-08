use std::collections::HashMap;

use super::scan::Hit;
use super::term::Term;

const VARIANT_MIN: usize = 3;
const PREFERRED_VARIANTS: &[&str] = &["en", "en-GB", "en-US", "en_US", "en_GB", "default"];

/// Doc comments and attributes put a definition's name a few lines below its first line.
const DECLARATION_LINES: usize = 10;

fn is_code(path: &str) -> bool {
    path.rsplit_once('.').is_some_and(|(_, ext)| {
        orbit_search::corpus::DEFAULT_SOURCE_EXTS.contains(&ext.to_ascii_lowercase().as_str())
    })
}

pub(super) fn is_test(path: &str) -> bool {
    static TEST: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(^|/)(tests?|spec|__tests__|fixtures?|scenarios|testdata|mocks?)/|(^|/)[\w-]*tests?[\w-]*/|(^|/)test_[^/]*$|_test\.|\.test\.|\.spec\.|-test\.",
        )
        .expect("valid test path regex")
    });
    TEST.is_match(path)
}

pub(super) fn names(hit: &Hit, alternatives: &[Term]) -> bool {
    hit.def.as_ref().is_some_and(|def| {
        (def.start..=def.start + DECLARATION_LINES).contains(&hit.line)
            && alternatives.iter().any(|a| a.names(&def.name))
    })
}

pub(super) fn assignment(raw: &str) -> Option<regex::Regex> {
    regex::Regex::new(&format!(
        r"(?i)^\s*(?:(?:const|let|var)\s+)?(?:[\w$]+\.)*{}\s*=[^=]",
        regex::escape(raw.trim())
    ))
    .ok()
}

fn assigns(hit: &Hit, alternatives: &[Term]) -> bool {
    alternatives
        .iter()
        .filter_map(|t| t.assign.as_ref())
        .any(|re| re.is_match(&hit.text))
}

fn defining_rank(hit: &Hit, alternatives: &[Term]) -> u8 {
    match (names(hit, alternatives), assigns(hit, alternatives)) {
        (true, _) => 0,
        (false, true) => 1,
        (false, false) => 2,
    }
}

fn collapse_variants(files: Vec<(String, Vec<&Hit>)>) -> (Vec<(String, Vec<&Hit>)>, usize, usize) {
    let mut templates: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, (file, _)) in files.iter().enumerate() {
        let parts: Vec<&str> = file.split('/').collect();
        for slot in 0..parts.len().saturating_sub(1) {
            let mut key = parts.clone();
            key[slot] = "*";
            templates.entry(key.join("/")).or_default().push(index);
        }
    }
    let mut groups: Vec<(String, Vec<usize>)> = templates
        .into_iter()
        .filter(|(_, members)| members.len() >= VARIANT_MIN)
        .collect();
    groups.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(&b.0)));
    let mut taken = vec![false; files.len()];
    let mut labels: HashMap<usize, String> = HashMap::new();
    let (mut merged_files, mut merged_lines) = (0, 0);
    for (template, members) in groups {
        let mut by_shape: HashMap<Vec<usize>, Vec<usize>> = HashMap::new();
        for index in members.into_iter().filter(|i| !taken[*i]) {
            by_shape
                .entry(files[index].1.iter().map(|h| h.line).collect())
                .or_default()
                .push(index);
        }
        let Some(same) = by_shape.into_values().max_by_key(|group| group.len()) else {
            continue;
        };
        if same.len() < VARIANT_MIN {
            continue;
        }
        let slot = template
            .split('/')
            .position(|part| part == "*")
            .unwrap_or(0);
        let variant = |i: usize| files[i].0.split('/').nth(slot).unwrap_or("").to_string();
        let pick = same
            .iter()
            .copied()
            .min_by_key(|i| {
                PREFERRED_VARIANTS
                    .iter()
                    .position(|p| *p == variant(*i))
                    .unwrap_or(usize::MAX)
            })
            .unwrap_or(same[0]);
        for index in &same {
            taken[*index] = true;
        }
        merged_files += same.len() - 1;
        merged_lines += (same.len() - 1) * files[pick].1.len();
        labels.insert(
            pick,
            template.replacen(
                '*',
                &format!("{{{},+{}}}", variant(pick), same.len() - 1),
                1,
            ),
        );
    }
    let rows = files
        .into_iter()
        .enumerate()
        .filter_map(|(index, (file, list))| match labels.remove(&index) {
            Some(label) => Some((label, list)),
            None if taken[index] => None,
            None => Some((file, list)),
        })
        .collect();
    (rows, merged_files, merged_lines)
}

type Row<'a> = (String, Vec<&'a Hit>);

/// Files in reading order: defining code first, then other code, tests, and text files, each
/// group by match count.
pub(super) fn ranked<'a>(hits: &'a [Hit], alternatives: &[Term], collapse: bool) -> Vec<Row<'a>> {
    let mut files: Vec<Row<'a>> = Vec::new();
    for hit in hits {
        match files.last_mut() {
            Some((file, list)) if *file == hit.file => list.push(hit),
            _ => files.push((hit.file.clone(), vec![hit])),
        }
    }
    let (code, text): (Vec<_>, Vec<_>) = files.into_iter().partition(|(file, _)| is_code(file));
    let text = match collapse {
        true => collapse_variants(text).0,
        false => text,
    };
    let matches = |list: &[&Hit]| list.iter().filter(|h| !h.context).count();
    let mut rows: Vec<(u8, usize, Row<'a>)> = code
        .into_iter()
        .map(|(file, list)| {
            let best = list
                .iter()
                .filter(|h| !h.context)
                .map(|h| defining_rank(h, alternatives))
                .min()
                .unwrap_or(2);
            let class = if is_test(&file) { 3 } else { best };
            (class, matches(&list), (file, list))
        })
        .chain(text.into_iter().map(|(file, list)| {
            let class = if is_test(&file) { 5 } else { 4 };
            (class, matches(&list), (file, list))
        }))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}
