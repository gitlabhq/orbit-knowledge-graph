//! Test/manual PoC for sparse, branch-scoped Code Graph overlays.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DefinitionRow {
    pub id: i64,
    pub path: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CallRow {
    pub source_id: i64,
    pub target_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CodeSnapshot {
    pub definitions: BTreeSet<DefinitionRow>,
    pub calls: BTreeSet<CallRow>,
}

impl CodeSnapshot {
    fn definition(&self, id: i64) -> Option<&DefinitionRow> {
        self.definitions
            .iter()
            .find(|definition| definition.id == id)
    }

    fn definitions_in<'a>(
        &'a self,
        paths: &'a BTreeSet<String>,
    ) -> impl Iterator<Item = &'a DefinitionRow> {
        self.definitions
            .iter()
            .filter(|definition| paths.contains(&definition.path))
    }

    fn calls_from<'a>(&'a self, paths: &'a BTreeSet<String>) -> impl Iterator<Item = &'a CallRow> {
        self.calls.iter().filter(|call| {
            self.definition(call.source_id)
                .is_some_and(|source| paths.contains(&source.path))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileChange {
    Add(String),
    Edit(String),
    Delete(String),
    Rename { from: String, to: String },
}

impl FileChange {
    fn changed_paths(&self, paths: &mut BTreeSet<String>) {
        match self {
            Self::Add(path) | Self::Edit(path) | Self::Delete(path) => {
                paths.insert(path.clone());
            }
            Self::Rename { from, to } => {
                paths.insert(from.clone());
                paths.insert(to.clone());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayRow<T> {
    pub branch: String,
    pub value: T,
    pub version: u64,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BranchOverlay {
    pub reparsed_paths: BTreeSet<String>,
    pub definitions: Vec<OverlayRow<DefinitionRow>>,
    pub calls: Vec<OverlayRow<CallRow>>,
    pub base_mask: BTreeSet<String>,
}

pub fn build_overlay(
    base: &CodeSnapshot,
    feature: &CodeSnapshot,
    branch: &str,
    changes: &[FileChange],
    version: u64,
) -> BranchOverlay {
    let mut reparsed_paths = BTreeSet::new();
    let mut base_mask = BTreeSet::new();
    for change in changes {
        change.changed_paths(&mut reparsed_paths);
        match change {
            FileChange::Delete(path) => {
                base_mask.insert(path.clone());
            }
            FileChange::Rename { from, .. } => {
                base_mask.insert(from.clone());
            }
            FileChange::Add(_) | FileChange::Edit(_) => {}
        }
    }

    add_inbound_caller_closure(base, feature, &mut reparsed_paths);

    let feature_definitions: BTreeMap<_, _> = feature
        .definitions_in(&reparsed_paths)
        .map(|definition| (definition.id, definition.clone()))
        .collect();
    let base_definitions: BTreeMap<_, _> = base
        .definitions_in(&reparsed_paths)
        .map(|definition| (definition.id, definition.clone()))
        .collect();
    let feature_calls: BTreeMap<_, _> = feature
        .calls_from(&reparsed_paths)
        .map(|call| ((call.source_id, call.target_id), call.clone()))
        .collect();
    let base_calls: BTreeMap<_, _> = base
        .calls_from(&reparsed_paths)
        .map(|call| ((call.source_id, call.target_id), call.clone()))
        .collect();

    let definitions = feature_definitions
        .values()
        .cloned()
        .map(|value| OverlayRow {
            branch: branch.to_string(),
            value,
            version,
            deleted: false,
        })
        .chain(
            base_definitions
                .iter()
                .filter(|(id, _)| !feature_definitions.contains_key(id))
                .map(|(_, value)| OverlayRow {
                    branch: branch.to_string(),
                    value: value.clone(),
                    version,
                    deleted: true,
                }),
        )
        .collect();
    let calls = feature_calls
        .values()
        .cloned()
        .map(|value| OverlayRow {
            branch: branch.to_string(),
            value,
            version,
            deleted: false,
        })
        .chain(
            base_calls
                .iter()
                .filter(|(key, _)| !feature_calls.contains_key(key))
                .map(|(_, value)| OverlayRow {
                    branch: branch.to_string(),
                    value: value.clone(),
                    version,
                    deleted: true,
                }),
        )
        .collect();

    BranchOverlay {
        reparsed_paths,
        definitions,
        calls,
        base_mask,
    }
}

fn add_inbound_caller_closure(
    base: &CodeSnapshot,
    feature: &CodeSnapshot,
    reparsed_paths: &mut BTreeSet<String>,
) {
    let mut changed_definition_ids = BTreeSet::new();
    for definition in base.definitions_in(reparsed_paths) {
        let changed = feature
            .definition(definition.id)
            .is_none_or(|candidate| candidate.signature != definition.signature);
        if changed {
            changed_definition_ids.insert(definition.id);
        }
    }

    let mut queue: VecDeque<_> = changed_definition_ids.into_iter().collect();
    let mut visited = BTreeSet::new();
    while let Some(target_id) = queue.pop_front() {
        if !visited.insert(target_id) {
            continue;
        }
        for call in base.calls.iter().filter(|call| call.target_id == target_id) {
            if let Some(caller) = base.definition(call.source_id) {
                reparsed_paths.insert(caller.path.clone());
                queue.push_back(caller.id);
            }
        }
    }
}

pub fn effective_snapshot(base: &CodeSnapshot, overlay: &BranchOverlay) -> CodeSnapshot {
    let deleted_definitions: BTreeSet<_> = overlay
        .definitions
        .iter()
        .filter(|row| row.deleted)
        .map(|row| row.value.id)
        .collect();
    let deleted_calls: BTreeSet<_> = overlay
        .calls
        .iter()
        .filter(|row| row.deleted)
        .map(|row| (row.value.source_id, row.value.target_id))
        .collect();

    let mut definitions: BTreeMap<_, _> = base
        .definitions
        .iter()
        .filter(|definition| {
            !overlay.base_mask.contains(&definition.path)
                && !deleted_definitions.contains(&definition.id)
        })
        .map(|definition| (definition.id, definition.clone()))
        .collect();
    for row in overlay.definitions.iter().filter(|row| !row.deleted) {
        definitions.insert(row.value.id, row.value.clone());
    }

    let mut calls: BTreeMap<_, _> = base
        .calls
        .iter()
        .filter(|call| !deleted_calls.contains(&(call.source_id, call.target_id)))
        .map(|call| ((call.source_id, call.target_id), call.clone()))
        .collect();
    for row in overlay.calls.iter().filter(|row| !row.deleted) {
        calls.insert(
            (row.value.source_id, row.value.target_id),
            row.value.clone(),
        );
    }

    CodeSnapshot {
        definitions: definitions.into_values().collect(),
        calls: calls.into_values().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAIN: &str = "main.py";
    const LIB: &str = "lib.py";
    const EXTRA: &str = "extra.py";

    fn definition(id: i64, path: &str, signature: &str) -> DefinitionRow {
        DefinitionRow {
            id,
            path: path.to_string(),
            signature: signature.to_string(),
        }
    }

    fn call(source_id: i64, target_id: i64) -> CallRow {
        CallRow {
            source_id,
            target_id,
        }
    }

    fn snapshot(definitions: Vec<DefinitionRow>, calls: Vec<CallRow>) -> CodeSnapshot {
        CodeSnapshot {
            definitions: definitions.into_iter().collect(),
            calls: calls.into_iter().collect(),
        }
    }

    fn base() -> CodeSnapshot {
        snapshot(
            vec![
                definition(1, MAIN, "run()"),
                definition(2, LIB, "load()"),
                definition(3, LIB, "save()"),
            ],
            vec![call(1, 2)],
        )
    }

    fn assert_differential(feature: CodeSnapshot, changes: Vec<FileChange>) -> BranchOverlay {
        let base = base();
        let overlay = build_overlay(&base, &feature, "feature", &changes, 7);
        assert_eq!(effective_snapshot(&base, &overlay), feature);
        overlay
    }

    #[test]
    fn differential_add() {
        let feature = snapshot(
            vec![
                definition(1, MAIN, "run()"),
                definition(2, LIB, "load()"),
                definition(3, LIB, "save()"),
                definition(4, EXTRA, "report()"),
            ],
            vec![call(1, 2), call(4, 3)],
        );
        let overlay = assert_differential(feature, vec![FileChange::Add(EXTRA.into())]);
        assert_eq!(overlay.reparsed_paths, BTreeSet::from([EXTRA.into()]));
    }

    #[test]
    fn differential_edit_and_call_change() {
        let feature = snapshot(
            vec![
                definition(1, MAIN, "run()"),
                definition(2, LIB, "load()"),
                definition(3, LIB, "save()"),
            ],
            vec![call(1, 3)],
        );
        assert_differential(feature, vec![FileChange::Edit(MAIN.into())]);
    }

    #[test]
    fn differential_delete_reparses_inbound_callers() {
        let feature = snapshot(
            vec![definition(1, MAIN, "run()"), definition(3, LIB, "save()")],
            vec![],
        );
        let overlay = assert_differential(feature, vec![FileChange::Delete(LIB.into())]);
        assert_eq!(
            overlay.reparsed_paths,
            BTreeSet::from([LIB.into(), MAIN.into()])
        );
        assert!(overlay.definitions.iter().any(|row| row.deleted));
        assert!(overlay.calls.iter().any(|row| {
            row.deleted
                && row.value
                    == CallRow {
                        source_id: 1,
                        target_id: 2,
                    }
        }));
        assert_eq!(overlay.base_mask, BTreeSet::from([LIB.into()]));
    }

    #[test]
    fn differential_rename_masks_only_old_base_path() {
        let renamed = "storage.py";
        let feature = snapshot(
            vec![
                definition(1, MAIN, "run()"),
                definition(2, renamed, "load()"),
                definition(3, renamed, "save()"),
            ],
            vec![call(1, 2)],
        );
        let overlay = assert_differential(
            feature,
            vec![FileChange::Rename {
                from: LIB.into(),
                to: renamed.into(),
            }],
        );
        assert_eq!(overlay.base_mask, BTreeSet::from([LIB.into()]));
    }

    #[test]
    fn differential_import_change() {
        let feature = snapshot(
            vec![
                definition(1, MAIN, "run()"),
                definition(2, LIB, "load()"),
                definition(3, LIB, "save()"),
                definition(4, EXTRA, "load()"),
            ],
            vec![call(1, 4)],
        );
        assert_differential(
            feature,
            vec![FileChange::Edit(MAIN.into()), FileChange::Add(EXTRA.into())],
        );
    }

    #[test]
    fn differential_signature_change_reparses_inbound_callers() {
        let feature = snapshot(
            vec![
                definition(1, MAIN, "run()"),
                definition(2, LIB, "load(path)"),
                definition(3, LIB, "save()"),
                definition(4, LIB, "load()"),
            ],
            vec![call(1, 4)],
        );
        let overlay = assert_differential(feature, vec![FileChange::Edit(LIB.into())]);
        assert_eq!(
            overlay.reparsed_paths,
            BTreeSet::from([LIB.into(), MAIN.into()])
        );
        assert!(overlay.calls.iter().any(|row| {
            row.deleted
                && row.value
                    == CallRow {
                        source_id: 1,
                        target_id: 2,
                    }
        }));
        assert!(overlay.calls.iter().any(|row| {
            !row.deleted
                && row.value
                    == CallRow {
                        source_id: 1,
                        target_id: 4,
                    }
        }));
    }

    #[test]
    fn inbound_closure_crosses_an_already_reparsed_caller_path() {
        let caller = "caller.py";
        let upstream = "upstream.py";
        let base = snapshot(
            vec![
                definition(1, upstream, "start()"),
                definition(2, caller, "run()"),
                definition(3, LIB, "load()"),
                definition(4, caller, "helper()"),
            ],
            vec![call(1, 2), call(2, 3)],
        );
        let feature = snapshot(
            vec![
                definition(1, upstream, "start()"),
                definition(2, caller, "run()"),
                definition(3, LIB, "load(path)"),
                definition(4, caller, "helper(updated)"),
            ],
            vec![call(1, 2), call(2, 3)],
        );

        let overlay = build_overlay(
            &base,
            &feature,
            "feature",
            &[
                FileChange::Edit(caller.into()),
                FileChange::Edit(LIB.into()),
            ],
            7,
        );

        assert_eq!(
            overlay.reparsed_paths,
            BTreeSet::from([caller.into(), LIB.into(), upstream.into()])
        );
    }
}
