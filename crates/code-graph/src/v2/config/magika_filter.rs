use orbit_utils::fs_walk::{
    ContentClass, Decision, FileInventoryEntry, FileLabel, FileStreamHooks, SkipReason,
};

pub struct MagikaFilter {
    session: magika::Session,
}

impl MagikaFilter {
    pub fn new() -> Result<Self, magika::Error> {
        Ok(Self {
            session: magika::Session::new()?,
        })
    }
}

impl FileStreamHooks for MagikaFilter {
    fn on_header(&mut self, file: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
        if file.label.content == ContentClass::Code {
            return Some((file.decision, file.label.clone()));
        }
        if file.label.skip.is_some() {
            return Some((file.decision, file.label.clone()));
        }
        None
    }

    fn on_content(&mut self, file: &FileInventoryEntry, content: &[u8]) -> (Decision, FileLabel) {
        let result = match self.session.identify_content_sync(content) {
            Ok(r) => r,
            Err(_) => return (file.decision, file.label.clone()),
        };
        let info = result.info();
        let mut label = file.label.clone();
        label.detail = Some(info.mime_type.to_string());

        if !info.is_text {
            label.content = ContentClass::Binary;
            label.skip = Some(SkipReason::Binary);
            return (Decision::ListOnly, label);
        }

        match info.group {
            "code" => {
                label.content = ContentClass::Code;
                (Decision::Parse, label)
            }
            _ => (file.decision, label),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_utils::fs_walk::FileInventory;

    fn text_entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Load,
            label: FileLabel {
                skip: None,
                content: ContentClass::Text,
                detail: None,
                extension: std::path::Path::new(path)
                    .extension()
                    .map(|e| e.to_string_lossy().into_owned()),
            },
        }
    }

    fn code_entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Parse,
            label: FileLabel {
                skip: None,
                content: ContentClass::Code,
                detail: None,
                extension: std::path::Path::new(path)
                    .extension()
                    .map(|e| e.to_string_lossy().into_owned()),
            },
        }
    }

    fn skipped_entry(path: &str) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size: 100,
            decision: Decision::ListOnly,
            label: FileLabel {
                skip: Some(SkipReason::Binary),
                content: ContentClass::Binary,
                detail: None,
                extension: None,
            },
        }
    }

    #[test]
    fn skips_code_entries() {
        let mut filter = MagikaFilter::new().unwrap();
        let entry = code_entry("main.rs", 100);
        let result = filter.on_header(&entry);
        assert!(result.is_some());
        let (decision, _) = result.unwrap();
        assert_eq!(decision, Decision::Parse);
    }

    #[test]
    fn skips_already_settled_entries() {
        let mut filter = MagikaFilter::new().unwrap();
        let entry = skipped_entry("data.bin");
        let result = filter.on_header(&entry);
        assert!(result.is_some());
        let (decision, _) = result.unwrap();
        assert_eq!(decision, Decision::ListOnly);
    }

    #[test]
    fn classifies_python_content() {
        let mut filter = MagikaFilter::new().unwrap();
        let entry = text_entry("utils.txt", 50);
        let content = b"#!/usr/bin/env python3\nimport os\nimport sys\n\ndef main():\n    print('hello')\n\nif __name__ == '__main__':\n    main()\n";

        assert!(filter.on_header(&entry).is_none());
        let (decision, label) = filter.on_content(&entry, content);
        assert_eq!(label.content, ContentClass::Code);
        assert_eq!(decision, Decision::Parse);
        assert!(label.detail.is_some());
    }

    #[test]
    fn classifies_shell_content() {
        let mut filter = MagikaFilter::new().unwrap();
        let entry = text_entry("run.sh", 30);
        let content = b"#!/bin/bash\nset -euo pipefail\necho 'hello world'\n";

        assert!(filter.on_header(&entry).is_none());
        let (decision, label) = filter.on_content(&entry, content);
        assert_eq!(label.content, ContentClass::Code);
        assert_eq!(decision, Decision::Parse);
    }

    #[test]
    fn keeps_text_for_non_code_content() {
        let mut filter = MagikaFilter::new().unwrap();
        let entry = text_entry("README.txt", 20);
        let content =
            b"This is a plain text readme file with some documentation about the project.\n";

        assert!(filter.on_header(&entry).is_none());
        let (decision, label) = filter.on_content(&entry, content);
        assert_eq!(decision, Decision::Load);
        assert!(label.detail.is_some());
    }

    #[test]
    fn refine_pass_upgrades_text_to_code() {
        let mut filter = MagikaFilter::new().unwrap();
        let python_src = b"import os\nimport sys\n\ndef process(items):\n    for item in items:\n        print(item)\n\nprocess([1, 2, 3])\n";

        let inv = FileInventory::new(vec![
            code_entry("main.rs", 100),
            text_entry("helper.txt", python_src.len() as u64),
            skipped_entry("logo.bin"),
        ]);

        let content_map: std::collections::HashMap<&str, &[u8]> =
            [("helper.txt", python_src.as_slice())].into();

        let inv = inv
            .refine(&mut filter, |path| {
                content_map.get(path).map(|b| b.to_vec())
            })
            .unwrap();

        assert_eq!(inv.find("main.rs").unwrap().decision, Decision::Parse);
        assert_eq!(
            inv.find("main.rs").unwrap().label.content,
            ContentClass::Code
        );

        let helper = inv.find("helper.txt").unwrap();
        assert_eq!(helper.decision, Decision::Parse);
        assert_eq!(helper.label.content, ContentClass::Code);
        assert!(helper.label.detail.is_some());

        assert_eq!(inv.find("logo.bin").unwrap().decision, Decision::ListOnly);
    }
}
