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
