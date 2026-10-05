use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub sources: Vec<SourceKind>,
    pub entries: Vec<Entry>,
    #[serde(default)]
    pub rules: Vec<Rule>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub options: Options,
    pub load_error: Option<String>,
    #[serde(default)]
    pub steps: Vec<Step>,
    pub changed: Option<Vec<String>>,
    pub truncate_archive: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Memory,
    Puts,
    Lazy,
    Checkout,
    Changed,
    Archive,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub path: String,
    #[serde(default)]
    pub data: Data,
    pub link: Option<String>,
    pub hardlink: Option<String>,
    pub archive_path: Option<String>,
    pub raw_type: Option<u8>,
    pub declared_size: Option<u64>,
    pub pax_size: Option<u64>,
    pub read_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Data {
    Text(String),
    Bytes(Vec<u8>),
    Repeat(Repeat),
}

impl Default for Data {
    fn default() -> Self {
        Self::Text(String::new())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Repeat {
    pub text: String,
    pub repeat: usize,
}

impl Data {
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Self::Text(text) => text.as_bytes().to_vec(),
            Self::Bytes(bytes) => bytes.clone(),
            Self::Repeat(value) => {
                assert!(
                    value.text.len().saturating_mul(value.repeat) <= 16 * 1024 * 1024,
                    "fixture too large"
                );
                value.text.repeat(value.repeat).into_bytes()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Tag {
    Source,
    #[default]
    Input,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Header,
    Content,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub phase: Phase,
    pub suffix: Option<String>,
    pub contains: Option<Data>,
    pub decision: Verdict,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Verdict {
    Pending,
    Keep(Tag),
    List(String),
    Drop(String),
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub file_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub files: Option<usize>,
    pub resident_bytes: Option<u64>,
    pub spilled_bytes: Option<u64>,
}

impl Limits {
    pub fn store(&self) -> orbit_utils::vfs::Limits {
        orbit_utils::vfs::Limits {
            file_bytes: self.file_bytes,
            total_bytes: self.total_bytes,
            files: self.files,
            resident_bytes: self.resident_bytes,
            spilled_bytes: self.spilled_bytes,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub compress_spill: bool,
    pub scratch: Scratch,
    pub cancel_after: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scratch {
    #[default]
    Default,
    Existing,
    Missing,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Read {
        path: String,
        expect: Outcome<Data>,
    },
    ReadDir {
        path: String,
        expect: Outcome<Vec<String>>,
    },
    Stat {
        path: String,
        expect: Outcome<Stat>,
    },
    Files {
        expect: Vec<Row>,
    },
    Subtree {
        path: String,
        expect: Vec<String>,
    },
    Usage {
        expect: Usage,
    },
    LazyReads {
        expect: usize,
    },
    Write {
        path: String,
        data: Data,
    },
    Remove {
        path: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Outcome<T> {
    Ok(Success<T>),
    Err(Failure),
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Success<T> {
    pub ok: T,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub error: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Stat {
    pub path: String,
    pub kind: Kind,
    pub len: u64,
    pub decision: Option<Verdict>,
    pub link: Option<String>,
}
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    File,
    Dir,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Row {
    pub path: String,
    pub size: u64,
    pub decision: Verdict,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Usage {
    pub files: Option<usize>,
    pub bytes: Option<u64>,
    pub kept: Option<u64>,
    pub resident: Option<u64>,
    pub spilled: Option<u64>,
    pub spilled_below: Option<u64>,
    pub deduped_bytes: Option<u64>,
    pub duplicate_paths: Option<usize>,
}
