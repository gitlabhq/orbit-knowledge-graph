use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Suite {
    Scenario(Box<Scenario>),
    Scenarios(Vec<Scenario>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    pub sources: Vec<SourceKind>,
    pub fixtures: Vec<Fixture>,
    #[serde(default)]
    pub rules: Vec<Rule>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub options: Options,
    pub load_error: Option<String>,
    #[serde(default)]
    pub tests: Vec<Test>,
    pub changed: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Memory,
    Lazy,
    Checkout,
    Changed,
    Archive,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub path: String,
    #[serde(default)]
    pub content: String,
    pub link: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Test {
    pub name: String,
    pub assert: Vec<Step>,
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
    pub contains: Option<String>,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Read {
        path: String,
        expect: Outcome<String>,
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
    Write {
        path: String,
        content: String,
    },
    Remove {
        path: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Outcome<T> {
    Ok(Success<T>),
    Err(Failure),
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Success<T> {
    pub ok: T,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub error: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Stat {
    pub path: String,
    pub kind: Kind,
    pub len: u64,
    pub decision: Option<Verdict>,
    pub link: Option<String>,
}
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    File,
    Dir,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Row {
    pub path: String,
    pub size: u64,
    pub decision: Verdict,
}

#[derive(Debug, Clone, Default, Deserialize)]
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
