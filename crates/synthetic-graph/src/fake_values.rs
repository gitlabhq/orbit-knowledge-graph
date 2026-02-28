use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::Utc;
use ontology::{DataType, Field};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
const NULL_THRESHOLD: u64 = 26;
const GOLDEN_RATIO_HASH: u64 = 0x9e3779b97f4a7c15;

/// Fast hex formatting into a String buffer without format! overhead.
#[inline]
fn push_hex_u64(buf: &mut String, mut val: u64) {
    if val == 0 {
        buf.push('0');
        return;
    }
    let leading_zeros = val.leading_zeros() as usize;
    let nibbles = 16 - (leading_zeros / 4);
    buf.reserve(nibbles);
    let start = buf.len();
    for _ in 0..nibbles {
        buf.push('0');
    }
    let bytes = unsafe { buf.as_bytes_mut() };
    for i in (0..nibbles).rev() {
        bytes[start + i] = HEX_DIGITS[(val & 0xf) as usize];
        val >>= 4;
    }
}

#[inline]
fn push_hex_u16(buf: &mut String, val: u16) {
    push_hex_u64(buf, val as u64);
}

/// Pre-computed field generation strategy to avoid runtime string matching.
///
/// Classify a field once at startup via [`FieldKind::classify`], then use
/// [`FakeValueGenerator::generate_with_kind`] for each row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    NameOrTitle,
    Email,
    Url,
    Path,
    ShaOrHash,
    DescriptionOrBody,
    Status,
    State,
    RefOrBranch,
    GenericString,
    Id,
    Iid,
    Weight,
    StarCount,
    Duration,
    GenericInt,
    IdList,
    Archived,
    Confidential,
    Draft,
    Squash,
    PrivateProfile,
    IsAdminOrAuditor,
    IsExternal,
    DiscussionLocked,
    Tag,
    GenericBool,
    Float,
    Date,
    DateTime,
    Enum,
    Uuid,
}

impl FieldKind {
    /// Classify an ontology field once at startup.
    pub fn classify(field: &Field) -> Self {
        if field.enum_values.is_some() {
            return FieldKind::Enum;
        }
        match field.data_type {
            DataType::Enum => FieldKind::Enum,
            DataType::Float => FieldKind::Float,
            DataType::Date => FieldKind::Date,
            DataType::DateTime => FieldKind::DateTime,
            DataType::String => Self::classify_string(&field.name),
            DataType::Int => Self::classify_int(&field.name),
            DataType::Bool => Self::classify_bool(&field.name),
            DataType::Uuid => FieldKind::Uuid,
        }
    }

    /// Classify a column by name (for schema-driven generation without ontology fields).
    pub fn classify_column(name: &str) -> Self {
        let lower = name.to_ascii_lowercase();

        if lower.ends_with("_ids") {
            return FieldKind::IdList;
        }
        if lower == "id" || lower.ends_with("_id") {
            return FieldKind::Id;
        }
        if lower == "iid" {
            return FieldKind::Iid;
        }
        if lower == "uuid" || lower.ends_with("_uuid") {
            return FieldKind::Uuid;
        }

        Self::classify_string(&lower)
    }

    fn classify_string(name: &str) -> Self {
        let lower = name.to_lowercase();
        if lower.contains("name") || lower.contains("title") || lower.contains("username") {
            FieldKind::NameOrTitle
        } else if lower.contains("email") {
            FieldKind::Email
        } else if lower.contains("url") {
            FieldKind::Url
        } else if lower.contains("path") {
            FieldKind::Path
        } else if lower.contains("sha") || lower.contains("hash") || lower.contains("fingerprint") {
            FieldKind::ShaOrHash
        } else if lower.contains("description") || lower.contains("body") || lower.contains("note")
        {
            FieldKind::DescriptionOrBody
        } else if lower.contains("status") {
            FieldKind::Status
        } else if lower.contains("state") {
            FieldKind::State
        } else if lower.contains("ref") || lower.contains("branch") {
            FieldKind::RefOrBranch
        } else {
            FieldKind::GenericString
        }
    }

    fn classify_int(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "iid" => FieldKind::Iid,
            "weight" => FieldKind::Weight,
            "star_count" => FieldKind::StarCount,
            "duration" => FieldKind::Duration,
            _ => FieldKind::GenericInt,
        }
    }

    fn classify_bool(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "archived" => FieldKind::Archived,
            "confidential" => FieldKind::Confidential,
            "draft" => FieldKind::Draft,
            "squash" => FieldKind::Squash,
            "private_profile" => FieldKind::PrivateProfile,
            "is_admin" | "is_auditor" => FieldKind::IsAdminOrAuditor,
            "is_external" => FieldKind::IsExternal,
            "discussion_locked" => FieldKind::DiscussionLocked,
            "tag" => FieldKind::Tag,
            _ => FieldKind::GenericBool,
        }
    }
}

/// A generated fake value.
#[derive(Debug, Clone)]
pub enum FakeValue {
    Null,
    String(Cow<'static, str>),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(i32),
    DateTime(i64),
    ListInt64(Vec<i64>),
}

impl FakeValue {
    pub fn is_null(&self) -> bool {
        matches!(self, FakeValue::Null)
    }

    #[inline]
    pub fn owned_string(s: String) -> Self {
        FakeValue::String(Cow::Owned(s))
    }

    #[inline]
    pub fn static_string(s: &'static str) -> Self {
        FakeValue::String(Cow::Borrowed(s))
    }
}

/// High-performance fake value generator using seeded RNG with counter mixing.
///
/// Shared by both the ontology-driven simulator and the schema-driven datalake
/// generator. Uses a counter XOR'd with the golden ratio hash for better bit
/// distribution across sequential calls.
pub struct FakeValueGenerator {
    rng: StdRng,
    counter: u64,
    now_millis: i64,
    buf: String,
}

impl Default for FakeValueGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeValueGenerator {
    pub fn new() -> Self {
        Self {
            rng: StdRng::from_entropy(),
            counter: 0,
            now_millis: Utc::now().timestamp_millis(),
            buf: String::with_capacity(64),
        }
    }

    pub fn with_seed(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            counter: 0,
            now_millis: Utc::now().timestamp_millis(),
            buf: String::with_capacity(64),
        }
    }

    /// Current timestamp used for DateTime generation (milliseconds).
    pub fn now_millis(&self) -> i64 {
        self.now_millis
    }

    #[inline]
    fn emit_buf(&self) -> Cow<'static, str> {
        Cow::Owned(self.buf.clone())
    }

    #[inline]
    pub fn next_random(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(1);
        let r = self.rng.r#gen::<u64>();
        r ^ self.counter.wrapping_mul(GOLDEN_RATIO_HASH)
    }

    /// Generate a value from an ontology field definition.
    pub fn generate(&mut self, field: &Field) -> FakeValue {
        let kind = FieldKind::classify(field);
        self.generate_with_kind(kind, field.nullable, field.enum_values.as_ref())
    }

    /// Generate a value using a pre-classified [`FieldKind`].
    #[inline]
    pub fn generate_with_kind(
        &mut self,
        kind: FieldKind,
        nullable: bool,
        enum_values: Option<&BTreeMap<i64, String>>,
    ) -> FakeValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return FakeValue::Null;
        }

        let low = bits as u32;
        let high = (bits >> 32) as u32;

        match kind {
            FieldKind::NameOrTitle => {
                const PREFIXES: [&str; 8] = [
                    "alpha_", "beta_", "gamma_", "delta_", "epsilon_", "zeta_", "theta_", "omega_",
                ];
                self.buf.clear();
                self.buf.push_str(PREFIXES[low as usize % PREFIXES.len()]);
                push_hex_u64(&mut self.buf, bits);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::Email => {
                const DOMAINS: [&str; 5] = [
                    "@example.com",
                    "@test.org",
                    "@demo.net",
                    "@sample.io",
                    "@mock.dev",
                ];
                self.buf.clear();
                self.buf.push_str("user");
                push_hex_u64(&mut self.buf, bits & 0xffffff);
                self.buf.push_str(DOMAINS[low as usize % DOMAINS.len()]);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::Url => {
                self.buf.clear();
                self.buf.push_str("https://example.com/");
                push_hex_u64(&mut self.buf, bits);
                self.buf.push('/');
                push_hex_u16(&mut self.buf, high as u16);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::Path => {
                self.buf.clear();
                self.buf.push_str("/p");
                push_hex_u64(&mut self.buf, bits & 0xff);
                self.buf.push_str("/d");
                push_hex_u64(&mut self.buf, (bits >> 8) & 0xff);
                self.buf.push('/');
                push_hex_u16(&mut self.buf, high as u16);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::ShaOrHash => {
                self.buf.clear();
                self.buf.reserve(40);
                let extra = self.next_random();
                let parts: [u64; 3] = [bits, low as u64, extra];
                for i in 0..40 {
                    let word = parts[i / 16];
                    let shift = (15 - (i % 16)) * 4;
                    let nibble = ((word >> shift) & 0xf) as usize;
                    self.buf.push(HEX_DIGITS[nibble] as char);
                }
                FakeValue::String(self.emit_buf())
            }
            FieldKind::DescriptionOrBody => {
                const WORDS: [&str; 12] = [
                    "Lorem",
                    "ipsum",
                    "dolor",
                    "sit",
                    "amet",
                    "consectetur",
                    "adipiscing",
                    "elit",
                    "sed",
                    "do",
                    "eiusmod",
                    "tempor",
                ];
                self.buf.clear();
                self.buf.push_str(WORDS[low as usize % WORDS.len()]);
                self.buf.push(' ');
                self.buf.push_str(WORDS[(low >> 8) as usize % WORDS.len()]);
                self.buf.push(' ');
                self.buf.push_str(WORDS[(low >> 16) as usize % WORDS.len()]);
                self.buf.push(' ');
                push_hex_u64(&mut self.buf, bits & 0xffff);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::Status => {
                const STATUSES: [&str; 5] = ["open", "closed", "merged", "pending", "active"];
                FakeValue::static_string(STATUSES[low as usize % STATUSES.len()])
            }
            FieldKind::State => {
                const STATES: [&str; 5] = ["pending", "running", "success", "failed", "canceled"];
                FakeValue::static_string(STATES[low as usize % STATES.len()])
            }
            FieldKind::RefOrBranch => {
                const PREFIXES: [&str; 6] = [
                    "feature/branch-",
                    "fix/branch-",
                    "hotfix/branch-",
                    "release/branch-",
                    "main/branch-",
                    "develop/branch-",
                ];
                self.buf.clear();
                self.buf.push_str(PREFIXES[low as usize % PREFIXES.len()]);
                push_hex_u64(&mut self.buf, bits & 0xffff);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::GenericString => {
                self.buf.clear();
                self.buf.push_str("val");
                push_hex_u64(&mut self.buf, bits);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::Id => FakeValue::Int((low % 99999 + 1) as i64),
            FieldKind::Iid => FakeValue::Int((low % 10000 + 1) as i64),
            FieldKind::Weight => FakeValue::Int((low % 19 + 1) as i64),
            FieldKind::StarCount => FakeValue::Int((low % 5000) as i64),
            FieldKind::Duration => FakeValue::Int((low % 7140 + 60) as i64),
            FieldKind::GenericInt => FakeValue::Int((low % 99999 + 1) as i64),
            FieldKind::IdList => {
                let count = (low % 6) as usize;
                let mut values = Vec::with_capacity(count);
                for i in 0..count {
                    let id_bits = if i == 0 { bits } else { self.next_random() };
                    values.push((id_bits % 9999 + 1) as i64);
                }
                FakeValue::ListInt64(values)
            }
            FieldKind::Archived => FakeValue::Bool(((bits >> 8) & 0xff) < 13),
            FieldKind::Confidential => FakeValue::Bool(((bits >> 8) & 0xff) < 26),
            FieldKind::Draft => FakeValue::Bool(((bits >> 8) & 0xff) < 51),
            FieldKind::Squash => FakeValue::Bool(((bits >> 8) & 0xff) < 77),
            FieldKind::PrivateProfile => FakeValue::Bool(((bits >> 8) & 0xff) < 26),
            FieldKind::IsAdminOrAuditor => FakeValue::Bool(((bits >> 8) & 0xff) < 5),
            FieldKind::IsExternal => FakeValue::Bool(((bits >> 8) & 0xff) < 13),
            FieldKind::DiscussionLocked => FakeValue::Bool(((bits >> 8) & 0xff) < 13),
            FieldKind::Tag => FakeValue::Bool(((bits >> 8) & 0xff) < 26),
            FieldKind::GenericBool => FakeValue::Bool(((bits >> 8) & 0xff) < 128),
            FieldKind::Float => {
                let f = (bits >> 32) as f64 / (u32::MAX as f64);
                FakeValue::Float(f * 10000.0)
            }
            FieldKind::Date => {
                let days_ago = ((bits >> 16) as i32) % 1825;
                FakeValue::Date(-days_ago)
            }
            FieldKind::DateTime => {
                let days_ago = ((bits >> 16) % 1825) as i64;
                let hour_offset = ((bits >> 8) % 24) as i64;
                let millis = (days_ago * 86400 + hour_offset * 3600) * 1000;
                FakeValue::DateTime(self.now_millis - millis)
            }
            FieldKind::Enum => {
                if let Some(enum_vals) = enum_values {
                    let values: Vec<&String> = enum_vals.values().collect();
                    if !values.is_empty() {
                        let index = (bits as usize) % values.len();
                        return FakeValue::owned_string(values[index].clone());
                    }
                }
                FakeValue::static_string("unknown")
            }
            FieldKind::Uuid => {
                self.buf.clear();
                self.buf.reserve(36);
                let bits2 = self.next_random();
                let bytes = [
                    (bits >> 56) as u8,
                    (bits >> 48) as u8,
                    (bits >> 40) as u8,
                    (bits >> 32) as u8,
                    (bits >> 24) as u8,
                    (bits >> 16) as u8,
                    (bits >> 8) as u8,
                    bits as u8,
                    (bits2 >> 56) as u8,
                    (bits2 >> 48) as u8,
                    (bits2 >> 40) as u8,
                    (bits2 >> 32) as u8,
                    (bits2 >> 24) as u8,
                    (bits2 >> 16) as u8,
                    (bits2 >> 8) as u8,
                    bits2 as u8,
                ];
                for (i, byte) in bytes.iter().enumerate() {
                    if i == 4 || i == 6 || i == 8 || i == 10 {
                        self.buf.push('-');
                    }
                    self.buf.push(HEX_DIGITS[(*byte >> 4) as usize] as char);
                    self.buf.push(HEX_DIGITS[(*byte & 0xf) as usize] as char);
                }
                FakeValue::String(self.emit_buf())
            }
        }
    }

    /// Pick a random value from a JSON pool (for field overrides).
    pub fn pick_from_pool(&mut self, pool: &[serde_json::Value]) -> FakeValue {
        let index = self.next_random() as usize % pool.len();
        json_to_fake_value(&pool[index])
    }

    /// Write a string value for the given kind directly into the internal buffer.
    ///
    /// Returns a reference to the buffer contents. Useful for zero-copy writing
    /// into Arrow StringBuilders.
    pub fn generate_string_buf(&mut self, kind: FieldKind, nullable: bool) -> Option<&str> {
        let bits = self.next_random();
        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return None;
        }
        self.buf.clear();
        let low = bits as u32;
        let high = (bits >> 32) as u32;
        match kind {
            FieldKind::NameOrTitle => {
                const PREFIXES: [&str; 8] = [
                    "alpha_", "beta_", "gamma_", "delta_", "epsilon_", "zeta_", "theta_", "omega_",
                ];
                self.buf.push_str(PREFIXES[low as usize % PREFIXES.len()]);
                push_hex_u64(&mut self.buf, bits);
            }
            FieldKind::Email => {
                const DOMAINS: [&str; 5] = [
                    "@example.com",
                    "@test.org",
                    "@demo.net",
                    "@sample.io",
                    "@mock.dev",
                ];
                self.buf.push_str("user");
                push_hex_u64(&mut self.buf, bits & 0xffffff);
                self.buf.push_str(DOMAINS[low as usize % DOMAINS.len()]);
            }
            FieldKind::Url => {
                self.buf.push_str("https://example.com/");
                push_hex_u64(&mut self.buf, bits);
                self.buf.push('/');
                push_hex_u16(&mut self.buf, high as u16);
            }
            FieldKind::Status => {
                const STATUSES: [&str; 5] = ["open", "closed", "merged", "pending", "active"];
                self.buf.push_str(STATUSES[low as usize % STATUSES.len()]);
            }
            FieldKind::State => {
                const STATES: [&str; 5] = ["pending", "running", "success", "failed", "canceled"];
                self.buf.push_str(STATES[low as usize % STATES.len()]);
            }
            _ => {
                self.buf.push_str("val");
                push_hex_u64(&mut self.buf, bits);
            }
        }
        Some(&self.buf)
    }
}

fn json_to_fake_value(value: &serde_json::Value) -> FakeValue {
    match value {
        serde_json::Value::String(s) => FakeValue::owned_string(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FakeValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                FakeValue::Float(f)
            } else {
                FakeValue::Null
            }
        }
        serde_json::Value::Bool(b) => FakeValue::Bool(*b),
        serde_json::Value::Null => FakeValue::Null,
        _ => FakeValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_with_seed() {
        let mut gen1 = FakeValueGenerator::with_seed(42);
        let mut gen2 = FakeValueGenerator::with_seed(42);

        for _ in 0..100 {
            let v1 = gen1.next_random();
            let v2 = gen2.next_random();
            assert_eq!(v1, v2);
        }
    }

    #[test]
    fn test_field_kind_classify_column() {
        assert_eq!(FieldKind::classify_column("id"), FieldKind::Id);
        assert_eq!(FieldKind::classify_column("project_id"), FieldKind::Id);
        assert_eq!(FieldKind::classify_column("member_ids"), FieldKind::IdList);
        assert_eq!(FieldKind::classify_column("iid"), FieldKind::Iid);
        assert_eq!(FieldKind::classify_column("email"), FieldKind::Email);
        assert_eq!(FieldKind::classify_column("name"), FieldKind::NameOrTitle);
    }

    #[test]
    fn test_null_rate_approximately_10_percent() {
        let mut faker = FakeValueGenerator::with_seed(42);
        let field = Field {
            name: "test".to_string(),
            source: "test".to_string(),
            data_type: DataType::String,
            nullable: true,
            enum_values: None,
            enum_type: ontology::EnumType::default(),
        };
        let mut nulls = 0;
        let total = 10_000;
        for _ in 0..total {
            if faker.generate(&field).is_null() {
                nulls += 1;
            }
        }
        let rate = nulls as f64 / total as f64;
        assert!(
            (0.05..0.15).contains(&rate),
            "null rate {rate} should be ~10%"
        );
    }
}
