//! Fast data generators for ontology data types.

use chrono::Utc;
use ontology::{DataType, Field};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::borrow::Cow;

/// Hex digits for fast formatting (avoids format! parsing overhead).
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Fast hex formatting into a String buffer.
#[inline]
fn push_hex_u64(buf: &mut String, mut val: u64) {
    if val == 0 {
        buf.push('0');
        return;
    }
    // Find the highest non-zero nibble
    let leading_zeros = val.leading_zeros() as usize;
    let nibbles = 16 - (leading_zeros / 4);

    // Reserve space
    buf.reserve(nibbles);

    // Build hex string from high to low nibbles
    let start = buf.len();
    for _ in 0..nibbles {
        buf.push('0'); // placeholder
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    // String kinds
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
    // Int kinds
    Iid,
    Weight,
    StarCount,
    Duration,
    GenericInt,
    // Bool kinds
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
    // Other (uses DataType)
    Float,
    Date,
    DateTime,
    Enum,
    Uuid,
}

impl FieldKind {
    /// Classify a field once at startup to avoid repeated string matching.
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

    fn classify_string(name: &str) -> Self {
        let lower = name.to_lowercase();
        if lower.contains("name") || lower.contains("title") {
            FieldKind::NameOrTitle
        } else if lower.contains("email") {
            FieldKind::Email
        } else if lower.contains("url") {
            FieldKind::Url
        } else if lower.contains("path") {
            FieldKind::Path
        } else if lower.contains("sha") || lower.contains("hash") {
            FieldKind::ShaOrHash
        } else if lower.contains("description") || lower.contains("body") {
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

/// Generates values for ontology fields using minimal randomness.
pub struct FakeValueGenerator {
    rng: StdRng,
    counter: u64,
    /// Cached current timestamp to avoid repeated syscalls.
    now_millis: i64,
    /// Reusable string buffer to avoid allocations.
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

    pub fn new_fast() -> Self {
        Self::new()
    }

    pub fn with_seed(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            counter: 0,
            now_millis: Utc::now().timestamp_millis(),
            buf: String::with_capacity(64),
        }
    }

    pub fn fast_with_seed(seed: u64) -> Self {
        Self::with_seed(seed)
    }

    /// Clone the buffer contents and return as Cow::Owned. Buffer is reused next call.
    #[inline]
    fn emit_buf(&self) -> Cow<'static, str> {
        Cow::Owned(self.buf.clone())
    }

    /// Generate a single u64 and use the counter for mixing.
    #[inline]
    fn next_random(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(1);
        let r = self.rng.r#gen::<u64>();
        r ^ self.counter.wrapping_mul(0x9e3779b97f4a7c15)
    }

    /// Generate a value for a field. Use FieldKind::classify() once per field,
    /// then call generate_with_kind() for each row.
    pub fn generate(&mut self, field: &Field) -> FakeValue {
        let kind = FieldKind::classify(field);
        self.generate_with_kind(kind, field.nullable, field.enum_values.as_ref())
    }

    /// Fast path: generate using pre-computed FieldKind.
    #[inline]
    pub fn generate_with_kind(
        &mut self,
        kind: FieldKind,
        nullable: bool,
        enum_values: Option<&std::collections::BTreeMap<i64, String>>,
    ) -> FakeValue {
        let bits = self.next_random();

        // Use lowest bits for nullable check (10% ≈ 26/256)
        if nullable && (bits & 0xff) < 26 {
            return FakeValue::Null;
        }

        let low = bits as u32;
        let high = (bits >> 32) as u32;

        match kind {
            // String kinds - use buffer + fast hex instead of format!()
            FieldKind::NameOrTitle => {
                const PREFIXES: [&str; 8] = [
                    "alpha_", "beta_", "gamma_", "delta_", "epsilon_", "zeta_", "theta_", "omega_",
                ];
                let prefix = PREFIXES[low as usize % PREFIXES.len()];
                self.buf.clear();
                self.buf.push_str(prefix);
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
                let domain = DOMAINS[low as usize % DOMAINS.len()];
                self.buf.clear();
                self.buf.push_str("user");
                push_hex_u64(&mut self.buf, bits & 0xffffff);
                self.buf.push_str(domain);
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
                // 40-char hex (160 bits) — only 128 bits of entropy available,
                // so the top 32 bits (8 hex digits) are filled from `high`.
                self.buf.clear();
                self.buf.reserve(40);
                let lo128 = ((bits as u128) << 64) | (low as u128);
                let hi32 = high;
                for i in (0..8).rev() {
                    let nibble = ((hi32 >> (i * 4)) & 0xf) as usize;
                    self.buf.push(HEX_DIGITS[nibble] as char);
                }
                for i in (0..32).rev() {
                    let nibble = ((lo128 >> (i * 4)) & 0xf) as usize;
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
                let w1 = WORDS[low as usize % WORDS.len()];
                let w2 = WORDS[(low >> 8) as usize % WORDS.len()];
                let w3 = WORDS[(low >> 16) as usize % WORDS.len()];
                self.buf.clear();
                self.buf.push_str(w1);
                self.buf.push(' ');
                self.buf.push_str(w2);
                self.buf.push(' ');
                self.buf.push_str(w3);
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
                let prefix = PREFIXES[low as usize % PREFIXES.len()];
                self.buf.clear();
                self.buf.push_str(prefix);
                push_hex_u64(&mut self.buf, bits & 0xffff);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::GenericString => {
                self.buf.clear();
                self.buf.push_str("val");
                push_hex_u64(&mut self.buf, bits);
                FakeValue::String(self.emit_buf())
            }
            // Int kinds
            FieldKind::Iid => FakeValue::Int((low % 10000 + 1) as i64),
            FieldKind::Weight => FakeValue::Int((low % 19 + 1) as i64),
            FieldKind::StarCount => FakeValue::Int((low % 5000) as i64),
            FieldKind::Duration => FakeValue::Int((low % 7140 + 60) as i64),
            FieldKind::GenericInt => FakeValue::Int((low % 99999 + 1) as i64),
            // Bool kinds
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
            // Other types
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
                let bits2 = self.next_random();
                self.buf.clear();
                self.buf.reserve(36);
                // 8-4-4-4-12 hex format (32 hex digits = 128 bits)
                // Use `bits` for the first 16 hex digits, `bits2` for the last 16.
                for i in (0..8).rev() {
                    self.buf
                        .push(HEX_DIGITS[((bits >> (i * 4)) & 0xf) as usize] as char);
                }
                self.buf.push('-');
                for i in (8..12).rev() {
                    self.buf
                        .push(HEX_DIGITS[((bits >> (i * 4)) & 0xf) as usize] as char);
                }
                self.buf.push('-');
                for i in (12..16).rev() {
                    self.buf
                        .push(HEX_DIGITS[((bits >> (i * 4)) & 0xf) as usize] as char);
                }
                self.buf.push('-');
                for i in (0..4).rev() {
                    self.buf
                        .push(HEX_DIGITS[((bits2 >> (i * 4)) & 0xf) as usize] as char);
                }
                self.buf.push('-');
                for i in (4..16).rev() {
                    self.buf
                        .push(HEX_DIGITS[((bits2 >> (i * 4)) & 0xf) as usize] as char);
                }
                FakeValue::String(self.emit_buf())
            }
        }
    }
}

/// A generated fake value.
#[derive(Debug, Clone)]
pub enum FakeValue {
    Null,
    /// String value - uses Cow to avoid allocation for static strings.
    String(Cow<'static, str>),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(i32),
    DateTime(i64),
}

impl FakeValue {
    pub fn is_null(&self) -> bool {
        matches!(self, FakeValue::Null)
    }

    /// Create a string value from an owned String.
    #[inline]
    pub fn owned_string(s: String) -> Self {
        FakeValue::String(Cow::Owned(s))
    }

    /// Create a string value from a static str (zero allocation).
    #[inline]
    pub fn static_string(s: &'static str) -> Self {
        FakeValue::String(Cow::Borrowed(s))
    }
}
