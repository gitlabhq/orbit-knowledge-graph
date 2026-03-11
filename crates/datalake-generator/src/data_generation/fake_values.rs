use arrow::array::StringBuilder;
use chrono::Utc;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
const NULL_THRESHOLD: u64 = 26;
const GOLDEN_RATIO_HASH: u64 = 0x9e3779b97f4a7c15;
const MAX_GENERATED_ID: u32 = 99999;
const MAX_IID: u32 = 10000;
const MAX_DAYS_AGO: u64 = 1825;
const MAX_LIST_LENGTH: u64 = 6;
const MAX_ID_IN_LIST: u64 = 9999;

#[inline]
fn push_hex(buf: &mut String, value: u64) {
    use std::fmt::Write;
    let _ = write!(buf, "{value:x}");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Id,
    Name,
    Email,
    Url,
    Path,
    Sha,
    Description,
    Status,
    State,
    Branch,
    GenericString,
    Iid,
    IdList,
    DateTime,
    Uuid,
}

impl ColumnKind {
    pub fn classify(column_name: &str) -> Self {
        let lower = column_name.to_ascii_lowercase();

        if lower.ends_with("_ids") {
            return ColumnKind::IdList;
        }
        if lower == "id" || lower.ends_with("_id") {
            return ColumnKind::Id;
        }
        if lower == "iid" {
            return ColumnKind::Iid;
        }
        if lower == "uuid" || lower.ends_with("_uuid") {
            return ColumnKind::Uuid;
        }

        if lower.contains("email") {
            return ColumnKind::Email;
        }
        if lower.contains("url") {
            return ColumnKind::Url;
        }
        if lower.contains("sha") || lower.contains("fingerprint") || lower.contains("hash") {
            return ColumnKind::Sha;
        }
        if lower.contains("path") {
            return ColumnKind::Path;
        }
        if lower.contains("name") || lower.contains("title") || lower.contains("username") {
            return ColumnKind::Name;
        }
        if lower.contains("description") || lower.contains("body") || lower.contains("note") {
            return ColumnKind::Description;
        }
        if lower.contains("status") {
            return ColumnKind::Status;
        }
        if lower.contains("state") {
            return ColumnKind::State;
        }
        if lower.contains("ref") || lower.contains("branch") {
            return ColumnKind::Branch;
        }

        ColumnKind::GenericString
    }
}

#[derive(Debug, Clone)]
pub enum SiphonValue {
    Null,
    String(String),
    Int64(i64),
    Int8(i8),
    Float64(f64),
    Bool(bool),
    Date32(i32),
    DateTime64(i64),
    ListInt64(Vec<i64>),
}

pub struct SiphonFakeValueGenerator {
    rng: SmallRng,
    counter: u64,
    now_micros: i64,
    buf: String,
}

impl SiphonFakeValueGenerator {
    pub fn with_seed(seed: u64) -> Self {
        Self::with_seed_and_time(seed, Utc::now().timestamp_micros())
    }

    pub fn with_seed_and_time(seed: u64, now_micros: i64) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            counter: 0,
            now_micros,
            buf: String::with_capacity(64),
        }
    }

    #[inline]
    fn next_random(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(1);
        let random_bits = self.rng.random::<u64>();
        random_bits ^ self.counter.wrapping_mul(GOLDEN_RATIO_HASH)
    }

    pub fn generate_string(&mut self, kind: ColumnKind, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        self.write_string_to_buf(kind, bits);
        SiphonValue::String(self.buf.clone())
    }

    // Generates a string and appends it directly to the StringBuilder,
    // avoiding the intermediate String allocation from generate_string.
    pub fn generate_string_into(
        &mut self,
        kind: ColumnKind,
        nullable: bool,
        builder: &mut StringBuilder,
    ) {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            builder.append_null();
            return;
        }

        self.write_string_to_buf(kind, bits);
        builder.append_value(&self.buf);
    }

    fn write_string_to_buf(&mut self, kind: ColumnKind, bits: u64) {
        let low = bits as u32;
        let high = (bits >> 32) as u32;

        self.buf.clear();
        match kind {
            ColumnKind::Name => {
                const PREFIXES: [&str; 8] = [
                    "alpha_", "beta_", "gamma_", "delta_", "epsilon_", "zeta_", "theta_", "omega_",
                ];
                self.buf.push_str(PREFIXES[low as usize % PREFIXES.len()]);
                push_hex(&mut self.buf, bits);
            }
            ColumnKind::Email => {
                const DOMAINS: [&str; 5] = [
                    "@example.com",
                    "@test.org",
                    "@demo.net",
                    "@sample.io",
                    "@mock.dev",
                ];
                self.buf.push_str("user");
                push_hex(&mut self.buf, bits & 0xffffff);
                self.buf.push_str(DOMAINS[low as usize % DOMAINS.len()]);
            }
            ColumnKind::Url => {
                self.buf.push_str("https://example.com/");
                push_hex(&mut self.buf, bits);
                self.buf.push('/');
                push_hex(&mut self.buf, high as u64);
            }
            ColumnKind::Path => {
                self.buf.push_str("/p");
                push_hex(&mut self.buf, bits & 0xff);
                self.buf.push_str("/d");
                push_hex(&mut self.buf, (bits >> 8) & 0xff);
                self.buf.push('/');
                push_hex(&mut self.buf, high as u64);
            }
            ColumnKind::Sha => {
                self.buf.reserve(40);
                let extra = self.next_random();
                let parts: [u64; 3] = [bits, low as u64, extra];
                for i in 0..40 {
                    let word = parts[i / 16];
                    let shift = (15 - (i % 16)) * 4;
                    let nibble = ((word >> shift) & 0xf) as usize;
                    self.buf.push(HEX_DIGITS[nibble] as char);
                }
            }
            ColumnKind::Description => {
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
                self.buf.push_str(WORDS[low as usize % WORDS.len()]);
                self.buf.push(' ');
                self.buf.push_str(WORDS[(low >> 8) as usize % WORDS.len()]);
                self.buf.push(' ');
                self.buf.push_str(WORDS[(low >> 16) as usize % WORDS.len()]);
                self.buf.push(' ');
                push_hex(&mut self.buf, bits & 0xffff);
            }
            ColumnKind::Status => {
                const STATUSES: [&str; 5] = ["open", "closed", "merged", "pending", "active"];
                self.buf.push_str(STATUSES[low as usize % STATUSES.len()]);
            }
            ColumnKind::State => {
                const STATES: [&str; 5] = ["pending", "running", "success", "failed", "canceled"];
                self.buf.push_str(STATES[low as usize % STATES.len()]);
            }
            ColumnKind::Branch => {
                const PREFIXES: [&str; 6] = [
                    "feature/branch-",
                    "fix/branch-",
                    "hotfix/branch-",
                    "release/branch-",
                    "main/branch-",
                    "develop/branch-",
                ];
                self.buf.push_str(PREFIXES[low as usize % PREFIXES.len()]);
                push_hex(&mut self.buf, bits & 0xffff);
            }
            ColumnKind::Uuid => {
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
            }
            ColumnKind::IdList => {
                use std::fmt::Write;
                let count = (low % MAX_LIST_LENGTH as u32) as usize;
                for j in 0..count {
                    if j > 0 {
                        self.buf.push('/');
                    }
                    let id_bits = if j == 0 { bits } else { self.next_random() };
                    let id_val = id_bits % MAX_ID_IN_LIST + 1;
                    let _ = write!(self.buf, "{}", id_val);
                }
            }
            _ => {
                self.buf.push_str("val");
                push_hex(&mut self.buf, bits);
            }
        }
    }

    pub fn generate_int64(&mut self, kind: ColumnKind, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        let low = bits as u32;

        match kind {
            ColumnKind::Id => SiphonValue::Int64((low % MAX_GENERATED_ID + 1) as i64),
            ColumnKind::Iid => SiphonValue::Int64((low % MAX_IID + 1) as i64),
            ColumnKind::DateTime => {
                let days_ago = ((bits >> 16) % MAX_DAYS_AGO) as i64;
                let hour_offset = ((bits >> 8) % 24) as i64;
                let micros = (days_ago * 86400 + hour_offset * 3600) * 1_000_000;
                SiphonValue::DateTime64(self.now_micros - micros)
            }
            _ => SiphonValue::Int64((low % MAX_GENERATED_ID + 1) as i64),
        }
    }

    pub fn generate_int8(&mut self, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        SiphonValue::Int8((bits % 10) as i8)
    }

    pub fn generate_bool(&mut self, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        SiphonValue::Bool(((bits >> 8) & 0xff) < 128)
    }

    pub fn generate_float64(&mut self, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        let fraction = (bits >> 32) as f64 / (u32::MAX as f64);
        SiphonValue::Float64(fraction * 10000.0)
    }

    pub fn generate_date32(&mut self, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        let days_ago = ((bits >> 16) as i32) % MAX_DAYS_AGO as i32;
        SiphonValue::Date32(-days_ago)
    }

    pub fn generate_datetime64(&mut self, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        let days_ago = ((bits >> 16) % MAX_DAYS_AGO) as i64;
        let hour_offset = ((bits >> 8) % 24) as i64;
        let micros = (days_ago * 86400 + hour_offset * 3600) * 1_000_000;
        SiphonValue::DateTime64(self.now_micros - micros)
    }

    pub fn generate_list_int64(&mut self, nullable: bool) -> SiphonValue {
        let bits = self.next_random();

        if nullable && (bits & 0xff) < NULL_THRESHOLD {
            return SiphonValue::Null;
        }

        let count = (bits % MAX_LIST_LENGTH) as usize;
        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            let id_bits = if i == 0 { bits } else { self.next_random() };
            values.push((id_bits % MAX_ID_IN_LIST + 1) as i64);
        }
        SiphonValue::ListInt64(values)
    }

    pub fn pick_from_pool(&mut self, pool: &[serde_json::Value]) -> SiphonValue {
        let index = self.next_random() as usize % pool.len();
        json_to_siphon_value(&pool[index])
    }
}

fn json_to_siphon_value(value: &serde_json::Value) -> SiphonValue {
    match value {
        serde_json::Value::String(s) => SiphonValue::String(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SiphonValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                SiphonValue::Float64(f)
            } else {
                SiphonValue::Null
            }
        }
        serde_json::Value::Bool(b) => SiphonValue::Bool(*b),
        serde_json::Value::Null => SiphonValue::Null,
        _ => SiphonValue::Null,
    }
}
