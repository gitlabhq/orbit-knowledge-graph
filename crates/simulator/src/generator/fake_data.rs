//! Fast data generators for ontology data types.

use chrono::Utc;
use ontology::{DataType, Field};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Generates values for ontology fields using minimal randomness.
pub struct FakeValueGenerator {
    rng: StdRng,
    counter: u64,
    /// Cached current timestamp to avoid repeated syscalls.
    now_millis: i64,
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
        }
    }

    pub fn fast_with_seed(seed: u64) -> Self {
        Self::with_seed(seed)
    }

    /// Generate a single u64 and use the counter for mixing.
    #[inline]
    fn next_random(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(1);
        let r = self.rng.r#gen::<u64>();
        r ^ self.counter.wrapping_mul(0x9e3779b97f4a7c15)
    }

    pub fn generate(&mut self, field: &Field) -> FakeValue {
        let bits = self.next_random();

        // Use lowest bits for nullable check (10% ≈ 26/256)
        if field.nullable && (bits & 0xff) < 26 {
            return FakeValue::Null;
        }

        if field.enum_values.is_some() {
            return self.generate_enum_from_bits(field, bits);
        }

        match field.data_type {
            DataType::Enum => self.generate_enum_from_bits(field, bits),
            DataType::String => self.generate_string_from_bits(&field.name, bits),
            DataType::Int => self.generate_int_from_bits(&field.name, bits),
            DataType::Float => {
                // Use upper 32 bits for float
                let f = (bits >> 32) as f64 / (u32::MAX as f64);
                FakeValue::Float(f * 10000.0)
            }
            DataType::Bool => self.generate_bool_from_bits(&field.name, bits),
            DataType::Date => {
                // Use bits for days in range [0, 1825) (5 years)
                let days_ago = ((bits >> 16) as i32) % 1825;
                FakeValue::Date(-days_ago)
            }
            DataType::DateTime => {
                // Extract days and hours from bits
                let days_ago = ((bits >> 16) % 1825) as i64;
                let hour_offset = ((bits >> 8) % 24) as i64;
                let millis = (days_ago * 86400 + hour_offset * 3600) * 1000;
                FakeValue::DateTime(self.now_millis - millis)
            }
        }
    }

    #[inline]
    fn generate_string_from_bits(&self, field_name: &str, bits: u64) -> FakeValue {
        let low = bits as u32;
        let high = (bits >> 32) as u32;
        let mixed = bits;

        let value = match field_name.to_lowercase().as_str() {
            name if name.contains("name") || name.contains("title") => {
                const PREFIXES: [&str; 8] =
                    ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta", "omega"];
                let prefix = PREFIXES[low as usize % PREFIXES.len()];
                format!("{}_{:x}", prefix, mixed)
            }
            name if name.contains("email") => {
                const DOMAINS: [&str; 5] =
                    ["example.com", "test.org", "demo.net", "sample.io", "mock.dev"];
                let domain = DOMAINS[low as usize % DOMAINS.len()];
                format!("user{:x}@{}", mixed & 0xffffff, domain)
            }
            name if name.contains("url") => {
                format!("https://example.com/{:x}/{:x}", mixed, high as u16)
            }
            name if name.contains("path") => {
                format!(
                    "/p{:x}/d{:x}/{:x}",
                    mixed & 0xff,
                    (mixed >> 8) & 0xff,
                    high as u16
                )
            }
            name if name.contains("sha") || name.contains("hash") => {
                format!("{:040x}", ((mixed as u128) << 64) | (low as u128))
            }
            name if name.contains("description") || name.contains("body") => {
                const WORDS: [&str; 12] = [
                    "Lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
                    "adipiscing", "elit", "sed", "do", "eiusmod", "tempor",
                ];
                let w1 = WORDS[low as usize % WORDS.len()];
                let w2 = WORDS[(low >> 8) as usize % WORDS.len()];
                let w3 = WORDS[(low >> 16) as usize % WORDS.len()];
                format!("{} {} {} {:x}", w1, w2, w3, mixed & 0xffff)
            }
            name if name.contains("status") => {
                const STATUSES: [&str; 5] = ["open", "closed", "merged", "pending", "active"];
                STATUSES[low as usize % STATUSES.len()].to_string()
            }
            name if name.contains("state") => {
                const STATES: [&str; 5] = ["pending", "running", "success", "failed", "canceled"];
                STATES[low as usize % STATES.len()].to_string()
            }
            name if name.contains("ref") || name.contains("branch") => {
                const PREFIXES: [&str; 6] = ["feature", "fix", "hotfix", "release", "main", "develop"];
                let prefix = PREFIXES[low as usize % PREFIXES.len()];
                format!("{}/branch-{:x}", prefix, mixed & 0xffff)
            }
            _ => {
                format!("val{:x}", mixed)
            }
        };
        FakeValue::String(value)
    }

    #[inline]
    fn generate_int_from_bits(&self, field_name: &str, bits: u64) -> FakeValue {
        let low = bits as u32;
        let value = match field_name.to_lowercase().as_str() {
            "iid" => (low % 10000 + 1) as i64,
            "weight" => (low % 19 + 1) as i64,
            "star_count" => (low % 5000) as i64,
            "duration" => (low % 7140 + 60) as i64,
            _ => (low % 99999 + 1) as i64,
        };
        FakeValue::Int(value)
    }

    #[inline]
    fn generate_bool_from_bits(&self, field_name: &str, bits: u64) -> FakeValue {
        // Use bits 8-15 for bool probability (0-255 scale)
        let threshold = match field_name.to_lowercase().as_str() {
            "archived" => 13,            // 5%
            "confidential" => 26,        // 10%
            "draft" => 51,               // 20%
            "squash" => 77,              // 30%
            "private_profile" => 26,     // 10%
            "is_admin" | "is_auditor" => 5, // 2%
            "is_external" => 13,         // 5%
            "discussion_locked" => 13,   // 5%
            "tag" => 26,                 // 10%
            _ => 128,                    // 50%
        };
        FakeValue::Bool(((bits >> 8) & 0xff) < threshold)
    }

    #[inline]
    fn generate_enum_from_bits(&self, field: &Field, bits: u64) -> FakeValue {
        if let Some(enum_values) = &field.enum_values {
            let values: Vec<&String> = enum_values.values().collect();
            if !values.is_empty() {
                let index = (bits as usize) % values.len();
                return FakeValue::String(values[index].clone());
            }
        }
        FakeValue::String("unknown".to_string())
    }
}

/// A generated fake value.
#[derive(Debug, Clone)]
pub enum FakeValue {
    Null,
    String(String),
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
}
