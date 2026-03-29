//! Fast data generators for ontology data types.

use crate::synth::config::{FakeDataConfig, StringKind};
use chrono::Utc;
use ontology::{DataType, Field};
use rand::rngs::Xoshiro256PlusPlus;
use rand::{RngExt, SeedableRng};
use std::borrow::Cow;
use std::collections::HashMap;

/// Hex digits for fast formatting (avoids format! parsing overhead).
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Fast hex formatting into a String buffer.
#[inline]
#[allow(unsafe_code)]
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
    // SAFETY: we only write ASCII hex digits (0-9, a-f) into positions
    // that already contain ASCII '0', so the buffer remains valid UTF-8.
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

/// Pre-computed field generation strategy.
///
/// String variants carry the `StringKind` from YAML classification rules.
/// Bool and Int carry pre-resolved parameters so generation is a single
/// comparison / modulo with no HashMap lookup on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// String field classified by YAML rules.
    String(StringKind),
    /// String field that matched no classification rule.
    GenericString,
    /// Bool field with pre-computed byte threshold (random_byte < threshold → true).
    Bool(u8),
    /// Int field with pre-computed (min, range_size) for `min + (random % range_size)`.
    Int(u32, u32),
    Float,
    Date,
    DateTime,
    Enum,
    Uuid,
}

impl FieldKind {
    /// Classify a field once at startup using data-driven rules from pools.
    pub fn classify(field: &Field, pools: &FakeDataPools) -> Self {
        if field.enum_values.is_some() {
            return FieldKind::Enum;
        }

        match field.data_type {
            DataType::Enum => FieldKind::Enum,
            DataType::Float => FieldKind::Float,
            DataType::Date => FieldKind::Date,
            DataType::DateTime => FieldKind::DateTime,
            DataType::Uuid => FieldKind::Uuid,
            DataType::String => pools.classify_string(&field.name),
            DataType::Int => pools.classify_int(&field.name),
            DataType::Bool => pools.classify_bool(&field.name),
        }
    }
}

/// A leaked string classification rule for runtime use.
pub struct StaticStringRule {
    pub patterns: &'static [&'static str],
    pub kind: StringKind,
}

/// Interned runtime pools for fake data generation.
///
/// String pools are `Box::leak`ed to get `&'static str` slices — the data lives
/// for the entire program lifetime (trivial size, few hundred bytes) and avoids
/// per-row allocation overhead. This gives identical perf to the old hardcoded
/// `const` arrays while allowing the values to come from YAML.
pub struct FakeDataPools {
    // String pools — leaked to &'static str for Cow::Borrowed returns
    pub name_prefixes: &'static [&'static str],
    pub email_domains: &'static [&'static str],
    pub description_words: &'static [&'static str],
    pub statuses: &'static [&'static str],
    pub states: &'static [&'static str],
    pub branch_prefixes: &'static [&'static str],

    // String classification rules (order matters — first match wins)
    pub string_rules: &'static [StaticStringRule],

    // Bool thresholds — field_name → u8 byte threshold
    pub bool_thresholds: HashMap<&'static str, u8>,
    pub default_bool_threshold: u8,

    // Int ranges — field_name → (min, range_size)
    pub int_ranges: HashMap<&'static str, (u32, u32)>,
    pub default_int_range: (u32, u32),
}

/// Leak a Vec<String> into a &'static [&'static str].
fn leak_string_pool(strings: Vec<String>) -> &'static [&'static str] {
    let leaked: Vec<&'static str> = strings
        .into_iter()
        .map(|s| -> &'static str { Box::leak(s.into_boxed_str()) })
        .collect();
    Box::leak(leaked.into_boxed_slice())
}

/// Convert a probability (0.0–1.0) to a byte threshold for `(random_byte < threshold)`.
fn prob_to_threshold(p: f64) -> u8 {
    (p * 256.0).ceil().min(255.0) as u8
}

/// Convert [min, max] inclusive range to (min, range_size) for `min + (random % range_size)`.
fn range_to_params(range: [u32; 2]) -> (u32, u32) {
    let min = range[0];
    let max = range[1];
    (min, max - min + 1)
}

impl FakeDataPools {
    /// Intern a `FakeDataConfig` into leaked static pools.
    pub fn intern(config: FakeDataConfig) -> &'static Self {
        let string_rules: Vec<StaticStringRule> = config
            .strings
            .classify
            .into_iter()
            .map(|rule| StaticStringRule {
                patterns: leak_string_pool(rule.contains),
                kind: rule.kind,
            })
            .collect();

        let bool_thresholds: HashMap<&'static str, u8> = config
            .bools
            .fields
            .into_iter()
            .map(|(k, v)| {
                let key: &'static str = Box::leak(k.into_boxed_str());
                (key, prob_to_threshold(v))
            })
            .collect();

        let int_ranges: HashMap<&'static str, (u32, u32)> = config
            .ints
            .fields
            .into_iter()
            .map(|(k, v)| {
                let key: &'static str = Box::leak(k.into_boxed_str());
                (key, range_to_params(v))
            })
            .collect();

        let pools = Self {
            name_prefixes: leak_string_pool(config.strings.pools.name_prefixes),
            email_domains: leak_string_pool(config.strings.pools.email_domains),
            description_words: leak_string_pool(config.strings.pools.description_words),
            statuses: leak_string_pool(config.strings.pools.statuses),
            states: leak_string_pool(config.strings.pools.states),
            branch_prefixes: leak_string_pool(config.strings.pools.branch_prefixes),

            string_rules: Box::leak(string_rules.into_boxed_slice()),

            default_bool_threshold: prob_to_threshold(config.bools.default),
            bool_thresholds,

            default_int_range: range_to_params(config.ints.default),
            int_ranges,
        };
        Box::leak(Box::new(pools))
    }

    /// Classify a string field using YAML rules (first match wins).
    fn classify_string(&self, name: &str) -> FieldKind {
        let lower = name.to_lowercase();
        for rule in self.string_rules {
            if rule.patterns.iter().any(|p| lower.contains(p)) {
                return FieldKind::String(rule.kind);
            }
        }
        FieldKind::GenericString
    }

    /// Classify a bool field — look up threshold by name, fall back to default.
    fn classify_bool(&self, name: &str) -> FieldKind {
        let lower = name.to_lowercase();
        let threshold = self
            .bool_thresholds
            .get(lower.as_str())
            .copied()
            .unwrap_or(self.default_bool_threshold);
        FieldKind::Bool(threshold)
    }

    /// Classify an int field — look up range by name, fall back to default.
    fn classify_int(&self, name: &str) -> FieldKind {
        let lower = name.to_lowercase();
        let (min, range) = self
            .int_ranges
            .get(lower.as_str())
            .copied()
            .unwrap_or(self.default_int_range);
        FieldKind::Int(min, range)
    }
}

/// Generates values for ontology fields using minimal randomness.
pub struct FakeValueGenerator {
    rng: Xoshiro256PlusPlus,
    counter: u64,
    /// Cached current timestamp to avoid repeated syscalls.
    now_millis: i64,
    /// Reusable string buffer to avoid allocations.
    buf: String,
    pools: &'static FakeDataPools,
}

impl FakeValueGenerator {
    pub fn new(pools: &'static FakeDataPools) -> Self {
        Self {
            rng: Xoshiro256PlusPlus::from_rng(&mut rand::rng()),
            counter: 0,
            now_millis: Utc::now().timestamp_millis(),
            buf: String::with_capacity(64),
            pools,
        }
    }

    pub fn new_fast(pools: &'static FakeDataPools) -> Self {
        Self::new(pools)
    }

    pub fn with_seed(seed: u64, pools: &'static FakeDataPools) -> Self {
        Self {
            rng: Xoshiro256PlusPlus::seed_from_u64(seed),
            counter: 0,
            now_millis: Utc::now().timestamp_millis(),
            buf: String::with_capacity(64),
            pools,
        }
    }

    pub fn fast_with_seed(seed: u64, pools: &'static FakeDataPools) -> Self {
        Self::with_seed(seed, pools)
    }

    /// Clone the buffer contents and return as Cow::Owned. Buffer is reused next call.
    #[inline]
    fn emit_buf(&self) -> Cow<'static, str> {
        Cow::Owned(self.buf.clone())
    }

    #[inline]
    fn generate_string(&mut self, sk: StringKind, bits: u64, low: u32, high: u32) -> FakeValue {
        let p = self.pools;
        match sk {
            StringKind::NameOrTitle => {
                let prefix = p.name_prefixes[low as usize % p.name_prefixes.len()];
                self.buf.clear();
                self.buf.push_str(prefix);
                push_hex_u64(&mut self.buf, bits);
                FakeValue::String(self.emit_buf())
            }
            StringKind::Email => {
                let domain = p.email_domains[low as usize % p.email_domains.len()];
                self.buf.clear();
                self.buf.push_str("user");
                push_hex_u64(&mut self.buf, bits & 0xffffff);
                self.buf.push_str(domain);
                FakeValue::String(self.emit_buf())
            }
            StringKind::Url => {
                self.buf.clear();
                self.buf.push_str("https://example.com/");
                push_hex_u64(&mut self.buf, bits);
                self.buf.push('/');
                push_hex_u16(&mut self.buf, high as u16);
                FakeValue::String(self.emit_buf())
            }
            StringKind::Path => {
                self.buf.clear();
                self.buf.push_str("/p");
                push_hex_u64(&mut self.buf, bits & 0xff);
                self.buf.push_str("/d");
                push_hex_u64(&mut self.buf, (bits >> 8) & 0xff);
                self.buf.push('/');
                push_hex_u16(&mut self.buf, high as u16);
                FakeValue::String(self.emit_buf())
            }
            StringKind::ShaOrHash => {
                self.buf.clear();
                self.buf.reserve(40);
                let lo128 = ((bits as u128) << 64) | (low as u128);
                for i in (0..8).rev() {
                    let nibble = ((high >> (i * 4)) & 0xf) as usize;
                    self.buf.push(HEX_DIGITS[nibble] as char);
                }
                for i in (0..32).rev() {
                    let nibble = ((lo128 >> (i * 4)) & 0xf) as usize;
                    self.buf.push(HEX_DIGITS[nibble] as char);
                }
                FakeValue::String(self.emit_buf())
            }
            StringKind::DescriptionOrBody => {
                let words = p.description_words;
                let w1 = words[low as usize % words.len()];
                let w2 = words[(low >> 8) as usize % words.len()];
                let w3 = words[(low >> 16) as usize % words.len()];
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
            StringKind::Status => {
                FakeValue::static_string(p.statuses[low as usize % p.statuses.len()])
            }
            StringKind::State => FakeValue::static_string(p.states[low as usize % p.states.len()]),
            StringKind::RefOrBranch => {
                let prefix = p.branch_prefixes[low as usize % p.branch_prefixes.len()];
                self.buf.clear();
                self.buf.push_str(prefix);
                push_hex_u64(&mut self.buf, bits & 0xffff);
                FakeValue::String(self.emit_buf())
            }
        }
    }

    fn generate_uuid(&mut self, bits: u64) -> FakeValue {
        let bits2 = self.next_random();
        self.buf.clear();
        self.buf.reserve(36);
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

    /// Generate a single u64 and use the counter for mixing.
    #[inline]
    fn next_random(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(1);
        let r = self.rng.random::<u64>();
        r ^ self.counter.wrapping_mul(0x9e3779b97f4a7c15)
    }

    /// Generate a value for a field. Use FieldKind::classify() once per field,
    /// then call generate_with_kind() for each row.
    pub fn generate(&mut self, field: &Field) -> FakeValue {
        let kind = FieldKind::classify(field, self.pools);
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
            FieldKind::String(sk) => self.generate_string(sk, bits, low, high),
            FieldKind::GenericString => {
                self.buf.clear();
                self.buf.push_str("val");
                push_hex_u64(&mut self.buf, bits);
                FakeValue::String(self.emit_buf())
            }
            FieldKind::Int(min, range) => FakeValue::Int((min + low % range) as i64),
            FieldKind::Bool(threshold) => FakeValue::Bool(((bits >> 8) & 0xff) < threshold as u64),
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
            FieldKind::Uuid => self.generate_uuid(bits),
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

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{DataType, FieldSource};
    use std::collections::BTreeMap;

    fn fake_data_path() -> String {
        crate::synth::fixture_path(crate::synth::constants::DEFAULT_FAKE_DATA_PATH)
    }

    fn test_pools() -> &'static FakeDataPools {
        FakeDataPools::intern(FakeDataConfig::load(fake_data_path()).unwrap())
    }

    #[test]
    fn test_classify_enum_takes_priority() {
        let pools = test_pools();
        let mut enum_vals = BTreeMap::new();
        enum_vals.insert(1, "open".to_string());
        enum_vals.insert(2, "closed".to_string());

        let field = Field {
            name: "status".to_string(),
            source: FieldSource::DatabaseColumn("status".to_string()),
            data_type: DataType::String,
            nullable: true,
            enum_values: Some(enum_vals),
            enum_type: ontology::EnumType::default(),
            ..Default::default()
        };

        assert_eq!(FieldKind::classify(&field, pools), FieldKind::Enum);
    }

    #[test]
    fn test_classify_dispatches_by_data_type() {
        let pools = test_pools();
        let make = |name: &str, dt: DataType| Field {
            name: name.to_string(),
            source: FieldSource::DatabaseColumn(name.to_string()),
            data_type: dt,
            nullable: false,
            enum_values: None,
            enum_type: ontology::EnumType::default(),
            ..Default::default()
        };

        assert_eq!(
            FieldKind::classify(&make("score", DataType::Float), pools),
            FieldKind::Float
        );
        assert_eq!(
            FieldKind::classify(&make("created_at", DataType::DateTime), pools),
            FieldKind::DateTime
        );
        assert_eq!(
            FieldKind::classify(&make("due_date", DataType::Date), pools),
            FieldKind::Date
        );
        assert_eq!(
            FieldKind::classify(&make("uuid", DataType::Uuid), pools),
            FieldKind::Uuid
        );
    }

    #[test]
    #[ignore = "audit helper: run manually to check string heuristic coverage on ontology fields"]
    fn test_classify_ontology_fields_no_generic_string() {
        let pools = test_pools();
        let ontology = ontology::Ontology::load_embedded().expect("should load embedded ontology");

        let mut generic_fields = Vec::new();
        for node in ontology.nodes() {
            for field in &node.fields {
                let kind = FieldKind::classify(field, pools);
                if kind == FieldKind::GenericString {
                    generic_fields.push(format!("{}.{}", node.name, field.name));
                }
            }
        }

        assert!(
            generic_fields.is_empty(),
            "String fields classified as generic ({} total). Add rules to fake_data.yaml:\n  {}",
            generic_fields.len(),
            generic_fields.join("\n  ")
        );
    }

    #[test]
    fn test_generate_produces_non_null_for_non_nullable() {
        let field = Field {
            name: "name".to_string(),
            source: FieldSource::DatabaseColumn("name".to_string()),
            data_type: DataType::String,
            nullable: false,
            enum_values: None,
            enum_type: ontology::EnumType::default(),
            ..Default::default()
        };

        let mut fvg = FakeValueGenerator::with_seed(42, test_pools());
        for _ in 0..1000 {
            let val = fvg.generate(&field);
            assert!(
                !val.is_null(),
                "Non-nullable field should never produce Null"
            );
        }
    }

    #[test]
    fn test_generate_enum_uses_defined_values() {
        let mut enum_vals = BTreeMap::new();
        enum_vals.insert(1, "alpha".to_string());
        enum_vals.insert(2, "beta".to_string());
        enum_vals.insert(3, "gamma".to_string());

        let field = Field {
            name: "category".to_string(),
            source: FieldSource::DatabaseColumn("category".to_string()),
            data_type: DataType::Enum,
            nullable: false,
            enum_values: Some(enum_vals),
            enum_type: ontology::EnumType::default(),
            ..Default::default()
        };

        let mut fvg = FakeValueGenerator::with_seed(42, test_pools());
        let valid = ["alpha", "beta", "gamma"];

        for _ in 0..100 {
            match fvg.generate(&field) {
                FakeValue::String(s) => {
                    assert!(
                        valid.contains(&s.as_ref()),
                        "Enum value '{}' not in defined values",
                        s
                    );
                }
                other => panic!("Expected String, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_generate_deterministic_with_seed() {
        let field = Field {
            name: "name".to_string(),
            source: FieldSource::DatabaseColumn("name".to_string()),
            data_type: DataType::String,
            nullable: false,
            enum_values: None,
            enum_type: ontology::EnumType::default(),
            ..Default::default()
        };

        let pools = test_pools();
        let mut fvg1 = FakeValueGenerator::with_seed(123, pools);
        let mut fvg2 = FakeValueGenerator::with_seed(123, pools);

        for _ in 0..50 {
            let v1 = format!("{:?}", fvg1.generate(&field));
            let v2 = format!("{:?}", fvg2.generate(&field));
            assert_eq!(v1, v2, "Same seed should produce identical output");
        }
    }

    #[test]
    fn test_prob_to_threshold() {
        assert_eq!(prob_to_threshold(0.0), 0);
        assert_eq!(prob_to_threshold(1.0), 255);
        assert_eq!(prob_to_threshold(0.50), 128);
        assert_eq!(prob_to_threshold(0.05), 13);
        assert_eq!(prob_to_threshold(0.10), 26);
        assert_eq!(prob_to_threshold(0.02), 6);
    }

    #[test]
    fn test_range_to_params() {
        assert_eq!(range_to_params([1, 10000]), (1, 10000));
        assert_eq!(range_to_params([0, 4999]), (0, 5000));
        assert_eq!(range_to_params([60, 7199]), (60, 7140));
    }

    #[test]
    fn test_yaml_loads_and_interns() {
        let config = FakeDataConfig::load(fake_data_path()).unwrap();
        let pools = FakeDataPools::intern(config);

        assert!(!pools.name_prefixes.is_empty());
        assert!(!pools.email_domains.is_empty());
        assert!(!pools.description_words.is_empty());
        assert!(!pools.statuses.is_empty());
        assert!(!pools.states.is_empty());
        assert!(!pools.branch_prefixes.is_empty());
        assert!(!pools.string_rules.is_empty());
        assert!(!pools.bool_thresholds.is_empty());
        assert!(!pools.int_ranges.is_empty());
    }

    #[test]
    fn test_custom_pools_affect_output() {
        let mut config = FakeDataConfig::load(fake_data_path()).unwrap();
        config.strings.pools.statuses = vec!["custom_status".to_string()];
        let pools = FakeDataPools::intern(config);

        let mut fvg = FakeValueGenerator::with_seed(42, pools);
        let val = fvg.generate_with_kind(FieldKind::String(StringKind::Status), false, None);
        match val {
            FakeValue::String(s) => assert_eq!(s.as_ref(), "custom_status"),
            other => panic!("Expected String, got {:?}", other),
        }
    }
}
