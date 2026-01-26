//! Fake data generators for ontology data types.

use chrono::{Duration, Utc};
use fake::Fake;
use fake::faker::company::en::*;
use fake::faker::internet::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::rand::Rng;
use ontology::{DataType, Field};

/// Generates fake values for ontology fields.
pub struct FakeValueGenerator {
    rng: fake::rand::rngs::ThreadRng,
}

impl Default for FakeValueGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeValueGenerator {
    /// Create a new generator.
    pub fn new() -> Self {
        Self {
            rng: fake::rand::thread_rng(),
        }
    }

    /// Generate a fake value for a field.
    pub fn generate(&mut self, field: &Field) -> FakeValue {
        // Handle nullable fields - 10% chance of null
        if field.nullable && self.rng.gen_bool(0.1) {
            return FakeValue::Null;
        }

        match field.data_type {
            DataType::String => self.generate_string(&field.name),
            DataType::Int => self.generate_int(&field.name),
            DataType::Float => self.generate_float(&field.name),
            DataType::Bool => self.generate_bool(&field.name),
            DataType::Date => self.generate_date(),
            DataType::DateTime => self.generate_datetime(),
        }
    }

    /// Generate a string value based on field name hints.
    fn generate_string(&mut self, field_name: &str) -> FakeValue {
        let value = match field_name.to_lowercase().as_str() {
            "username" => Username().fake_with_rng(&mut self.rng),
            "email" | "public_email" => SafeEmail().fake_with_rng(&mut self.rng),
            "name" | "display_name" => Name().fake_with_rng(&mut self.rng),
            "first_name" => FirstName().fake_with_rng(&mut self.rng),
            "last_name" => LastName().fake_with_rng(&mut self.rng),
            "title" => Sentence(3..8).fake_with_rng(&mut self.rng),
            "description" => Paragraph(2..5).fake_with_rng(&mut self.rng),
            "full_path" | "path" => self.generate_path(),
            "sha" | "merge_commit_sha" => self.generate_sha(),
            "ref" | "source_branch" | "target_branch" => self.generate_branch_name(),
            "state" => self.pick_enum(&["opened", "closed", "merged", "locked"]),
            "status" => self.pick_enum(&[
                "created", "pending", "running", "success", "failed", "canceled",
            ]),
            "user_type" => self.pick_enum(&["human", "service_user", "project_bot", "ghost"]),
            "work_item_type" => {
                self.pick_enum(&["issue", "incident", "task", "epic", "requirement"])
            }
            "merge_status" => {
                self.pick_enum(&["unchecked", "can_be_merged", "cannot_be_merged", "checking"])
            }
            "source" => self.pick_enum(&["push", "web", "api", "schedule", "merge_request_event"]),
            "visibility_level" => self.pick_enum(&["private", "internal", "public"]),
            "failure_reason" => {
                self.pick_enum(&["unknown_failure", "config_error", "user_not_verified"])
            }
            "preferred_language" => self.pick_enum(&["en", "es", "fr", "de", "ja", "zh"]),
            "avatar_url" => format!(
                "https://gitlab.com/uploads/-/avatar/{}.png",
                self.rng.r#gen::<u32>()
            ),
            _ => Words(1..4)
                .fake_with_rng::<Vec<String>, _>(&mut self.rng)
                .join(" "),
        };
        FakeValue::String(value)
    }

    /// Generate an int value based on field name hints.
    fn generate_int(&mut self, field_name: &str) -> FakeValue {
        let value = match field_name.to_lowercase().as_str() {
            "iid" => self.rng.gen_range(1..10000) as i64,
            "weight" => self.rng.gen_range(1..20) as i64,
            "star_count" => self.rng.gen_range(0..5000) as i64,
            "duration" => self.rng.gen_range(60..7200) as i64, // 1 min to 2 hours in seconds
            _ => self.rng.gen_range(1..1000000) as i64,
        };
        FakeValue::Int(value)
    }

    /// Generate a float value.
    fn generate_float(&mut self, _field_name: &str) -> FakeValue {
        FakeValue::Float(self.rng.gen_range(0.0..100.0))
    }

    /// Generate a bool value based on field name hints.
    fn generate_bool(&mut self, field_name: &str) -> FakeValue {
        // Some fields have semantic defaults
        let probability = match field_name.to_lowercase().as_str() {
            "archived" => 0.05,    // 5% archived
            "confidential" => 0.1, // 10% confidential
            "draft" => 0.2,        // 20% draft
            "squash" => 0.3,       // 30% squash
            "private_profile" => 0.1,
            "is_admin" | "is_auditor" => 0.02,
            "is_external" => 0.05,
            "discussion_locked" => 0.05,
            "tag" => 0.1,
            _ => 0.5,
        };
        FakeValue::Bool(self.rng.gen_bool(probability))
    }

    /// Generate a date value (days since epoch).
    fn generate_date(&mut self) -> FakeValue {
        let days_ago = self.rng.gen_range(1..365 * 3); // Last 3 years
        let date = Utc::now() - Duration::days(days_ago);
        // Date32 is days since epoch
        let days_since_epoch = (date.timestamp() / 86400) as i32;
        FakeValue::Date(days_since_epoch)
    }

    /// Generate a datetime value (unix timestamp in milliseconds).
    fn generate_datetime(&mut self) -> FakeValue {
        let days_ago = self.rng.gen_range(1..365 * 3);
        let hours_offset = self.rng.gen_range(0..24);
        let date = Utc::now() - Duration::days(days_ago) - Duration::hours(hours_offset);
        FakeValue::DateTime(date.timestamp_millis())
    }

    /// Generate a path-like string.
    fn generate_path(&mut self) -> String {
        let count = self.rng.gen_range(2..5);
        let mut parts: Vec<String> = Vec::with_capacity(count);
        for _ in 0..count {
            let name: String = CompanyName().fake_with_rng(&mut self.rng);
            parts.push(name.to_lowercase().replace(' ', "-"));
        }
        parts.join("/")
    }

    /// Generate a git SHA.
    fn generate_sha(&mut self) -> String {
        (0..40)
            .map(|_| format!("{:x}", self.rng.r#gen::<u8>() % 16))
            .collect()
    }

    /// Generate a branch name.
    fn generate_branch_name(&mut self) -> String {
        let prefixes = ["feature", "fix", "hotfix", "release", "main", "develop"];
        let prefix = prefixes[self.rng.gen_range(0..prefixes.len())];
        let suffix: String = Words(1..3)
            .fake_with_rng::<Vec<String>, _>(&mut self.rng)
            .join("-");
        format!("{}/{}", prefix, suffix.to_lowercase())
    }

    /// Pick a random value from an enum list.
    fn pick_enum(&mut self, values: &[&str]) -> String {
        values[self.rng.gen_range(0..values.len())].to_string()
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
    Date(i32),     // Days since epoch
    DateTime(i64), // Milliseconds since epoch
}

impl FakeValue {
    /// Check if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, FakeValue::Null)
    }
}
