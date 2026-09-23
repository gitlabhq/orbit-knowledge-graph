use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanScenario {
    pub name: String,
    pub input: Value,
    #[serde(default)]
    pub expect: Vec<String>,
    #[serde(default)]
    pub reject: Vec<String>,
    #[serde(default)]
    pub plan: Option<String>,
}

impl PlanScenario {
    pub fn query(&self) -> String {
        match &self.input {
            Value::String(query) => query.clone(),
            input => serde_json::to_string(input).expect("plan scenario input serializes"),
        }
    }

    pub fn validate(&self) {
        assert!(
            !self.expect.is_empty() || !self.reject.is_empty() || self.plan.is_some(),
            "{}: fixture has no expect, reject, or plan",
            self.name
        );
    }
}
