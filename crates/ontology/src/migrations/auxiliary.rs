use serde::{Deserialize, Serialize};

pub const AUXILIARY_LEDGER_FILE: &str = "auxiliary-migrations.yaml";

const EMBEDDED_LEDGER: &str =
    include_str!(concat!(env!("CONFIG_DIR"), "/auxiliary-migrations.yaml"));

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryLedger {
    #[serde(default)]
    pub migrations: Vec<AuxiliaryMigration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryMigration {
    pub id: u32,
    pub sql: String,
    #[serde(default)]
    pub note: Option<String>,
}

impl AuxiliaryLedger {
    pub fn parse(content: &str) -> Result<Self, String> {
        let ledger: Self = serde_yaml::from_str(content)
            .map_err(|e| format!("parsing auxiliary migration ledger: {e}"))?;
        ledger.validate()?;
        Ok(ledger)
    }

    pub fn load_embedded() -> Result<Self, String> {
        Self::parse(EMBEDDED_LEDGER)
    }

    fn validate(&self) -> Result<(), String> {
        for (i, entry) in self.migrations.iter().enumerate() {
            if i > 0 && entry.id <= self.migrations[i - 1].id {
                return Err(format!(
                    "auxiliary migration IDs must be strictly ascending: \
                     id {} follows id {}",
                    entry.id,
                    self.migrations[i - 1].id
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_ledger() {
        let ledger = AuxiliaryLedger::parse("migrations: []\n").unwrap();
        assert!(ledger.migrations.is_empty());
    }

    #[test]
    fn parses_ledger_with_entries() {
        let yaml = "migrations:\n  - id: 1\n    sql: DROP TABLE IF EXISTS old\n  - id: 2\n    sql: ALTER TABLE t ADD COLUMN IF NOT EXISTS c Int64\n    note: added column\n";
        let ledger = AuxiliaryLedger::parse(yaml).unwrap();
        assert_eq!(ledger.migrations.len(), 2);
        assert_eq!(ledger.migrations[0].id, 1);
        assert_eq!(ledger.migrations[1].id, 2);
    }

    #[test]
    fn rejects_non_ascending_ids() {
        let yaml = "migrations:\n  - id: 2\n    sql: SELECT 1\n  - id: 1\n    sql: SELECT 2\n";
        assert!(AuxiliaryLedger::parse(yaml).is_err());
    }

    #[test]
    fn rejects_duplicate_ids() {
        let yaml = "migrations:\n  - id: 1\n    sql: SELECT 1\n  - id: 1\n    sql: SELECT 2\n";
        assert!(AuxiliaryLedger::parse(yaml).is_err());
    }

    #[test]
    fn embedded_ledger_loads() {
        AuxiliaryLedger::load_embedded().unwrap();
    }
}
