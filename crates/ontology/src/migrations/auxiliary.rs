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
        let ledger: Self = orbit_utils::yaml::from_str(content)
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
    fn parses_and_validates() {
        assert!(
            AuxiliaryLedger::parse("migrations: []\n")
                .unwrap()
                .migrations
                .is_empty()
        );

        let two = AuxiliaryLedger::parse("migrations:\n  - id: 1\n    sql: DROP TABLE IF EXISTS old\n  - id: 2\n    sql: ALTER TABLE t ADD COLUMN IF NOT EXISTS c Int64\n    note: added\n").unwrap();
        assert_eq!(two.migrations.len(), 2);

        assert!(
            AuxiliaryLedger::parse("migrations:\n  - id: 2\n    sql: S\n  - id: 1\n    sql: S\n")
                .is_err()
        );
        assert!(
            AuxiliaryLedger::parse("migrations:\n  - id: 1\n    sql: S\n  - id: 1\n    sql: S\n")
                .is_err()
        );
    }

    #[test]
    fn embedded_ledger_loads() {
        AuxiliaryLedger::load_embedded().unwrap();
    }
}
