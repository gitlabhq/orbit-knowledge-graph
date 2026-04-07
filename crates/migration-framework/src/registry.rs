use crate::types::Migration;

pub struct MigrationRegistry {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }

    pub fn register(&mut self, migration: Box<dyn Migration>) {
        if let Some(previous) = self.migrations.last() {
            assert!(
                migration.version() > previous.version(),
                "migration version {} must be strictly greater than previous version {}",
                migration.version(),
                previous.version()
            );
        }

        // V1 only executes additive migrations. Convergent and Finalization are
        // accepted here for forward compatibility; the reconciler introduced in
        // follow-up work enforces additive-only execution semantics.
        self.migrations.push(migration);
    }

    pub fn migrations(&self) -> &[Box<dyn Migration>] {
        &self.migrations
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }
}

impl Default for MigrationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub fn build_migration_registry() -> MigrationRegistry {
    MigrationRegistry::new()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_trait::async_trait;

    use crate::{Migration, MigrationContext, MigrationRegistry, MigrationType};

    struct TestMigration {
        version: u64,
        name: &'static str,
    }

    #[async_trait]
    impl Migration for TestMigration {
        fn version(&self) -> u64 {
            self.version
        }

        fn name(&self) -> &str {
            self.name
        }

        fn migration_type(&self) -> MigrationType {
            MigrationType::Additive
        }

        async fn prepare(&self, _ctx: &MigrationContext) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn registry_preserves_order() {
        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(TestMigration {
            version: 1,
            name: "one",
        }));
        registry.register(Box::new(TestMigration {
            version: 2,
            name: "two",
        }));

        let versions: Vec<u64> = registry
            .migrations()
            .iter()
            .map(|migration| migration.version())
            .collect();
        assert_eq!(versions, vec![1, 2]);
    }

    #[test]
    #[should_panic(expected = "must be strictly greater")]
    fn registry_panics_on_non_monotonic_versions() {
        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(TestMigration {
            version: 2,
            name: "two",
        }));
        registry.register(Box::new(TestMigration {
            version: 2,
            name: "duplicate",
        }));
    }
}
