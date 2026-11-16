use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::FutureExt;
use tokio::sync::OnceCell;

use super::format::{QueryStep, ScenarioConfig};
use super::seed_preset;
use crate::context::TestContext;
use crate::scenario::{self, Seed};

pub(super) enum Database {
    Base,
    Shared(String),
    Private(String),
}

impl Database {
    pub(super) fn for_scenario(config: &ScenarioConfig, steps: &[QueryStep], name: &str) -> Self {
        if steps.iter().any(|step| !step.seed.is_empty()) {
            return Self::Private(scenario::database_name(name));
        }
        if steps.iter().all(|step| step.expect.compile_error.is_some())
            || config.seed.is_none()
                && config.extra_seed.is_empty()
                && config.ontology_overlay.is_none()
        {
            return Self::Base;
        }
        let key = serde_json::to_string(&(
            &config.seed,
            &config.extra_seed,
            config.unmerged_seed,
            &config.ontology_overlay,
        ))
        .unwrap();
        Self::Shared(format!(
            "dataset_{}",
            &ontology::migrations::sha256_hex(&key)[..12]
        ))
    }
}

pub(super) struct Datasets {
    entries: HashMap<String, (OnceCell<Result<TestContext, String>>, AtomicUsize)>,
}

impl Datasets {
    pub(super) fn new<'a>(databases: impl Iterator<Item = &'a Database>) -> Self {
        let mut entries: HashMap<String, (OnceCell<Result<TestContext, String>>, AtomicUsize)> =
            HashMap::new();
        for database in databases {
            if let Database::Shared(name) = database {
                entries
                    .entry(name.clone())
                    .or_insert_with(|| (OnceCell::new(), AtomicUsize::new(0)))
                    .1
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Self { entries }
    }

    pub(super) async fn acquire(
        &self,
        name: &str,
        prepare: impl Future<Output = TestContext>,
    ) -> TestContext {
        let built = self.entries[name]
            .0
            .get_or_init(|| async {
                AssertUnwindSafe(prepare)
                    .catch_unwind()
                    .await
                    .map_err(|payload| crate::panic_message(&*payload))
            })
            .await;
        match built {
            Ok(database) => database.clone(),
            Err(message) => panic!("shared dataset {name} failed to build: {message}"),
        }
    }

    pub(super) async fn release(&self, base: &TestContext, name: &str) {
        if self.entries[name].1.fetch_sub(1, Ordering::AcqRel) == 1 {
            base.drop_fork(name).await;
        }
    }
}

pub(super) async fn prepare(
    base: &TestContext,
    config: &ScenarioConfig,
    database: &str,
    presets: &Path,
    name: &str,
) -> TestContext {
    let forked = match &config.seed {
        Some(_) => base.empty_fork(database).await,
        None => base.fork(database).await,
    };
    if let Some(overlay) = &config.ontology_overlay {
        let ontology = crate::load_ontology_overlay(overlay);
        let schema = orbit_migrations::schema::GraphSchema::from_ontology(&ontology).unwrap();
        let credentials = orbit_migrations::schema::DictionaryCredentials {
            database: forked.config.database.clone(),
            user: forked.config.username.clone(),
            password: forked.config.password.clone(),
        };
        orbit_migrations::execute::create_all_versioned_tables(
            &forked.create_client(),
            &schema,
            &credentials,
            &crate::TABLE_PREFIX,
        )
        .await
        .unwrap();
    }
    let columns = scenario::seed::fetch_table_columns(&forked).await;
    if let Some(preset) = &config.seed {
        let rows = seed_preset(presets, preset);
        scenario::seed::apply_seed(&forked, &rows, &Default::default(), &columns, name).await;
        forked.optimize_if_needed().await;
    }
    if config.unmerged_seed {
        let settings = std::collections::BTreeMap::from([(
            "optimize_on_insert".to_string(),
            serde_json::json!(0),
        )]);
        for table in config.extra_seed.keys() {
            let table = scenario::seed::prefix_graph_table(table);
            assert!(
                columns.contains_key(&table),
                "{name}: unknown table '{table}'"
            );
            forked.execute(&format!("SYSTEM STOP MERGES {table}")).await;
        }
        for (table, rows) in &config.extra_seed {
            for row in rows {
                let seed = Seed::from([(table.clone(), vec![row.clone()])]);
                scenario::seed::apply_seed(&forked, &seed, &settings, &columns, name).await;
            }
        }
    } else {
        scenario::seed::apply_seed(
            &forked,
            &config.extra_seed,
            &Default::default(),
            &columns,
            name,
        )
        .await;
        forked.optimize_if_needed().await;
    }
    forked
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_scenario::QueryScenario;

    const QUERY: &str = "query: {gql: 'MATCH (u:User {id: 1}) RETURN u'}\n";

    fn database(yaml: &str) -> Database {
        let scenario: QueryScenario = orbit_utils::yaml::from_str(yaml).unwrap();
        let (config, steps) = scenario.into_steps("case");
        Database::for_scenario(&config, &steps, "case")
    }

    fn shared(database: &Database) -> &str {
        match database {
            Database::Shared(name) => name,
            _ => panic!("expected a shared database"),
        }
    }

    #[test]
    fn cases_share_a_database_only_when_their_dataset_matches() {
        let seeded = |rows: &str| {
            format!(
                "config: {{extra_seed: {{gl_user: [{rows}]}}}}\n{QUERY}expect: {{node_count: 1}}"
            )
        };
        let (first, same, other) = (
            database(&seeded("{id: 1}")),
            database(&seeded("{id: 1}")),
            database(&seeded("{id: 2}")),
        );
        assert_eq!(shared(&first), shared(&same));
        assert_ne!(shared(&first), shared(&other));
        let preset = database(&format!(
            "config: {{seed: other}}\n{QUERY}expect: {{node_count: 1}}"
        ));
        assert_ne!(shared(&preset), shared(&first));

        let datasets = Datasets::new([&first, &same, &other].into_iter());
        let users =
            |database: &Database| datasets.entries[shared(database)].1.load(Ordering::Relaxed);
        assert_eq!((users(&first), users(&other)), (2, 1));
    }

    #[test]
    fn base_and_private_databases_follow_the_steps() {
        assert!(matches!(
            database(&format!("{QUERY}expect: {{node_count: 1}}")),
            Database::Base
        ));
        assert!(matches!(
            database(&format!(
                "config: {{extra_seed: {{gl_user: [{{id: 1}}]}}}}\n{QUERY}expect: {{compile_error: schema}}"
            )),
            Database::Base
        ));
        assert!(matches!(
            database(&format!(
                "steps:\n  - seed: {{gl_user: [{{id: 1}}]}}\n    {QUERY}    expect: {{node_count: 1}}"
            )),
            Database::Private(_)
        ));
    }
}
