//! The demo ClickHouse: queries run as the read-only user, and only the seed step signs in as the
//! user that may create, truncate and insert.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, bail};
use clickhouse_client::ArrowClickHouseClient;
use query_data_model::{Catalog, ClickHouse};

use crate::config::{Checked, Login};
use crate::seed::TableRows;

pub struct Store {
    pub reader: Arc<ArrowClickHouseClient>,
    seeder: ArrowClickHouseClient,
    database: String,
}

fn client(address: SocketAddr, database: &str, login: &Login) -> ArrowClickHouseClient {
    ArrowClickHouseClient::new(
        &format!("http://{address}"),
        database,
        &login.user,
        Some(&login.password),
        &HashMap::new(),
        &HashMap::new(),
        "orbit-demo",
    )
}

impl Store {
    pub fn new(checked: &Checked) -> Self {
        let config = &checked.config.clickhouse;
        Self {
            reader: Arc::new(client(checked.clickhouse, &config.database, &config.reader)),
            seeder: client(checked.clickhouse, &config.database, &config.seeder),
            database: config.database.clone(),
        }
    }

    /// Recreates every graph table of the model, so the schema always matches the packages.
    pub async fn create(
        &self,
        catalog: &Catalog<ClickHouse>,
        address: SocketAddr,
        login: &Login,
    ) -> anyhow::Result<()> {
        let bootstrap = client(address, "default", login);
        bootstrap
            .execute(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .await
            .context("creating the demo database")?;
        let schema = catalog
            .schema()
            .context("the model has no ClickHouse schema")?;
        for table in schema.tables() {
            self.seeder
                .execute(&format!("DROP TABLE IF EXISTS {}", table.name))
                .await?;
            self.seeder
                .execute(&table.to_create_sql(""))
                .await
                .with_context(|| format!("creating {}", table.name))?;
        }
        Ok(())
    }

    /// Empties every graph table and inserts the seeded rows.
    pub async fn reload(
        &self,
        catalog: &Catalog<ClickHouse>,
        tables: &[TableRows],
    ) -> anyhow::Result<usize> {
        let schema = catalog
            .schema()
            .context("the model has no ClickHouse schema")?;
        let known: BTreeSet<&str> = schema
            .tables()
            .iter()
            .map(|table| table.name.as_str())
            .collect();
        for seeded in tables {
            if !known.contains(seeded.table.as_str()) {
                bail!(
                    "the seed names table {}, which the packages do not declare",
                    seeded.table
                );
            }
        }
        for table in &known {
            self.seeder
                .execute(&format!("TRUNCATE TABLE {table}"))
                .await?;
        }
        let mut inserted = 0;
        for seeded in tables.iter().filter(|seeded| !seeded.rows.is_empty()) {
            let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", seeded.table);
            for row in &seeded.rows {
                body.push_str(&serde_json::to_string(row)?.replace('?', "??"));
                body.push('\n');
            }
            self.seeder
                .execute(&body)
                .await
                .with_context(|| format!("seeding {}", seeded.table))?;
            inserted += seeded.rows.len();
        }
        Ok(inserted)
    }
}
