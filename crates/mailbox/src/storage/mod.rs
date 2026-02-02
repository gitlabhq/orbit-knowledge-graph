//! ClickHouse storage for plugin metadata and migrations.

mod migration_store;
mod plugin_store;
mod traversal_path;

pub use migration_store::MigrationStore;
pub use plugin_store::PluginStore;
pub use traversal_path::TraversalPathResolver;

pub const PLUGINS_TABLE: &str = "gl_mailbox_plugins";
pub const MIGRATIONS_TABLE: &str = "gl_mailbox_migrations";

pub fn plugins_table_ddl() -> &'static str {
    r#"CREATE TABLE IF NOT EXISTS gl_mailbox_plugins (
    plugin_id String,
    namespace_id Int64,
    api_key_hash String,
    schema String,
    schema_version Int64,
    created_at DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (namespace_id, plugin_id)"#
}

pub fn migrations_table_ddl() -> &'static str {
    r#"CREATE TABLE IF NOT EXISTS gl_mailbox_migrations (
    plugin_id String,
    schema_version Int64,
    node_kind String,
    table_name String,
    ddl_hash String,
    applied_at DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (plugin_id, schema_version, node_kind)"#
}
