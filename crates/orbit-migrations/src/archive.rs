use std::path::{Path, PathBuf};

use orbit_utils::source_archive::{
    ArchiveLayout, SourceArchive, SourceArchiveError, file_name, parse_file_name,
};
use query_data_model::{Packages, Tier};
use rust_embed::Embed;

const LAYOUT: ArchiveLayout = ArchiveLayout {
    directory: "model-archives",
    root: "model",
};

#[derive(Embed)]
#[folder = "$ORBIT_CONFIG_DIR/model-archives"]
#[include = "*.tar.gz"]
struct BundledArchives;

#[derive(Debug)]
pub struct ModelArchive(SourceArchive);

impl ModelArchive {
    pub fn bundled(schema_version: u32) -> Result<Option<Self>, SourceArchiveError> {
        BundledArchives::get(&file_name(schema_version))
            .map(|file| Self::from_bytes(schema_version, &file.data))
            .transpose()
    }

    pub fn bundled_versions() -> Result<Vec<u32>, SourceArchiveError> {
        BundledArchives::iter()
            .map(|path| parse_file_name(&path))
            .collect()
    }

    pub fn path(config_dir: &Path, schema_version: u32) -> PathBuf {
        LAYOUT.path(config_dir, schema_version)
    }

    pub fn from_packages(
        schema_version: u32,
        packages: &Packages,
    ) -> Result<Self, SourceArchiveError> {
        SourceArchive::pack(LAYOUT, schema_version, packages.sources()).map(Self)
    }

    pub fn from_bytes(schema_version: u32, bytes: &[u8]) -> Result<Self, SourceArchiveError> {
        SourceArchive::unpack(LAYOUT, schema_version, bytes).map(Self)
    }

    pub fn schema_version(&self) -> u32 {
        self.0.schema_version()
    }

    pub fn bytes(&self) -> &[u8] {
        self.0.bytes()
    }

    pub fn packages(&self) -> Packages {
        Packages::new(Tier::Platform, self.0.sources().clone())
    }

    pub fn matches(&self, packages: &Packages) -> bool {
        self.0.matches_sources(packages.sources())
    }

    pub fn write_atomic(&self, path: &Path) -> Result<(), SourceArchiveError> {
        self.0.write_atomic(path)
    }
}

#[cfg(test)]
mod tests {
    use query_data_model::implementations::clickhouse::spec::TableRole;
    use query_data_model::{Catalog, ClickHouse};

    use super::ModelArchive;

    #[test]
    fn every_bundled_version_declares_its_engine_tables_and_enabled_namespaces() {
        for version in ModelArchive::bundled_versions().unwrap() {
            let model = ModelArchive::bundled(version)
                .unwrap()
                .unwrap()
                .packages()
                .load()
                .unwrap();
            let catalog = Catalog::<ClickHouse>::from_model(&model).unwrap();
            let schema = catalog.schema().unwrap();
            for role in [
                TableRole::PlanProgress,
                TableRole::CodeProgress,
                TableRole::DeletionSchedule,
            ] {
                assert!(schema.role_table(role).is_some(), "v{version} {role:?}");
            }
            assert!(
                model.sources().enabled_namespaces().is_some(),
                "v{version} lists no enabled namespaces"
            );
        }
    }
}
