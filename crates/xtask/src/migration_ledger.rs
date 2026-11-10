//! `cargo xtask migration-ledger {bump,check}`: maintain and verify the schema
//! migration ledger against the scope derived from the model archives.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use orbit_migrations::archive::ModelArchive;
use orbit_migrations::diff::derive_scope;
use orbit_migrations::ledger::{
    LEDGER_FILE, MigrationEntry, MigrationLedger, verify_committed,
    verify_entry_covers_derived_scope,
};
use orbit_migrations::scope::{EdgeRows, MigrationScope};
use query_data_model::{Catalog, ClickHouse, Model, Packages};

const DEFAULT_BASE: &str = "origin/main";

const VERSIONS_REPO_PATH: &str = "config/versions.yaml";

fn config_dir() -> PathBuf {
    PathBuf::from(env!("ORBIT_CONFIG_DIR"))
}

fn ledger_path() -> PathBuf {
    config_dir().join(LEDGER_FILE)
}

fn read_ledger() -> Result<MigrationLedger> {
    let path = ledger_path();
    let content =
        fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    MigrationLedger::parse(&content).map_err(|e| anyhow!(e))
}

fn write_schema_version(version: u32) -> Result<()> {
    let path = env!("VERSIONS_FILE");
    let content = fs::read_to_string(path)?;
    let current = format!("\nschema: {}\n", orbit_versions::VERSIONS.schema);
    if !content.contains(&current) {
        bail!("{path} has no `schema:` line matching the compiled-in version");
    }
    fs::write(
        path,
        content.replace(&current, &format!("\nschema: {version}\n")),
    )
    .with_context(|| format!("writing {path}"))
}

#[derive(serde::Deserialize)]
struct SchemaVersion {
    schema: u32,
}

fn schema_version_at(base: &str) -> Option<u32> {
    let content = String::from_utf8(git_show(base, VERSIONS_REPO_PATH)?).ok()?;
    Some(
        orbit_utils::yaml::from_str::<SchemaVersion>(&content)
            .ok()?
            .schema,
    )
}

fn git_show(base: &str, repo_path: &str) -> Option<Vec<u8>> {
    let output = Command::new("git")
        .args(["show", &format!("{base}:{repo_path}")])
        .output()
        .ok()?;
    output.status.success().then_some(output.stdout)
}

fn model_archive_repo_path(version: u32) -> String {
    format!("config/model-archives/v{version}.tar.gz")
}

fn read_model_archive(version: u32) -> Result<ModelArchive> {
    let path = ModelArchive::path(&config_dir(), version);
    let bytes = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(ModelArchive::from_bytes(version, &bytes)?)
}

fn load(archive: &ModelArchive) -> Result<Arc<Model>> {
    archive
        .packages()
        .load()
        .with_context(|| format!("loading the v{} model archive", archive.schema_version()))
}

fn parse_explicit_scope(
    scope: Option<String>,
    entities: Option<String>,
) -> Result<Option<MigrationScope>> {
    let Some(scope) = scope else {
        if entities.is_some() {
            bail!("--entities requires --scope sdlc");
        }
        return Ok(None);
    };

    let entities: BTreeSet<String> = entities
        .map(|s| {
            s.split(',')
                .map(|e| e.trim().to_string())
                .filter(|e| !e.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let scope = match scope.as_str() {
        "sdlc" => {
            return Ok(Some(MigrationScope::Sdlc {
                entities,
                edge_rows: EdgeRows::Changed,
            }));
        }
        "*" => MigrationScope::Full,
        "code" => MigrationScope::Code,
        "none" => MigrationScope::None,
        other => bail!("unknown scope '{other}'; expected '*', 'sdlc', 'code', or 'none'"),
    };
    if !entities.is_empty() {
        bail!("--entities is only valid with --scope sdlc");
    }
    Ok(Some(scope))
}

/// The declared scope: the derived one, or an explicit one that covers it.
fn declared_scope(
    derived: &MigrationScope,
    explicit: Option<MigrationScope>,
) -> Result<MigrationScope> {
    match explicit {
        None => Ok(derived.clone()),
        Some(explicit) if explicit.covers(derived) => Ok(explicit),
        Some(explicit) => bail!(
            "--scope declares {explicit}, narrower than the derived {derived}; flags may only widen it"
        ),
    }
}

pub fn check(base: Option<String>) -> Result<()> {
    let schema_version = orbit_versions::VERSIONS.schema;
    let ledger = verify_committed(&config_dir(), schema_version).map_err(|e| anyhow!(e))?;

    if let Some(base) = base {
        check_against_base(&ledger, schema_version, &base)?;
    }

    println!(
        "migration ledger is up to date (version {schema_version}, {} entries)",
        ledger.migrations.len()
    );
    Ok(())
}

/// A changed model needs exactly a `base + 1` bump whose entry covers the scope derived from the base version's archive.
fn check_against_base(ledger: &MigrationLedger, schema_version: u32, base: &str) -> Result<()> {
    let base_version = schema_version_at(base)
        .ok_or_else(|| anyhow!("could not read {base}:{VERSIONS_REPO_PATH}"))?;
    if schema_version == base_version {
        if let Some(bytes) = git_show(base, &model_archive_repo_path(base_version))
            && !ModelArchive::from_bytes(base_version, &bytes)?.matches(&Packages::embedded())
        {
            bail!(
                "the packages differ from the v{base_version} model archive on {base}. \
                 Run `mise schema:bump` to record the change."
            );
        }
        return Ok(());
    }
    // A gap would leave an entry-less version, which the migration path treats
    // as a full rebuild; one MR bumps exactly one version.
    if schema_version != base_version + 1 {
        bail!(
            "SCHEMA_VERSION must be exactly base + 1 (base {base_version}, expected {}, got \
             {schema_version}). Run `mise schema:bump` to record the change.",
            base_version + 1
        );
    }
    let previous = load(&read_model_archive(base_version)?)?;
    verify_entry_covers_derived_scope(ledger, &previous, schema_version).map_err(|e| anyhow!(e))?;
    Ok(())
}

pub fn bump(
    scope: Option<String>,
    entities: Option<String>,
    note: Option<String>,
    base: Option<String>,
    amend: bool,
    new: bool,
) -> Result<()> {
    if amend && new {
        bail!("--amend and --new are mutually exclusive");
    }
    let explicit = parse_explicit_scope(scope, entities)?;

    let working_version = orbit_versions::VERSIONS.schema;
    let base_ref = base.unwrap_or_else(|| DEFAULT_BASE.to_string());
    let is_new = match schema_version_at(&base_ref) {
        Some(base_version) => working_version == base_version,
        None if amend => false,
        None if new => true,
        None => bail!(
            "could not read {base_ref}:{VERSIONS_REPO_PATH} to decide bump vs amend; \
             pass --new or --amend"
        ),
    };
    let final_version = if is_new {
        working_version + 1
    } else {
        working_version
    };

    let previous = read_model_archive(final_version - 1)?;
    let packages = Packages::embedded();
    if is_new && explicit.is_none() && previous.matches(&packages) {
        bail!(
            "nothing changed since the v{working_version} archive. Pass --scope to re-index \
             for a change the model cannot show, such as indexer code."
        );
    }
    let derived = derive_scope(&load(&previous)?, &packages.load()?)?;
    let declared = declared_scope(&derived.scope, explicit)?;

    let mut ledger = read_ledger()?;
    if is_new {
        if ledger.migrations.iter().any(|e| e.version == final_version) {
            bail!("ledger already has an entry for version {final_version}");
        }
        ledger
            .migrations
            .insert(0, MigrationEntry::new(final_version, &declared, note));
    } else {
        let latest = ledger
            .migrations
            .first_mut()
            .ok_or_else(|| anyhow!("cannot amend: the ledger has no entries"))?;
        let widened = latest.to_migration_scope().merged_with(&declared);
        *latest = MigrationEntry::new(final_version, &widened, note.or(latest.note.take()));
    }

    let catalog = Catalog::<ClickHouse>::from_model(&packages.load()?)?;
    ledger
        .validate(&catalog, final_version)
        .map_err(|e| anyhow!(e))?;

    write_archive(&embedded_archive(final_version)?, !is_new)?;
    if is_new {
        write_schema_version(final_version)?;
    }
    fs::write(ledger_path(), ledger.render_as_yaml()).context("writing ledger")?;

    let entry = ledger.latest_entry().expect("just validated non-empty");
    println!(
        "{} version {final_version}: {}; changed tables {:?}",
        if is_new { "bumped to" } else { "amended" },
        entry.to_migration_scope(),
        derived.tables,
    );
    Ok(())
}

fn embedded_archive(version: u32) -> Result<ModelArchive> {
    let archive = ModelArchive::from_packages(version, &Packages::embedded())?;
    load(&archive)?;
    Ok(archive)
}

fn write_archive(archive: &ModelArchive, replace: bool) -> Result<()> {
    let path = ModelArchive::path(&config_dir(), archive.schema_version());
    let bytes = archive.bytes();
    if path.exists() {
        let existing = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        if existing == bytes {
            return Ok(());
        }
        if !replace {
            bail!(
                "archive already exists with different contents: {}",
                path.display()
            );
        }
    }
    fs::write(&path, bytes).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sdlc(entities: &[&str]) -> MigrationScope {
        MigrationScope::Sdlc {
            entities: entities.iter().map(|e| e.to_string()).collect(),
            edge_rows: EdgeRows::Changed,
        }
    }

    #[test]
    fn schema_version_ignores_unrelated_pins() {
        for yaml in [
            "schema: 100\n",
            "schema: 100\nraw_output_format: 5.0.3\n",
            "schema: 100\ngql_output_format: 1.0.0\nfuture_pin: {version: 2}\n",
        ] {
            assert_eq!(
                orbit_utils::yaml::from_str::<SchemaVersion>(yaml)
                    .unwrap()
                    .schema,
                100
            );
        }
        for yaml in ["gql_output_format: 1.0.0", "schema: -1", "schema: bogus"] {
            assert!(orbit_utils::yaml::from_str::<SchemaVersion>(yaml).is_err());
        }
    }

    #[test]
    fn parse_explicit_scope_reads_every_scope() {
        let parse = |scope: &str, entities: Option<&str>| {
            parse_explicit_scope(Some(scope.to_string()), entities.map(str::to_string))
        };
        assert_eq!(parse("none", None).unwrap(), Some(MigrationScope::None));
        assert_eq!(parse("*", None).unwrap(), Some(MigrationScope::Full));
        assert_eq!(
            parse("sdlc", Some("Note, Issue")).unwrap(),
            Some(sdlc(&["Issue", "Note"]))
        );
        let err = parse("none", Some("Note")).unwrap_err();
        assert!(
            err.to_string().contains("--entities is only valid"),
            "{err}"
        );
        let err = parse("bogus", None).unwrap_err();
        assert!(err.to_string().contains("unknown scope"), "{err}");
    }

    #[test]
    fn explicit_scope_may_only_widen_the_derived_scope() {
        assert_eq!(
            declared_scope(&sdlc(&["Note"]), None).unwrap(),
            sdlc(&["Note"])
        );
        assert_eq!(
            declared_scope(&sdlc(&["Note"]), Some(MigrationScope::Full)).unwrap(),
            MigrationScope::Full
        );
        assert_eq!(
            declared_scope(&MigrationScope::None, Some(MigrationScope::None)).unwrap(),
            MigrationScope::None
        );
        for narrower in [MigrationScope::None, MigrationScope::Code, sdlc(&["Issue"])] {
            let err = declared_scope(&sdlc(&["Note"]), Some(narrower)).unwrap_err();
            assert!(
                err.to_string().contains("narrower than the derived"),
                "{err}"
            );
        }
    }
}
