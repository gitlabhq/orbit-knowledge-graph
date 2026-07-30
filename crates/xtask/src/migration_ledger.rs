//! `cargo xtask migration-ledger {bump,check}`: maintain and verify the schema
//! migration ledger and its fingerprint snapshot.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use ontology::Ontology;
use ontology::migrations::{
    self, Fingerprints, LedgerScope, MigrationEntry, MigrationLedger, MigrationScope, derive_scope,
};

const DEFAULT_BASE: &str = "origin/main";

const FINGERPRINT_REPO_PATH: &str = "config/schema-migrations.fingerprint.yaml";
const SCHEMA_VERSION_REPO_PATH: &str = "config/SCHEMA_VERSION";

const REMEDIATION: &str = "Run `mise schema:bump` to record the change and re-snapshot.";

fn config_dir() -> PathBuf {
    PathBuf::from(env!("CONFIG_DIR"))
}

fn ledger_path() -> PathBuf {
    config_dir().join(migrations::LEDGER_FILE)
}

fn fingerprint_path() -> PathBuf {
    config_dir().join(migrations::FINGERPRINT_FILE)
}

fn schema_version_path() -> PathBuf {
    config_dir().join("SCHEMA_VERSION")
}

fn current_fingerprints(ontology: &Ontology) -> Fingerprints {
    Fingerprints {
        sources: migrations::source_fingerprints(),
        ddl: query_engine::compiler::ddl_fingerprints(ontology),
        auxiliary_schema: query_engine::compiler::auxiliary_schema_fingerprints(ontology),
    }
}

fn read_committed_fingerprints() -> Result<Option<Fingerprints>> {
    let path = fingerprint_path();
    if !path.exists() {
        return Ok(None);
    }
    let content =
        fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    Fingerprints::parse(&content)
        .map(Some)
        .map_err(|e| anyhow!(e))
}

fn read_ledger() -> Result<MigrationLedger> {
    let path = ledger_path();
    let content =
        fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    MigrationLedger::parse(&content).map_err(|e| anyhow!(e))
}

fn read_schema_version() -> Result<u32> {
    let path = schema_version_path();
    let content =
        fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    content
        .trim()
        .parse()
        .with_context(|| format!("{} must contain a u32", path.display()))
}

fn write_schema_version(version: u32) -> Result<()> {
    fs::write(schema_version_path(), format!("{version}\n")).context("writing SCHEMA_VERSION")
}

fn git_show(base: &str, repo_path: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["show", &format!("{base}:{repo_path}")])
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        None
    }
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

    let scope = match scope.as_str() {
        "*" => LedgerScope::All,
        "sdlc" => LedgerScope::Sdlc,
        "code" => LedgerScope::Code,
        "none" => LedgerScope::None,
        other => bail!("unknown scope '{other}'; expected '*', 'sdlc', 'code', or 'none'"),
    };

    let entities: BTreeSet<String> = entities
        .map(|s| {
            s.split(',')
                .map(|e| e.trim().to_string())
                .filter(|e| !e.is_empty())
                .collect()
        })
        .unwrap_or_default();

    if !entities.is_empty() && scope != LedgerScope::Sdlc {
        bail!("--entities is only valid with --scope sdlc");
    }

    Ok(Some(match scope {
        LedgerScope::All => MigrationScope::Full,
        LedgerScope::Code => MigrationScope::Code,
        LedgerScope::Sdlc => MigrationScope::Sdlc(entities),
        LedgerScope::None => MigrationScope::None,
    }))
}

fn widen_amended_entry_scope(
    existing: &MigrationScope,
    declared: &MigrationScope,
) -> Result<MigrationScope> {
    if matches!(declared, MigrationScope::None) && !matches!(existing, MigrationScope::None) {
        bail!(
            "--scope none cannot amend an entry with {existing}: the wider scope already covers \
             this version's drift, so drop --scope none; hand-edit the ledger YAML only if the \
             entire version span is output-neutral"
        );
    }
    Ok(existing.widened_with(declared))
}

/// Lowers a [`MigrationScope`] into the wire `scope:` + `entities:` fields of a ledger entry.
fn split_scope_into_ledger_fields(scope: MigrationScope) -> (LedgerScope, BTreeSet<String>) {
    match scope {
        MigrationScope::Full => (LedgerScope::All, BTreeSet::new()),
        MigrationScope::Code => (LedgerScope::Code, BTreeSet::new()),
        MigrationScope::Sdlc(entities) => (LedgerScope::Sdlc, entities),
        MigrationScope::None => (LedgerScope::None, BTreeSet::new()),
    }
}

pub fn check(base: Option<String>) -> Result<()> {
    let ontology = Ontology::load_embedded().map_err(|e| anyhow!(e.to_string()))?;
    let current = current_fingerprints(&ontology);

    let committed = read_committed_fingerprints()?.ok_or_else(|| {
        anyhow!(
            "fingerprint snapshot {} is missing. {REMEDIATION}",
            fingerprint_path().display()
        )
    })?;

    let schema_version = read_schema_version()?;
    let ledger = read_ledger()?;

    migrations::verify_snapshot(&ontology, &current, &committed, &ledger, schema_version)
        .map_err(|e| anyhow!(e))?;

    if let Some(base) = base {
        check_under_declaration(&ontology, &committed, &ledger, schema_version, &base)?;
    }

    println!(
        "migration ledger is up to date (version {schema_version}, {} entries)",
        ledger.migrations.len()
    );
    Ok(())
}

/// Enforce that a fingerprint change in this MR carries a `base + 1` bump and a
/// ledger entry covering the derived drift.
fn check_under_declaration(
    ontology: &Ontology,
    committed: &Fingerprints,
    ledger: &MigrationLedger,
    schema_version: u32,
    base: &str,
) -> Result<()> {
    let Some(base_content) = git_show(base, FINGERPRINT_REPO_PATH) else {
        return Ok(());
    };
    let base_fps = Fingerprints::parse(&base_content).map_err(|e| anyhow!(e))?;
    if committed.has_same_versioned_fingerprints_as(&base_fps) {
        return Ok(());
    }

    let base_version: u32 = git_show(base, SCHEMA_VERSION_REPO_PATH)
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| anyhow!("could not read {base}:{SCHEMA_VERSION_REPO_PATH}"))?;
    // A gap would leave an entry-less version, which the migration path treats
    // as a full rebuild; one MR bumps exactly one version.
    if schema_version != base_version + 1 {
        bail!(
            "fingerprint snapshot changed, so SCHEMA_VERSION must be exactly base + 1 \
             (base {base_version}, expected {}, got {schema_version}). {REMEDIATION}",
            base_version + 1
        );
    }

    let entry = ledger
        .migrations
        .iter()
        .find(|e| e.version == schema_version)
        .ok_or_else(|| {
            anyhow!("no ledger entry for the bumped version {schema_version}. {REMEDIATION}")
        })?;

    let (changed_sources, changed_tables) = committed.get_versioned_diff_keys_between(&base_fps);
    let source_contents = migrations::embedded_sources();
    // A `none` entry may under-declare drift; its gate is the note check in `MigrationLedger::validate`.
    if let Some(required) = derive_scope(
        ontology,
        &source_contents,
        &changed_sources,
        &changed_tables,
    ) && !matches!(entry.migration_scope(), MigrationScope::None)
        && !entry.migration_scope().covers_scope_of(&required)
    {
        bail!(
            "ledger entry for version {schema_version} under-declares the change: \
             detected {required} but the entry does not cover it. Widen the entry (or run `mise schema:bump`)."
        );
    }

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

    let ontology = Ontology::load_embedded().map_err(|e| anyhow!(e.to_string()))?;
    let current = current_fingerprints(&ontology);

    let Some(committed) = read_committed_fingerprints()? else {
        return write_initial_snapshot(&ontology, &current);
    };

    let explicit = parse_explicit_scope(scope, entities)?;
    let (changed_sources, changed_tables) = current.get_versioned_diff_keys_between(&committed);
    let source_contents = migrations::embedded_sources();
    let derived = derive_scope(
        &ontology,
        &source_contents,
        &changed_sources,
        &changed_tables,
    );

    let entry_declaration = if matches!(explicit, Some(MigrationScope::None)) {
        if note.as_ref().is_none_or(|n| n.trim().is_empty()) {
            bail!(
                "--scope none requires --note certifying the source change is output-neutral \
                 (produces byte-identical output)"
            );
        }
        MigrationScope::None
    } else {
        match (derived, explicit) {
            (None, None) => bail!(
                "no ontology drift detected. Pass --scope for a lowering-code-only change \
                 (e.g. `--scope sdlc --entities Note`)."
            ),
            (None, Some(explicit)) => explicit,
            (Some(derived), None) => derived,
            (Some(derived), Some(explicit)) => {
                if !explicit.covers_scope_of(&derived) {
                    bail!(
                        "--scope narrows below the detected drift ({derived}); flags may only widen it"
                    );
                }
                derived.widened_with(&explicit)
            }
        }
    };

    let working_version = read_schema_version()?;
    let base_ref = base.unwrap_or_else(|| DEFAULT_BASE.to_string());
    let base_version =
        git_show(&base_ref, SCHEMA_VERSION_REPO_PATH).and_then(|s| s.trim().parse::<u32>().ok());

    let is_new = match base_version {
        Some(bv) => working_version == bv,
        None => {
            if amend {
                false
            } else if new {
                true
            } else {
                bail!(
                    "could not read {base_ref}:{SCHEMA_VERSION_REPO_PATH} to decide bump vs amend; \
                     pass --new or --amend"
                );
            }
        }
    };

    let mut ledger = read_ledger()?;
    let final_version = if is_new {
        let new_version = working_version + 1;
        if ledger.migrations.iter().any(|e| e.version == new_version) {
            bail!("ledger already has an entry for version {new_version}");
        }
        let (scope, entities) = split_scope_into_ledger_fields(entry_declaration);
        ledger.migrations.insert(
            0,
            MigrationEntry {
                version: new_version,
                scope,
                entities,
                note,
            },
        );
        new_version
    } else {
        let latest = ledger
            .migrations
            .first_mut()
            .ok_or_else(|| anyhow!("cannot amend: the ledger has no entries"))?;
        let widened = widen_amended_entry_scope(&latest.migration_scope(), &entry_declaration)?;
        let (scope, entities) = split_scope_into_ledger_fields(widened);
        latest.scope = scope;
        latest.entities = entities;
        if note.is_some() {
            latest.note = note;
        }
        latest.version
    };

    ledger
        .validate(&ontology, final_version)
        .map_err(|e| anyhow!(e))?;

    if is_new {
        write_schema_version(final_version)?;
    }
    fs::write(ledger_path(), ledger.render()).context("writing ledger")?;
    fs::write(fingerprint_path(), current.render()).context("writing fingerprint snapshot")?;

    let entry = ledger.latest().expect("just validated non-empty");
    println!(
        "{} version {final_version}: {}",
        if is_new { "bumped to" } else { "amended" },
        entry.migration_scope(),
    );
    Ok(())
}

pub fn snapshot() -> Result<()> {
    let ontology = Ontology::load_embedded().map_err(|e| anyhow!(e.to_string()))?;
    let current = current_fingerprints(&ontology);
    let committed = read_committed_fingerprints()?.ok_or_else(|| {
        anyhow!(
            "fingerprint snapshot {} is missing. Run `mise schema:bump` to create it.",
            fingerprint_path().display()
        )
    })?;
    if !current.has_same_versioned_fingerprints_as(&committed) {
        let (changed_sources, changed_tables) = current.get_versioned_diff_keys_between(&committed);
        bail!(
            "schema:snapshot cannot record versioned schema drift.\n  changed sources: {}\n  changed tables: {}\nRun `mise schema:bump` instead.",
            format_set(&changed_sources),
            format_set(&changed_tables),
        );
    }
    fs::write(fingerprint_path(), current.render()).context("writing fingerprint snapshot")?;
    println!(
        "regenerated fingerprint snapshot at {}",
        fingerprint_path().display()
    );
    Ok(())
}

fn format_set(values: &BTreeSet<String>) -> String {
    if values.is_empty() {
        "(none)".to_string()
    } else {
        values.iter().cloned().collect::<Vec<_>>().join(", ")
    }
}

/// First-time snapshot: write the fingerprint file without bumping.
fn write_initial_snapshot(ontology: &Ontology, current: &Fingerprints) -> Result<()> {
    let version = read_schema_version()?;
    let ledger = read_ledger()?;
    ledger.validate(ontology, version).map_err(|e| anyhow!(e))?;
    fs::write(fingerprint_path(), current.render()).context("writing fingerprint snapshot")?;
    println!("wrote initial fingerprint snapshot for version {version}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_explicit_scope_accepts_none() {
        let scope = parse_explicit_scope(Some("none".to_string()), None).unwrap();
        assert_eq!(scope, Some(MigrationScope::None));
    }

    #[test]
    fn parse_explicit_scope_none_rejects_entities() {
        let err =
            parse_explicit_scope(Some("none".to_string()), Some("Note".to_string())).unwrap_err();
        assert!(
            err.to_string().contains("--entities is only valid"),
            "{err}"
        );
    }

    #[test]
    fn parse_explicit_scope_rejects_unknown() {
        let err = parse_explicit_scope(Some("bogus".to_string()), None).unwrap_err();
        assert!(err.to_string().contains("unknown scope"), "{err}");
    }

    #[test]
    fn amend_rejects_scope_none_over_wider_entry() {
        for existing in [
            MigrationScope::Full,
            MigrationScope::Code,
            MigrationScope::Sdlc(BTreeSet::new()),
        ] {
            let err = widen_amended_entry_scope(&existing, &MigrationScope::None).unwrap_err();
            assert!(err.to_string().contains("cannot amend"), "{err}");
        }
    }

    #[test]
    fn amend_widens_none_entry_to_declared_scope() {
        assert_eq!(
            widen_amended_entry_scope(&MigrationScope::None, &MigrationScope::Code).unwrap(),
            MigrationScope::Code
        );
        assert_eq!(
            widen_amended_entry_scope(&MigrationScope::None, &MigrationScope::None).unwrap(),
            MigrationScope::None
        );
    }
}
