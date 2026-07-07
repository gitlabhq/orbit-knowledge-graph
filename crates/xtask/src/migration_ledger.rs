//! `cargo xtask migration-ledger {bump,check}`.
//!
//! `bump` recomputes the fingerprint snapshot, derives the minimal invalidation
//! scope from the drift, appends (or amends) a ledger entry, and increments
//! `config/SCHEMA_VERSION`. `check` recomputes and compares against the
//! committed snapshot, validates the ledger, and (with `--base`) enforces that
//! the entry for the bumped version covers the derived drift.
//!
//! Amend-awareness keeps one MR from burning several versions: `bump` compares
//! the working `SCHEMA_VERSION` against `git show <base>:config/SCHEMA_VERSION`.
//! Equal means a fresh bump (+1, new entry); already ahead means the last entry
//! is amended (scope/entities widened, version unchanged).

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use ontology::Ontology;
use ontology::migrations::{
    self, Fingerprints, MigrationEntry, MigrationLedger, Scope, ScopeDeclaration, derive_scope,
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
) -> Result<Option<ScopeDeclaration>> {
    let Some(scope) = scope else {
        if entities.is_some() {
            bail!("--entities requires --scope sdlc");
        }
        return Ok(None);
    };

    let scope = match scope.as_str() {
        "*" => Scope::All,
        "sdlc" => Scope::Sdlc,
        "code" => Scope::Code,
        other => bail!("unknown scope '{other}'; expected '*', 'sdlc', or 'code'"),
    };

    let entities: BTreeSet<String> = entities
        .map(|s| {
            s.split(',')
                .map(|e| e.trim().to_string())
                .filter(|e| !e.is_empty())
                .collect()
        })
        .unwrap_or_default();

    if !entities.is_empty() && scope != Scope::Sdlc {
        bail!("--entities is only valid with --scope sdlc");
    }

    Ok(Some(ScopeDeclaration { scope, entities }))
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

    if current != committed {
        let (sources, ddl) = current.diff(&committed);
        bail!(
            "ontology drift not reflected in {}.\n  changed sources: {}\n  changed tables: {}\n{REMEDIATION}",
            fingerprint_path().display(),
            format_set(&sources),
            format_set(&ddl),
        );
    }

    let schema_version = read_schema_version()?;
    let ledger = read_ledger()?;
    ledger
        .validate(&ontology, schema_version)
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

/// With a base ref: if the fingerprint snapshot changed in this MR, require a
/// SCHEMA_VERSION bump, a ledger entry for the new version, and that the entry
/// covers the drift derived from the base-vs-current snapshot diff. A base with
/// no snapshot file (pre-feature history) skips this guard.
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
    if &base_fps == committed {
        return Ok(());
    }

    let base_version: u32 = git_show(base, SCHEMA_VERSION_REPO_PATH)
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| anyhow!("could not read {base}:{SCHEMA_VERSION_REPO_PATH}"))?;
    if schema_version <= base_version {
        bail!(
            "fingerprint snapshot changed but SCHEMA_VERSION ({schema_version}) is not ahead of \
             base ({base_version}). {REMEDIATION}"
        );
    }

    let entry = ledger
        .migrations
        .iter()
        .find(|e| e.version == schema_version)
        .ok_or_else(|| {
            anyhow!("no ledger entry for the bumped version {schema_version}. {REMEDIATION}")
        })?;

    let (changed_sources, changed_tables) = committed.diff(&base_fps);
    let source_contents = migrations::embedded_sources();
    if let Some(required) = derive_scope(
        ontology,
        &source_contents,
        &changed_sources,
        &changed_tables,
    ) && !entry.declaration().covers(&required)
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
    let (changed_sources, changed_tables) = current.diff(&committed);
    let source_contents = migrations::embedded_sources();
    let derived = derive_scope(
        &ontology,
        &source_contents,
        &changed_sources,
        &changed_tables,
    );

    let entry_declaration = match (derived, explicit) {
        (None, None) => bail!(
            "no ontology drift detected. Pass --scope for a lowering-code-only change \
             (e.g. `--scope sdlc --entities Note`)."
        ),
        (None, Some(explicit)) => explicit,
        (Some(derived), None) => derived,
        (Some(derived), Some(explicit)) => {
            if !explicit.covers(&derived) {
                bail!(
                    "--scope narrows below the detected drift ({derived}); flags may only widen it"
                );
            }
            derived.widen(&explicit)
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
        ledger.migrations.push(MigrationEntry {
            version: new_version,
            scope: entry_declaration.scope,
            entities: entry_declaration.entities,
            note,
        });
        write_schema_version(new_version)?;
        new_version
    } else {
        let last = ledger
            .migrations
            .last_mut()
            .ok_or_else(|| anyhow!("cannot amend: the ledger has no entries"))?;
        let widened = last.declaration().widen(&entry_declaration);
        last.scope = widened.scope;
        last.entities = widened.entities;
        if note.is_some() {
            last.note = note;
        }
        last.version
    };

    ledger
        .validate(&ontology, final_version)
        .map_err(|e| anyhow!(e))?;

    fs::write(ledger_path(), ledger.render()).context("writing ledger")?;
    fs::write(fingerprint_path(), current.render()).context("writing fingerprint snapshot")?;

    let entry = ledger.last().expect("just validated non-empty");
    println!(
        "{} version {final_version}: {}",
        if is_new { "bumped to" } else { "amended" },
        entry.declaration(),
    );
    Ok(())
}

/// First-time snapshot: the ledger is seeded by hand for the current version
/// but no fingerprint file exists yet. Write it without bumping.
fn write_initial_snapshot(ontology: &Ontology, current: &Fingerprints) -> Result<()> {
    let version = read_schema_version()?;
    let ledger = read_ledger()?;
    ledger.validate(ontology, version).map_err(|e| anyhow!(e))?;
    fs::write(fingerprint_path(), current.render()).context("writing fingerprint snapshot")?;
    println!("wrote initial fingerprint snapshot for version {version}");
    Ok(())
}

fn format_set(set: &BTreeSet<String>) -> String {
    if set.is_empty() {
        "(none)".to_string()
    } else {
        set.iter().cloned().collect::<Vec<_>>().join(", ")
    }
}
