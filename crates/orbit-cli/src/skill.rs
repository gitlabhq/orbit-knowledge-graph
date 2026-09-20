//! Resolves the instance-matched Orbit skill, validates and atomically caches
//! its remote tree, then composes it with the binary's embedded local tree.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::remote::client::{OrbitClient, SkillHttpResponse};
use crate::remote::error::map_http_error;

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit-cli"]
struct SkillAssets;

const MANIFEST: &str = "SKILL.md";
const CACHE_MANIFEST: &str = ".orbit-cache.json";
const DEFAULT_SKILL: &str = "orbit";
const LOCK_WAIT: Duration = Duration::from_secs(10);
pub(crate) const INSTALL_DIR_NAME: &str = "orbit-cli";

pub(crate) fn embedded_files() -> impl Iterator<Item = (String, Vec<u8>)> {
    SkillAssets::iter()
        .filter_map(|path| Some((path.to_string(), SkillAssets::get(&path)?.data.into_owned())))
}

#[derive(Debug, PartialEq, Eq)]
enum Request {
    List,
    Print { name: String, path: String },
}

#[derive(Debug, Deserialize)]
struct LocalFrontmatter {
    description: String,
}

#[derive(Debug, Deserialize)]
struct ListEnvelope {
    skills: Vec<ListedSkill>,
}

#[derive(Debug, Deserialize)]
struct ListedSkill {
    name: String,
    description: String,
}

#[derive(Clone, Debug, Deserialize)]
struct RemoteEnvelope {
    name: String,
    version: String,
    tree_sha256: String,
    files: Vec<RemoteFile>,
}

#[derive(Clone, Debug, Deserialize)]
struct RemoteFile {
    path: String,
    sha256: String,
    content: String,
}

#[derive(Clone, Debug)]
struct ValidatedTree {
    name: String,
    version: String,
    tree_sha256: String,
    etag: String,
    files: BTreeMap<String, String>,
    file_hashes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CacheManifest {
    origin: String,
    name: String,
    version: String,
    tree_sha256: String,
    etag: String,
    file_hashes: BTreeMap<String, String>,
    validated_at: u64,
}

#[derive(Clone, Debug)]
struct CachedTree {
    tree: ValidatedTree,
    validated_at: u64,
}

pub(crate) async fn run(name_or_path: Option<String>, path: Option<String>) -> Result<()> {
    execute(resolve(name_or_path.as_deref(), path.as_deref())?).await
}

pub(crate) async fn get(name: String, path: String) -> Result<()> {
    execute(resolve_named(&name, &path)?).await
}

async fn execute(request: Request) -> Result<()> {
    match request {
        Request::List => list_skills().await,
        Request::Print { name, path } => print_skill_file(&name, &path).await,
    }
}

fn resolve_named(name: &str, path: &str) -> Result<Request> {
    ensure_known_skill(name)?;
    Ok(Request::Print {
        name: name.to_string(),
        path: path.to_string(),
    })
}

fn resolve(name_or_path: Option<&str>, path: Option<&str>) -> Result<Request> {
    let Some(first) = name_or_path else {
        return Ok(Request::List);
    };
    if is_skill_name(first) {
        ensure_known_skill(first)?;
        return Ok(Request::Print {
            name: first.to_string(),
            path: path.unwrap_or(MANIFEST).to_string(),
        });
    }
    if path.is_some() {
        bail!("a path shorthand cannot be followed by another path");
    }
    Ok(Request::Print {
        name: DEFAULT_SKILL.to_string(),
        path: first.to_string(),
    })
}

fn ensure_known_skill(name: &str) -> Result<()> {
    if name != DEFAULT_SKILL {
        bail!(
            "unknown skill name {name:?}. Known skills:\n  {DEFAULT_SKILL}\n\nUse `{} skills get <name> [path]`.",
            crate::commands::setup::spec::launcher()
        );
    }
    Ok(())
}

fn is_skill_name(value: &str) -> bool {
    !value.contains(['/', '.'])
        && !value.is_empty()
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

async fn list_skills() -> Result<()> {
    let Some(client) = OrbitClient::from_skill_env()? else {
        return print_local_list();
    };
    match client.list_skills().await {
        Ok(response) if response.status == 200 => {
            let envelope: ListEnvelope = serde_json::from_slice(&response.body)
                .context("invalid Orbit skills listing response")?;
            if envelope.skills.is_empty() {
                bail!("Orbit skills listing response is empty");
            }
            for skill in envelope.skills {
                println!("{} — {}", skill.name, one_line(&skill.description));
            }
            Ok(())
        }
        Ok(response) if response.status == 404 => {
            eprintln!(
                "warning: this GitLab instance does not serve Orbit skills; using the embedded local skill"
            );
            print_local_list()
        }
        Ok(response) if matches!(response.status, 401 | 403) => {
            Err(map_http_error(response.status, body_text(&response)).into())
        }
        // Collection responses are not cached, so there is no validated remote listing to reuse.
        Ok(response) if response.status >= 500 => {
            Err(map_http_error(response.status, body_text(&response)).into())
        }
        Ok(response) => Err(map_http_error(response.status, body_text(&response)).into()),
        Err(error) => {
            eprintln!("warning: {error}; using the embedded local skill");
            print_local_list()
        }
    }
}

fn print_local_list() -> Result<()> {
    let manifest = local_tree()
        .remove(MANIFEST)
        .ok_or_else(|| anyhow!("embedded {MANIFEST} missing"))?;
    let yaml = manifest
        .strip_prefix("---\n")
        .and_then(|content| content.split_once("\n---\n"))
        .map(|(yaml, _)| yaml)
        .ok_or_else(|| anyhow!("embedded {MANIFEST} has invalid frontmatter"))?;
    let frontmatter: LocalFrontmatter = orbit_utils::yaml::from_str(yaml)?;
    println!("{DEFAULT_SKILL} — {}", one_line(&frontmatter.description));
    Ok(())
}

fn one_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

async fn print_skill_file(name: &str, requested: &str) -> Result<()> {
    let local = local_tree();
    let view = match OrbitClient::from_skill_env()? {
        None => local,
        Some(client) => match resolve_remote_tree(&client, name).await? {
            Some(remote) => match compose_tree(remote.files, local.clone()) {
                Ok(composed) => composed,
                Err(error) => {
                    eprintln!(
                        "warning: could not compose the instance Orbit skill ({error}); using the embedded local skill"
                    );
                    local
                }
            },
            None => local,
        },
    };

    let Some(contents) = view.get(requested) else {
        bail!(
            "unknown skill file {requested:?}. Available files:\n{}\n\nUse `{} skills get <name> [path]`.",
            available_list(&view),
            crate::commands::setup::spec::launcher()
        );
    };
    print!("{contents}");
    Ok(())
}

async fn resolve_remote_tree(client: &OrbitClient, name: &str) -> Result<Option<ValidatedTree>> {
    let origin = client.origin()?;
    let cached = newest_cached_tree(&origin, name);
    let etag = cached.as_ref().map(|cached| cached.tree.etag.as_str());
    let response = match client.get_skill(name, etag).await {
        Ok(response) => response,
        Err(error) => {
            if let Some(cached) = cached {
                eprintln!("warning: {error}; using the last validated Orbit skill for {origin}");
                return Ok(Some(cached.tree));
            }
            eprintln!("warning: {error}; using the embedded local skill");
            return Ok(None);
        }
    };

    match response.status {
        200 => {
            let etag = response
                .etag
                .ok_or_else(|| anyhow!("Orbit skill response has no ETag"))?;
            let tree = validate_remote_envelope(name, &etag, &response.body)?;
            if let Some(cached) = &cached
                && cached.tree.version == tree.version
                && cached.tree.tree_sha256 != tree.tree_sha256
            {
                eprintln!(
                    "warning: Orbit skill version {} changed tree hash; replacing the colliding cache entry",
                    tree.version
                );
            }
            publish_cache(&origin, &tree)?;
            Ok(Some(tree))
        }
        304 => {
            let cached = cached.ok_or_else(|| {
                anyhow!("Orbit skill server returned 304 but no validated cache entry exists")
            })?;
            // The request always revalidates the newest entry, so refreshing its
            // timestamp cannot affect prune order and would rewrite the whole tree.
            Ok(Some(cached.tree))
        }
        404 => {
            eprintln!(
                "warning: this GitLab instance does not serve Orbit skills; using the embedded local skill"
            );
            Ok(None)
        }
        401 | 403 => Err(map_http_error(response.status, body_text(&response)).into()),
        status if status >= 500 => {
            if let Some(cached) = cached {
                eprintln!(
                    "warning: Orbit skill request returned HTTP {status}; using the last validated tree for {origin}"
                );
                Ok(Some(cached.tree))
            } else {
                Err(map_http_error(status, body_text(&response)).into())
            }
        }
        status => Err(map_http_error(status, body_text(&response)).into()),
    }
}

fn body_text(response: &SkillHttpResponse) -> &str {
    std::str::from_utf8(&response.body).unwrap_or("")
}

fn validate_remote_envelope(name: &str, etag: &str, body: &[u8]) -> Result<ValidatedTree> {
    let envelope: RemoteEnvelope =
        serde_json::from_slice(body).context("invalid Orbit skill response")?;
    if envelope.name != name {
        bail!(
            "Orbit skill response name {:?} does not match requested name {name:?}",
            envelope.name
        );
    }
    validate_component("skill version", &envelope.version)?;
    validate_sha256("tree_sha256", &envelope.tree_sha256)?;
    if envelope.files.is_empty() {
        bail!("Orbit skill response contains no files");
    }

    let mut files = BTreeMap::new();
    let mut file_hashes = BTreeMap::new();
    for file in envelope.files {
        validate_relative_path(&file.path)?;
        validate_sha256("file sha256", &file.sha256)?;
        let actual = sha256_hex(file.content.as_bytes());
        if actual != file.sha256 {
            bail!(
                "Orbit skill file {:?} hash mismatch: expected {}, got {actual}",
                file.path,
                file.sha256
            );
        }
        if files.insert(file.path.clone(), file.content).is_some() {
            bail!("Orbit skill response has duplicate path {:?}", file.path);
        }
        file_hashes.insert(file.path, file.sha256);
    }
    let actual_tree = tree_sha256(&files);
    if actual_tree != envelope.tree_sha256 {
        bail!(
            "Orbit skill tree hash mismatch: expected {}, got {actual_tree}",
            envelope.tree_sha256
        );
    }
    let manifest = files
        .get(MANIFEST)
        .ok_or_else(|| anyhow!("Orbit skill response is missing {MANIFEST}"))?;
    let frontmatter = orbit_prompts::parse_skill_frontmatter(manifest)
        .map_err(|error| anyhow!("remote {error}"))?;
    if frontmatter.name != envelope.name || frontmatter.version.to_string() != envelope.version {
        bail!("Orbit skill envelope metadata does not match {MANIFEST} frontmatter");
    }
    Ok(ValidatedTree {
        name: envelope.name,
        version: envelope.version,
        tree_sha256: envelope.tree_sha256,
        etag: etag.to_string(),
        files,
        file_hashes,
    })
}

fn compose_tree(
    remote: BTreeMap<String, String>,
    local: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let remote_manifest = remote
        .get(MANIFEST)
        .ok_or_else(|| anyhow!("validated remote tree is missing {MANIFEST}"))?;
    let local_manifest = local
        .get(MANIFEST)
        .ok_or_else(|| anyhow!("embedded local tree is missing {MANIFEST}"))?;
    let collisions: Vec<_> = remote
        .keys()
        .filter(|path| path.as_str() != MANIFEST && local.contains_key(*path))
        .cloned()
        .collect();
    if !collisions.is_empty() {
        bail!("remote and local skill paths collide: {collisions:?}");
    }

    let composed_manifest = orbit_prompts::compose_skill_manifests(remote_manifest, local_manifest)
        .map_err(|error| anyhow!("composing Orbit skill: {error}"))?;
    let mut view = remote;
    for (path, content) in local {
        if path != MANIFEST {
            view.insert(path, content);
        }
    }
    view.insert(MANIFEST.to_string(), composed_manifest);
    Ok(view)
}

fn local_tree() -> BTreeMap<String, String> {
    SkillAssets::iter()
        .filter_map(|path| {
            let content = SkillAssets::get(&path)?;
            String::from_utf8(content.data.into_owned())
                .ok()
                .map(|content| (path.into_owned(), content))
        })
        .collect()
}

fn available_list(files: &BTreeMap<String, String>) -> String {
    files
        .keys()
        .map(|path| format!("  {path}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_relative_path(path: &str) -> Result<()> {
    if path.is_empty() || path.contains(['\\', '\0']) {
        bail!("Orbit skill path is unsafe: {path:?}");
    }
    let parsed = Path::new(path);
    let mut segments = Vec::new();
    for component in parsed.components() {
        match component {
            Component::Normal(segment) => segments.push(
                segment
                    .to_str()
                    .ok_or_else(|| anyhow!("Orbit skill path is not UTF-8: {path:?}"))?,
            ),
            _ => bail!("Orbit skill path is not normalized and relative: {path:?}"),
        }
    }
    if segments.join("/") != path {
        bail!("Orbit skill path is not normalized and relative: {path:?}");
    }
    Ok(())
}

fn validate_component(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\', '\0']) {
        bail!("Orbit {kind} is unsafe as a cache path component: {value:?}");
    }
    Ok(())
}

fn validate_sha256(kind: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("Orbit skill {kind} is not a lowercase SHA-256 digest: {value:?}");
    }
    Ok(())
}

fn tree_sha256(files: &BTreeMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    for (path, content) in files {
        hasher.update(path.as_bytes());
        hasher.update([0]);
        hasher.update((content.len() as u64).to_be_bytes());
        hasher.update(content.as_bytes());
    }
    hex_digest(hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in digest.as_ref() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn cache_root() -> Result<PathBuf> {
    dirs::cache_dir()
        .map(|root| root.join("orbit/skills"))
        .ok_or_else(|| anyhow!("could not determine the operating system user cache directory"))
}

fn skill_cache_root(origin: &str, name: &str) -> Result<PathBuf> {
    validate_component("skill name", name)?;
    Ok(cache_root()?.join(sha256_hex(origin.as_bytes())).join(name))
}

fn newest_cached_tree(origin: &str, name: &str) -> Option<CachedTree> {
    let root = skill_cache_root(origin, name).ok()?;
    if !root.exists() {
        return None;
    }
    let _lock = CacheLock::acquire(&root).ok()?;
    let mut candidates = Vec::new();
    for entry in fs::read_dir(root).ok()?.flatten() {
        let file_type = entry.file_type().ok()?;
        if !file_type.is_dir() || entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        if let Ok(cached) = read_cached_tree(&entry.path(), origin, name) {
            candidates.push(cached);
        }
    }
    candidates
        .into_iter()
        .max_by_key(|cached| cached.validated_at)
}

fn read_cached_tree(directory: &Path, origin: &str, name: &str) -> Result<CachedTree> {
    let manifest: CacheManifest = serde_json::from_slice(
        &fs::read(directory.join(CACHE_MANIFEST)).context("reading skill cache manifest")?,
    )
    .context("parsing skill cache manifest")?;
    if manifest.origin != origin || manifest.name != name {
        bail!("skill cache manifest identity mismatch");
    }
    if directory.file_name().and_then(|value| value.to_str()) != Some(&manifest.version) {
        bail!("skill cache directory does not match manifest version");
    }
    let mut files = BTreeMap::new();
    for (path, expected_hash) in &manifest.file_hashes {
        validate_relative_path(path)?;
        let bytes =
            fs::read(directory.join(path)).with_context(|| format!("reading cached {path}"))?;
        let content =
            String::from_utf8(bytes).with_context(|| format!("cached {path} is not UTF-8"))?;
        if sha256_hex(content.as_bytes()) != *expected_hash {
            bail!("cached {path} hash mismatch");
        }
        files.insert(path.clone(), content);
    }
    if tree_sha256(&files) != manifest.tree_sha256 {
        bail!("cached Orbit skill tree hash mismatch");
    }
    Ok(CachedTree {
        tree: ValidatedTree {
            name: manifest.name,
            version: manifest.version,
            tree_sha256: manifest.tree_sha256,
            etag: manifest.etag,
            files,
            file_hashes: manifest.file_hashes,
        },
        validated_at: manifest.validated_at,
    })
}

fn publish_cache(origin: &str, tree: &ValidatedTree) -> Result<()> {
    let root = skill_cache_root(origin, &tree.name)?;
    fs::create_dir_all(&root).context("creating Orbit skill cache directory")?;
    let stage = root.join(format!(".stage-{}-{}", std::process::id(), Uuid::new_v4()));
    fs::create_dir(&stage).context("creating Orbit skill staging directory")?;
    let result =
        stage_tree(&stage, origin, tree).and_then(|()| publish_staged(&root, &stage, tree));
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    } else {
        let _ = prune_cache(&root);
    }
    result
}

fn stage_tree(stage: &Path, origin: &str, tree: &ValidatedTree) -> Result<()> {
    for (path, content) in &tree.files {
        validate_relative_path(path)?;
        let destination = stage.join(path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating cache path for {path}"))?;
        }
        let mut file = File::create(&destination).with_context(|| format!("staging {path}"))?;
        file.write_all(content.as_bytes())
            .with_context(|| format!("staging {path}"))?;
        file.sync_all()
            .with_context(|| format!("syncing staged {path}"))?;
    }
    let manifest = CacheManifest {
        origin: origin.to_string(),
        name: tree.name.clone(),
        version: tree.version.clone(),
        tree_sha256: tree.tree_sha256.clone(),
        etag: tree.etag.clone(),
        file_hashes: tree.file_hashes.clone(),
        validated_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    };
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    let mut file = File::create(stage.join(CACHE_MANIFEST))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_directory_tree(stage)?;
    Ok(())
}

fn publish_staged(root: &Path, stage: &Path, tree: &ValidatedTree) -> Result<()> {
    let _lock = CacheLock::acquire(root)?;
    let target = root.join(&tree.version);
    let old = root.join(format!(".old-{}", Uuid::new_v4()));
    if target.exists() {
        fs::rename(&target, &old).context("moving existing Orbit skill cache entry aside")?;
    }
    if let Err(error) = fs::rename(stage, &target) {
        if old.exists() {
            let _ = fs::rename(&old, &target);
        }
        return Err(error).context("publishing Orbit skill cache entry");
    }
    sync_directory(root)?;
    if old.exists() {
        let _ = fs::remove_dir_all(old);
    }
    Ok(())
}

fn sync_directory_tree(root: &Path) -> Result<()> {
    let mut directories = vec![root.to_path_buf()];
    for entry in walk_directories(root)? {
        directories.push(entry);
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn walk_directories(root: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            result.extend(walk_directories(&entry.path())?);
            result.push(entry.path());
        }
    }
    Ok(result)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

struct CacheLock {
    path: PathBuf,
}

impl CacheLock {
    fn acquire(root: &Path) -> Result<Self> {
        let path = root.join(".populate.lock");
        let deadline = std::time::Instant::now() + LOCK_WAIT;
        loop {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    writeln!(file, "{}", std::process::id())?;
                    file.sync_all()?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if std::time::Instant::now() >= deadline {
                        bail!("timed out waiting for concurrent Orbit skill cache writer");
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => return Err(error).context("locking Orbit skill cache"),
            }
        }
    }
}

impl Drop for CacheLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn prune_cache(root: &Path) -> Result<()> {
    let _lock = CacheLock::acquire(root)?;
    let mut versions = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let validated_at = fs::read(entry.path().join(CACHE_MANIFEST))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<CacheManifest>(&bytes).ok())
            .map_or(0, |manifest| manifest.validated_at);
        versions.push((validated_at, entry.path()));
    }
    versions.sort_by_key(|(validated_at, _)| std::cmp::Reverse(*validated_at));
    for (_, path) in versions.into_iter().skip(2) {
        let _ = fs::remove_dir_all(path);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote_tree(version: &str, manifest: &str) -> ValidatedTree {
        let files = BTreeMap::from([
            (MANIFEST.to_string(), manifest.to_string()),
            ("references/remote.md".to_string(), "remote\n".to_string()),
        ]);
        let file_hashes = files
            .iter()
            .map(|(path, content)| (path.clone(), sha256_hex(content.as_bytes())))
            .collect();
        ValidatedTree {
            name: DEFAULT_SKILL.to_string(),
            version: version.to_string(),
            tree_sha256: tree_sha256(&files),
            etag: format!("\"{version}:etag\""),
            files,
            file_hashes,
        }
    }

    #[test]
    fn arguments_keep_get_and_compatibility_grammar() {
        assert_eq!(resolve(None, None).unwrap(), Request::List);
        assert_eq!(
            resolve(Some("references/local/sql.md"), None).unwrap(),
            Request::Print {
                name: "orbit".to_string(),
                path: "references/local/sql.md".to_string(),
            }
        );
        assert_eq!(
            resolve(Some("orbit"), None).unwrap(),
            Request::Print {
                name: "orbit".to_string(),
                path: MANIFEST.to_string(),
            }
        );
        assert!(resolve_named("unknown", MANIFEST).is_err());
        assert!(
            resolve(Some("references/local/sql.md"), Some(MANIFEST))
                .unwrap_err()
                .to_string()
                .contains("path shorthand")
        );
    }

    #[test]
    fn path_validation_rejects_absolute_traversal_and_non_normalized_paths() {
        for path in [
            "",
            "/etc/passwd",
            "../secret",
            "references/../SKILL.md",
            "./SKILL.md",
            "references\\secret.md",
            "references//guide.md",
        ] {
            assert!(validate_relative_path(path).is_err(), "{path:?}");
        }
        assert!(validate_relative_path("references/guide.md").is_ok());
    }

    #[test]
    fn canonical_tree_hash_matches_server_contract_vector() {
        let files = BTreeMap::from([
            ("SKILL.md".to_string(), "alpha\n".to_string()),
            ("references/guide.md".to_string(), "beta".to_string()),
        ]);
        assert_eq!(
            tree_sha256(&files),
            "7966df3b2283aa44b6d29826c89044f1739aa99f25fc84f44a775eaa41ba7817"
        );
    }

    #[test]
    fn envelope_validation_checks_file_and_tree_hashes() {
        let manifest = "---\nname: orbit\nversion: 1.0.0\ndescription: Test\n---\nbody\n";
        let mut tree = remote_tree("1.0.0", manifest);
        let envelope = RemoteEnvelope {
            name: tree.name.clone(),
            version: tree.version.clone(),
            tree_sha256: tree.tree_sha256.clone(),
            files: tree
                .files
                .iter()
                .map(|(path, content)| RemoteFile {
                    path: path.clone(),
                    sha256: tree.file_hashes[path].clone(),
                    content: content.clone(),
                })
                .collect(),
        };
        let body = serde_json::to_vec(&serde_json::json!({
            "name": envelope.name,
            "version": envelope.version,
            "tree_sha256": envelope.tree_sha256,
            "files": envelope.files.iter().map(|file| serde_json::json!({
                "path": file.path, "sha256": file.sha256, "content": file.content
            })).collect::<Vec<_>>()
        }))
        .unwrap();
        validate_remote_envelope("orbit", "\"etag\"", &body).unwrap();

        tree.tree_sha256.replace_range(..1, "0");
        let bad = String::from_utf8(body)
            .unwrap()
            .replace(&envelope.tree_sha256, &tree.tree_sha256);
        assert!(validate_remote_envelope("orbit", "\"etag\"", bad.as_bytes()).is_err());
    }

    #[test]
    fn runtime_skew_fixtures_compose_in_both_directions() {
        for (remote, local, expected, absent) in [
            (
                include_str!("../tests/fixtures/skills/new-remote/SKILL.md"),
                include_str!("../tests/fixtures/skills/old-local/SKILL.md"),
                "## Old local shared",
                "orbit:include local:new-only",
            ),
            (
                include_str!("../tests/fixtures/skills/old-remote/SKILL.md"),
                include_str!("../tests/fixtures/skills/new-local/SKILL.md"),
                "## New local extra",
                "orbit:section extra",
            ),
        ] {
            let composed = orbit_prompts::compose_skill_manifests(remote, local).unwrap();
            assert!(composed.starts_with("---\nname: orbit\n"));
            assert!(composed.contains(expected), "{composed}");
            assert!(!composed.contains(absent), "{composed}");
            assert!(!composed.contains("<!-- orbit:"), "{composed}");
            assert!(!composed.contains("<!-- /orbit:"), "{composed}");
        }
    }

    #[test]
    fn union_exposes_namespaced_local_files_and_rejects_collisions() {
        let remote = BTreeMap::from([
            (
                MANIFEST.to_string(),
                "<!-- orbit:include local:a -->\n".to_string(),
            ),
            ("references/remote.md".to_string(), "remote".to_string()),
        ]);
        let local = BTreeMap::from([
            (
                MANIFEST.to_string(),
                "<!-- orbit:section a -->\nlocal\n<!-- /orbit:section -->\n".to_string(),
            ),
            ("references/local/sql.md".to_string(), "sql".to_string()),
        ]);
        let composed = compose_tree(remote.clone(), local.clone()).unwrap();
        assert_eq!(composed["references/remote.md"], "remote");
        assert_eq!(composed["references/local/sql.md"], "sql");
        assert_eq!(composed[MANIFEST], "local\n");

        let mut colliding = local;
        colliding.insert("references/remote.md".to_string(), "bad".to_string());
        assert!(compose_tree(remote, colliding).is_err());
    }

    #[test]
    fn cache_round_trip_contains_remote_bytes_only() {
        let root = tempfile::tempdir().unwrap();
        let origin = "https://gitlab.example.com:8443";
        let manifest = "---\nname: orbit\nversion: 1.0.0\ndescription: Test\n---\nremote\n";
        let tree = remote_tree("1.0.0", manifest);
        let stage = root.path().join(".stage");
        fs::create_dir(&stage).unwrap();
        stage_tree(&stage, origin, &tree).unwrap();
        publish_staged(root.path(), &stage, &tree).unwrap();

        let cached = read_cached_tree(&root.path().join("1.0.0"), origin, "orbit").unwrap();
        assert_eq!(cached.tree.files, tree.files);
        assert_eq!(
            fs::read(root.path().join("1.0.0/SKILL.md")).unwrap(),
            manifest.as_bytes()
        );
        assert!(!root.path().join("1.0.0/references/local").exists());
        assert!(
            !fs::read_to_string(root.path().join("1.0.0/SKILL.md"))
                .unwrap()
                .contains("Local CLI")
        );
    }

    #[test]
    fn concurrent_population_publishes_only_complete_trees() {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().to_path_buf();
        let origin = "https://gitlab.example.com";
        let manifest = "---\nname: orbit\nversion: 1.0.0\ndescription: Test\n---\nremote\n";
        let tree = remote_tree("1.0.0", manifest);
        let mut writers = Vec::new();
        for index in 0..2 {
            let root = root_path.clone();
            let tree = tree.clone();
            writers.push(thread::spawn(move || {
                let stage = root.join(format!(".stage-{index}"));
                fs::create_dir(&stage).unwrap();
                stage_tree(&stage, origin, &tree).unwrap();
                publish_staged(&root, &stage, &tree).unwrap();
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }
        let cached = read_cached_tree(&root_path.join("1.0.0"), origin, "orbit").unwrap();
        assert_eq!(cached.tree.files, tree.files);
    }

    #[test]
    fn origins_have_isolated_cache_keys() {
        assert_ne!(
            sha256_hex(b"https://gitlab.example.com"),
            sha256_hex(b"https://gitlab.example.com:8443")
        );
    }

    #[test]
    fn unsafe_remote_paths_are_rejected_before_staging() {
        let content = "bad";
        let hash = sha256_hex(content.as_bytes());
        for path in ["../secret", "/etc/passwd", "references/../secret", "a\\b"] {
            let body = serde_json::to_vec(&serde_json::json!({
                "name": "orbit",
                "version": "1.0.0",
                "tree_sha256": "0".repeat(64),
                "files": [{"path": path, "sha256": hash, "content": content}]
            }))
            .unwrap();
            assert!(
                validate_remote_envelope("orbit", "\"etag\"", &body).is_err(),
                "{path}"
            );
        }
    }
}
