use super::sysroot::EmbeddedSysroot;
use super::workspace::{discover_manifest_paths_for_root, normalize_existing_path};
use super::*;

/// Upper bound on a Cargo.toml file we will read off disk. Real manifests are
/// well under this; anything larger is rejected before the read allocates.
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

pub(super) struct ManifestCache {
    pub(super) root_path: PathBuf,
    pub(super) manifest_paths: Vec<PathBuf>,
    parsed: HashMap<PathBuf, ParsedCargoManifest>,
}

#[derive(Clone)]
pub(super) struct ParsedCargoManifest {
    manifest_path: PathBuf,
    root_dir: PathBuf,
    manifest: cargo_manifest::TomlManifest,
}

struct SyntheticCargoWorkspace {
    workspace_manifest_path: PathBuf,
    metadata: Metadata,
}

#[derive(Clone)]
struct LocalWorkspacePackage {
    package_id: String,
    package_name: String,
    manifest_path: PathBuf,
    version: String,
    edition: String,
    features: BTreeMap<String, Vec<String>>,
    targets: Vec<LocalTargetSpec>,
    dependencies: Vec<ResolvedDependencyCandidate>,
    is_member: bool,
}

#[derive(Clone)]
struct LocalTargetSpec {
    name: String,
    kind: Vec<&'static str>,
    crate_types: Vec<&'static str>,
    required_features: Vec<String>,
    src_path: PathBuf,
    edition: String,
    doctest: bool,
    test: bool,
    doc: bool,
}

#[derive(Clone)]
struct ResolvedDependencyCandidate {
    manifest_name: String,
    code_name: String,
    target_package_name: String,
    target_manifest_path: PathBuf,
    kind: &'static str,
    target: Option<String>,
    optional: bool,
    uses_default_features: bool,
    features: Vec<String>,
}

struct WorkspaceDescriptor {
    is_workspace_root: bool,
    workspace_manifest_path: PathBuf,
    workspace_root: PathBuf,
    members: Vec<PathBuf>,
    default_members: Vec<PathBuf>,
    exclude: Vec<String>,
    workspace_package: Option<cargo_manifest::InheritablePackage>,
    workspace_dependencies: BTreeMap<String, cargo_manifest::TomlDependency>,
}

impl ManifestCache {
    pub(super) fn new(root_path: &str) -> Result<Self> {
        let root_path = PathBuf::from(root_path);
        let mut manifest_paths =
            discover_manifest_paths_for_root(root_path.to_string_lossy().as_ref())
                .into_iter()
                .filter_map(|path| normalize_existing_path(&path).or(Some(path)))
                .filter(|path| path.starts_with(&root_path))
                .collect::<Vec<_>>();
        manifest_paths.sort();
        manifest_paths.dedup();

        Ok(Self {
            root_path,
            manifest_paths,
            parsed: HashMap::new(),
        })
    }

    fn load(&mut self, manifest_path: &Path) -> Result<&ParsedCargoManifest> {
        let Some(manifest_path) = self.normalize_manifest_path(manifest_path)? else {
            bail!(
                "manifest {} is outside the indexed repository",
                manifest_path.display()
            );
        };

        if !self.parsed.contains_key(&manifest_path) {
            // Cargo manifests are human-authored config. Anything multi-megabyte is
            // almost certainly adversarial or malformed; bail before `read_to_string`
            // allocates unbounded memory on the indexer.
            if let Ok(meta) = std::fs::metadata(&manifest_path)
                && meta.len() > MAX_MANIFEST_BYTES
            {
                bail!(
                    "Cargo manifest {} is {} bytes, exceeds {} byte cap",
                    manifest_path.display(),
                    meta.len(),
                    MAX_MANIFEST_BYTES
                );
            }
            let source = std::fs::read_to_string(&manifest_path).with_context(|| {
                format!("failed to read Cargo manifest {}", manifest_path.display())
            })?;
            let manifest =
                toml::from_str::<cargo_manifest::TomlManifest>(&source).with_context(|| {
                    format!("failed to parse Cargo manifest {}", manifest_path.display())
                })?;
            let root_dir = manifest_path
                .parent()
                .ok_or_else(|| anyhow!("manifest {} has no parent", manifest_path.display()))?
                .to_path_buf();
            self.parsed.insert(
                manifest_path.clone(),
                ParsedCargoManifest {
                    manifest_path: manifest_path.clone(),
                    root_dir,
                    manifest,
                },
            );
        }

        Ok(self.parsed.get(&manifest_path).expect("manifest inserted"))
    }

    fn normalize_manifest_path(&self, manifest_path: &Path) -> Result<Option<PathBuf>> {
        let normalized = normalize_existing_path(manifest_path).unwrap_or_else(|| {
            if manifest_path.is_absolute() {
                manifest_path.to_path_buf()
            } else {
                self.root_path.join(manifest_path)
            }
        });

        if !normalized.starts_with(&self.root_path) {
            return Ok(None);
        }
        if normalized
            .file_name()
            .is_none_or(|name| name != "Cargo.toml")
        {
            return Ok(None);
        }
        if !normalized.is_file() {
            return Ok(None);
        }

        Ok(Some(normalized))
    }

    fn dependency_manifest_path(&self, dependency_dir: &Path) -> Result<Option<PathBuf>> {
        self.normalize_manifest_path(&dependency_dir.join("Cargo.toml"))
    }

    pub(super) fn workspace_manifest_path_for(&mut self, manifest_path: &Path) -> Result<PathBuf> {
        let manifest_path = self
            .normalize_manifest_path(manifest_path)?
            .ok_or_else(|| {
                anyhow!(
                    "manifest {} is outside the indexed repository",
                    manifest_path.display()
                )
            })?;

        let current = self.load(&manifest_path)?.clone();
        if current.manifest.workspace.is_some() {
            return Ok(current.manifest_path.clone());
        }

        if let Some(workspace_root) = current
            .manifest
            .package()
            .and_then(|package| package.workspace.as_ref())
        {
            let candidate = current.root_dir.join(workspace_root).join("Cargo.toml");
            if self
                .load(&candidate)
                .is_ok_and(|manifest| manifest.manifest.workspace.is_some())
            {
                return self.normalize_manifest_path(&candidate)?.ok_or_else(|| {
                    anyhow!(
                        "workspace root {} is outside the indexed repository",
                        candidate.display()
                    )
                });
            }
        }

        let mut ancestor = current.root_dir.parent();
        while let Some(dir) = ancestor {
            if !dir.starts_with(&self.root_path) {
                break;
            }
            let candidate = dir.join("Cargo.toml");
            if candidate != manifest_path
                && self
                    .load(&candidate)
                    .is_ok_and(|manifest| manifest.manifest.workspace.is_some())
                && self.manifest_declares_member(&candidate, &manifest_path)?
            {
                return self.normalize_manifest_path(&candidate)?.ok_or_else(|| {
                    anyhow!(
                        "workspace root {} is outside the indexed repository",
                        candidate.display()
                    )
                });
            }
            ancestor = dir.parent();
        }

        Ok(manifest_path)
    }

    fn manifest_declares_member(
        &mut self,
        workspace_manifest_path: &Path,
        member_manifest_path: &Path,
    ) -> Result<bool> {
        let workspace_manifest = self.load(workspace_manifest_path)?.clone();
        let Some(workspace) = workspace_manifest.manifest.workspace.as_ref() else {
            return Ok(false);
        };
        let Some(patterns) = workspace.members.as_deref() else {
            return Ok(false);
        };

        let member_relative =
            relative_workspace_path(&workspace_manifest.root_dir, member_manifest_path);
        if workspace_path_is_excluded(
            workspace.exclude.as_deref().unwrap_or_default(),
            &workspace_manifest.root_dir,
            member_manifest_path,
        )? {
            return Ok(false);
        }

        matches_workspace_patterns(patterns, &member_relative)
    }
}

pub(super) fn build_project_workspace(
    manifest_path: &Path,
    manifest_cache: &mut ManifestCache,
    embedded_sysroot: &EmbeddedSysroot,
) -> Result<ProjectWorkspace> {
    let synthetic = build_synthetic_workspace(manifest_path, manifest_cache)?;
    let utf8_manifest_path = Utf8PathBuf::from_path_buf(synthetic.workspace_manifest_path.clone())
        .map_err(|path| {
            anyhow!(
                "workspace manifest path is not valid UTF-8: {}",
                path.display()
            )
        })?;
    let manifest_path = ManifestPath::try_from(AbsPathBuf::assert(utf8_manifest_path))
        .map_err(|path| anyhow!("workspace manifest path is not absolute: {path}"))?;
    let cargo = CargoWorkspace::new(synthetic.metadata, manifest_path, Env::default(), false);

    Ok(ProjectWorkspace {
        kind: ProjectWorkspaceKind::Cargo {
            cargo,
            error: None,
            build_scripts: WorkspaceBuildScripts::default(),
            rustc: Err(None),
        },
        sysroot: embedded_sysroot.project_workspace_sysroot()?,
        rustc_cfg: server_rustc_cfg(),
        toolchain: None,
        target: Ok(server_target_data()),
        cfg_overrides: CfgOverrides::default(),
        extra_includes: Vec::new(),
        set_test: false,
    })
}

fn build_synthetic_workspace(
    manifest_path: &Path,
    manifest_cache: &mut ManifestCache,
) -> Result<SyntheticCargoWorkspace> {
    let descriptor = build_workspace_descriptor(manifest_path, manifest_cache)?;
    let mut packages = HashMap::new();
    let mut queue = VecDeque::from(descriptor.members.clone());
    let mut member_set = descriptor.members.iter().cloned().collect::<HashSet<_>>();
    let mut visited = HashSet::new();

    while let Some(package_manifest_path) = queue.pop_front() {
        if !visited.insert(package_manifest_path.clone()) {
            continue;
        }

        let local_package = resolve_local_package(
            &package_manifest_path,
            &descriptor,
            manifest_cache,
            member_set.contains(&package_manifest_path),
        )?;

        for dependency in &local_package.dependencies {
            if descriptor.is_workspace_root
                && dependency
                    .target_manifest_path
                    .starts_with(&descriptor.workspace_root)
                && !workspace_path_is_excluded(
                    &descriptor.exclude,
                    &descriptor.workspace_root,
                    &dependency.target_manifest_path,
                )?
            {
                member_set.insert(dependency.target_manifest_path.clone());
            }
            queue.push_back(dependency.target_manifest_path.clone());
        }

        packages.insert(package_manifest_path, local_package);
    }

    for manifest_path in &member_set {
        if let Some(package) = packages.get_mut(manifest_path) {
            package.is_member = true;
        }
    }

    let package_values = packages.into_values().collect::<Vec<_>>();
    let metadata = synthetic_metadata_from_packages(&descriptor, package_values)?;
    Ok(SyntheticCargoWorkspace {
        workspace_manifest_path: descriptor.workspace_manifest_path,
        metadata,
    })
}

fn build_workspace_descriptor(
    manifest_path: &Path,
    manifest_cache: &mut ManifestCache,
) -> Result<WorkspaceDescriptor> {
    let workspace_manifest_path = manifest_cache.workspace_manifest_path_for(manifest_path)?;
    let workspace_manifest = manifest_cache.load(&workspace_manifest_path)?.clone();

    let Some(workspace) = workspace_manifest.manifest.workspace.as_ref() else {
        return Ok(WorkspaceDescriptor {
            is_workspace_root: false,
            workspace_manifest_path: workspace_manifest.manifest_path.clone(),
            workspace_root: workspace_manifest.root_dir.clone(),
            members: vec![workspace_manifest.manifest_path.clone()],
            default_members: vec![workspace_manifest.manifest_path.clone()],
            exclude: Vec::new(),
            workspace_package: None,
            workspace_dependencies: BTreeMap::new(),
        });
    };

    let mut members = workspace_members_for_root(&workspace_manifest, manifest_cache)?;
    if workspace_manifest.manifest.package().is_some()
        && !workspace_path_is_excluded(
            workspace.exclude.as_deref().unwrap_or_default(),
            &workspace_manifest.root_dir,
            &workspace_manifest.manifest_path,
        )?
    {
        members.push(workspace_manifest.manifest_path.clone());
    }
    members.sort();
    members.dedup();

    let mut descriptor = WorkspaceDescriptor {
        is_workspace_root: true,
        workspace_manifest_path: workspace_manifest.manifest_path.clone(),
        workspace_root: workspace_manifest.root_dir.clone(),
        members,
        default_members: Vec::new(),
        exclude: workspace.exclude.clone().unwrap_or_default(),
        workspace_package: workspace.package.clone(),
        workspace_dependencies: workspace_dependencies_map(workspace),
    };

    expand_workspace_members_via_path_dependencies(&mut descriptor, manifest_cache)?;
    descriptor.default_members = workspace_default_members(
        &workspace_manifest,
        workspace,
        &descriptor.members,
        manifest_cache,
    )?;

    Ok(descriptor)
}

fn relative_workspace_path(workspace_root: &Path, manifest_path: &Path) -> String {
    let member_dir = manifest_path.parent().unwrap_or(manifest_path);
    let root_components = workspace_root.components().collect::<Vec<_>>();
    let member_components = member_dir.components().collect::<Vec<_>>();

    let mut shared_prefix_len = 0;
    while shared_prefix_len < root_components.len()
        && shared_prefix_len < member_components.len()
        && root_components[shared_prefix_len] == member_components[shared_prefix_len]
    {
        shared_prefix_len += 1;
    }

    let mut relative = PathBuf::new();
    for _ in shared_prefix_len..root_components.len() {
        relative.push("..");
    }
    for component in &member_components[shared_prefix_len..] {
        relative.push(component.as_os_str());
    }

    let relative = relative.to_string_lossy().replace('\\', "/");
    if relative.is_empty() {
        ".".to_string()
    } else {
        relative
    }
}

fn compile_glob_matchers(patterns: &[String]) -> Result<Vec<GlobMatcher>> {
    patterns
        .iter()
        .map(|pattern| {
            Glob::new(pattern)
                .with_context(|| format!("invalid workspace glob `{pattern}`"))
                .map(|glob| glob.compile_matcher())
        })
        .collect()
}

fn matches_workspace_patterns(patterns: &[String], relative_path: &str) -> Result<bool> {
    if patterns.is_empty() {
        return Ok(false);
    }
    let matchers = compile_glob_matchers(patterns)?;
    Ok(matchers.iter().any(|matcher| {
        matcher.is_match(relative_path) || (relative_path == "." && matcher.is_match(""))
    }))
}

fn workspace_path_is_excluded(
    exclude_entries: &[String],
    workspace_root: &Path,
    manifest_path: &Path,
) -> Result<bool> {
    let relative_path = relative_workspace_path(workspace_root, manifest_path);
    Ok(path_has_prefix(&relative_path, exclude_entries))
}

// Cargo's `workspace.exclude` is a list of directory prefixes, not glob patterns.
// `exclude = ["vendor"]` must exclude both `vendor` and `vendor/sub`.
fn path_has_prefix(relative_path: &str, exclude_entries: &[String]) -> bool {
    let normalized = relative_path.trim_end_matches('/');
    exclude_entries.iter().any(|entry| {
        let entry = entry.trim_end_matches('/');
        if entry.is_empty() {
            return false;
        }
        normalized == entry
            || normalized
                .strip_prefix(entry)
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

fn workspace_dependencies_map(
    workspace: &cargo_manifest::TomlWorkspace,
) -> BTreeMap<String, cargo_manifest::TomlDependency> {
    workspace
        .dependencies
        .as_ref()
        .map(|dependencies| {
            dependencies
                .iter()
                .map(|(name, dependency)| (name.to_string(), dependency.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn workspace_members_for_root(
    workspace_manifest: &ParsedCargoManifest,
    manifest_cache: &ManifestCache,
) -> Result<Vec<PathBuf>> {
    let Some(workspace) = workspace_manifest.manifest.workspace.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(patterns) = workspace.members.as_deref() else {
        return Ok(Vec::new());
    };

    let include_matchers = compile_glob_matchers(patterns)?;
    let exclude_entries = workspace.exclude.as_deref().unwrap_or_default();
    let mut members = Vec::new();

    for manifest_path in &manifest_cache.manifest_paths {
        let relative_path = relative_workspace_path(&workspace_manifest.root_dir, manifest_path);
        if include_matchers
            .iter()
            .any(|matcher| matcher.is_match(&relative_path))
            && !path_has_prefix(&relative_path, exclude_entries)
        {
            members.push(manifest_path.clone());
        }
    }

    Ok(members)
}

fn workspace_default_members(
    workspace_manifest: &ParsedCargoManifest,
    workspace: &cargo_manifest::TomlWorkspace,
    members: &[PathBuf],
    manifest_cache: &ManifestCache,
) -> Result<Vec<PathBuf>> {
    if let Some(default_members) = workspace.default_members.as_deref() {
        let default_matchers = compile_glob_matchers(default_members)?;
        let mut result = Vec::new();
        for manifest_path in &manifest_cache.manifest_paths {
            let relative_path =
                relative_workspace_path(&workspace_manifest.root_dir, manifest_path);
            if default_matchers
                .iter()
                .any(|matcher| matcher.is_match(&relative_path))
            {
                result.push(manifest_path.clone());
            }
        }
        result.sort();
        result.dedup();
        return Ok(result);
    }

    if workspace_manifest.manifest.package().is_some() {
        return Ok(vec![workspace_manifest.manifest_path.clone()]);
    }

    Ok(members.to_vec())
}

fn expand_workspace_members_via_path_dependencies(
    descriptor: &mut WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
) -> Result<()> {
    let mut queue = VecDeque::from(descriptor.members.clone());
    let mut seen = descriptor.members.iter().cloned().collect::<HashSet<_>>();

    while let Some(manifest_path) = queue.pop_front() {
        let parsed = manifest_cache.load(&manifest_path)?.clone();
        for dependency in resolve_local_dependency_candidates(&parsed, descriptor, manifest_cache)?
        {
            if !dependency
                .target_manifest_path
                .starts_with(&descriptor.workspace_root)
                || workspace_path_is_excluded(
                    &descriptor.exclude,
                    &descriptor.workspace_root,
                    &dependency.target_manifest_path,
                )?
            {
                continue;
            }
            if seen.insert(dependency.target_manifest_path.clone()) {
                descriptor
                    .members
                    .push(dependency.target_manifest_path.clone());
                queue.push_back(dependency.target_manifest_path);
            }
        }
    }

    descriptor.members.sort();
    descriptor.members.dedup();
    Ok(())
}

fn resolve_local_package(
    manifest_path: &Path,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
    is_member: bool,
) -> Result<LocalWorkspacePackage> {
    let parsed = manifest_cache.load(manifest_path)?.clone();
    let package = parsed.manifest.package().ok_or_else(|| {
        anyhow!(
            "manifest {} has no [package] section",
            parsed.manifest_path.display()
        )
    })?;

    let package_name = package
        .name
        .as_ref()
        .map(ToString::to_string)
        .ok_or_else(|| {
            anyhow!(
                "manifest {} has no package.name",
                parsed.manifest_path.display()
            )
        })?;
    let version = resolve_package_version(package, descriptor)?.ok_or_else(|| {
        anyhow!(
            "manifest {} has no package.version",
            parsed.manifest_path.display()
        )
    })?;
    let edition = resolve_package_edition(package, descriptor);
    let features = parsed
        .manifest
        .features()
        .map(|features| {
            features
                .iter()
                .map(|(name, values)| (name.to_string(), values.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let dependencies = resolve_local_dependency_candidates(&parsed, descriptor, manifest_cache)?;
    let targets = collect_target_specs(
        &parsed.manifest,
        &parsed.root_dir,
        &manifest_cache.root_path,
        &package_name,
        &edition,
    );

    Ok(LocalWorkspacePackage {
        package_id: package_id_for(&parsed.manifest_path, &package_name, &version),
        package_name,
        manifest_path: parsed.manifest_path.clone(),
        version,
        edition,
        features,
        targets,
        dependencies,
        is_member,
    })
}

fn resolve_local_dependency_candidates(
    parsed: &ParsedCargoManifest,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
) -> Result<Vec<ResolvedDependencyCandidate>> {
    let mut dependencies = Vec::new();
    collect_dependency_candidates(
        parsed.manifest.dependencies.as_ref(),
        "normal",
        None,
        parsed,
        descriptor,
        manifest_cache,
        &mut dependencies,
    )?;
    collect_dependency_candidates(
        parsed.manifest.dev_dependencies(),
        "dev",
        None,
        parsed,
        descriptor,
        manifest_cache,
        &mut dependencies,
    )?;
    collect_dependency_candidates(
        parsed.manifest.build_dependencies(),
        "build",
        None,
        parsed,
        descriptor,
        manifest_cache,
        &mut dependencies,
    )?;

    for (target, platform) in parsed.manifest.target.iter().flatten() {
        collect_dependency_candidates(
            platform.dependencies.as_ref(),
            "normal",
            Some(target),
            parsed,
            descriptor,
            manifest_cache,
            &mut dependencies,
        )?;
        collect_dependency_candidates(
            platform.dev_dependencies(),
            "dev",
            Some(target),
            parsed,
            descriptor,
            manifest_cache,
            &mut dependencies,
        )?;
        collect_dependency_candidates(
            platform.build_dependencies(),
            "build",
            Some(target),
            parsed,
            descriptor,
            manifest_cache,
            &mut dependencies,
        )?;
    }

    Ok(dependencies)
}

fn collect_dependency_candidates(
    dependencies: Option<
        &BTreeMap<cargo_manifest::PackageName, cargo_manifest::InheritableDependency>,
    >,
    kind: &'static str,
    target: Option<&str>,
    parsed: &ParsedCargoManifest,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
    output: &mut Vec<ResolvedDependencyCandidate>,
) -> Result<()> {
    let Some(dependencies) = dependencies else {
        return Ok(());
    };

    for (name, dependency) in dependencies {
        let Some(candidate) = resolve_dependency_candidate(
            name.as_ref(),
            dependency,
            kind,
            target,
            parsed,
            descriptor,
            manifest_cache,
        )?
        else {
            continue;
        };
        output.push(candidate);
    }

    Ok(())
}

fn resolve_dependency_candidate(
    manifest_name: &str,
    dependency: &cargo_manifest::InheritableDependency,
    kind: &'static str,
    target: Option<&str>,
    parsed: &ParsedCargoManifest,
    descriptor: &WorkspaceDescriptor,
    manifest_cache: &mut ManifestCache,
) -> Result<Option<ResolvedDependencyCandidate>> {
    let (dependency, path_base) = match dependency {
        cargo_manifest::InheritableDependency::Value(dependency) => {
            (dependency.clone(), parsed.root_dir.clone())
        }
        cargo_manifest::InheritableDependency::Inherit(inherited) => {
            let workspace_dependency = descriptor
                .workspace_dependencies
                .get(manifest_name)
                .ok_or_else(|| anyhow!("workspace dependency `{manifest_name}` is not defined"))?;
            (
                merge_workspace_dependency(inherited, workspace_dependency.clone()),
                descriptor.workspace_root.clone(),
            )
        }
    };

    let cargo_manifest::TomlDependency::Detailed(detailed) = dependency else {
        return Ok(None);
    };
    let Some(path) = detailed.path.as_ref() else {
        return Ok(None);
    };
    if detailed.base.is_some() {
        return Ok(None);
    }

    let dependency_dir = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        path_base.join(path)
    };
    let Some(target_manifest_path) = manifest_cache.dependency_manifest_path(&dependency_dir)?
    else {
        tracing::debug!(
            rejected_path = %dependency_dir.display(),
            source_manifest = %parsed.manifest_path.display(),
            dependency = manifest_name,
            "dropping path dependency resolving outside the indexed repository",
        );
        return Ok(None);
    };
    let target_manifest = manifest_cache.load(&target_manifest_path)?;
    let target_package_name = target_manifest
        .manifest
        .package()
        .and_then(|package| package.name.as_ref())
        .map(ToString::to_string)
        .ok_or_else(|| {
            anyhow!(
                "dependency manifest {} has no package.name",
                target_manifest.manifest_path.display()
            )
        })?;
    let code_name = normalize_crate_name(manifest_name);
    let target = target.and_then(parse_target_platform);

    Ok(Some(ResolvedDependencyCandidate {
        manifest_name: manifest_name.to_string(),
        code_name: code_name.clone(),
        target_package_name,
        target_manifest_path,
        kind,
        target,
        optional: detailed.optional.unwrap_or(false),
        uses_default_features: detailed.default_features().unwrap_or(true),
        features: detailed.features.unwrap_or_default(),
    }))
}

fn merge_workspace_dependency(
    inherited: &cargo_manifest::TomlInheritedDependency,
    workspace_dependency: cargo_manifest::TomlDependency,
) -> cargo_manifest::TomlDependency {
    let mut detailed = match workspace_dependency {
        cargo_manifest::TomlDependency::Simple(version) => cargo_manifest::TomlDetailedDependency {
            version: Some(version),
            ..Default::default()
        },
        cargo_manifest::TomlDependency::Detailed(detailed) => detailed,
    };

    if let Some(features) = &inherited.features {
        let mut merged = detailed.features.unwrap_or_default();
        merged.extend(features.clone());
        detailed.features = Some(merged);
    }
    if let Some(optional) = inherited.optional {
        detailed.optional = Some(optional);
    }
    if let Some(default_features) = inherited.default_features() {
        detailed.default_features = Some(default_features);
        detailed.default_features2 = None;
    }
    if let Some(public) = inherited.public {
        detailed.public = Some(public);
    }

    cargo_manifest::TomlDependency::Detailed(detailed)
}

fn resolve_package_version(
    package: &cargo_manifest::TomlPackage,
    descriptor: &WorkspaceDescriptor,
) -> Result<Option<String>> {
    let version = match package.version.as_ref() {
        Some(cargo_manifest::InheritableField::Value(version)) => Some(version.to_string()),
        Some(cargo_manifest::InheritableField::Inherit(_)) => descriptor
            .workspace_package
            .as_ref()
            .and_then(|workspace| workspace.version.as_ref())
            .map(ToString::to_string),
        None => None,
    };
    Ok(version)
}

fn resolve_package_edition(
    package: &cargo_manifest::TomlPackage,
    descriptor: &WorkspaceDescriptor,
) -> String {
    match package.edition.as_ref() {
        Some(cargo_manifest::InheritableField::Value(edition)) => edition.clone(),
        Some(cargo_manifest::InheritableField::Inherit(_)) => descriptor
            .workspace_package
            .as_ref()
            .and_then(|workspace| workspace.edition.as_ref())
            .cloned()
            .unwrap_or_else(|| "2015".to_string()),
        None => "2015".to_string(),
    }
}

fn normalize_crate_name(name: &str) -> String {
    name.replace('-', "_")
}

fn package_id_for(manifest_path: &Path, package_name: &str, version: &str) -> String {
    format!(
        "path+file://{}#{}@{}",
        manifest_path
            .parent()
            .unwrap_or(manifest_path)
            .to_string_lossy(),
        package_name,
        version
    )
}

fn parse_target_platform(target: &str) -> Option<String> {
    match target.parse::<Platform>() {
        Ok(platform) => {
            let roundtrip = platform.to_string();
            if roundtrip == target {
                Some(roundtrip)
            } else {
                Some(target.to_string())
            }
        }
        Err(_) => Some(target.to_string()),
    }
}

fn cfg_flag(name: &str) -> CfgAtom {
    CfgAtom::Flag(Symbol::intern(name))
}

fn cfg_key_value(key: &str, value: &str) -> CfgAtom {
    CfgAtom::KeyValue {
        key: Symbol::intern(key),
        value: Symbol::intern(value),
    }
}

// Pins cfg atoms to linux/x86_64 to match the server indexing environment.
// Trades local-platform accuracy for reproducibility across indexer runs.
fn server_rustc_cfg() -> Vec<CfgAtom> {
    vec![
        cfg_flag("unix"),
        cfg_key_value("panic", "unwind"),
        cfg_key_value("target_arch", "x86_64"),
        cfg_key_value("target_endian", "little"),
        cfg_key_value("target_env", "gnu"),
        cfg_key_value("target_family", "unix"),
        cfg_key_value("target_os", "linux"),
        cfg_key_value("target_pointer_width", "64"),
        cfg_key_value("target_vendor", "unknown"),
    ]
}

fn server_target_data() -> TargetData {
    TargetData {
        data_layout:
            "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-i128:128-f80:128-n8:16:32:64-S128".into(),
        arch: Arch::Other,
    }
}

fn collect_target_specs(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    repo_root: &Path,
    package_name: &str,
    package_edition: &str,
) -> Vec<LocalTargetSpec> {
    let mut targets = Vec::new();
    let default_lib_name = normalize_crate_name(package_name);

    if let Some(lib_target) = collect_lib_target(
        manifest,
        package_root,
        repo_root,
        package_edition,
        &default_lib_name,
    ) {
        targets.push(lib_target);
    }

    collect_bin_targets(
        manifest,
        package_root,
        repo_root,
        package_edition,
        package_name,
        &mut targets,
    );
    collect_directory_target_specs(
        manifest,
        manifest.example.as_ref(),
        package_root,
        repo_root,
        package_edition,
        "examples",
        "example",
        &mut targets,
    );
    collect_directory_target_specs(
        manifest,
        manifest.test.as_ref(),
        package_root,
        repo_root,
        package_edition,
        "tests",
        "test",
        &mut targets,
    );
    collect_directory_target_specs(
        manifest,
        manifest.bench.as_ref(),
        package_root,
        repo_root,
        package_edition,
        "benches",
        "bench",
        &mut targets,
    );
    collect_build_target(
        manifest,
        package_root,
        repo_root,
        package_edition,
        &mut targets,
    );

    dedupe_targets(targets)
}

fn collect_lib_target(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    repo_root: &Path,
    package_edition: &str,
    default_lib_name: &str,
) -> Option<LocalTargetSpec> {
    let lib = manifest.lib.as_ref();
    let path = lib
        .and_then(|lib| {
            lib.path.as_ref().and_then(|path| {
                repo_local_existing_file(package_root.join(path.0.clone()), repo_root)
            })
        })
        .or_else(|| repo_local_existing_file(package_root.join("src/lib.rs"), repo_root))?;
    let name = lib
        .and_then(|lib| lib.name.clone())
        .unwrap_or_else(|| default_lib_name.to_string());
    let is_proc_macro = lib.and_then(|lib| lib.proc_macro()).unwrap_or(false);

    Some(LocalTargetSpec {
        name,
        kind: vec![if is_proc_macro { "proc-macro" } else { "lib" }],
        crate_types: vec![if is_proc_macro { "proc-macro" } else { "lib" }],
        required_features: lib
            .and_then(|lib| lib.required_features.clone())
            .unwrap_or_default(),
        src_path: path,
        edition: lib
            .and_then(|lib| lib.edition.clone())
            .unwrap_or_else(|| package_edition.to_string()),
        doctest: lib.and_then(|lib| lib.doctest).unwrap_or(true),
        test: lib.and_then(|lib| lib.test).unwrap_or(true),
        doc: lib.and_then(|lib| lib.doc).unwrap_or(true),
    })
}

fn collect_bin_targets(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    repo_root: &Path,
    package_edition: &str,
    package_name: &str,
    targets: &mut Vec<LocalTargetSpec>,
) {
    let mut seen = HashSet::new();
    if let Some(explicit_bins) = manifest.bin.as_ref() {
        for bin in explicit_bins {
            let Some((name, path)) = resolve_explicit_target_path(
                bin,
                package_root,
                repo_root,
                package_name,
                "src/bin",
                Some("src/main.rs"),
            ) else {
                continue;
            };
            let dedupe_key = format!("bin:{}:{}", name, path.display());
            if !seen.insert(dedupe_key) {
                continue;
            }
            targets.push(LocalTargetSpec {
                name,
                kind: vec!["bin"],
                crate_types: vec!["bin"],
                required_features: bin.required_features.clone().unwrap_or_default(),
                src_path: path,
                edition: bin
                    .edition
                    .clone()
                    .unwrap_or_else(|| package_edition.to_string()),
                doctest: bin.doctest.unwrap_or(true),
                test: bin.test.unwrap_or(true),
                doc: bin.doc.unwrap_or(true),
            });
        }
    }

    if manifest
        .package()
        .and_then(|package| package.autobins)
        .unwrap_or(true)
    {
        if let Some(main_rs) = repo_local_existing_file(package_root.join("src/main.rs"), repo_root)
        {
            let dedupe_key = format!("bin:{}:{}", package_name, main_rs.display());
            if seen.insert(dedupe_key) {
                targets.push(LocalTargetSpec {
                    name: package_name.to_string(),
                    kind: vec!["bin"],
                    crate_types: vec!["bin"],
                    required_features: Vec::new(),
                    src_path: main_rs,
                    edition: package_edition.to_string(),
                    doctest: true,
                    test: true,
                    doc: true,
                });
            }
        }

        for (name, path) in infer_directory_targets(&package_root.join("src/bin")) {
            let Some(path) = repo_local_existing_file(path, repo_root) else {
                continue;
            };
            let dedupe_key = format!("bin:{}:{}", name, path.display());
            if seen.insert(dedupe_key) {
                targets.push(LocalTargetSpec {
                    name,
                    kind: vec!["bin"],
                    crate_types: vec!["bin"],
                    required_features: Vec::new(),
                    src_path: path,
                    edition: package_edition.to_string(),
                    doctest: true,
                    test: true,
                    doc: true,
                });
            }
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "target collection keeps explicit Cargo-derived inputs separate"
)]
fn collect_directory_target_specs(
    manifest: &cargo_manifest::TomlManifest,
    explicit_targets: Option<&Vec<cargo_manifest::TomlTarget>>,
    package_root: &Path,
    repo_root: &Path,
    package_edition: &str,
    default_dir: &str,
    kind: &'static str,
    targets: &mut Vec<LocalTargetSpec>,
) {
    let mut seen = HashSet::new();
    if let Some(explicit_targets) = explicit_targets {
        for target in explicit_targets {
            let Some((name, path)) = resolve_explicit_target_path(
                target,
                package_root,
                repo_root,
                default_dir,
                default_dir,
                None,
            ) else {
                continue;
            };
            let dedupe_key = format!("{kind}:{name}:{}", path.display());
            if !seen.insert(dedupe_key) {
                continue;
            }
            targets.push(LocalTargetSpec {
                name,
                kind: vec![kind],
                crate_types: vec!["bin"],
                required_features: target.required_features.clone().unwrap_or_default(),
                src_path: path,
                edition: target
                    .edition
                    .clone()
                    .unwrap_or_else(|| package_edition.to_string()),
                doctest: target.doctest.unwrap_or(true),
                test: target.test.unwrap_or(true),
                doc: target.doc.unwrap_or(true),
            });
        }
    }

    let autodiscover = match kind {
        "example" => manifest_autodiscover(manifest, |package| package.autoexamples),
        "test" => manifest_autodiscover(manifest, |package| package.autotests),
        "bench" => manifest_autodiscover(manifest, |package| package.autobenches),
        _ => true,
    };

    if autodiscover {
        for (name, path) in infer_directory_targets(&package_root.join(default_dir)) {
            let Some(path) = repo_local_existing_file(path, repo_root) else {
                continue;
            };
            let dedupe_key = format!("{kind}:{name}:{}", path.display());
            if seen.insert(dedupe_key) {
                targets.push(LocalTargetSpec {
                    name,
                    kind: vec![kind],
                    crate_types: vec!["bin"],
                    required_features: Vec::new(),
                    src_path: path,
                    edition: package_edition.to_string(),
                    doctest: true,
                    test: true,
                    doc: true,
                });
            }
        }
    }
}

fn collect_build_target(
    manifest: &cargo_manifest::TomlManifest,
    package_root: &Path,
    repo_root: &Path,
    package_edition: &str,
    targets: &mut Vec<LocalTargetSpec>,
) {
    let build = manifest
        .package()
        .and_then(|package| package.build.as_ref());
    let build_path = match build {
        Some(cargo_manifest::TomlPackageBuild::SingleScript(path)) => {
            repo_local_existing_file(package_root.join(path), repo_root)
        }
        Some(cargo_manifest::TomlPackageBuild::MultipleScript(_)) => None,
        Some(cargo_manifest::TomlPackageBuild::Auto(true)) => {
            repo_local_existing_file(package_root.join("build.rs"), repo_root)
        }
        Some(cargo_manifest::TomlPackageBuild::Auto(false)) => None,
        None => repo_local_existing_file(package_root.join("build.rs"), repo_root),
    };

    let Some(build_path) = build_path else {
        return;
    };
    targets.push(LocalTargetSpec {
        name: "build-script-build".to_string(),
        kind: vec!["custom-build"],
        crate_types: vec!["bin"],
        required_features: Vec::new(),
        src_path: build_path,
        edition: package_edition.to_string(),
        doctest: false,
        test: false,
        doc: false,
    });
}

fn manifest_autodiscover(
    manifest: &cargo_manifest::TomlManifest,
    selector: impl Fn(&cargo_manifest::TomlPackage) -> Option<bool>,
) -> bool {
    manifest
        .package()
        .and_then(|package| selector(package))
        .unwrap_or(true)
}

fn resolve_explicit_target_path(
    target: &cargo_manifest::TomlTarget,
    package_root: &Path,
    repo_root: &Path,
    default_name: &str,
    default_dir: &str,
    fallback_main: Option<&str>,
) -> Option<(String, PathBuf)> {
    let name = target.name.clone().or_else(|| {
        target.path.as_ref().and_then(|path| {
            path.0
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(|stem| stem.to_string())
        })
    })?;

    if let Some(path) = target.path.as_ref() {
        return repo_local_existing_file(package_root.join(path.0.clone()), repo_root)
            .map(|path| (name, path));
    }

    if let Some(main) = fallback_main
        && name == default_name
        && let Some(main_path) = repo_local_existing_file(package_root.join(main), repo_root)
    {
        return Some((name, main_path));
    }

    if let Some(file_path) = repo_local_existing_file(
        package_root.join(default_dir).join(format!("{name}.rs")),
        repo_root,
    ) {
        return Some((name, file_path));
    }
    if let Some(nested_main) = repo_local_existing_file(
        package_root.join(default_dir).join(&name).join("main.rs"),
        repo_root,
    ) {
        return Some((name, nested_main));
    }

    None
}

pub(super) fn repo_local_existing_file(path: PathBuf, repo_root: &Path) -> Option<PathBuf> {
    let normalized = normalize_existing_path(&path)?;
    let normalized_root =
        normalize_existing_path(repo_root).unwrap_or_else(|| repo_root.to_path_buf());
    (normalized.is_file() && normalized.starts_with(&normalized_root)).then_some(normalized)
}

fn infer_directory_targets(directory: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };

    let mut targets = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = entry.file_name();
        if file_name.to_string_lossy().starts_with('.') {
            continue;
        }

        if path.is_file() && path.extension().is_some_and(|extension| extension == "rs") {
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            targets.push((name.to_string(), path));
            continue;
        }

        if path.is_dir() {
            let main_rs = path.join("main.rs");
            if main_rs.is_file()
                && let Some(name) = path.file_name().and_then(|name| name.to_str())
            {
                targets.push((name.to_string(), main_rs));
            }
        }
    }

    targets
}

fn dedupe_targets(mut targets: Vec<LocalTargetSpec>) -> Vec<LocalTargetSpec> {
    let mut seen = HashSet::new();
    targets.retain(|target| {
        let key = format!(
            "{}:{}:{}",
            target.kind.first().copied().unwrap_or("unknown"),
            target.name,
            target.src_path.display()
        );
        seen.insert(key)
    });
    targets
}

fn synthetic_metadata_from_packages(
    descriptor: &WorkspaceDescriptor,
    packages: Vec<LocalWorkspacePackage>,
) -> Result<Metadata> {
    let package_ids = packages
        .iter()
        .map(|package| (package.manifest_path.clone(), package.package_id.clone()))
        .collect::<HashMap<_, _>>();

    let package_values = packages
        .iter()
        .map(|package| {
            let dependency_values = package
                .dependencies
                .iter()
                .filter_map(|dependency| {
                    package_ids.get(&dependency.target_manifest_path)?;
                    if dependency.optional && !is_dep_feature_activated(package, dependency) {
                        return None;
                    }
                    Some(serde_json::json!({
                        "name": dependency.target_package_name,
                        "source": serde_json::Value::Null,
                        "req": "*",
                        "kind": dependency.kind,
                        "optional": dependency.optional,
                        "uses_default_features": dependency.uses_default_features,
                        "features": dependency.features,
                        "target": dependency.target,
                        "rename": rename_field(dependency),
                        "registry": serde_json::Value::Null,
                        "path": dependency.target_manifest_path.parent().map(|path| path.to_string_lossy().to_string()),
                    }))
                })
                .collect::<Vec<_>>();
            let target_values = package
                .targets
                .iter()
                .map(|target| {
                    serde_json::json!({
                        "name": target.name,
                        "kind": target.kind,
                        "crate_types": target.crate_types,
                        "required_features": target.required_features,
                        "src_path": target.src_path.to_string_lossy(),
                        "edition": target.edition,
                        "doctest": target.doctest,
                        "test": target.test,
                        "doc": target.doc,
                    })
                })
                .collect::<Vec<_>>();

            serde_json::json!({
                "name": package.package_name,
                "version": package.version,
                "id": package.package_id,
                "source": serde_json::Value::Null,
                "description": serde_json::Value::Null,
                "dependencies": dependency_values,
                "license": serde_json::Value::Null,
                "license_file": serde_json::Value::Null,
                "targets": target_values,
                "features": package.features,
                "manifest_path": package.manifest_path.to_string_lossy(),
                "categories": [],
                "keywords": [],
                "readme": serde_json::Value::Null,
                "repository": serde_json::Value::Null,
                "homepage": serde_json::Value::Null,
                "documentation": serde_json::Value::Null,
                "edition": package.edition,
                "metadata": serde_json::json!({}),
                "links": serde_json::Value::Null,
                "publish": serde_json::Value::Null,
                "default_run": serde_json::Value::Null,
                "rust_version": serde_json::Value::Null,
                "authors": [],
            })
        })
        .collect::<Vec<_>>();

    let resolve_nodes = packages
        .iter()
        .map(|package| {
            let mut deps = BTreeMap::<(String, String), Vec<serde_json::Value>>::new();
            let mut dependency_ids = Vec::new();
            for dependency in &package.dependencies {
                let Some(target_package_id) = package_ids.get(&dependency.target_manifest_path)
                else {
                    continue;
                };
                if dependency.optional && !is_dep_feature_activated(package, dependency) {
                    continue;
                }
                dependency_ids.push(target_package_id.clone());
                deps.entry((dependency.code_name.clone(), target_package_id.clone()))
                    .or_default()
                    .push(serde_json::json!({
                        "kind": dependency.kind,
                        "target": dependency.target,
                    }));
            }
            dependency_ids.sort();
            dependency_ids.dedup();

            let dep_values = deps
                .into_iter()
                .map(|((name, package_id), dep_kinds)| {
                    serde_json::json!({
                        "name": name,
                        "pkg": package_id,
                        "dep_kinds": dep_kinds,
                    })
                })
                .collect::<Vec<_>>();

            serde_json::json!({
                "id": package.package_id,
                "deps": dep_values,
                "dependencies": dependency_ids,
                "features": [],
            })
        })
        .collect::<Vec<_>>();

    let workspace_members = packages
        .iter()
        .filter(|package| package.is_member)
        .map(|package| package.package_id.clone())
        .collect::<Vec<_>>();
    let workspace_default_members = descriptor
        .default_members
        .iter()
        .filter_map(|manifest_path| package_ids.get(manifest_path).cloned())
        .collect::<Vec<_>>();
    let root_package_id = package_ids
        .get(&descriptor.workspace_manifest_path)
        .cloned();

    let metadata = serde_json::json!({
        "packages": package_values,
        "workspace_members": workspace_members,
        "workspace_default_members": workspace_default_members,
        "resolve": {
            "nodes": resolve_nodes,
            "root": root_package_id,
        },
        "workspace_root": descriptor.workspace_root.to_string_lossy(),
        "target_directory": descriptor.workspace_root.join("target").to_string_lossy(),
        "build_directory": descriptor.workspace_root.join("target").to_string_lossy(),
        "metadata": serde_json::json!({}),
        "version": 1,
    });

    serde_json::from_value(metadata).context("failed to deserialize synthetic cargo metadata")
}

fn rename_field(dependency: &ResolvedDependencyCandidate) -> Option<String> {
    Some(dependency.manifest_name.clone())
        .filter(|rename| rename != &dependency.target_package_name)
}

// An optional dependency is considered activated only when it is named by a
// feature that is transitively reachable from `default`. We do not track the
// full feature resolution that Cargo performs; the conservative rule keeps
// rust-analyzer from pulling in optional crates that the user has disabled.
fn is_dep_feature_activated(
    package: &LocalWorkspacePackage,
    dependency: &ResolvedDependencyCandidate,
) -> bool {
    let default = match package.features.get("default") {
        Some(values) => values,
        None => return false,
    };
    let mut visited = HashSet::new();
    let mut queue = default.iter().cloned().collect::<VecDeque<_>>();
    while let Some(entry) = queue.pop_front() {
        if !visited.insert(entry.clone()) {
            continue;
        }
        if entry == format!("dep:{}", dependency.manifest_name) || entry == dependency.manifest_name
        {
            return true;
        }
        if let Some((feature, _)) = entry.split_once('/')
            && feature == dependency.manifest_name
        {
            return true;
        }
        if let Some(sub) = package.features.get(&entry) {
            for value in sub {
                queue.push_back(value.clone());
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn oversized_cargo_manifest_is_rejected() {
        let temp = tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let manifest = root.join("Cargo.toml");
        // Write a manifest whose bytes exceed MAX_MANIFEST_BYTES with filler in
        // a comment so the TOML grammar would otherwise be valid.
        let padding = "#".repeat((MAX_MANIFEST_BYTES + 1) as usize);
        std::fs::write(
            &manifest,
            format!(
                "[package]\nname = \"big\"\nversion = \"0.0.0\"\nedition = \"2021\"\n{padding}\n"
            ),
        )
        .unwrap();

        let mut cache =
            ManifestCache::new(root.to_string_lossy().as_ref()).expect("cache should open");
        let err = cache
            .load(&manifest)
            .err()
            .expect("oversized manifest must be rejected");
        assert!(
            err.to_string().contains("exceeds"),
            "unexpected error: {err}"
        );
    }
}
