//! Kubernetes helpers built on kube-rs.
//!
//! Pure k8s primitives — no domain knowledge (GitLab, ClickHouse, etc.).
//! Domain-specific helpers that depend on `Config` live in `utils.rs`.
//!
//! Helm operations remain as shell-outs — no mature Rust Helm library exists.
//!
//! Architecture:
//! - One async exec primitive (`exec_in_pod`) handles all pod exec patterns
//! - One dynamic-API delete primitive handles all resource deletion
//! - SSA (server-side apply) handles all manifest application
//! - Tar-over-exec handles all file copying

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::e2e::{constants as c, ui};
use anyhow::{Context, Result, anyhow, bail};
use k8s_openapi::ByteString;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Pod, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use kube::api::{
    Api, AttachParams, DeleteParams, DynamicObject, ListParams, LogParams, Patch, PatchParams,
};
use kube::core::{ApiResource, GroupVersionKind, TypeMeta};
use kube::runtime::wait::{Condition, await_condition};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// =============================================================================
// Client
// =============================================================================

async fn client() -> Result<Client> {
    Client::try_default()
        .await
        .context("creating kube client from default kubeconfig")
}

/// Check if the k8s cluster is reachable.
pub async fn cluster_reachable() -> bool {
    client().await.is_ok()
}

// =============================================================================
// Exec primitive
// =============================================================================

/// Result of executing a command in a pod.
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
}

impl ExecResult {
    /// Consume the result, returning stdout on success or bailing with
    /// stderr (or stdout if stderr is empty) on failure.
    pub fn strict(self, context: &str) -> Result<String> {
        if self.success {
            return Ok(self.stdout);
        }
        let msg = if self.stderr.is_empty() {
            &self.stdout
        } else {
            &self.stderr
        };
        bail!("{context}: {msg}");
    }
}

/// Execute a command in a pod with optional stdin.
///
/// This is the single async primitive that all pod-exec operations build on.
/// It never bails on non-zero exit — callers decide how to handle failure.
async fn exec_in_pod(
    client: &Client,
    ns: &str,
    pod: &str,
    command: Vec<String>,
    stdin_data: Option<Vec<u8>>,
) -> Result<ExecResult> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    let ap = AttachParams {
        stdin: stdin_data.is_some(),
        stdout: true,
        stderr: true,
        ..Default::default()
    };
    let mut attached = pods
        .exec(pod, command, &ap)
        .await
        .with_context(|| format!("exec in pod {pod}"))?;

    if let Some(data) = stdin_data
        && let Some(mut writer) = attached.stdin()
    {
        writer.write_all(&data).await?;
        writer.shutdown().await?;
    }

    let mut stdout = String::new();
    if let Some(mut reader) = attached.stdout() {
        reader.read_to_string(&mut stdout).await?;
    }
    let mut stderr = String::new();
    if let Some(mut reader) = attached.stderr() {
        reader.read_to_string(&mut stderr).await?;
    }

    let status = attached
        .take_status()
        .ok_or_else(|| anyhow!("no status channel from exec in {pod}"))?
        .await;

    let success = status
        .as_ref()
        .is_some_and(|s| s.status.as_deref() == Some("Success"));

    Ok(ExecResult {
        stdout: stdout.trim().to_string(),
        stderr: stderr.trim().to_string(),
        success,
    })
}

// =============================================================================
// Public exec helpers
// =============================================================================

/// Run a command in a pod with optional stdin. Returns stdout. Bails on failure.
pub async fn pod_exec(
    ns: &str,
    pod: &str,
    command: &[&str],
    stdin: Option<&[u8]>,
) -> Result<String> {
    let client = client().await?;
    let cmd_vec: Vec<String> = command.iter().map(|s| s.to_string()).collect();
    exec_in_pod(&client, ns, pod, cmd_vec, stdin.map(|s| s.to_vec()))
        .await?
        .strict(&format!("exec in {pod}"))
}

/// Run `bash -c <script> <args...>` in a pod. Returns the full `ExecResult`
/// without bailing — callers decide how to handle failure (use `.strict()`
/// for the common bail-on-error path).
pub async fn exec_bash_output(
    ns: &str,
    pod: &str,
    script: &str,
    args: &[&str],
) -> Result<ExecResult> {
    let client = client().await?;
    let mut cmd_vec: Vec<String> = vec!["bash".into(), "-c".into(), script.into()];
    cmd_vec.extend(args.iter().map(|a| a.to_string()));
    exec_in_pod(&client, ns, pod, cmd_vec, None).await
}

// =============================================================================
// Secrets
// =============================================================================

/// Read a k8s secret field (base64-decoded on the wire by serde).
pub async fn read_secret(namespace: &str, secret_name: &str, key: &str) -> Result<String> {
    let client = client().await?;
    let secrets: Api<Secret> = Api::namespaced(client, namespace);
    let secret = secrets
        .get(secret_name)
        .await
        .with_context(|| format!("reading secret {secret_name} in {namespace}"))?;

    let data = secret
        .data
        .ok_or_else(|| anyhow!("secret {secret_name} has no data"))?;

    let bytes = data
        .get(key)
        .ok_or_else(|| anyhow!("key {key} not found in secret {secret_name}"))?;

    String::from_utf8(bytes.0.clone())
        .with_context(|| format!("decoding {key} in secret {secret_name}"))
}

/// Create or update a secret with a single key-value pair (idempotent via SSA).
pub async fn apply_secret(ns: &str, name: &str, key: &str, value: &str) -> Result<()> {
    let client = client().await?;
    let secrets: Api<Secret> = Api::namespaced(client, ns);

    let mut data = BTreeMap::new();
    data.insert(key.to_string(), ByteString(value.as_bytes().to_vec()));

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    };

    let pp = PatchParams::apply(c::SSA_FIELD_MANAGER).force();
    secrets
        .patch(name, &pp, &Patch::Apply(&secret))
        .await
        .with_context(|| format!("applying secret {name}"))?;
    Ok(())
}

// =============================================================================
// ConfigMaps
// =============================================================================

/// Read a ConfigMap data field.
pub async fn read_configmap_field(ns: &str, name: &str, key: &str) -> Result<String> {
    let client = client().await?;
    let cms: Api<ConfigMap> = Api::namespaced(client, ns);
    let cm = cms
        .get(name)
        .await
        .with_context(|| format!("reading configmap {name} in {ns}"))?;

    cm.data
        .as_ref()
        .and_then(|d| d.get(key).cloned())
        .ok_or_else(|| anyhow!("key {key} not found in configmap {name}"))
}

/// Patch a single data field in a ConfigMap using a strategic merge patch.
pub async fn patch_configmap_field(ns: &str, name: &str, key: &str, value: &str) -> Result<()> {
    let client = client().await?;
    let cms: Api<ConfigMap> = Api::namespaced(client, ns);

    let patch = serde_json::json!({
        "data": {
            key: value
        }
    });

    cms.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .with_context(|| format!("patching configmap {name} key {key} in {ns}"))?;
    Ok(())
}

// =============================================================================
// Apply YAML manifests (server-side apply)
// =============================================================================

/// Apply a YAML string (one or more documents) via server-side apply.
pub async fn apply_yaml(yaml: &str) -> Result<()> {
    let client = client().await?;
    for doc in split_yaml_docs(yaml) {
        if doc.trim().is_empty() {
            continue;
        }
        apply_single_doc(&client, doc)
            .await
            .with_context(|| format!("applying YAML document:\n{}", truncate(doc, 200)))?;
    }
    Ok(())
}

fn split_yaml_docs(yaml: &str) -> Vec<&str> {
    let mut docs = Vec::new();
    let mut start = 0;
    for (i, line) in yaml.lines().enumerate() {
        if line == "---" {
            let byte_pos = yaml.lines().take(i).map(|l| l.len() + 1).sum::<usize>();
            if byte_pos > start {
                let doc = &yaml[start..byte_pos.saturating_sub(1)];
                if !doc.trim().is_empty() {
                    docs.push(doc);
                }
            }
            start = byte_pos + line.len() + 1;
        }
    }
    if start < yaml.len() {
        let doc = &yaml[start..];
        if !doc.trim().is_empty() {
            docs.push(doc);
        }
    }
    if docs.is_empty() && !yaml.trim().is_empty() {
        docs.push(yaml);
    }
    docs
}

async fn apply_single_doc(client: &Client, yaml: &str) -> Result<()> {
    let tm: TypeMeta =
        serde_yaml::from_str(yaml).context("YAML document missing apiVersion/kind")?;
    let gvk = GroupVersionKind::try_from(&tm)
        .map_err(|e| anyhow!("invalid apiVersion '{}': {e}", tm.api_version))?;
    let ar = ApiResource::from_gvk(&gvk);

    let obj: DynamicObject =
        serde_yaml::from_str(yaml).context("deserializing YAML into DynamicObject")?;
    let name = obj
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow!("YAML document has no metadata.name"))?;
    let ns = obj.metadata.namespace.as_deref().unwrap_or("default");

    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &ar);
    let pp = PatchParams::apply(c::SSA_FIELD_MANAGER).force();
    api.patch(name, &pp, &Patch::Apply(&obj))
        .await
        .with_context(|| format!("server-side apply of {}/{name}", tm.kind))?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

// =============================================================================
// Resource deletion (dynamic API, fire-and-forget)
// =============================================================================

/// What to delete: specific resources by name, or a collection by label.
pub enum DeleteTarget<'a> {
    /// Delete specific resources by name. Ignores not-found.
    Names(&'a [&'a str]),
    /// Delete all resources matching a label selector (empty = all).
    Label(&'a str),
}

/// Delete resources. Ignores not-found for named deletes.
///
/// `api_version` is the k8s apiVersion (e.g. `"v1"`, `"apps/v1"`, `"batch/v1"`).
/// `kind` is the resource kind (e.g. `"Secret"`, `"StatefulSet"`, `"Job"`).
pub async fn delete(
    ns: &str,
    api_version: &str,
    kind: &str,
    target: DeleteTarget<'_>,
) -> Result<()> {
    let ar = make_api_resource(api_version, kind);
    let client = client().await?;
    let api: Api<DynamicObject> = Api::namespaced_with(client, ns, &ar);

    match target {
        DeleteTarget::Names(names) => {
            let dp = if kind == "Job" {
                DeleteParams {
                    propagation_policy: Some(kube::api::PropagationPolicy::Background),
                    ..Default::default()
                }
            } else {
                DeleteParams::default()
            };
            for name in names {
                match api.delete(name, &dp).await {
                    Ok(_) => {}
                    Err(kube::Error::Api(ref s)) if s.code == 404 => {}
                    Err(e) => return Err(e).context(format!("deleting {kind}/{name}")),
                }
            }
        }
        DeleteTarget::Label(label) => {
            let lp = if label.is_empty() {
                ListParams::default()
            } else {
                ListParams::default().labels(label)
            };
            let _ = api.delete_collection(&DeleteParams::default(), &lp).await;
        }
    }
    Ok(())
}

/// Build an `ApiResource` from an apiVersion string and kind.
fn make_api_resource(api_version: &str, kind: &str) -> ApiResource {
    let (group, version) = match api_version.split_once('/') {
        Some((g, v)) => (g, v),
        None => ("", api_version),
    };
    let gvk = GroupVersionKind::gvk(group, version, kind);
    ApiResource::from_gvk(&gvk)
}

// =============================================================================
// Namespace management
// =============================================================================

/// Create a namespace (idempotent via SSA).
pub async fn create_namespace(ns: &str) -> Result<()> {
    let client = client().await?;
    let namespaces: Api<Namespace> = Api::all(client);
    let ns_obj = Namespace {
        metadata: ObjectMeta {
            name: Some(ns.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let pp = PatchParams::apply(c::SSA_FIELD_MANAGER).force();
    namespaces
        .patch(ns, &pp, &Patch::Apply(&ns_obj))
        .await
        .with_context(|| format!("creating namespace {ns}"))?;
    Ok(())
}

/// Delete a namespace (ignore if not found).
pub async fn delete_namespace(ns: &str) -> Result<()> {
    let client = client().await?;
    let namespaces: Api<Namespace> = Api::all(client);
    match namespaces.delete(ns, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref s)) if s.code == 404 => Ok(()),
        Err(e) => Err(e).context(format!("deleting namespace {ns}")),
    }
}

// =============================================================================
// Pod readiness / Job waiting
// =============================================================================

fn pod_is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conds| {
            conds
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
}

fn is_pod_ready() -> impl Condition<Pod> {
    |obj: Option<&Pod>| obj.is_some_and(pod_is_ready)
}

fn is_job_complete() -> impl Condition<Job> {
    |obj: Option<&Job>| {
        obj.and_then(|j| j.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|conds| {
                conds
                    .iter()
                    .any(|c| c.type_ == "Complete" && c.status == "True")
            })
    }
}

fn parse_k8s_duration(s: &str) -> Result<Duration> {
    if let Some(secs) = s.strip_suffix('s') {
        Ok(Duration::from_secs(secs.parse()?))
    } else if let Some(mins) = s.strip_suffix('m') {
        Ok(Duration::from_secs(mins.parse::<u64>()? * 60))
    } else {
        bail!("unsupported duration format: {s}")
    }
}

/// Wait until a pod matching `label` in `namespace` is ready, or warn on timeout.
pub async fn wait_for_pod(label: &str, namespace: &str, timeout: &str) -> Result<()> {
    ui::info(&format!(
        "Waiting for pod ({label}) in {namespace} (timeout {timeout})"
    ))?;

    let duration = parse_k8s_duration(timeout)?;

    let ok: Result<()> = async {
        let client = client().await?;
        let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
        let lp = ListParams::default().labels(label);

        let list = pods.list(&lp).await.context("listing pods")?;
        let futs: Vec<_> = list
            .items
            .iter()
            .filter_map(|pod| pod.metadata.name.as_deref())
            .map(|name| {
                let cond = await_condition(pods.clone(), name, is_pod_ready());
                async move {
                    tokio::time::timeout(duration, cond)
                        .await
                        .with_context(|| format!("timeout waiting for pod {name}"))?
                        .with_context(|| format!("watching pod {name}"))?;
                    Ok::<_, anyhow::Error>(())
                }
            })
            .collect();
        futures::future::try_join_all(futs).await?;
        Ok(())
    }
    .await;

    if let Err(e) = ok {
        ui::warn(&format!(
            "Pod {label} not ready after {timeout}: {e:#}. Continuing..."
        ))?;
    }
    Ok(())
}

/// Wait for multiple pods by label in parallel. Each label/timeout pair is
/// awaited concurrently, turning `sum(timeouts)` into `max(timeouts)`.
pub async fn wait_for_pods_parallel(
    pods: &[(&str, &str, &str)], // (label, namespace, timeout)
) -> Result<()> {
    let futs: Vec<_> = pods
        .iter()
        .map(|(label, ns, timeout)| wait_for_pod(label, ns, timeout))
        .collect();
    futures::future::join_all(futs)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(())
}

/// Wait for a Job to complete. Returns true if successful, false on timeout.
pub async fn wait_for_job(ns: &str, name: &str, timeout: &str) -> Result<bool> {
    let duration = parse_k8s_duration(timeout)?;

    let client = client().await?;
    let jobs: Api<Job> = Api::namespaced(client, ns);
    let cond = await_condition(jobs, name, is_job_complete());
    match tokio::time::timeout(duration, cond).await {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) => Err(e).context(format!("watching job {name}")),
        Err(_) => Ok(false),
    }
}

// =============================================================================
// Rollout restart (patch deployment annotation)
// =============================================================================

/// Trigger a rolling restart of a Deployment by patching its pod template
/// annotation with the current timestamp — the same mechanism as
/// `kubectl rollout restart deployment/<name>`.
pub async fn rollout_restart(ns: &str, deployment: &str) -> Result<()> {
    let client = client().await?;
    let deployments: Api<Deployment> = Api::namespaced(client, ns);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let patch = serde_json::json!({
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": now.to_string()
                    }
                }
            }
        }
    });

    deployments
        .patch(deployment, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .with_context(|| format!("rollout restart of {deployment} in {ns}"))?;

    ui::info(&format!("Rollout restart triggered for {deployment}"))?;
    Ok(())
}

// =============================================================================
// Logs
// =============================================================================

/// Get logs from pods matching a label selector. Returns combined log text.
pub async fn get_logs(ns: &str, label: &str, tail: i64) -> Result<String> {
    let client = client().await?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    let lp = ListParams::default().labels(label);
    let list = pods.list(&lp).await.context("listing pods for logs")?;

    let log_params = LogParams {
        tail_lines: Some(tail),
        ..Default::default()
    };
    let mut combined = String::new();
    for pod in &list.items {
        if let Some(name) = &pod.metadata.name
            && let Ok(logs) = pods.logs(name, &log_params).await
        {
            if !combined.is_empty() {
                combined.push('\n');
            }
            combined.push_str(&logs);
        }
    }
    Ok(combined)
}

// =============================================================================
// Pod listing
// =============================================================================

/// List pods in a namespace and print a formatted status table.
pub async fn print_pod_status(ns: &str) -> Result<()> {
    let client = client().await?;
    let pods: Api<Pod> = Api::namespaced(client, ns);
    let list = pods.list(&ListParams::default()).await?;

    for pod in &list.items {
        let name = pod.metadata.name.as_deref().unwrap_or("<unknown>");
        let phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("Unknown");
        let ready_str = if pod_is_ready(pod) {
            "Ready"
        } else {
            "NotReady"
        };
        ui::info(&format!("  {name:<50} {phase:<12} {ready_str}"))?;
    }
    Ok(())
}

/// Find the first pod matching a label selector.
pub async fn find_pod(namespace: &str, label: &str) -> Result<Option<String>> {
    let client = client().await?;
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let lp = ListParams::default().labels(label);
    let list = pods.list(&lp).await.context("listing pods")?;
    Ok(list.items.first().and_then(|p| p.metadata.name.clone()))
}

// =============================================================================
// File copying (tar-over-exec)
// =============================================================================

/// Copy local files into a pod directory.
///
/// `files` is a list of `(local_path, archive_name)` pairs.
async fn tar_to_pod_with(
    client: &Client,
    ns: &str,
    pod: &str,
    pod_dir: &str,
    files: &[(&Path, &str)],
) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }

    let mut tar_buf = Vec::new();
    {
        let mut ar = tar::Builder::new(&mut tar_buf);
        for (path, name) in files {
            let data = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            ar.append_data(&mut header, name, data.as_slice())
                .with_context(|| format!("adding {} to tar", path.display()))?;
        }
        ar.finish()?;
    }

    exec_in_pod(
        client,
        ns,
        pod,
        vec![
            "tar".into(),
            "xf".into(),
            "-".into(),
            "-C".into(),
            pod_dir.into(),
        ],
        Some(tar_buf),
    )
    .await?
    .strict(&format!("copying files to {pod}:{pod_dir}"))?;
    Ok(())
}

/// Copy local files into a pod directory (creates `pod_dir` first).
pub async fn cp_to_pod(ns: &str, pod: &str, local_paths: &[&Path], pod_dir: &str) -> Result<()> {
    if local_paths.is_empty() {
        return Ok(());
    }

    let client = client().await?;
    let mkdir_cmd = vec!["mkdir".into(), "-p".into(), pod_dir.into()];
    exec_in_pod(&client, ns, pod, mkdir_cmd, None)
        .await?
        .strict(&format!("mkdir -p {pod_dir} in {pod}"))?;

    let files: Vec<(&Path, String)> = local_paths
        .iter()
        .map(|p| {
            let name = p
                .file_name()
                .ok_or_else(|| anyhow!("no filename for {}", p.display()))
                .map(|n| n.to_string_lossy().to_string())?;
            Ok((*p, name))
        })
        .collect::<Result<_>>()?;

    let refs: Vec<(&Path, &str)> = files.iter().map(|(p, n)| (*p, n.as_str())).collect();
    tar_to_pod_with(&client, ns, pod, pod_dir, &refs).await
}

/// Copy a file from a pod to a local path.
pub async fn cp_from_pod(ns: &str, pod: &str, pod_path: &str, local_path: &Path) -> Result<()> {
    let pod_dir = pod_path.rsplit_once('/').map(|(d, _)| d).unwrap_or("/");
    let pod_file = pod_path
        .rsplit_once('/')
        .map(|(_, f)| f)
        .unwrap_or(pod_path);

    let client = client().await?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    let ap = AttachParams {
        stdout: true,
        stderr: true,
        ..Default::default()
    };
    let mut attached = pods
        .exec(
            pod,
            vec![
                "tar".to_string(),
                "cf".to_string(),
                "-".to_string(),
                "-C".to_string(),
                pod_dir.to_string(),
                pod_file.to_string(),
            ],
            &ap,
        )
        .await
        .with_context(|| format!("exec tar in pod {pod}"))?;

    let mut buf = Vec::new();
    if let Some(mut reader) = attached.stdout() {
        reader.read_to_end(&mut buf).await?;
    }

    let mut archive = tar::Archive::new(buf.as_slice());
    if let Some(entry) = archive.entries()?.next() {
        let mut entry = entry?;
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::File::create(local_path)?;
        std::io::copy(&mut entry, &mut file)?;
        return Ok(());
    }

    bail!("no files found in tar archive from {pod}:{pod_path}")
}
