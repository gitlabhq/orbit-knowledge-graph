use anyhow::{anyhow, Result};
use reqwest::{Client, Response, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, info, warn};

const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 500;

// Global counter for generating unique IDs in dry-run mode
static DRY_RUN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_dry_run_id() -> u64 {
    DRY_RUN_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
    base_url: String,
    token: String,
    dry_run: bool,
}

impl ApiClient {
    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        Self::with_options(base_url, token, false, 30)
    }

    pub fn with_dry_run(base_url: &str, token: &str, dry_run: bool) -> Result<Self> {
        Self::with_options(base_url, token, dry_run, 30)
    }

    pub fn with_options(
        base_url: &str,
        token: &str,
        dry_run: bool,
        request_timeout_secs: u64,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(request_timeout_secs))
            .danger_accept_invalid_certs(true)
            .build()?;

        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            dry_run,
        })
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/api/v4{}", self.base_url, path)
    }

    fn is_retryable_error(err: &reqwest::Error) -> bool {
        err.is_timeout() || err.is_connect() || err.is_request()
    }

    async fn retry_delay(attempt: u32) {
        let delay_ms = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempt);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.api_url(path);
        debug!("GET {}", url);

        if self.dry_run {
            info!("[DRY-RUN] GET {}", url);
            return serde_json::from_str("[]")
                .map_err(|e| anyhow!("Dry-run JSON parse error: {}", e));
        }

        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                warn!("Retrying GET {} (attempt {}/{})", url, attempt + 1, MAX_RETRIES + 1);
                Self::retry_delay(attempt - 1).await;
            }

            match self
                .client
                .get(&url)
                .header("PRIVATE-TOKEN", &self.token)
                .send()
                .await
            {
                Ok(response) => return self.handle_response(response).await,
                Err(e) if Self::is_retryable_error(&e) && attempt < MAX_RETRIES => {
                    warn!("GET {} failed (retryable): {}", url, e);
                    last_error = Some(e);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(last_error.map(Into::into).unwrap_or_else(|| anyhow!("Request failed after retries")))
    }

    pub async fn get_optional<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        let url = self.api_url(path);
        debug!("GET {}", url);

        if self.dry_run {
            info!("[DRY-RUN] GET {} -> None", url);
            return Ok(None);
        }

        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                warn!("Retrying GET {} (attempt {}/{})", url, attempt + 1, MAX_RETRIES + 1);
                Self::retry_delay(attempt - 1).await;
            }

            match self
                .client
                .get(&url)
                .header("PRIVATE-TOKEN", &self.token)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status() == StatusCode::NOT_FOUND {
                        return Ok(None);
                    }
                    return Ok(Some(self.handle_response(response).await?));
                }
                Err(e) if Self::is_retryable_error(&e) && attempt < MAX_RETRIES => {
                    warn!("GET {} failed (retryable): {}", url, e);
                    last_error = Some(e);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(last_error.map(Into::into).unwrap_or_else(|| anyhow!("Request failed after retries")))
    }

    pub async fn post<T: DeserializeOwned + DryRunnable, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.api_url(path);
        debug!("POST {}", url);

        if self.dry_run {
            let body_json = serde_json::to_string_pretty(body).unwrap_or_default();
            info!("[DRY-RUN] POST {} with body:\n{}", url, body_json);
            return Ok(T::dry_run_response(path));
        }

        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                warn!("Retrying POST {} (attempt {}/{})", url, attempt + 1, MAX_RETRIES + 1);
                Self::retry_delay(attempt - 1).await;
            }

            match self
                .client
                .post(&url)
                .header("PRIVATE-TOKEN", &self.token)
                .json(body)
                .send()
                .await
            {
                Ok(response) => return self.handle_response(response).await,
                Err(e) if Self::is_retryable_error(&e) && attempt < MAX_RETRIES => {
                    warn!("POST {} failed (retryable): {}", url, e);
                    last_error = Some(e);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(last_error.map(Into::into).unwrap_or_else(|| anyhow!("Request failed after retries")))
    }

    pub async fn post_empty<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.api_url(path);
        debug!("POST {}", url);

        if self.dry_run {
            info!("[DRY-RUN] POST {} (empty body)", url);
            return Err(anyhow!("Dry-run: post_empty not fully supported"));
        }

        let response = self
            .client
            .post(&url)
            .header("PRIVATE-TOKEN", &self.token)
            .send()
            .await?;

        self.handle_response(response).await
    }

    pub async fn put<T: DeserializeOwned + DryRunnable, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.api_url(path);
        debug!("PUT {}", url);

        if self.dry_run {
            let body_json = serde_json::to_string_pretty(body).unwrap_or_default();
            info!("[DRY-RUN] PUT {} with body:\n{}", url, body_json);
            return Ok(T::dry_run_response(path));
        }

        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                warn!("Retrying PUT {} (attempt {}/{})", url, attempt + 1, MAX_RETRIES + 1);
                Self::retry_delay(attempt - 1).await;
            }

            match self
                .client
                .put(&url)
                .header("PRIVATE-TOKEN", &self.token)
                .json(body)
                .send()
                .await
            {
                Ok(response) => return self.handle_response(response).await,
                Err(e) if Self::is_retryable_error(&e) && attempt < MAX_RETRIES => {
                    warn!("PUT {} failed (retryable): {}", url, e);
                    last_error = Some(e);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(last_error.map(Into::into).unwrap_or_else(|| anyhow!("Request failed after retries")))
    }

    pub async fn post_with_status<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<(StatusCode, String)> {
        let url = self.api_url(path);
        debug!("POST {}", url);

        if self.dry_run {
            let body_json = serde_json::to_string_pretty(body).unwrap_or_default();
            info!("[DRY-RUN] POST {} with body:\n{}", url, body_json);
            return Ok((StatusCode::CREATED, "{}".to_string()));
        }

        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                warn!("Retrying POST {} (attempt {}/{})", url, attempt + 1, MAX_RETRIES + 1);
                Self::retry_delay(attempt - 1).await;
            }

            match self
                .client
                .post(&url)
                .header("PRIVATE-TOKEN", &self.token)
                .json(body)
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await?;
                    return Ok((status, body));
                }
                Err(e) if Self::is_retryable_error(&e) && attempt < MAX_RETRIES => {
                    warn!("POST {} failed (retryable): {}", url, e);
                    last_error = Some(e);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(last_error.map(Into::into).unwrap_or_else(|| anyhow!("Request failed after retries")))
    }

    pub async fn put_with_status<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<(StatusCode, String)> {
        let url = self.api_url(path);
        debug!("PUT {}", url);

        if self.dry_run {
            let body_json = serde_json::to_string_pretty(body).unwrap_or_default();
            info!("[DRY-RUN] PUT {} with body:\n{}", url, body_json);
            return Ok((StatusCode::OK, "{}".to_string()));
        }

        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                warn!("Retrying PUT {} (attempt {}/{})", url, attempt + 1, MAX_RETRIES + 1);
                Self::retry_delay(attempt - 1).await;
            }

            match self
                .client
                .put(&url)
                .header("PRIVATE-TOKEN", &self.token)
                .json(body)
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await?;
                    return Ok((status, body));
                }
                Err(e) if Self::is_retryable_error(&e) && attempt < MAX_RETRIES => {
                    warn!("PUT {} failed (retryable): {}", url, e);
                    last_error = Some(e);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(last_error.map(Into::into).unwrap_or_else(|| anyhow!("Request failed after retries")))
    }

    async fn handle_response<T: DeserializeOwned>(&self, response: Response) -> Result<T> {
        let status = response.status();
        let url = response.url().to_string();

        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60);

            warn!("Rate limited. Waiting {} seconds", retry_after);
            tokio::time::sleep(Duration::from_secs(retry_after)).await;

            return Err(anyhow!("Rate limited, please retry"));
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "API request failed: {} {} - {}",
                status,
                url,
                body
            ));
        }

        let body = response.text().await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("Failed to parse response: {} - {}", e, body))
    }
}

// Trait for types that can generate dry-run responses
pub trait DryRunnable {
    fn dry_run_response(path: &str) -> Self;
}

// API response types
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Group {
    pub id: u64,
    pub name: String,
    pub path: String,
    pub web_url: String,
}

impl DryRunnable for Group {
    fn dry_run_response(path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            id,
            name: format!("dry-run-group-{}", id),
            path: path.split('/').last().unwrap_or("group").to_string(),
            web_url: format!("http://localhost/groups/{}", id),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User {
    pub id: u64,
    pub username: String,
    pub email: Option<String>,
    pub name: String,
}

impl DryRunnable for User {
    fn dry_run_response(_path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            id,
            username: format!("dry-run-user-{}", id),
            email: Some(format!("dry-run-{}@example.com", id)),
            name: format!("Dry Run User {}", id),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PersonalAccessToken {
    pub id: u64,
    pub token: String,
}

impl DryRunnable for PersonalAccessToken {
    fn dry_run_response(_path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            id,
            token: format!("glpat-dry-run-token-{}", id),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Project {
    pub id: u64,
    pub name: String,
    pub path: String,
    pub path_with_namespace: String,
    pub web_url: String,
    pub default_branch: Option<String>,
}

impl DryRunnable for Project {
    fn dry_run_response(_path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            id,
            name: format!("dry-run-project-{}", id),
            path: format!("dry-run-project-{}", id),
            path_with_namespace: format!("namespace/dry-run-project-{}", id),
            web_url: format!("http://localhost/projects/{}", id),
            default_branch: Some("main".to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Issue {
    pub id: u64,
    pub iid: u64,
    pub project_id: u64,
    pub title: String,
    pub state: String,
}

impl DryRunnable for Issue {
    fn dry_run_response(path: &str) -> Self {
        let id = next_dry_run_id();
        let project_id = path
            .split('/')
            .find(|s| s.parse::<u64>().is_ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        Self {
            id,
            iid: id,
            project_id,
            title: format!("Dry Run Issue {}", id),
            state: "opened".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MergeRequestAuthor {
    pub id: u64,
    pub username: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MergeRequest {
    pub id: u64,
    pub iid: u64,
    pub project_id: u64,
    pub title: String,
    pub state: String,
    pub source_branch: String,
    pub target_branch: String,
    pub author: MergeRequestAuthor,
}

impl MergeRequest {
    pub fn author_id(&self) -> u64 {
        self.author.id
    }
}

impl DryRunnable for MergeRequest {
    fn dry_run_response(path: &str) -> Self {
        let id = next_dry_run_id();
        let project_id = path
            .split('/')
            .find(|s| s.parse::<u64>().is_ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        Self {
            id,
            iid: id,
            project_id,
            title: format!("Dry Run MR {}", id),
            state: "opened".to_string(),
            source_branch: format!("feature-{}", id),
            target_branch: "main".to_string(),
            author: MergeRequestAuthor {
                id: 1,
                username: "dry-run-author".to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Milestone {
    pub id: u64,
    pub iid: u64,
    pub project_id: Option<u64>,
    pub title: String,
}

impl DryRunnable for Milestone {
    fn dry_run_response(path: &str) -> Self {
        let id = next_dry_run_id();
        let project_id = path
            .split('/')
            .find(|s| s.parse::<u64>().is_ok())
            .and_then(|s| s.parse().ok());
        Self {
            id,
            iid: id,
            project_id,
            title: format!("Dry Run Milestone {}", id),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NoteAuthor {
    pub id: u64,
    pub username: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Note {
    pub id: u64,
    pub body: String,
    pub author: NoteAuthor,
    pub noteable_type: Option<String>,
    pub noteable_id: Option<u64>,
    pub noteable_iid: Option<u64>,
}

/// Enriched note with project context for cross-agent interaction
#[derive(Debug, Clone)]
pub struct PublishedNote {
    pub note_id: u64,
    pub project_id: u64,
    pub noteable_type: String, // "Issue" or "MergeRequest"
    pub noteable_iid: u64,
    pub author_id: u64,
    pub body: String,
}

impl DryRunnable for Note {
    fn dry_run_response(_path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            id,
            body: format!("Dry run comment {}", id),
            author: NoteAuthor {
                id: 1,
                username: "dry-run-commenter".to_string(),
            },
            noteable_type: Some("Issue".to_string()),
            noteable_id: Some(1),
            noteable_iid: Some(1),
        }
    }
}

/// Discussion on a merge request (for threaded comments)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Discussion {
    pub id: String,
    pub notes: Vec<DiscussionNote>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiscussionNote {
    pub id: u64,
    pub body: String,
    pub author: NoteAuthor,
}

/// Enriched discussion with project context for cross-agent replies
#[derive(Debug, Clone)]
pub struct PublishedDiscussion {
    pub discussion_id: String,
    pub project_id: u64,
    pub mr_iid: u64,
    pub author_id: u64,
}

impl DryRunnable for Discussion {
    fn dry_run_response(_path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            id: format!("discussion-{}", id),
            notes: vec![DiscussionNote {
                id,
                body: format!("Dry run discussion note {}", id),
                author: NoteAuthor {
                    id: 1,
                    username: "dry-run-commenter".to_string(),
                },
            }],
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Branch {
    pub name: String,
}

impl DryRunnable for Branch {
    fn dry_run_response(_path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            name: format!("dry-run-branch-{}", id),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileResponse {
    pub file_path: String,
    pub branch: String,
}

impl DryRunnable for FileResponse {
    fn dry_run_response(_path: &str) -> Self {
        let id = next_dry_run_id();
        Self {
            file_path: format!("src/DryRunFile{}.java", id),
            branch: "main".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IssueLink {
    pub source_issue: Issue,
    pub target_issue: Issue,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApprovalResponse {
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Member {
    pub id: u64,
    pub access_level: u32,
}
