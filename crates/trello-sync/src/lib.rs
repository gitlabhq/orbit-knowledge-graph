use std::path::Path;

use mailbox::{
    EdgeDefinition, EdgePayload, NodeDefinition, NodePayload, NodeReference, PluginSchema,
    PropertyDefinition, PropertyType,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tracing::{info, warn};

const PLUGIN_ID: &str = "trello";
const BOARD_NODE_KIND: &str = "trello_Board";
const CARD_NODE_KIND: &str = "trello_Card";

const CARD_IN_BOARD_EDGE: &str = "trello_card_in_board";
const CARD_MENTIONS_MR_EDGE: &str = "trello_card_mentions_merge_request";
const ASSIGNED_TO_EDGE: &str = "trello_assigned_to";

#[derive(Debug, Error)]
pub enum TrelloSyncError {
    #[error("failed to read config: {0}")]
    ConfigRead(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    ConfigParse(#[from] serde_yaml::Error),
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("mailbox request failed with status {status}: {body}")]
    MailboxRequest { status: u16, body: String },
    #[error("trello API request failed with status {status}: {body}")]
    TrelloApi { status: u16, body: String },
    #[error("gitlab API request failed with status {status}: {body}")]
    GitlabApi { status: u16, body: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub trello: TrelloConfig,
    pub mailbox: MailboxConfig,
    pub gitlab: GitlabConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrelloConfig {
    pub api_key: String,
    pub api_token: String,
    pub board_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MailboxConfig {
    pub url: String,
    pub api_key: String,
    pub namespace_id: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitlabConfig {
    pub base_url: String,
    pub personal_access_token: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TrelloBoard {
    id: String,
    name: String,
    url: String,
    #[serde(rename = "desc")]
    description: String,
    closed: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct TrelloCard {
    id: String,
    name: String,
    desc: String,
    url: String,
    #[serde(rename = "idList")]
    list_id: String,
    #[serde(rename = "idMembers")]
    member_ids: Vec<String>,
    closed: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct TrelloMember {
    id: String,
    #[serde(rename = "fullName")]
    full_name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct GitlabUser {
    id: i64,
    name: String,
}

impl TrelloBoard {
    fn to_node_payload(&self) -> NodePayload {
        NodePayload::new(&self.id, BOARD_NODE_KIND).with_properties(json!({
            "name": self.name,
            "description": self.description,
            "url": self.url,
            "closed": self.closed,
        }))
    }
}

impl TrelloCard {
    fn to_node_payload(&self) -> NodePayload {
        NodePayload::new(&self.id, CARD_NODE_KIND).with_properties(json!({
            "name": self.name,
            "description": self.desc,
            "url": self.url,
            "list_id": self.list_id,
            "closed": self.closed,
        }))
    }
}

fn build_plugin_schema() -> PluginSchema {
    PluginSchema::new()
        .with_node(
            NodeDefinition::new(BOARD_NODE_KIND)
                .with_property(PropertyDefinition::new("name", PropertyType::String))
                .with_property(PropertyDefinition::new("description", PropertyType::String).nullable())
                .with_property(PropertyDefinition::new("url", PropertyType::String))
                .with_property(PropertyDefinition::new("closed", PropertyType::Boolean)),
        )
        .with_node(
            NodeDefinition::new(CARD_NODE_KIND)
                .with_property(PropertyDefinition::new("name", PropertyType::String))
                .with_property(PropertyDefinition::new("description", PropertyType::String).nullable())
                .with_property(PropertyDefinition::new("url", PropertyType::String))
                .with_property(PropertyDefinition::new("list_id", PropertyType::String))
                .with_property(PropertyDefinition::new("closed", PropertyType::Boolean)),
        )
        .with_edge(
            EdgeDefinition::new(CARD_IN_BOARD_EDGE)
                .from_kinds(vec![CARD_NODE_KIND.to_string()])
                .to_kinds(vec![BOARD_NODE_KIND.to_string()]),
        )
        .with_edge(
            EdgeDefinition::new(CARD_MENTIONS_MR_EDGE)
                .from_kinds(vec![CARD_NODE_KIND.to_string()])
                .to_kinds(vec!["MergeRequest".to_string()]),
        )
        .with_edge(
            EdgeDefinition::new(ASSIGNED_TO_EDGE)
                .from_kinds(vec!["User".to_string()])
                .to_kinds(vec![CARD_NODE_KIND.to_string()]),
        )
}

fn extract_merge_request_ids(description: &str, gitlab_base_url: &str) -> Vec<i64> {
    let base_url = gitlab_base_url.trim_end_matches('/');
    let pattern = format!(r"{}/.+/-/merge_requests/(\d+)", regex::escape(base_url));

    let Ok(regex) = Regex::new(&pattern) else {
        return Vec::new();
    };

    regex
        .captures_iter(description)
        .filter_map(|cap| cap.get(1)?.as_str().parse().ok())
        .collect()
}

fn match_gitlab_user_by_first_name<'a>(
    trello_member: &TrelloMember,
    gitlab_users: &'a [GitlabUser],
) -> Option<&'a GitlabUser> {
    let trello_first_name = trello_member
        .full_name
        .split_whitespace()
        .next()?
        .to_lowercase();


    info!("trello_first_name: {}", trello_first_name);
    gitlab_users.iter().find(|user| {
        user.name
            .split_whitespace()
            .next()
            .map(|first| first.to_lowercase() == trello_first_name)
            .unwrap_or(false)
    })
}

async fn register_plugin(
    http_client: &reqwest::Client,
    config: &MailboxConfig,
) -> Result<(), TrelloSyncError> {
    let register_url = format!("{}/plugins", config.url);
    let schema = build_plugin_schema();

    let response = http_client
        .post(&register_url)
        .header("X-Plugin-Token", &config.api_key)
        .json(&json!({
            "plugin_id": PLUGIN_ID,
            "namespace_id": config.namespace_id,
            "api_key": config.api_key,
            "schema": schema,
        }))
        .send()
        .await?;

    let status = response.status();
    if status.is_success() {
        info!("plugin registered successfully");
        Ok(())
    } else if status.as_u16() == 409 {
        info!("plugin already registered");
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(TrelloSyncError::MailboxRequest {
            status: status.as_u16(),
            body,
        })
    }
}

async fn fetch_trello_board(
    http_client: &reqwest::Client,
    config: &TrelloConfig,
) -> Result<TrelloBoard, TrelloSyncError> {
    let url = format!(
        "https://api.trello.com/1/boards/{}?key={}&token={}",
        config.board_id, config.api_key, config.api_token
    );

    let response = http_client.get(&url).send().await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(TrelloSyncError::TrelloApi {
            status: status.as_u16(),
            body,
        });
    }

    let board: TrelloBoard = response.json().await?;
    Ok(board)
}

async fn fetch_trello_cards(
    http_client: &reqwest::Client,
    config: &TrelloConfig,
) -> Result<Vec<TrelloCard>, TrelloSyncError> {
    let url = format!(
        "https://api.trello.com/1/boards/{}/cards?key={}&token={}",
        config.board_id, config.api_key, config.api_token
    );

    let response = http_client.get(&url).send().await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(TrelloSyncError::TrelloApi {
            status: status.as_u16(),
            body,
        });
    }

    let cards: Vec<TrelloCard> = response.json().await?;
    Ok(cards)
}

async fn fetch_trello_board_members(
    http_client: &reqwest::Client,
    config: &TrelloConfig,
) -> Result<Vec<TrelloMember>, TrelloSyncError> {
    let url = format!(
        "https://api.trello.com/1/boards/{}/members?key={}&token={}",
        config.board_id, config.api_key, config.api_token
    );

    let response = http_client.get(&url).send().await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(TrelloSyncError::TrelloApi {
            status: status.as_u16(),
            body,
        });
    }

    let members: Vec<TrelloMember> = response.json().await?;
    Ok(members)
}

async fn fetch_gitlab_users(
    http_client: &reqwest::Client,
    config: &GitlabConfig,
) -> Result<Vec<GitlabUser>, TrelloSyncError> {
    let base_url = config.base_url.trim_end_matches('/');
    let mut all_users = Vec::new();
    let mut page = 1;

    loop {
        let url = format!("{}/api/v4/users?per_page=100&page={}", base_url, page);

        let response = http_client
            .get(&url)
            .header("PRIVATE-TOKEN", &config.personal_access_token)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(TrelloSyncError::GitlabApi {
                status: status.as_u16(),
                body,
            });
        }

        let users: Vec<GitlabUser> = response.json().await?;
        let users_count = users.len();
        all_users.extend(users);

        if users_count < 100 {
            break;
        }

        page += 1;
    }

    Ok(all_users)
}

fn build_edges(
    board: &TrelloBoard,
    cards: &[TrelloCard],
    trello_members: &[TrelloMember],
    gitlab_users: &[GitlabUser],
    gitlab_base_url: &str,
) -> Vec<EdgePayload> {
    let mut edges = Vec::new();

    for card in cards {
        let card_in_board_edge_id = format!("{}_{}", card.id, board.id);
        edges.push(EdgePayload::new(
            card_in_board_edge_id,
            CARD_IN_BOARD_EDGE,
            NodeReference::new(CARD_NODE_KIND, &card.id),
            NodeReference::new(BOARD_NODE_KIND, &board.id),
        ));

        let mr_ids = extract_merge_request_ids(&card.desc, gitlab_base_url);
        for mr_id in mr_ids {
            let edge_id = format!("{}_{}", card.id, mr_id);
            edges.push(EdgePayload::new(
                edge_id,
                CARD_MENTIONS_MR_EDGE,
                NodeReference::new(CARD_NODE_KIND, &card.id),
                NodeReference::new("MergeRequest", mr_id.to_string()),
            ));
        }

        for member_id in &card.member_ids {
            let Some(member) = trello_members.iter().find(|m| &m.id == member_id) else {
                continue;
            };

            let Some(gitlab_user) = match_gitlab_user_by_first_name(member, gitlab_users) else {
                warn!(
                    trello_member = %member.full_name,
                    "no matching GitLab user found"
                );
                continue;
            };

            let edge_id = format!("{}_{}", gitlab_user.id, card.id);
            edges.push(EdgePayload::new(
                edge_id,
                ASSIGNED_TO_EDGE,
                NodeReference::new("User", gitlab_user.id.to_string()),
                NodeReference::new(CARD_NODE_KIND, &card.id),
            ));
        }
    }

    edges
}

async fn submit_data(
    http_client: &reqwest::Client,
    config: &MailboxConfig,
    nodes: Vec<NodePayload>,
    edges: Vec<EdgePayload>,
) -> Result<(), TrelloSyncError> {
    let submit_url = format!("{}/messages", config.url);
    let message_id = uuid::Uuid::new_v4().to_string();

    let response = http_client
        .post(&submit_url)
        .header("X-Plugin-Token", &config.api_key)
        .header("X-Plugin-Id", PLUGIN_ID)
        .json(&json!({
            "message_id": message_id,
            "plugin_id": PLUGIN_ID,
            "nodes": nodes,
            "edges": edges,
        }))
        .send()
        .await?;

    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(TrelloSyncError::MailboxRequest {
            status: status.as_u16(),
            body,
        })
    }
}

pub async fn run(config_path: &Path) -> Result<(), TrelloSyncError> {
    info!(path = %config_path.display(), "loading config");
    let config_content = std::fs::read_to_string(config_path)?;
    let config: Config = serde_yaml::from_str(&config_content)?;

    let http_client = reqwest::Client::new();

    register_plugin(&http_client, &config.mailbox).await?;

    info!(board_id = %config.trello.board_id, "fetching board from Trello");
    let board = fetch_trello_board(&http_client, &config.trello).await?;
    info!(board_name = %board.name, "fetched board");

    info!("fetching cards from Trello");
    let cards = fetch_trello_cards(&http_client, &config.trello).await?;
    info!(count = cards.len(), "fetched cards");

    info!("fetching board members from Trello");
    let trello_members = fetch_trello_board_members(&http_client, &config.trello).await?;
    info!(count = trello_members.len(), "fetched members");

    info!("fetching users from GitLab");
    let gitlab_users = fetch_gitlab_users(&http_client, &config.gitlab).await?;
    info!(count = gitlab_users.len(), "fetched GitLab users");

    let mut nodes: Vec<NodePayload> = Vec::new();
    nodes.push(board.to_node_payload());
    nodes.extend(cards.iter().map(TrelloCard::to_node_payload));

    let edges = build_edges(
        &board,
        &cards,
        &trello_members,
        &gitlab_users,
        &config.gitlab.base_url,
    );
    info!(
        node_count = nodes.len(),
        edge_count = edges.len(),
        "submitting data to mailbox"
    );

    submit_data(&http_client, &config.mailbox, nodes, edges).await?;
    info!("sync completed successfully");

    Ok(())
}
