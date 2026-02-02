//! HTTP route handlers for mailbox endpoints.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use etl_engine::nats::NatsServices;
use etl_engine::types::Envelope;
use ontology::Ontology;
use tracing::{error, info};

use crate::auth::{PluginAuth, hash_api_key, verify_api_key};
use crate::error::MailboxError;
use crate::http::{
    ErrorResponse, MessageAcceptedResponse, PluginInfoResponse, PluginListResponse,
    RegisterPluginRequest, SubmitMessageRequest,
};
use crate::schema_generator::generate_create_table_ddl;
use crate::storage::{MigrationStore, PluginStore};
use crate::types::{MailboxMessage, Plugin};
use crate::validation::{MessageValidator, SchemaValidator};

#[derive(Clone)]
pub struct MailboxState {
    pub plugin_store: Arc<PluginStore>,
    pub migration_store: Arc<MigrationStore>,
    pub nats: Arc<dyn NatsServices>,
    pub ontology: Arc<Ontology>,
}

pub fn create_mailbox_router(state: MailboxState) -> Router {
    Router::new()
        .route("/plugins", post(register_plugin))
        .route("/plugins/{plugin_id}", get(get_plugin))
        .route("/plugins/{plugin_id}", delete(delete_plugin))
        .route("/namespaces/{namespace_id}/plugins", get(list_plugins))
        .route("/messages", post(submit_message))
        .with_state(state)
}

async fn register_plugin(
    State(state): State<MailboxState>,
    Json(request): Json<RegisterPluginRequest>,
) -> impl IntoResponse {
    let validator = SchemaValidator::new((*state.ontology).clone());

    if let Err(e) = validator.validate(&request.plugin_id, &request.schema) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(e.to_string())),
        )
            .into_response();
    }

    if let Ok(Some(existing)) = state
        .plugin_store
        .get_by_namespace(request.namespace_id, &request.plugin_id)
        .await
    {
        let api_key_matches = match verify_api_key(&request.api_key, &existing.api_key_hash) {
            Ok(matches) => matches,
            Err(e) => {
                error!(error = %e, "failed to verify API key");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new("Failed to verify API key")),
                )
                    .into_response();
            }
        };

        if api_key_matches && request.schema == existing.schema {
            info!(
                plugin_id = %existing.plugin_id,
                namespace_id = %existing.namespace_id,
                "plugin already registered with matching api key and schema"
            );
            return (
                StatusCode::OK,
                Json(PluginInfoResponse::from(crate::types::PluginInfo::from(
                    existing,
                ))),
            )
                .into_response();
        }

        return (
            StatusCode::CONFLICT,
            Json(ErrorResponse::new(format!(
                "plugin '{}' already exists in namespace {} with different configuration",
                request.plugin_id, request.namespace_id
            ))),
        )
            .into_response();
    }

    let api_key_hash = match hash_api_key(&request.api_key) {
        Ok(hash) => hash,
        Err(e) => {
            error!(error = %e, "failed to hash API key");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to process API key")),
            )
                .into_response();
        }
    };

    let plugin = Plugin::new(
        request.plugin_id.clone(),
        request.namespace_id,
        api_key_hash,
        request.schema.clone(),
    );

    if let Err(e) = state.plugin_store.insert(&plugin).await {
        error!(error = %e, "failed to store plugin");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Failed to store plugin")),
        )
            .into_response();
    }

    for node in &request.schema.nodes {
        let ddl = generate_create_table_ddl(&plugin, node);

        if let Err(e) = state.migration_store.execute_ddl(&ddl).await {
            error!(error = %e, node = %node.name, "failed to create table");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!(
                    "Failed to create table for node '{}'",
                    node.name
                ))),
            )
                .into_response();
        }

        let ddl_hash = compute_ddl_hash(&ddl);

        if let Err(e) = state
            .migration_store
            .record_migration(
                &plugin.plugin_id,
                plugin.schema_version,
                &node.name,
                &plugin.table_name_for_node(&node.name),
                &ddl_hash,
            )
            .await
        {
            error!(error = %e, node = %node.name, "failed to record migration");
        }
    }

    info!(
        plugin_id = %plugin.plugin_id,
        namespace_id = %plugin.namespace_id,
        "registered plugin"
    );

    (
        StatusCode::CREATED,
        Json(PluginInfoResponse::from(crate::types::PluginInfo::from(
            plugin,
        ))),
    )
        .into_response()
}

async fn get_plugin(
    State(state): State<MailboxState>,
    Path(plugin_id): Path<String>,
) -> impl IntoResponse {
    match state.plugin_store.get(&plugin_id).await {
        Ok(Some(plugin)) => (
            StatusCode::OK,
            Json(PluginInfoResponse::from(crate::types::PluginInfo::from(
                plugin,
            ))),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::new(format!(
                "Plugin '{}' not found",
                plugin_id
            ))),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "failed to get plugin");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to retrieve plugin")),
            )
                .into_response()
        }
    }
}

async fn delete_plugin(
    State(state): State<MailboxState>,
    Path(plugin_id): Path<String>,
) -> impl IntoResponse {
    match state.plugin_store.delete(&plugin_id).await {
        Ok(()) => {
            info!(plugin_id = %plugin_id, "deleted plugin");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(MailboxError::PluginNotFound { .. }) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::new(format!(
                "Plugin '{}' not found",
                plugin_id
            ))),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "failed to delete plugin");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to delete plugin")),
            )
                .into_response()
        }
    }
}

async fn list_plugins(
    State(state): State<MailboxState>,
    Path(namespace_id): Path<i64>,
) -> impl IntoResponse {
    match state.plugin_store.list_by_namespace(namespace_id).await {
        Ok(plugins) => {
            let response = PluginListResponse {
                plugins: plugins.into_iter().map(PluginInfoResponse::from).collect(),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!(error = %e, "failed to list plugins");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to list plugins")),
            )
                .into_response()
        }
    }
}

async fn submit_message(
    State(state): State<MailboxState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<SubmitMessageRequest>,
) -> impl IntoResponse {
    let auth = match PluginAuth::from_headers(&headers, &state.plugin_store).await {
        Ok(auth) => auth,
        Err(MailboxError::Authentication(msg)) => {
            return (StatusCode::UNAUTHORIZED, Json(ErrorResponse::new(msg))).into_response();
        }
        Err(MailboxError::PluginNotFound { plugin_id }) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::new(format!(
                    "Plugin '{}' not found",
                    plugin_id
                ))),
            )
                .into_response();
        }
        Err(e) => {
            error!(error = %e, "authentication error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Authentication failed")),
            )
                .into_response();
        }
    };

    let message: MailboxMessage = request.into();

    if let Err(e) = MessageValidator::validate(&message, &auth.plugin) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(e.to_string())),
        )
            .into_response();
    }

    let envelope = match Envelope::new(&message) {
        Ok(env) => env,
        Err(e) => {
            error!(error = %e, "failed to create envelope");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to process message")),
            )
                .into_response();
        }
    };

    let topic = <MailboxMessage as etl_engine::types::Event>::topic();

    if let Err(e) = state.nats.publish(&topic, &envelope).await {
        error!(error = %e, "failed to publish message");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Failed to accept message")),
        )
            .into_response();
    }

    info!(
        message_id = %message.message_id,
        plugin_id = %message.plugin_id,
        nodes = message.nodes.len(),
        edges = message.edges.len(),
        "accepted message"
    );

    (
        StatusCode::ACCEPTED,
        Json(MessageAcceptedResponse::new(message.message_id)),
    )
        .into_response()
}

fn compute_ddl_hash(ddl: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    ddl.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}
