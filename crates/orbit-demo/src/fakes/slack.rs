//! A fake of Slack behind the Data Extraction Service: it takes the extraction request the live
//! executor renders, signed in as the caller's handle, and answers from the seeded workspaces.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use integration_testkit::seeded_remote::SeededDataExtraction;
use query_data_model::catalog::Row;
use query_data_model::implementations::live::{
    DataExtraction, ExtractionRequest, RequestFilter, Window,
};
use serde::Deserialize;
use serde_json::Value;

use super::Shared;

pub const PATH: &str = "/v1/extract";

/// `ExtractionRequest` as it arrives on the wire.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Extract {
    pub operation: String,
    pub workspaces: Vec<String>,
    pub filters: Vec<Filter>,
    pub window: Option<WireWindow>,
    pub limit: Option<u64>,
    pub fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub field: String,
    pub op: String,
    pub value: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireWindow {
    pub field: String,
    pub from: Option<String>,
    pub to: Option<String>,
}

fn op(name: &str) -> Result<&'static str, String> {
    match name {
        "eq" => Ok("eq"),
        "contains" => Ok("contains"),
        "in" => Ok("in"),
        other => Err(format!("the fake Slack has no {other} filter")),
    }
}

pub async fn extract(
    State(store): State<Shared<SeededDataExtraction>>,
    headers: HeaderMap,
    Json(request): Json<Extract>,
) -> Result<Json<Vec<Row>>, (StatusCode, String)> {
    let user = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or((StatusCode::UNAUTHORIZED, "no Slack sign-in".to_string()))?;
    let filters = request
        .filters
        .into_iter()
        .map(|filter| {
            Ok(RequestFilter {
                field: filter.field,
                op: op(&filter.op)?,
                value: filter.value,
            })
        })
        .collect::<Result<_, String>>()
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let request = ExtractionRequest {
        operation: request.operation,
        workspaces: request.workspaces,
        filters,
        window: request.window.map(|window| Window {
            field: window.field,
            from: window.from,
            to: window.to,
        }),
        limit: request.limit,
        fields: request.fields,
    };
    super::current(&store)
        .extract(user, &request)
        .map(Json)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.0))
}
