use serde::Serialize;
use serde_json::Value;

use super::Live;
use crate::catalog::{Catalog, Render};
use crate::plan::live::{Condition, LiveError, LiveLanguage, LiveOperation, LivePlan};

/// The call the Data Extraction Service runs with the caller's own sign-in.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExtractionRequest {
    pub operation: String,
    pub workspaces: Vec<String>,
    pub filters: Vec<RequestFilter>,
    pub window: Option<Window>,
    pub limit: Option<u64>,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RequestFilter {
    pub field: String,
    pub op: &'static str,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Window {
    pub field: String,
    pub from: Option<String>,
    pub to: Option<String>,
}

impl Render for Catalog<Live> {
    type Language = LiveLanguage;
    type Request = ExtractionRequest;
    type Error = LiveError;

    fn render(&self, plan: &LivePlan) -> Result<ExtractionRequest, LiveError> {
        let name = |output| plan.name(output).unwrap_or_default().to_string();
        let mut operation = None;
        let mut workspaces = None;
        let (mut filters, mut window, mut limit) = (Vec::new(), None, None::<u64>);
        for node in plan.nodes() {
            match plan.operation(node) {
                LiveOperation::Call(call) => {
                    let endpoint = self
                        .endpoint(call.entity)
                        .ok_or(LiveError::Incomplete("an operation"))?;
                    operation = Some(endpoint.operation.clone());
                }
                LiveOperation::Filter(conditions) => {
                    filters.extend(conditions.iter().map(|condition| match condition {
                        Condition::Equals {
                            field,
                            value: literal,
                        } => RequestFilter {
                            field: name(*field),
                            op: "eq",
                            value: Value::from(literal),
                        },
                        Condition::Contains { field, text } => RequestFilter {
                            field: name(*field),
                            op: "contains",
                            value: Value::from(text.as_str()),
                        },
                        Condition::OneOf { field, values } => RequestFilter {
                            field: name(*field),
                            op: "in",
                            value: Value::Array(values.iter().map(Value::from).collect()),
                        },
                    }))
                }
                LiveOperation::Window { field, from, to } => {
                    window = Some(Window {
                        field: name(*field),
                        from: from.clone(),
                        to: to.clone(),
                    })
                }
                LiveOperation::Workspaces(allowed) => workspaces = Some(allowed.clone()),
                LiveOperation::Limit(rows) => {
                    limit = Some(limit.map_or(*rows, |limit| limit.min(*rows)))
                }
                LiveOperation::Select(_) => {}
            }
        }
        Ok(ExtractionRequest {
            operation: operation.ok_or(LiveError::Incomplete("an operation"))?,
            workspaces: workspaces.ok_or(LiveError::NoWorkspace)?,
            filters,
            window,
            limit,
            fields: plan.outputs(plan.root()).map(name).collect(),
        })
    }
}
