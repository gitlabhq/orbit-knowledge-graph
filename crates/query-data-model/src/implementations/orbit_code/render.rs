use serde::Serialize;

use super::OrbitCode;
use crate::catalog::{Catalog, Render};
use crate::plan::code::{CodeError, CodeLanguage, CodeOperation, CodePlan, CodeValue};

/// The structural search request the code indexing web servers take over gRPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StructuralSearchRequest {
    pub repositories: Vec<i64>,
    /// The branch heads to search, as (repository, branch); empty searches each default branch.
    pub heads: Vec<(i64, String)>,
    pub languages: Vec<String>,
    pub trigrams: Vec<String>,
    pub pattern: String,
    pub limit: Option<u64>,
    pub fields: Vec<RequestField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestField {
    Path,
    Line,
    Capture(String),
    Repository(String),
    Branch(String),
}

impl Render for Catalog<OrbitCode> {
    type Language = CodeLanguage;
    type Request = StructuralSearchRequest;
    type Error = CodeError;

    fn render(&self, plan: &CodePlan) -> Result<StructuralSearchRequest, CodeError> {
        let mut repositories: Option<Vec<i64>> = None;
        let mut narrow = |projects: &[i64]| {
            repositories = Some(match repositories.take() {
                None => projects.to_vec(),
                Some(current) => current
                    .into_iter()
                    .filter(|project| projects.contains(project))
                    .collect(),
            });
        };
        let (mut languages, mut trigrams, mut heads) = (Vec::new(), Vec::new(), Vec::new());
        let (mut pattern, mut limit) = (None, None::<u64>);
        for node in plan.nodes() {
            match plan.operation(node) {
                CodeOperation::Repositories { projects, .. }
                | CodeOperation::Restrict(projects) => narrow(projects),
                CodeOperation::Languages(named) => languages.extend(
                    named
                        .iter()
                        .map(|language| <&str>::from(*language).to_string()),
                ),
                CodeOperation::Prefilter(atoms) => trigrams.extend(atoms.iter().cloned()),
                CodeOperation::Branches(branches) => heads.extend(branches.iter().cloned()),
                CodeOperation::Structural(matched) => pattern = Some(matched.text().to_string()),
                CodeOperation::Top(count) => {
                    limit = Some(limit.map_or(*count, |limit| limit.min(*count)))
                }
                CodeOperation::Select(_) => {}
            }
        }
        let repositories = repositories.filter(|repositories| !repositories.is_empty());
        let readable = |head: &(i64, String)| {
            repositories
                .as_ref()
                .is_some_and(|kept| kept.contains(&head.0))
        };
        heads.retain(readable);
        let fields = plan
            .outputs(plan.root())
            .map(|output| {
                let name = plan.name(output).unwrap_or_default();
                match plan.value(output) {
                    CodeValue::Path => RequestField::Path,
                    CodeValue::Line => RequestField::Line,
                    CodeValue::Capture => RequestField::Capture(name.to_string()),
                    CodeValue::Repository => RequestField::Repository(name.to_string()),
                    CodeValue::Branch => RequestField::Branch(name.to_string()),
                }
            })
            .collect();
        Ok(StructuralSearchRequest {
            repositories: repositories.ok_or(CodeError::NoReadableRepository)?,
            heads,
            languages,
            trigrams,
            pattern: pattern.ok_or(CodeError::Incomplete("a structural pattern"))?,
            limit,
            fields,
        })
    }
}
