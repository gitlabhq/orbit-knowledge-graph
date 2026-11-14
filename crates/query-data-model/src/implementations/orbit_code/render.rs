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
    /// A file column the searched store lists, by its stored name.
    Column(String),
    /// The repository each match is in, under the store's repository column.
    Repository(String),
    /// The branch whose head each match is at, under the store's branch column.
    Branch(String),
    Line,
    Capture(String),
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
        let (mut pattern, mut limit, mut index) = (None, None::<u64>, None);
        for node in plan.nodes() {
            match plan.operation(node) {
                CodeOperation::Repositories {
                    projects,
                    index: read,
                } => {
                    index = Some(*read);
                    narrow(projects)
                }
                CodeOperation::Restrict(projects) => narrow(projects),
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
        let index = index.ok_or(CodeError::Incomplete("repositories"))?;
        let store = &self.model().stores()[index.store];
        let repository = self
            .repository(store.id(), index.entity)
            .ok_or(CodeError::Incomplete("repository columns"))?;
        let fields = plan
            .outputs(plan.root())
            .map(|output| {
                let name = plan.name(output).unwrap_or_default();
                Ok(match plan.value(output) {
                    CodeValue::Column(property) => {
                        let column = store
                            .column(*property)
                            .ok_or(CodeError::Incomplete("a stored column"))?
                            .to_string();
                        match *property {
                            held if held == repository.project => RequestField::Repository(column),
                            held if held == repository.branch => RequestField::Branch(column),
                            _ => RequestField::Column(column),
                        }
                    }
                    CodeValue::Line => RequestField::Line,
                    CodeValue::Capture => RequestField::Capture(name.to_string()),
                })
            })
            .collect::<Result<_, CodeError>>()?;
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
