use super::{
    CodeError, CodeLanguage, CodeOperation, CodePlan, CodeValue, Index, Language, Pattern,
};
use crate::PropertyId;
use crate::catalog::Catalog;
use crate::catalog::SearchMode;
use crate::implementations::orbit_code::OrbitCode;
use crate::plan::logical::Literal;
use crate::plan::tracking::{self, Construction, Declared, Ref};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    output: Ref,
    name: String,
    value: CodeValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigrams(Vec<String>);

impl Trigrams {
    pub fn of<'a>(atoms: impl IntoIterator<Item = &'a str>) -> Self {
        Self(atoms.into_iter().map(str::to_string).collect())
    }
}

#[derive(Debug)]
pub struct Matches {
    pub(super) rows: tracking::Rows,
    fields: Vec<Field>,
    index: Option<Index>,
}

impl Matches {
    /// The column of the index's store that holds `property`.
    pub fn column(&self, property: PropertyId) -> Result<Field, CodeError> {
        self.fields
            .iter()
            .find(|field| field.value == CodeValue::Column(property))
            .cloned()
            .ok_or_else(|| CodeError::UnknownField {
                name: format!("{property:?}"),
                available: self.names(|value| matches!(value, CodeValue::Column(_))),
            })
    }

    /// The line each match starts at.
    pub fn line(&self) -> Result<Field, CodeError> {
        self.fields
            .iter()
            .find(|field| field.value == CodeValue::Line)
            .cloned()
            .ok_or(CodeError::Incomplete("a structural match before its line"))
    }

    pub fn capture(&self, name: &str) -> Result<Field, CodeError> {
        self.find(name, |value| value == CodeValue::Capture)
            .ok_or_else(|| CodeError::UnknownCapture {
                name: name.to_string(),
                available: self.names(|value| value == CodeValue::Capture),
            })
    }

    pub(super) fn into_inner(self) -> tracking::Rows {
        self.rows
    }

    fn find(&self, name: &str, kind: impl Fn(CodeValue) -> bool) -> Option<Field> {
        self.fields
            .iter()
            .find(|field| field.name == name && kind(field.value))
            .cloned()
    }

    fn names(&self, kind: impl Fn(CodeValue) -> bool) -> Vec<String> {
        self.fields
            .iter()
            .filter(|field| kind(field.value))
            .map(|field| field.name.clone())
            .collect()
    }

    fn matched(&self) -> bool {
        self.fields
            .iter()
            .any(|field| field.value == CodeValue::Line)
    }
}

pub struct CodeBuilder<'c> {
    pub(super) catalog: &'c Catalog<OrbitCode>,
    pub(super) construction: Construction<CodeLanguage>,
    index: Option<Index>,
}

impl<'c> CodeBuilder<'c> {
    pub(super) fn new(catalog: &'c Catalog<OrbitCode>) -> Self {
        Self {
            catalog,
            construction: Construction::new(catalog.model().id()),
            index: None,
        }
    }

    pub(super) fn finish(self, root: Result<Matches, CodeError>) -> Result<CodePlan, CodeError> {
        Ok(self.construction.finish(root?.rows)?)
    }

    pub(super) fn view(&mut self, rows: tracking::Rows) -> Result<Matches, CodeError> {
        if let CodeOperation::Repositories { index, .. } = self.construction.operation(&rows)? {
            self.index = Some(*index);
        }
        let mut fields = Vec::new();
        for output in self.construction.outputs(&rows)? {
            if let (Some(name), value) = self.construction.value(output)? {
                fields.push(Field {
                    output,
                    name: name.to_string(),
                    value: *value,
                });
            }
        }
        Ok(Matches {
            rows,
            fields,
            index: self.index,
        })
    }

    pub(super) fn files(inputs: &[Matches], step: &'static str) -> Result<Index, CodeError> {
        match inputs {
            [files] if files.matched() => Err(CodeError::AfterMatch(step)),
            [files] => files.index.ok_or(CodeError::Incomplete("repositories")),
            _ => Err(CodeError::Incomplete("one input")),
        }
    }

    pub(super) fn searchable(&self, index: Index, mode: SearchMode) -> Result<(), CodeError> {
        let graph = self.catalog.graph();
        let store = self.catalog.model().stores()[index.store].id();
        let facts = self.catalog.facts(store);
        let property = facts.and_then(|facts| facts.structural(index.entity));
        match (facts, property) {
            (Some(facts), Some(property)) if facts.supports(index.entity, property, mode) => Ok(()),
            _ => Err(CodeError::Unsupported(
                graph.entity(index.entity).name.clone(),
                match mode {
                    SearchMode::Trigram => "trigram",
                    SearchMode::Structural => "structural pattern",
                },
            )),
        }
    }

    /// The files of the repositories `projects` names, each a key of the index's repository
    /// column.
    pub fn repositories(
        &mut self,
        index: Declared<Index>,
        projects: impl IntoIterator<Item = Literal>,
    ) -> Result<Matches, CodeError> {
        let index = self
            .construction
            .attempt(|construction| construction.declared(index))?;
        let mut projects = projects
            .into_iter()
            .map(|project| match project {
                Literal::Int(project) => Ok(project),
                other => Err(CodeError::RepositoryKey(format!("{other:?}"))),
            })
            .collect::<Result<Vec<i64>, _>>()?;
        projects.sort_unstable();
        projects.dedup();
        self.apply(CodeOperation::Repositories { index, projects }, Vec::new())
    }

    pub fn restrict(
        &mut self,
        files: Matches,
        readable: impl IntoIterator<Item = i64>,
    ) -> Result<Matches, CodeError> {
        let mut readable: Vec<i64> = readable.into_iter().collect();
        readable.sort_unstable();
        readable.dedup();
        self.apply(CodeOperation::Restrict(readable), vec![files])
    }

    pub fn branches(
        &mut self,
        files: Matches,
        heads: impl IntoIterator<Item = (Literal, String)>,
    ) -> Result<Matches, CodeError> {
        let mut heads = heads
            .into_iter()
            .map(|(project, branch)| match project {
                Literal::Int(project) => Ok((project, branch)),
                other => Err(CodeError::RepositoryKey(format!("{other:?}"))),
            })
            .collect::<Result<Vec<(i64, String)>, _>>()?;
        heads.sort();
        heads.dedup();
        self.apply(CodeOperation::Branches(heads), vec![files])
    }

    pub fn languages(
        &mut self,
        files: Matches,
        languages: impl IntoIterator<Item = Language>,
    ) -> Result<Matches, CodeError> {
        let languages = languages.into_iter().collect();
        self.apply(CodeOperation::Languages(languages), vec![files])
    }

    pub fn prefilter(&mut self, files: Matches, trigrams: Trigrams) -> Result<Matches, CodeError> {
        self.apply(CodeOperation::Prefilter(trigrams.0), vec![files])
    }

    pub fn structural(&mut self, files: Matches, pattern: Pattern) -> Result<Matches, CodeError> {
        self.apply(CodeOperation::Structural(pattern), vec![files])
    }

    pub fn top(&mut self, matches: Matches, count: u64) -> Result<Matches, CodeError> {
        self.apply(CodeOperation::Top(count), vec![matches])
    }

    pub fn select(
        &mut self,
        matches: Matches,
        fields: impl IntoIterator<Item = Field>,
    ) -> Result<Matches, CodeError> {
        let refs = fields.into_iter().map(|field| field.output).collect();
        self.apply(CodeOperation::Select(refs), vec![matches])
    }
}
