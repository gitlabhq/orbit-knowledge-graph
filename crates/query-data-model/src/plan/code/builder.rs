use super::{
    CodeError, CodeLanguage, CodeOperation, CodePlan, CodeValue, Index, Language, Pattern,
};
use crate::catalog::Catalog;
use crate::implementations::orbit_code::{OrbitCode, SearchMode};
use crate::plan::tracking::{
    self, Construction, Declare, Declared, Output, Outputs, Rebuilder, Ref,
};

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
    rows: tracking::Rows,
    fields: Vec<Field>,
    index: Option<Index>,
}

impl Matches {
    pub fn field(&self, name: &str) -> Result<Field, CodeError> {
        self.find(name, |value| value != CodeValue::Capture)
            .ok_or_else(|| CodeError::UnknownField {
                name: name.to_string(),
                available: self.names(|value| value != CodeValue::Capture),
            })
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
    catalog: &'c Catalog<OrbitCode>,
    construction: Construction<CodeLanguage>,
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

    fn view(&mut self, rows: tracking::Rows) -> Result<Matches, CodeError> {
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

    pub fn operation(&self, matches: &Matches) -> Result<&CodeOperation, CodeError> {
        Ok(self.construction.operation(&matches.rows)?)
    }

    fn connect(
        &mut self,
        operation: CodeOperation,
        inputs: Vec<Matches>,
        check: impl FnOnce(&[Matches]) -> Result<Outputs<CodeValue>, CodeError>,
    ) -> Result<Matches, CodeError> {
        let rows = self.construction.attempt(|construction| {
            let outputs = check(&inputs)?;
            let inputs = inputs.into_iter().map(Matches::into_inner).collect();
            Ok::<_, CodeError>(construction.connect(operation, inputs, outputs)?.rows)
        })?;
        self.view(rows)
    }

    fn files(inputs: &[Matches], step: &'static str) -> Result<Index, CodeError> {
        match inputs {
            [files] if files.matched() => Err(CodeError::AfterMatch(step)),
            [files] => files.index.ok_or(CodeError::Incomplete("repositories")),
            _ => Err(CodeError::Incomplete("one input")),
        }
    }

    fn searchable(&self, index: Index, mode: SearchMode) -> Result<(), CodeError> {
        let graph = self.catalog.graph();
        let store = &self.catalog.model().stores()[index.store];
        let facts = self.catalog.backend(&store.name);
        let property = facts.and_then(|facts| facts.structural(index.entity));
        match (facts, property) {
            (Some(facts), Some(property)) if facts.supports(property, mode) => Ok(()),
            _ => Err(CodeError::Unsupported(
                graph.entity(index.entity).name.clone(),
                match mode {
                    SearchMode::Trigram => "trigram",
                    SearchMode::Structural => "structural pattern",
                },
            )),
        }
    }

    pub fn repositories(
        &mut self,
        index: Declared<Index>,
        projects: impl IntoIterator<Item = i64>,
    ) -> Result<Matches, CodeError> {
        let index = self
            .construction
            .attempt(|construction| construction.declared(index))?;
        let mut projects: Vec<i64> = projects.into_iter().collect();
        projects.sort_unstable();
        projects.dedup();
        let operation = CodeOperation::Repositories {
            index,
            projects: projects.clone(),
        };
        self.connect(operation, Vec::new(), |_| match projects.is_empty() {
            true => Err(CodeError::NoRepositories),
            false => Ok(Outputs::extend([Declare::named("path", CodeValue::Path)])),
        })
    }

    pub fn restrict(
        &mut self,
        files: Matches,
        readable: impl IntoIterator<Item = i64>,
    ) -> Result<Matches, CodeError> {
        let mut readable: Vec<i64> = readable.into_iter().collect();
        readable.sort_unstable();
        readable.dedup();
        let empty = readable.is_empty();
        self.connect(CodeOperation::Restrict(readable), vec![files], |inputs| {
            Self::files(inputs, "a repository restriction")?;
            match empty {
                true => Err(CodeError::NoReadableRepository),
                false => Ok(Outputs::preserve()),
            }
        })
    }

    pub fn languages(
        &mut self,
        files: Matches,
        languages: impl IntoIterator<Item = Language>,
    ) -> Result<Matches, CodeError> {
        let languages: Vec<Language> = languages.into_iter().collect();
        let empty = languages.is_empty();
        self.connect(CodeOperation::Languages(languages), vec![files], |inputs| {
            Self::files(inputs, "a language filter")?;
            match empty {
                true => Err(CodeError::Incomplete("at least one language")),
                false => Ok(Outputs::preserve()),
            }
        })
    }

    pub fn prefilter(&mut self, files: Matches, trigrams: Trigrams) -> Result<Matches, CodeError> {
        let index = Self::files(std::slice::from_ref(&files), "a prefilter");
        let short = trigrams
            .0
            .iter()
            .find(|atom| atom.chars().count() < 3)
            .cloned();
        let searchable = index
            .clone()
            .and_then(|index| self.searchable(index, SearchMode::Trigram));
        self.connect(CodeOperation::Prefilter(trigrams.0), vec![files], |_| {
            index?;
            searchable?;
            match short {
                Some(atom) => Err(CodeError::ShortAtom(atom)),
                None => Ok(Outputs::preserve()),
            }
        })
    }

    pub fn structural(&mut self, files: Matches, pattern: Pattern) -> Result<Matches, CodeError> {
        let index = Self::files(std::slice::from_ref(&files), "a structural match");
        let searchable = index
            .clone()
            .and_then(|index| self.searchable(index, SearchMode::Structural));
        let declared: Vec<Declare<CodeValue>> =
            std::iter::once(Declare::named("line", CodeValue::Line))
                .chain(
                    pattern
                        .captures()
                        .iter()
                        .map(|capture| Declare::named(capture, CodeValue::Capture)),
                )
                .collect();
        self.connect(CodeOperation::Structural(pattern), vec![files], |_| {
            index?;
            searchable?;
            Ok(Outputs::extend(declared))
        })
    }

    pub fn top(&mut self, matches: Matches, count: u64) -> Result<Matches, CodeError> {
        self.connect(CodeOperation::Top(count), vec![matches], |_| match count {
            0 => Err(CodeError::EmptyTop),
            _ => Ok(Outputs::preserve()),
        })
    }

    pub fn select(
        &mut self,
        matches: Matches,
        fields: impl IntoIterator<Item = Field>,
    ) -> Result<Matches, CodeError> {
        let (refs, outputs): (Vec<_>, Vec<_>) = fields
            .into_iter()
            .map(|field| {
                let declare = Declare::named(field.name, field.value);
                (field.output, Output::Declare(declare))
            })
            .unzip();
        self.connect(CodeOperation::Select(refs), vec![matches], |_| {
            Ok(Outputs::replace(outputs))
        })
    }
}

impl Rebuilder for CodeBuilder<'_> {
    type Language = CodeLanguage;
    type Old = CodePlan;
    type Rows = Matches;
    type Error = CodeError;

    fn plan(old: &CodePlan) -> &CodePlan {
        old
    }

    fn construction(&mut self) -> &mut Construction<CodeLanguage> {
        &mut self.construction
    }

    fn rows(rows: &Matches) -> &tracking::Rows {
        &rows.rows
    }

    fn replay(
        &mut self,
        operation: CodeOperation,
        mut inputs: Vec<Matches>,
        _outputs: Outputs<CodeValue>,
    ) -> Result<Matches, CodeError> {
        let mut input = || match inputs.is_empty() {
            true => Err(CodeError::Incomplete("one input")),
            false => Ok(inputs.remove(0)),
        };
        match operation {
            CodeOperation::Repositories { index, projects } => {
                let index = Declared::new(self.catalog.model().id(), index);
                self.repositories(index, projects)
            }
            CodeOperation::Restrict(readable) => self.restrict(input()?, readable),
            CodeOperation::Languages(languages) => self.languages(input()?, languages),
            CodeOperation::Prefilter(atoms) => self.prefilter(input()?, Trigrams(atoms)),
            CodeOperation::Structural(pattern) => self.structural(input()?, pattern),
            CodeOperation::Top(count) => self.top(input()?, count),
            CodeOperation::Select(refs) => {
                let matches = input()?;
                let fields = refs.iter().map(|output| {
                    let field = matches.fields.iter().find(|field| field.output == *output);
                    field
                        .cloned()
                        .ok_or(CodeError::Incomplete("a selected field"))
                });
                let fields = fields.collect::<Result<Vec<_>, _>>()?;
                self.select(matches, fields)
            }
        }
    }
}
