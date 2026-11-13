use super::{CodeBuilder, CodeError, CodeLanguage, CodeOperation, CodePlan, CodeValue, Matches};
use crate::implementations::orbit_code::SearchMode;
use crate::plan::tracking::{self, Construction, Declare, Output, Outputs, Rebuilder};

impl CodeBuilder<'_> {
    /// Checks `operation` on `inputs` and appends it.
    pub fn apply(
        &mut self,
        operation: CodeOperation,
        inputs: Vec<Matches>,
    ) -> Result<Matches, CodeError> {
        let outputs = self.outputs(&operation, &inputs);
        let rows = self.construction.attempt(|construction| {
            let outputs = outputs?;
            let inputs = inputs.into_iter().map(Matches::into_inner).collect();
            Ok::<_, CodeError>(construction.connect(operation, inputs, outputs)?.rows)
        })?;
        self.view(rows)
    }

    fn outputs(
        &self,
        operation: &CodeOperation,
        inputs: &[Matches],
    ) -> Result<Outputs<CodeValue>, CodeError> {
        let preserve = |empty: bool, error: CodeError| match empty {
            true => Err(error),
            false => Ok(Outputs::preserve()),
        };
        match operation {
            CodeOperation::Repositories { projects, index } => match projects.is_empty() {
                true => Err(CodeError::NoRepositories),
                false => {
                    let graph = self.catalog.graph();
                    let store = &self.catalog.model().stores()[index.store];
                    Ok(Outputs::extend(store.returns(index.entity).map(
                        |property| {
                            Declare::named(
                                graph.property(property).name.as_str(),
                                CodeValue::Column(property),
                            )
                        },
                    )))
                }
            },
            CodeOperation::Branches(heads) => {
                Self::files(inputs, "a branch restriction")?;
                match heads.is_empty() {
                    true => Err(CodeError::Incomplete("at least one branch head")),
                    false => Ok(Outputs::preserve()),
                }
            }
            CodeOperation::Restrict(readable) => {
                Self::files(inputs, "a repository restriction")?;
                preserve(readable.is_empty(), CodeError::NoReadableRepository)
            }
            CodeOperation::Languages(languages) => {
                Self::files(inputs, "a language filter")?;
                let missing = CodeError::Incomplete("at least one language");
                preserve(languages.is_empty(), missing)
            }
            CodeOperation::Prefilter(atoms) => {
                let index = Self::files(inputs, "a prefilter")?;
                self.searchable(index, SearchMode::Trigram)?;
                match atoms.iter().find(|atom| atom.chars().count() < 3) {
                    Some(atom) => Err(CodeError::ShortAtom(atom.clone())),
                    None => Ok(Outputs::preserve()),
                }
            }
            CodeOperation::Structural(pattern) => {
                let index = Self::files(inputs, "a structural match")?;
                self.searchable(index, SearchMode::Structural)?;
                let captures = pattern.captures().iter();
                let captures = captures.map(|capture| Declare::named(capture, CodeValue::Capture));
                let line = Declare::named(crate::plan::logical::MATCH_LINE, CodeValue::Line);
                Ok(Outputs::extend(std::iter::once(line).chain(captures)))
            }
            CodeOperation::Top(count) => preserve(*count == 0, CodeError::EmptyTop),
            CodeOperation::Select(refs) => {
                let mut outputs = Vec::new();
                for output in refs {
                    let (name, value) = self.construction.value(*output)?;
                    let name = name.map(str::to_string);
                    outputs.push(Output::Declare(Declare {
                        name,
                        value: *value,
                    }));
                }
                Ok(Outputs::replace(outputs))
            }
        }
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

    fn apply(
        &mut self,
        operation: CodeOperation,
        inputs: Vec<Matches>,
        _outputs: Outputs<CodeValue>,
    ) -> Result<Matches, CodeError> {
        CodeBuilder::apply(self, operation, inputs)
    }
}
