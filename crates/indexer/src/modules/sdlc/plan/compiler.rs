use ontology::Ontology;

use super::{CompiledEtl, assemble, input};

pub(in crate::modules::sdlc) struct EtlCompiler<'a> {
    ontology: &'a Ontology,
    global_batch_size: u64,
    namespaced_batch_size: u64,
    batch_size_overrides: &'a std::collections::HashMap<String, u64>,
}

impl<'a> EtlCompiler<'a> {
    pub(in crate::modules::sdlc) fn new(ontology: &'a Ontology) -> Self {
        Self {
            ontology,
            global_batch_size: 1_000_000,
            namespaced_batch_size: 1_000_000,
            batch_size_overrides: &EMPTY_OVERRIDES,
        }
    }

    pub(in crate::modules::sdlc) fn with_global_batch_size(mut self, batch_size: u64) -> Self {
        self.global_batch_size = batch_size;
        self
    }

    pub(in crate::modules::sdlc) fn with_namespaced_batch_size(mut self, batch_size: u64) -> Self {
        self.namespaced_batch_size = batch_size;
        self
    }

    pub(in crate::modules::sdlc) fn with_batch_size_overrides(
        mut self,
        overrides: &'a std::collections::HashMap<String, u64>,
    ) -> Self {
        self.batch_size_overrides = overrides;
        self
    }

    pub(in crate::modules::sdlc) fn compile(self) -> CompiledEtl {
        let inputs = input::from_ontology(self.ontology);
        let plans = assemble::assemble(
            inputs.clone(),
            self.ontology,
            self.global_batch_size,
            self.namespaced_batch_size,
            self.batch_size_overrides,
        );
        CompiledEtl { inputs, plans }
    }
}

static EMPTY_OVERRIDES: std::sync::LazyLock<std::collections::HashMap<String, u64>> =
    std::sync::LazyLock::new(std::collections::HashMap::new);
