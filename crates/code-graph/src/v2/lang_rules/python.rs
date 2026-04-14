use crate::linker::v2::reaching::HasRules;
use crate::linker::v2::rules::*;

pub struct PythonRules;

impl HasRules for PythonRules {
    fn resolution_config() -> ResolutionConfig {
        ResolutionConfig {
            name: "python",
            import_strategies: vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::FilePath,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName { max_candidates: 3 },
            ],
            chain_mode: ChainMode::ValueFlow,
            receiver: ReceiverMode::Convention {
                instance_decorators: &[],
                classmethod_decorators: &["classmethod"],
                staticmethod_decorators: &["staticmethod"],
            },
            fqn_separator: ".",
        }
    }
}
