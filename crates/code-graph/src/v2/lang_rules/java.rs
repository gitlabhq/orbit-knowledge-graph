use crate::linker::v2::reaching::HasRules;
use crate::linker::v2::rules::*;

pub struct JavaRules;

impl HasRules for JavaRules {
    fn resolution_config() -> ResolutionConfig {
        ResolutionConfig {
            name: "java",
            import_strategies: vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::WildcardImport,
                ImportStrategy::SamePackage,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName { max_candidates: 3 },
            ],
            chain_mode: ChainMode::TypeFlow,
            receiver: ReceiverMode::Keyword,
            fqn_separator: ".",
        }
    }
}
