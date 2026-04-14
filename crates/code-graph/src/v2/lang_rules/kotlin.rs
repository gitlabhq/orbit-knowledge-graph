use crate::linker::v2::reaching::HasRules;
use crate::linker::v2::rules::*;

pub struct KotlinRules;

impl HasRules for KotlinRules {
    fn resolution_config() -> ResolutionConfig {
        ResolutionConfig {
            name: "kotlin",
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
