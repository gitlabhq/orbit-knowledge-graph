#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum Phase {
    Unknown,
    NotStarted,
    Syncing,
    Ready,
    Error,
    SourceUnavailable,
}

impl Phase {
    pub fn is_settled(self) -> bool {
        matches!(self, Phase::Ready | Phase::Error | Phase::SourceUnavailable)
    }

    fn is_indexed(self) -> bool {
        matches!(self, Phase::Ready | Phase::SourceUnavailable)
    }
}

pub fn combine_phases(phases: impl IntoIterator<Item = Phase>) -> Option<Phase> {
    let phases: Vec<Phase> = phases.into_iter().collect();
    let all = |phase: Phase| phases.iter().all(|each| *each == phase);

    if phases.is_empty() {
        None
    } else if phases.contains(&Phase::Unknown) {
        Some(Phase::Unknown)
    } else if all(Phase::SourceUnavailable) {
        Some(Phase::SourceUnavailable)
    } else if phases.iter().all(|each| each.is_indexed()) {
        Some(Phase::Ready)
    } else if phases.iter().all(|each| each.is_settled()) {
        Some(Phase::Error)
    } else if all(Phase::NotStarted) {
        Some(Phase::NotStarted)
    } else {
        Some(Phase::Syncing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_unavailable_folds_like_ready_unless_everything_is_unavailable() {
        use Phase::*;
        assert_eq!(combine_phases([SourceUnavailable]), Some(SourceUnavailable));
        assert_eq!(combine_phases([SourceUnavailable, Ready]), Some(Ready));
        assert_eq!(combine_phases([SourceUnavailable, Error]), Some(Error));
        assert_eq!(combine_phases([SourceUnavailable, Syncing]), Some(Syncing));
        assert_eq!(
            combine_phases([SourceUnavailable, NotStarted]),
            Some(Syncing)
        );
        assert_eq!(combine_phases([SourceUnavailable, Unknown]), Some(Unknown));
    }
}
