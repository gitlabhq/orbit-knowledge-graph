#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Unknown,
    NotStarted,
    Syncing,
    Ready,
}

pub fn fold_phases(phases: impl IntoIterator<Item = Phase>) -> Option<Phase> {
    let phases: Vec<Phase> = phases.into_iter().collect();
    let all = |phase: Phase| phases.iter().all(|each| *each == phase);

    if phases.is_empty() {
        None
    } else if phases.contains(&Phase::Unknown) {
        Some(Phase::Unknown)
    } else if all(Phase::Ready) {
        Some(Phase::Ready)
    } else if all(Phase::NotStarted) {
        Some(Phase::NotStarted)
    } else {
        Some(Phase::Syncing)
    }
}
