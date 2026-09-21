use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use jobs::{CampaignId, CampaignKind, CampaignSummary, JobKind, JobState, PhaseSummary};

const TEST_KIND: JobKind = JobKind::new("test_kind");
const TEST_CAMPAIGN: CampaignKind = CampaignKind::new("test_campaign");

#[test]
fn const_kinds_expose_their_name() {
    assert_eq!(TEST_KIND.as_str(), "test_kind");
    assert_eq!(TEST_CAMPAIGN.as_str(), "test_campaign");
}

#[test]
fn parse_accepts_lowercase_names_with_digits_and_underscores() {
    for name in ["code", "namespace_data", "a", "k9", &"a".repeat(64)] {
        assert_eq!(JobKind::parse(name).unwrap().as_str(), name);
    }
}

#[test]
fn parse_rejects_names_outside_the_grammar() {
    for name in ["", "Code", "1abc", "a-b", "a b", "a.b", &"a".repeat(65)] {
        assert!(JobKind::parse(name).is_err(), "{name:?} was accepted");
        assert!(CampaignKind::parse(name).is_err(), "{name:?} was accepted");
    }
}

#[test]
fn kinds_serialize_as_bare_strings_and_reject_invalid_input() {
    let json = serde_json::to_string(&TEST_KIND).unwrap();
    assert_eq!(json, "\"test_kind\"");

    let parsed: JobKind = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, TEST_KIND);

    assert!(serde_json::from_str::<JobKind>("\"Bad-Name\"").is_err());
}

#[test]
fn job_state_ranks_increase_in_declared_order() {
    let ranks: Vec<u64> = JobState::ALL.iter().map(|state| state.rank()).collect();
    let mut sorted = ranks.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(ranks, sorted);
    assert_eq!(JobState::Pending.rank(), 0);
    assert_eq!(JobState::Succeeded.rank(), 7);
}

#[test]
fn job_state_names_round_trip() {
    for state in JobState::ALL {
        assert_eq!(JobState::parse(state.as_str()).unwrap(), state);
    }
    assert!(JobState::parse("done").is_err());
}

#[test]
fn terminal_states_are_failed_skipped_and_succeeded() {
    let terminal: Vec<JobState> = JobState::ALL
        .into_iter()
        .filter(|state| state.is_terminal())
        .collect();
    assert_eq!(
        terminal,
        vec![JobState::Failed, JobState::Skipped, JobState::Succeeded]
    );
}

fn phase(required: bool, discovery_closed: bool, counts: &[(JobState, u64)]) -> PhaseSummary {
    PhaseSummary {
        kind: TEST_KIND,
        required,
        discovery_closed,
        abandoned: false,
        counts_by_state: BTreeMap::from_iter(counts.iter().copied()),
    }
}

fn campaign(phases: Vec<PhaseSummary>) -> CampaignSummary {
    CampaignSummary {
        id: CampaignId {
            kind: TEST_CAMPAIGN,
            subject: "42".into(),
            generation: Utc.with_ymd_and_hms(2026, 9, 21, 0, 0, 0).unwrap(),
        },
        phases,
    }
}

#[test]
fn campaign_is_complete_when_every_phase_is_closed_and_terminal() {
    let summary = campaign(vec![
        phase(
            true,
            true,
            &[(JobState::Succeeded, 3), (JobState::Failed, 1)],
        ),
        phase(false, true, &[(JobState::Skipped, 2)]),
    ]);
    assert!(summary.is_complete());
    assert!(summary.is_ready());
    assert!(!summary.is_abandoned());
    assert_eq!(summary.count(JobState::Failed), 1);
    assert_eq!(summary.total(), 6);
}

#[test]
fn campaign_with_open_discovery_is_not_complete_even_with_no_jobs() {
    let summary = campaign(vec![phase(true, false, &[])]);
    assert!(!summary.is_complete());
    assert!(!summary.is_ready());
}

#[test]
fn campaign_is_ready_when_only_optional_phases_are_unfinished() {
    let summary = campaign(vec![
        phase(true, true, &[(JobState::Succeeded, 5)]),
        phase(false, true, &[(JobState::Running, 1)]),
    ]);
    assert!(summary.is_ready());
    assert!(!summary.is_complete());
}

#[test]
fn campaign_with_a_pending_job_in_a_closed_phase_is_not_complete() {
    let summary = campaign(vec![phase(
        true,
        true,
        &[(JobState::Succeeded, 9), (JobState::Pending, 1)],
    )]);
    assert!(!summary.is_complete());
}

#[test]
fn campaign_is_abandoned_when_any_phase_is_abandoned() {
    let mut abandoned = phase(true, false, &[]);
    abandoned.abandoned = true;
    let summary = campaign(vec![
        phase(true, true, &[(JobState::Succeeded, 1)]),
        abandoned,
    ]);
    assert!(summary.is_abandoned());
    assert!(!summary.is_complete());
}
