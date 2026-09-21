use jobs::{CampaignKind, JobKind, JobState};

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
fn job_state_ranks_rise_from_pending_to_succeeded() {
    let ranks: Vec<u64> = JobState::ALL.iter().map(|state| state.rank()).collect();

    assert_eq!(ranks, [0, 1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(JobState::ALL[0], JobState::Pending);
    assert_eq!(JobState::ALL[7], JobState::Succeeded);
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
        [JobState::Failed, JobState::Skipped, JobState::Succeeded]
    );
}
