use orbit_cli_graphql::queries::graph_status::{
    OrbitIndexingProjects, OrbitIndexingState, OrbitItemCountDomain, OrbitItemCountEntity,
    OrbitStatus,
};

use crate::tui;

const SHOWN_ENTITIES_PER_DOMAIN: usize = 2;

pub(super) struct Summary<'a> {
    status: &'a OrbitStatus,
}

impl<'a> Summary<'a> {
    pub(super) fn new(status: &'a OrbitStatus) -> Self {
        Self { status }
    }

    pub(super) fn format_indexing(&self) -> String {
        let indexing = self.status.indexing.as_ref();
        let mut rows: Vec<(&str, String)> = indexing
            .and_then(|indexing| indexing.areas.as_deref())
            .unwrap_or_default()
            .iter()
            .map(|area| (area.name.as_str(), label(area.state).to_string()))
            .collect();
        if rows.is_empty() {
            rows.push(("Indexing", label(self.get_overall_state()).to_string()));
        }
        let projects = indexing.and_then(|indexing| indexing.projects.as_ref());
        rows.push(("Projects", describe_coverage(projects)));
        tui::align_columns(rows.into_iter())
    }

    pub(super) fn format_contents(&self) -> String {
        let rows: Vec<(&str, String)> = self
            .status
            .item_counts
            .as_ref()
            .and_then(|item_counts| item_counts.domains.as_deref())
            .unwrap_or_default()
            .iter()
            .filter_map(|domain| Some((domain.name.as_str(), describe_largest_entities(domain)?)))
            .collect();
        if rows.is_empty() {
            return "nothing yet".to_string();
        }
        tui::align_columns(rows.into_iter())
    }

    pub(super) fn describe_next_step(&self) -> &'static str {
        match self.get_overall_state() {
            Some(OrbitIndexingState::Ready) => "Ready.",
            Some(OrbitIndexingState::Indexing) => "Indexing. Counts grow as data arrives.",
            Some(OrbitIndexingState::NotStarted) => {
                "Not indexed yet. If this does not change, check that Orbit is enabled in Orbit > Configuration."
            }
            Some(OrbitIndexingState::Failed) => {
                "Indexing has errors. If this does not clear, contact an instance administrator."
            }
            Some(OrbitIndexingState::Unknown) | None => "Indexing state unknown.",
        }
    }

    fn get_overall_state(&self) -> Option<OrbitIndexingState> {
        self.status.indexing.as_ref()?.state
    }
}

fn label(state: Option<OrbitIndexingState>) -> &'static str {
    match state {
        Some(OrbitIndexingState::NotStarted) => "not indexed",
        Some(OrbitIndexingState::Indexing) => "indexing",
        Some(OrbitIndexingState::Ready) => "indexed",
        Some(OrbitIndexingState::Failed) => "error",
        Some(OrbitIndexingState::Unknown) | None => "unknown",
    }
}

fn describe_coverage(projects: Option<&OrbitIndexingProjects>) -> String {
    let indexed = to_count(projects.and_then(|projects| projects.indexed));
    let total_known = to_count(projects.and_then(|projects| projects.total_known));
    let gaps = to_count(projects.and_then(|projects| projects.gaps));
    if total_known == 0 {
        return "none yet".to_string();
    }
    let coverage = format!(
        "{} of {} indexed",
        tui::format_with_thousands(indexed),
        tui::format_with_thousands(total_known)
    );
    match gaps {
        0 => coverage,
        gaps => format!("{coverage} · {} failed", tui::format_with_thousands(gaps)),
    }
}

fn describe_largest_entities(domain: &OrbitItemCountDomain) -> Option<String> {
    let mut counted: Vec<(&str, usize)> = domain
        .entities
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|entity| (entity.name.as_str(), parse_count(entity)))
        .filter(|(_, count)| *count > 0)
        .collect();
    counted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

    let mut parts: Vec<String> = counted
        .iter()
        .take(SHOWN_ENTITIES_PER_DOMAIN)
        .map(|(name, count)| format!("{name} {}", tui::format_with_thousands(*count)))
        .collect();
    let hidden = counted.len().saturating_sub(SHOWN_ENTITIES_PER_DOMAIN);
    if hidden > 0 {
        parts.push(format!("{hidden} more"));
    }
    (!parts.is_empty()).then(|| parts.join(" · "))
}

fn to_count(count: Option<i32>) -> usize {
    count
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or_default()
}

fn parse_count(entity: &OrbitItemCountEntity) -> usize {
    entity
        .count
        .as_ref()
        .and_then(|count| count.0.parse().ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_status(json: &str) -> OrbitStatus {
        serde_json::from_str(json).expect("graph status JSON")
    }

    #[test]
    fn contents_hide_empty_entities_and_fold_the_smallest() {
        let status = parse_status(
            r#"{"indexing":null,"itemCounts":{"domains":[
                {"name":"ci","entities":[
                    {"name":"Job","count":"514898"},{"name":"Stage","count":"118598"},
                    {"name":"Pipeline","count":"17864"},{"name":"Deployment","count":"0"}]},
                {"name":"plan","entities":[{"name":"Milestone","count":"0"}]}]}}"#,
        );

        assert_eq!(
            Summary::new(&status).format_contents(),
            "ci   Job 514,898 · Stage 118,598 · 1 more"
        );
    }

    #[test]
    fn indexing_shows_each_area_with_failed_projects() {
        let status = parse_status(
            r#"{"itemCounts":null,"indexing":{"state":"INDEXING",
                "areas":[{"name":"ci","state":"READY"},{"name":"source_code","state":"INDEXING"}],
                "projects":{"indexed":40,"totalKnown":1200,"gaps":3}}}"#,
        );
        let summary = Summary::new(&status);

        assert_eq!(
            summary.format_indexing(),
            "ci            indexed\n\
             source_code   indexing\n\
             Projects      40 of 1,200 indexed · 3 failed"
        );
        assert_eq!(
            summary.describe_next_step(),
            "Indexing. Counts grow as data arrives."
        );
    }

    #[test]
    fn null_leaves_read_as_unknown_or_zero() {
        let status = parse_status(
            r#"{"indexing":{"state":null,"areas":[{"name":"ci","state":null}],"projects":null},
                "itemCounts":{"domains":[{"name":"ci","entities":[{"name":"Job","count":null}]},
                {"name":"plan","entities":null}]}}"#,
        );
        let summary = Summary::new(&status);

        assert_eq!(
            summary.format_indexing(),
            "ci         unknown\nProjects   none yet"
        );
        assert_eq!(summary.format_contents(), "nothing yet");
    }

    #[test]
    fn indexing_falls_back_to_the_overall_state() {
        let status = parse_status(
            r#"{"itemCounts":null,"indexing":{"state":"NOT_STARTED","areas":null,
                "projects":{"indexed":0,"totalKnown":0,"gaps":null}}}"#,
        );

        assert_eq!(
            Summary::new(&status).format_indexing(),
            "Indexing   not indexed\nProjects   none yet"
        );
    }
}
