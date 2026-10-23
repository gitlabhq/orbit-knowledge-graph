use serde::Deserialize;

use crate::tui;

const SHOWN_ENTITIES_PER_DOMAIN: usize = 2;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GraphStatus {
    indexing: Option<Indexing>,
    item_counts: Option<ItemCounts>,
}

#[derive(Deserialize)]
struct Indexing {
    state: Option<IndexingState>,
    areas: Option<Vec<Area>>,
    projects: Option<Projects>,
}

#[derive(Deserialize)]
struct Area {
    name: String,
    state: Option<IndexingState>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Projects {
    indexed: Option<usize>,
    total_known: Option<usize>,
    gaps: Option<usize>,
}

#[derive(Deserialize)]
struct ItemCounts {
    domains: Option<Vec<Domain>>,
}

#[derive(Deserialize)]
struct Domain {
    name: String,
    entities: Option<Vec<Entity>>,
}

#[derive(Deserialize)]
struct Entity {
    name: String,
    count: Option<String>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum IndexingState {
    NotStarted,
    Indexing,
    Ready,
    Failed,
    #[serde(other)]
    Unknown,
}

impl GraphStatus {
    pub(super) fn format_indexing(&self) -> String {
        let indexing = self.indexing.as_ref();
        let mut rows: Vec<(&str, String)> = indexing
            .and_then(|indexing| indexing.areas.as_deref())
            .unwrap_or_default()
            .iter()
            .map(|area| {
                let state = area.state.unwrap_or(IndexingState::Unknown);
                (area.name.as_str(), state.label().to_string())
            })
            .collect();
        if rows.is_empty() {
            rows.push(("Indexing", self.get_overall_state().label().to_string()));
        }
        let projects = indexing.and_then(|indexing| indexing.projects.as_ref());
        rows.push(("Projects", describe_coverage(projects)));
        tui::align_columns(rows.into_iter())
    }

    pub(super) fn format_contents(&self) -> String {
        let rows: Vec<(&str, String)> = self
            .item_counts
            .as_ref()
            .and_then(|item_counts| item_counts.domains.as_deref())
            .unwrap_or_default()
            .iter()
            .filter_map(|domain| Some((domain.name.as_str(), domain.describe_largest_entities()?)))
            .collect();
        if rows.is_empty() {
            return "nothing yet".to_string();
        }
        tui::align_columns(rows.into_iter())
    }

    pub(super) fn describe_next_step(&self) -> &'static str {
        self.get_overall_state().describe_next_step()
    }

    fn get_overall_state(&self) -> IndexingState {
        self.indexing
            .as_ref()
            .and_then(|indexing| indexing.state)
            .unwrap_or(IndexingState::Unknown)
    }
}

fn describe_coverage(projects: Option<&Projects>) -> String {
    let count = |field: fn(&Projects) -> Option<usize>| projects.and_then(field).unwrap_or(0);
    let (indexed, total_known, gaps) = (
        count(|p| p.indexed),
        count(|p| p.total_known),
        count(|p| p.gaps),
    );
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

impl Domain {
    fn describe_largest_entities(&self) -> Option<String> {
        let mut counted: Vec<(&str, usize)> = self
            .entities
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|entity| (entity.name.as_str(), entity.parse_count()))
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
}

impl Entity {
    fn parse_count(&self) -> usize {
        self.count
            .as_deref()
            .and_then(|count| count.parse().ok())
            .unwrap_or(0)
    }
}

impl IndexingState {
    fn label(self) -> &'static str {
        match self {
            IndexingState::NotStarted => "not indexed",
            IndexingState::Indexing => "indexing",
            IndexingState::Ready => "indexed",
            IndexingState::Failed => "error",
            IndexingState::Unknown => "unknown",
        }
    }

    fn describe_next_step(self) -> &'static str {
        match self {
            IndexingState::Ready => "Ready.",
            IndexingState::Indexing => "Indexing. Counts grow as data arrives.",
            IndexingState::NotStarted => {
                "Not indexed yet. If this does not change, check that Orbit is enabled in Orbit > Configuration."
            }
            IndexingState::Failed => {
                "Indexing has errors. If this does not clear, contact an instance administrator."
            }
            IndexingState::Unknown => "Indexing state unknown.",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_status(json: &str) -> GraphStatus {
        serde_json::from_str(json).expect("graph status JSON")
    }

    #[test]
    fn contents_hide_empty_entities_and_fold_the_smallest() {
        let status = parse_status(
            r#"{"itemCounts":{"domains":[
                {"name":"ci","entities":[
                    {"name":"Job","count":"514898"},{"name":"Stage","count":"118598"},
                    {"name":"Pipeline","count":"17864"},{"name":"Deployment","count":"0"}]},
                {"name":"plan","entities":[{"name":"Milestone","count":"0"}]}]}}"#,
        );

        assert_eq!(
            status.format_contents(),
            "ci   Job 514,898 · Stage 118,598 · 1 more"
        );
    }

    #[test]
    fn indexing_shows_each_area_with_failed_projects() {
        let status = parse_status(
            r#"{"indexing":{"state":"INDEXING",
                "areas":[{"name":"ci","state":"READY"},{"name":"source_code","state":"INDEXING"}],
                "projects":{"indexed":40,"totalKnown":1200,"gaps":3}}}"#,
        );

        assert_eq!(
            status.format_indexing(),
            "ci            indexed\n\
             source_code   indexing\n\
             Projects      40 of 1,200 indexed · 3 failed"
        );
        assert_eq!(
            status.describe_next_step(),
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

        assert_eq!(
            status.format_indexing(),
            "ci         unknown\nProjects   none yet"
        );
        assert_eq!(status.format_contents(), "nothing yet");
    }

    #[test]
    fn indexing_falls_back_to_the_overall_state() {
        let status = parse_status(
            r#"{"indexing":{"state":"NOT_STARTED","projects":{"indexed":0,"totalKnown":0}}}"#,
        );

        assert_eq!(
            status.format_indexing(),
            "Indexing   not indexed\nProjects   none yet"
        );
    }
}
