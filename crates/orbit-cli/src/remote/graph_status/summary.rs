use serde::{Deserialize, Deserializer};

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
    #[serde(default)]
    areas: Vec<Area>,
    projects: Option<Projects>,
}

#[derive(Deserialize)]
struct Area {
    name: String,
    state: Option<IndexingState>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Projects {
    indexed: usize,
    total_known: usize,
    #[serde(default)]
    gaps: usize,
}

#[derive(Deserialize)]
struct ItemCounts {
    #[serde(default)]
    domains: Vec<Domain>,
}

#[derive(Deserialize)]
struct Domain {
    name: String,
    #[serde(default)]
    entities: Vec<Entity>,
}

#[derive(Deserialize)]
struct Entity {
    name: String,
    #[serde(deserialize_with = "deserialize_big_int")]
    count: usize,
}

#[derive(Deserialize, Clone, Copy)]
enum IndexingState {
    #[serde(rename = "NOT_STARTED")]
    NotIndexed,
    #[serde(rename = "INDEXING")]
    Backfilling,
    #[serde(rename = "READY")]
    Indexed,
    #[serde(rename = "FAILED")]
    Error,
    #[serde(other)]
    Unknown,
}

fn deserialize_big_int<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    String::deserialize(deserializer)?
        .parse()
        .map_err(serde::de::Error::custom)
}

impl GraphStatus {
    pub(super) fn format_indexing(&self) -> String {
        let indexing = self.indexing.as_ref();
        let mut rows: Vec<(&str, String)> = indexing
            .into_iter()
            .flat_map(|indexing| &indexing.areas)
            .map(|area| (area.name.as_str(), label_state(area.state).to_string()))
            .collect();
        if rows.is_empty() {
            rows.push(("Indexing", self.get_overall_state().label().to_string()));
        }
        let projects = indexing.and_then(|indexing| indexing.projects.as_ref());
        rows.push((
            "Projects",
            projects.unwrap_or(&Projects::default()).describe_coverage(),
        ));
        tui::align_columns(rows.into_iter())
    }

    pub(super) fn format_contents(&self) -> String {
        let rows: Vec<(&str, String)> = self
            .item_counts
            .iter()
            .flat_map(|item_counts| &item_counts.domains)
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

fn label_state(state: Option<IndexingState>) -> &'static str {
    state.unwrap_or(IndexingState::Unknown).label()
}

impl Projects {
    fn describe_coverage(&self) -> String {
        if self.total_known == 0 {
            return "none yet".to_string();
        }
        let coverage = format!(
            "{} of {} indexed",
            tui::format_with_thousands(self.indexed),
            tui::format_with_thousands(self.total_known)
        );
        match self.gaps {
            0 => coverage,
            gaps => format!("{coverage} · {} failed", tui::format_with_thousands(gaps)),
        }
    }
}

impl Domain {
    fn describe_largest_entities(&self) -> Option<String> {
        let mut counted: Vec<&Entity> = self
            .entities
            .iter()
            .filter(|entity| entity.count > 0)
            .collect();
        counted.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.name.cmp(&b.name)));

        let mut parts: Vec<String> = counted
            .iter()
            .take(SHOWN_ENTITIES_PER_DOMAIN)
            .map(|entity| {
                format!(
                    "{} {}",
                    entity.name,
                    tui::format_with_thousands(entity.count)
                )
            })
            .collect();
        let hidden = counted.len().saturating_sub(SHOWN_ENTITIES_PER_DOMAIN);
        if hidden > 0 {
            parts.push(format!("{hidden} more"));
        }
        (!parts.is_empty()).then(|| parts.join(" · "))
    }
}

impl IndexingState {
    fn label(self) -> &'static str {
        match self {
            IndexingState::NotIndexed => "not indexed",
            IndexingState::Backfilling => "backfilling",
            IndexingState::Indexed => "indexed",
            IndexingState::Error => "error",
            IndexingState::Unknown => "unknown",
        }
    }

    fn describe_next_step(self) -> &'static str {
        match self {
            IndexingState::Indexed => "Ready.",
            IndexingState::Backfilling => "Indexing. Counts grow as data arrives.",
            IndexingState::NotIndexed => {
                "Not indexed yet. If this does not change, check that Orbit is enabled in Orbit > Configuration."
            }
            IndexingState::Error => {
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
             source_code   backfilling\n\
             Projects      40 of 1,200 indexed · 3 failed"
        );
        assert_eq!(
            status.describe_next_step(),
            "Indexing. Counts grow as data arrives."
        );
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
