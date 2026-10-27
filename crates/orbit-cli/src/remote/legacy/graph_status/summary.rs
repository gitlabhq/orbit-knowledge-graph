use serde::Deserialize;

use crate::tui;

const SHOWN_ENTITIES_PER_DOMAIN: usize = 2;

#[derive(Deserialize)]
pub(super) struct GraphStatus {
    #[serde(default)]
    projects: Projects,
    #[serde(default)]
    domains: Vec<Domain>,
    indexing: Option<IndexingStatus>,
    sdlc_indexing: Option<IndexingStatus>,
    code_indexing: Option<IndexingStatus>,
}

#[derive(Deserialize, Default)]
struct Projects {
    indexed: usize,
    total_known: usize,
    #[serde(default)]
    gaps: usize,
}

#[derive(Deserialize)]
struct Domain {
    name: String,
    items: Vec<Entity>,
}

#[derive(Deserialize)]
struct Entity {
    name: String,
    count: usize,
}

#[derive(Deserialize)]
struct IndexingStatus {
    state: IndexingState,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum IndexingState {
    NotIndexed,
    Backfilling,
    Indexing,
    Indexed,
    Error,
    #[serde(other)]
    Unknown,
}

impl GraphStatus {
    pub(super) fn format_indexing(&self) -> String {
        let mut rows: Vec<(&str, String)> = [
            ("SDLC data", &self.sdlc_indexing),
            ("Code", &self.code_indexing),
        ]
        .into_iter()
        .filter_map(|(label, status)| Some((label, status.as_ref()?.state.label().to_string())))
        .collect();
        if rows.is_empty() {
            rows.push(("Indexing", self.get_overall_state().label().to_string()));
        }
        rows.push(("Projects", self.projects.describe_coverage()));
        tui::align_columns(rows.into_iter())
    }

    pub(super) fn format_contents(&self) -> String {
        let rows: Vec<(&str, String)> = self
            .domains
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
            .map_or(IndexingState::Unknown, |status| status.state)
    }
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
            .items
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
            IndexingState::Indexing => "indexing",
            IndexingState::Indexed => "indexed",
            IndexingState::Error => "error",
            IndexingState::Unknown => "unknown",
        }
    }

    fn describe_next_step(self) -> &'static str {
        match self {
            IndexingState::Indexed => "Ready.",
            IndexingState::Backfilling | IndexingState::Indexing => {
                "Indexing. Counts grow as data arrives."
            }
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
            r#"{"domains":[
                {"name":"ci","items":[
                    {"name":"Job","count":514898},{"name":"Stage","count":118598},
                    {"name":"Pipeline","count":17864},{"name":"Deployment","count":0}]},
                {"name":"plan","items":[{"name":"Milestone","count":0}]}]}"#,
        );

        assert_eq!(
            status.format_contents(),
            "ci   Job 514,898 · Stage 118,598 · 1 more"
        );
    }

    #[test]
    fn indexing_shows_sdlc_and_code_separately_with_failed_projects() {
        let status = parse_status(
            r#"{"projects":{"indexed":40,"total_known":1200,"gaps":3},
                "indexing":{"state":"backfilling"},
                "sdlc_indexing":{"state":"indexed"},
                "code_indexing":{"state":"backfilling"}}"#,
        );

        assert_eq!(
            status.format_indexing(),
            "SDLC data   indexed\n\
             Code        backfilling\n\
             Projects    40 of 1,200 indexed · 3 failed"
        );
        assert_eq!(
            status.describe_next_step(),
            "Indexing. Counts grow as data arrives."
        );
    }

    #[test]
    fn indexing_falls_back_to_the_overall_state() {
        let status = parse_status(
            r#"{"projects":{"indexed":0,"total_known":0},"indexing":{"state":"not_indexed"}}"#,
        );

        assert_eq!(
            status.format_indexing(),
            "Indexing   not indexed\nProjects   none yet"
        );
    }
}
