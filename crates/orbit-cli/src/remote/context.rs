use std::sync::LazyLock;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError};

pub(crate) async fn fetch(client: OrbitClient, tokens: &[String]) -> Result<Vec<u8>, RemoteError> {
    let tasks: Vec<_> = tokens
        .iter()
        .map(|token| {
            let (client, token) = (client.clone(), token.clone());
            tokio::spawn(async move { fetch_one(client, token).await })
        })
        .collect();
    let mut out = Vec::new();
    for task in tasks {
        let mut body = task
            .await
            .map_err(|error| RemoteError::new(EXIT_GENERIC, error.to_string()))??;
        if !body.ends_with(b"\n") {
            body.push(b'\n');
        }
        out.extend(body);
    }
    Ok(out)
}

async fn fetch_one(client: OrbitClient, token: String) -> Result<Vec<u8>, RemoteError> {
    let body = serde_json::to_vec(&serde_json::json!({
        "tokens": [token],
        "response_format": "llm",
        "expand": true,
    }))
    .map_err(|error| RemoteError::new(EXIT_GENERIC, error.to_string()))?;
    client
        .post_context(body)
        .await
        .map_err(|error| match error.exit_code {
            EXIT_UNAVAILABLE => RemoteError::new(
                EXIT_UNAVAILABLE,
                "Orbit context endpoint not available\n\n\
                 `POST /api/v4/orbit/context` returned 404. This GitLab instance may not\n\
                 serve entity context yet, or Orbit is not enabled for you.",
            ),
            _ => error,
        })
}

static TRACE_TIMESTAMP: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^(\s*\| (?:\d+: )?)\d{4}-\d\d-\d\dT[\d:.]+Z \d+[OE]\+? ?")
        .expect("valid pattern")
});

static TRACE_LINE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^\s*\| (?:\d+: )?(.*)$").expect("valid pattern"));

static GRAPH_KEY: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^(query_type|nodes|edges|pagination)\b").expect("valid pattern")
});

pub(crate) fn tidy(text: &str, drop_failed_jobs: bool) -> String {
    let mut out = String::with_capacity(text.len());
    let (mut in_graph, mut in_failed_jobs) = (false, false);
    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        if in_graph && (indent > 0 || GRAPH_KEY.is_match(line)) {
            continue;
        }
        in_graph = false;
        if in_failed_jobs && (indent > 2 || line.trim().is_empty()) {
            continue;
        }
        in_failed_jobs = false;
        if line.starts_with("orbit_context version=") {
            continue;
        }
        if line == "graph:" {
            in_graph = true;
            continue;
        }
        if drop_failed_jobs && indent == 2 && line[indent..].starts_with("failed_jobs[") {
            in_failed_jobs = true;
            continue;
        }
        let line = TRACE_TIMESTAMP.replace(line, "$1");
        if TRACE_LINE
            .captures(&line)
            .is_some_and(|found| super::sdlc::EPILOGUE.is_match(&found[1]))
        {
            continue;
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

pub(crate) fn shorten(text: &str, project: &str) -> String {
    text.replace(&format!("mr:{project}!"), "mr:")
        .replace(&format!("issue:{project}#"), "issue:")
}

const COMPACT_DESCRIPTION_LINES: usize = 30;
const COMPACT_NOTES: usize = 15;
const COMPACT_NOTE_LINES: usize = 10;

pub(crate) fn compact(text: &str) -> (String, bool) {
    #[derive(PartialEq)]
    enum Block {
        Other,
        Description,
        Discussions,
    }
    let mut out = String::with_capacity(text.len());
    let (mut block, mut kept, mut notes, mut trimmed) = (Block::Other, 0, 0, false);
    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        let header = indent <= 2 && !line.trim().is_empty();
        if header {
            block = match line.trim_start() {
                "description:" => Block::Description,
                rest if rest.starts_with("discussions[") => Block::Discussions,
                _ => Block::Other,
            };
            (kept, notes) = (0, 0);
        } else if block == Block::Discussions && indent == 4 {
            notes += 1;
            kept = 0;
        }
        let keep = match block {
            _ if header => true,
            Block::Other => true,
            Block::Description => {
                kept += 1;
                kept <= COMPACT_DESCRIPTION_LINES
            }
            Block::Discussions if indent == 4 => notes <= COMPACT_NOTES,
            Block::Discussions => {
                kept += 1;
                notes <= COMPACT_NOTES && kept <= COMPACT_NOTE_LINES
            }
        };
        trimmed |= !keep;
        if keep {
            out.push_str(line);
            out.push('\n');
        }
    }
    (out, trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tidy_drops_the_graph_timestamps_and_repeated_failed_jobs() {
        let text = "orbit_context version=1.1.0 entities=1\n\
            - mr:1 found=true\n  title=\"Fix\"\n  failed_jobs[1]:\n    lint (test) url\n      \
            | 100: 2026-10-17T17:26:38.950589Z 01O x.go:1: bad\n  discussions[0]:\n\
            graph:\nquery_type: neighbors\nnodes:\n  MergeRequest[1]{id}:\n    100\n\
            pagination:\n  has_more: false\n- issue:2 found=true\n";
        assert_eq!(
            tidy(text, false),
            "- mr:1 found=true\n  title=\"Fix\"\n  failed_jobs[1]:\n    lint (test) url\n      \
             | 100: x.go:1: bad\n  discussions[0]:\n- issue:2 found=true\n"
        );
        assert_eq!(
            tidy(text, true),
            "- mr:1 found=true\n  title=\"Fix\"\n  discussions[0]:\n- issue:2 found=true\n"
        );
    }

    #[test]
    fn compact_keeps_the_start_of_descriptions_and_discussions() {
        let mut text = String::from("- issue:1 found=true\n  title=\"T\"\n  description:\n");
        text.extend((0..40).map(|n| format!("    d{n}\n")));
        text.push_str("  discussions[2]:\n    @a 2026-01-01:\n");
        text.extend((0..12).map(|n| format!("      a{n}\n")));
        text.push_str("    @b 2026-01-02:\n      b0\n");
        let (out, trimmed) = compact(&text);
        assert!(trimmed);
        assert!(out.contains("    d29\n") && !out.contains("    d30\n"));
        assert!(out.contains("      a9\n") && !out.contains("      a10\n"));
        assert!(out.ends_with("    @b 2026-01-02:\n      b0\n"));
    }
}
