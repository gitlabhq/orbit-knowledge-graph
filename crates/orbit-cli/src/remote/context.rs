use std::sync::LazyLock;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError};

pub(crate) async fn fetch(client: OrbitClient, tokens: &[String]) -> Result<Vec<u8>, RemoteError> {
    let tasks: Vec<_> = tokens
        .iter()
        .map(|token| {
            let (client, token) = (client.clone(), token.clone());
            let expand = expandable(&token);
            tokio::spawn(async move { request(client, token, "llm", expand).await })
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

/// Pipelines, branches, and commits (`path@ref`) have no graph expansion, so they are not asked for one.
fn expandable(token: &str) -> bool {
    !token.starts_with("Pipeline[") && !token.contains('@')
}

/// The first entity of a token's raw context, without expansion.
pub(crate) async fn fetch_entity(
    client: &OrbitClient,
    token: &str,
) -> Result<serde_json::Value, RemoteError> {
    let body = request(client.clone(), token.to_string(), "raw", false).await?;
    let response: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|error| RemoteError::new(EXIT_GENERIC, format!("unreadable response: {error}")))?;
    Ok(response["entities"][0].clone())
}

async fn request(
    client: OrbitClient,
    token: String,
    format: &str,
    expand: bool,
) -> Result<Vec<u8>, RemoteError> {
    let body = serde_json::to_vec(&serde_json::json!({
        "tokens": [token],
        "response_format": format,
        "expand": expand,
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

/// Drops HTML comments outside code fences, as rendered Markdown does, so PR templates and
/// bot markers in descriptions and notes cost nothing. A line left empty by a removed comment
/// goes, and so does a blank line after another blank line.
fn strip_html_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let (mut in_comment, mut fenced, mut blank) = (false, false, false);
    for line in text.lines() {
        if !in_comment && line.trim_start().starts_with("```") {
            fenced = !fenced;
        }
        let (mut kept, mut touched, mut rest) = (String::new(), in_comment, line);
        match fenced {
            true => kept.push_str(line),
            false => loop {
                match in_comment {
                    true => match rest.find("-->") {
                        Some(at) => (rest, in_comment) = (&rest[at + 3..], false),
                        None => break,
                    },
                    false => match rest.find("<!--") {
                        Some(at) => {
                            kept.push_str(&rest[..at]);
                            (rest, in_comment, touched) = (&rest[at + 4..], true, true);
                        }
                        None => {
                            kept.push_str(rest);
                            break;
                        }
                    },
                }
            },
        }
        let empty = kept.trim().is_empty();
        if empty && (touched || blank) {
            continue;
        }
        blank = empty;
        out.push_str(kept.trim_end());
        out.push('\n');
    }
    out
}

static EMPTY_GRAPH: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?m)^graph:\nquery_type: \S+\npagination:\n(?:  .*\n)*")
        .expect("valid pattern")
});

pub(crate) fn tidy(text: &str, drop_failed_jobs: bool) -> String {
    let text = strip_html_comments(text);
    let mut out = String::with_capacity(text.len());
    let mut in_failed_jobs = false;
    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        if in_failed_jobs && (indent > 2 || line.trim().is_empty()) {
            continue;
        }
        in_failed_jobs = false;
        if line.starts_with("orbit_context version=") {
            continue;
        }
        if drop_failed_jobs && indent == 2 && line[indent..].starts_with("failed_jobs[") {
            in_failed_jobs = true;
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    EMPTY_GRAPH.replace_all(&out, "").into_owned()
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
    fn only_types_with_a_graph_expansion_ask_for_one() {
        assert!(expandable("g/p!42"));
        assert!(expandable("Job[9]"));
        assert!(!expandable("Pipeline[7]"));
        assert!(!expandable("g/p@main"));
    }

    #[test]
    fn tidy_drops_html_comments_outside_code_fences() {
        let text = "  description:\n    Fixes a crash.\n    <!--\n    Thanks for your contribution!\n    -->\n    \n    \n    \
                    <!-- gh-issue-number: gh-1 -->\n    * Issue: gh-1 <!-- bot --> done\n    ```html\n    <!-- kept -->\n    ```\n";
        assert_eq!(
            tidy(text, false),
            "  description:\n    Fixes a crash.\n\n    * Issue: gh-1  done\n    ```html\n    <!-- kept -->\n    ```\n"
        );
    }

    #[test]
    fn tidy_drops_empty_graphs_timestamps_and_repeated_failed_jobs() {
        let text = "orbit_context version=1.1.0 entities=1\n- mr:1 found=true\n  failed_jobs[1]:\n    lint url\n\
                    graph:\nquery_type: neighbors\nnodes:\n  x\npagination:\n  truncated: false\n- issue:2 found=true\n\
                    graph:\nquery_type: neighbors\npagination:\n  has_more: false\n  truncated: false\n";
        assert_eq!(
            tidy(text, true),
            "- mr:1 found=true\ngraph:\nquery_type: neighbors\nnodes:\n  x\npagination:\n  truncated: false\n\
             - issue:2 found=true\n"
        );
    }
}
