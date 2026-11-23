use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::Path;

use indexing_engine_next::task::TaskId;
use indexing_engine_next::testkit::simulation::MessageState;

use super::{Cluster, Holder};

pub type StateId = u128;

#[derive(Default)]
pub struct StateGraph {
    pub initial: HashSet<StateId>,
    pub states: HashSet<StateId>,
    pub steps: HashSet<(StateId, StateId)>,
}

pub fn state_id(canonical: &str) -> StateId {
    let half = |salt: u8| {
        let mut hasher = DefaultHasher::new();
        salt.hash(&mut hasher);
        canonical.hash(&mut hasher);
        hasher.finish()
    };
    (u128::from(half(1)) << 64) | u128::from(half(2))
}

pub fn canonical(lines: impl IntoIterator<Item = String>) -> String {
    let mut lines: Vec<String> = lines
        .into_iter()
        .map(|line| line.chars().filter(|c| !c.is_whitespace()).collect())
        .collect();
    lines.sort();
    lines.join("\n")
}

pub(super) fn state_lines(cluster: &Cluster) -> Vec<String> {
    let holder_name = |h: Option<usize>| h.map_or("NoPod".to_string(), |h| format!("p{}", h + 1));
    let per_holder = |value: &dyn Fn(&Holder) -> String| {
        let entries: Vec<String> = cluster
            .holders
            .iter()
            .map(|holder| format!("{} :> {}", holder.id.as_str(), value(holder)))
            .collect();
        format!("({})", entries.join(" @@ "))
    };
    let ids = |tasks: &mut dyn Iterator<Item = &TaskId>| {
        let ids: Vec<String> = tasks.map(|task| spec_id(*task).to_string()).collect();
        format!("{{{}}}", ids.join(", "))
    };

    let (holder, expires_at) = match cluster.lease.get() {
        Some(lease) => (
            lease.holder.as_str().to_string(),
            cluster.clock.ticks_at(lease.expires_at),
        ),
        None => ("NoPod".to_string(), 0),
    };
    let copies: Vec<String> = cluster
        .broker
        .messages()
        .iter()
        .map(|message| {
            let state = match message.state {
                MessageState::Ready => "ready",
                MessageState::Inflight => "inflight",
                MessageState::Acknowledged => "acked",
                MessageState::Lost => "lost",
            };
            format!(
                "[id |-> {}, state |-> \"{state}\", deadline |-> {}, pod |-> {}]",
                spec_id(message.task),
                message.deadline,
                holder_name(message.consumer)
            )
        })
        .collect();
    let planner = match cluster.publishing {
        Some(task) => format!("[id |-> {}, phase |-> \"publishing\"]", spec_id(task)),
        None => "[id |-> 0, phase |-> \"idle\"]".to_string(),
    };
    let mut cover: Vec<String> = cluster
        .covers
        .iter()
        .map(|task| spec_id(*task).to_string())
        .collect();
    cover.resize(cluster.bounds.max_events, "0".to_string());
    let fence = cluster.database.highest_fence();

    let values = BTreeMap::from([
        ("now", cluster.clock.ticks().to_string()),
        ("offset", per_holder(&|holder| holder.skew.to_string())),
        (
            "lease",
            format!(
                "[holder |-> {holder}, expiresAt |-> {expires_at}, rev |-> {}]",
                cluster.lease.revision().0
            ),
        ),
        (
            "marker",
            cluster
                .pending
                .get()
                .copied()
                .map_or(0, spec_id)
                .to_string(),
        ),
        ("nextId", cluster.next_id.to_string()),
        ("copies", format!("<<{}>>", copies.join(", "))),
        ("planner", planner),
        ("submitted", cluster.covers.len().to_string()),
        ("cover", format!("<<{}>>", cover.join(", "))),
        ("started", ids(&mut cluster.started.iter())),
        (
            "pc",
            per_holder(&|holder| format!("\"{}\"", holder.step.name())),
        ),
        (
            "held",
            per_holder(&|holder| holder.held.map_or(0, |c| c + 1).to_string()),
        ),
        (
            "token",
            per_holder(&|holder| {
                format!(
                    "<<{}, {}>>",
                    holder.last_hold.token_generation, holder.last_hold.token
                )
            }),
        ),
        (
            "seenRev",
            per_holder(&|holder| holder.last_hold.seen.to_string()),
        ),
        (
            "expiry",
            per_holder(&|holder| holder.last_hold.expiry.to_string()),
        ),
        (
            "renewAt",
            per_holder(&|holder| holder.last_hold.renew_at.to_string()),
        ),
        (
            "stalled",
            per_holder(&|holder| tla_bool(holder.stalled).to_string()),
        ),
        ("stallsLeft", cluster.faults.stalls.to_string()),
        ("crashesLeft", cluster.faults.crashes.to_string()),
        (
            "storeFence",
            format!("<<{}, {}>>", fence.generation.0, fence.revision.0),
        ),
        ("staleWrite", "FALSE".to_string()),
        (
            "rejectedCurrent",
            tla_bool(cluster.rejected_current).to_string(),
        ),
        ("generation", cluster.database.generation().0.to_string()),
        (
            "natsGeneration",
            cluster
                .generation
                .get()
                .map_or(0, |generation| generation.0)
                .to_string(),
        ),
        ("wipesLeft", cluster.faults.wipes.to_string()),
        ("duplicatesLeft", cluster.faults.duplicates.to_string()),
        ("lossesLeft", cluster.faults.losses.to_string()),
        ("lostInWipe", ids(&mut cluster.lost_in_wipe.iter())),
    ]);

    values
        .into_iter()
        .map(|(name, value)| format!("/\\ {name} = {value}"))
        .collect()
}

fn spec_id(task: TaskId) -> u128 {
    task.as_uuid().as_u128()
}

fn tla_bool(value: bool) -> &'static str {
    if value { "TRUE" } else { "FALSE" }
}

pub struct SpecGraph {
    pub graph: StateGraph,
    pub labels: HashMap<StateId, String>,
}

pub fn read_spec_graph(path: &Path, keep_labels_of: &HashSet<StateId>) -> SpecGraph {
    let file = File::open(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let mut graph = StateGraph::default();
    let mut labels = HashMap::new();
    let mut ids_by_tlc_node: HashMap<i64, StateId> = HashMap::new();
    let mut steps_by_tlc_node: Vec<(i64, i64)> = Vec::new();

    for line in BufReader::new(file).lines() {
        let line = line.expect("a readable TLC graph");
        let Some((node, rest)) = line.split_once(' ') else {
            continue;
        };
        let Ok(node) = node.parse::<i64>() else {
            continue;
        };
        if let Some(target) = rest.strip_prefix("-> ") {
            let target = target.split_once(' ').map_or(target, |(id, _)| id);
            if let Ok(target) = target.parse::<i64>() {
                steps_by_tlc_node.push((node, target));
            }
            continue;
        }
        let Some(label) = rest
            .strip_prefix("[label=\"")
            .and_then(|label| label.rsplit_once('"').map(|(label, _)| label))
        else {
            continue;
        };
        let state = canonical(tlc_label_lines(label));
        let id = state_id(&state);
        if rest.ends_with("style = filled]") {
            graph.initial.insert(id);
        }
        if keep_labels_of.contains(&id) {
            labels.insert(id, state);
        }
        graph.states.insert(id);
        ids_by_tlc_node.insert(node, id);
    }

    for (from, to) in steps_by_tlc_node {
        graph
            .steps
            .insert((ids_by_tlc_node[&from], ids_by_tlc_node[&to]));
    }
    SpecGraph { graph, labels }
}

fn tlc_label_lines(label: &str) -> Vec<String> {
    let mut text = String::with_capacity(label.len());
    let mut chars = label.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            text.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => text.push('\n'),
            Some(other) => text.push(other),
            None => text.push('\\'),
        }
    }

    let mut lines: Vec<String> = Vec::new();
    for line in text.lines() {
        match lines.last_mut() {
            Some(last) if !line.trim_start().starts_with("/\\") => last.push_str(line),
            _ => lines.push(line.to_string()),
        }
    }
    lines
}
