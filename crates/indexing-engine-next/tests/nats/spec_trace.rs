use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;

use indexing_engine_next::task::TaskId;
use indexing_engine_next::{EngineEvent, Observer, TaskStep};

const PLANNER: &str = "planner";
const SPEC_PODS: [&str; 2] = ["p1", "p2"];
const SPEC_TIME_UNIT: Duration = Duration::from_millis(500);

#[derive(Clone, Default)]
pub struct Journal(Arc<Mutex<Vec<Entry>>>);

pub struct ActorObserver {
    actor: &'static str,
    journal: Journal,
}

enum Entry {
    Step {
        at: SystemTime,
        actor: &'static str,
        key: String,
        task: TaskId,
        step: TaskStep,
    },
    Wipe {
        at: SystemTime,
    },
    Crash {
        at: SystemTime,
        actor: &'static str,
    },
}

struct KeyTrace {
    origin: SystemTime,
    wipes: usize,
    ids: BTreeMap<TaskId, usize>,
    actors: BTreeMap<&'static str, Vec<String>>,
}

impl Journal {
    pub fn observer_for(&self, actor: &'static str) -> ActorObserver {
        ActorObserver {
            actor,
            journal: self.clone(),
        }
    }

    pub fn record_wipe(&self) {
        self.0.lock().push(Entry::Wipe {
            at: SystemTime::now(),
        });
    }

    pub fn record_crash(&self, actor: &'static str) {
        self.0.lock().push(Entry::Crash {
            at: SystemTime::now(),
            actor,
        });
    }

    pub fn mark(&self) -> usize {
        self.0.lock().len()
    }

    pub async fn wait_until_keys_finish(&self, keys: usize, since: usize) {
        for _ in 0..300 {
            if self.list_finished_keys(since).len() == keys {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "only the keys {:?} finished",
            self.list_finished_keys(since)
        );
    }

    pub fn write_tla_traces(&self, folder: &Path) -> std::io::Result<()> {
        let entries = self.0.lock();
        let origin = entries
            .iter()
            .map(Entry::at)
            .min()
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let keys: BTreeSet<&String> = entries
            .iter()
            .filter_map(|entry| match entry {
                Entry::Step { key, .. } => Some(key),
                Entry::Wipe { .. } | Entry::Crash { .. } => None,
            })
            .collect();
        for (index, key) in keys.into_iter().enumerate() {
            let trace = KeyTrace::from_entries(&entries, key, origin);
            let key_folder = folder.join(format!("key{index}"));
            std::fs::create_dir_all(&key_folder)?;
            std::fs::write(key_folder.join("NatsTrace.tla"), trace.format_module())?;
        }
        Ok(())
    }

    fn list_finished_keys(&self, since: usize) -> BTreeSet<String> {
        self.0
            .lock()
            .iter()
            .skip(since)
            .filter_map(|entry| match entry {
                Entry::Step {
                    key,
                    step: TaskStep::Finished { .. },
                    ..
                } => Some(key.clone()),
                Entry::Step { .. } | Entry::Wipe { .. } | Entry::Crash { .. } => None,
            })
            .collect()
    }
}

impl Observer for ActorObserver {
    fn observe(&self, event: &EngineEvent) {
        if let EngineEvent::Task(event) = event {
            self.journal.0.lock().push(Entry::Step {
                at: event.at,
                actor: self.actor,
                key: event.task.key.to_string(),
                task: event.task.id,
                step: event.step.clone(),
            });
        }
    }
}

impl Entry {
    fn at(&self) -> SystemTime {
        match self {
            Entry::Step { at, .. } | Entry::Wipe { at } | Entry::Crash { at, .. } => *at,
        }
    }
}

impl KeyTrace {
    fn from_entries(entries: &[Entry], key: &str, origin: SystemTime) -> Self {
        let mut trace = Self {
            origin,
            wipes: 0,
            ids: BTreeMap::new(),
            actors: BTreeMap::new(),
        };
        for entry in entries {
            match entry {
                Entry::Step {
                    at,
                    actor,
                    key: entry_key,
                    task,
                    step,
                } if entry_key == key => trace.record_step(*at, actor, *task, step),
                Entry::Step { .. } => {}
                Entry::Wipe { at } => {
                    let t = trace.spec_time(*at);
                    trace.push(PLANNER, format!("[a |-> \"NatsWipe\", t |-> {t}"));
                    trace.wipes += 1;
                }
                Entry::Crash { at, actor } => {
                    let t = trace.spec_time(*at);
                    let pod = spec_pod(actor);
                    let observation =
                        format!("[a |-> \"Crash\", p |-> {}, t |-> {t}", pod.to_uppercase());
                    trace.push(pod, observation);
                }
            }
        }
        trace
    }

    fn record_step(&mut self, at: SystemTime, actor: &'static str, task: TaskId, step: &TaskStep) {
        if let TaskStep::Submitted {
            already_pending: false,
        } = step
        {
            let next = self.ids.len() + 1;
            self.ids.entry(task).or_insert(next);
        }
        let id = self.ids.get(&task).copied().unwrap_or(0);
        let action = match step {
            TaskStep::Submitted { .. } => "Submit",
            TaskStep::Published => "Publish",
            TaskStep::Delivered => "Deliver",
            TaskStep::LeaseAcquired(_) => "AcquireLease",
            TaskStep::GivenBack { .. } => "GiveBack",
            TaskStep::Started { .. } => "StartTask",
            TaskStep::Renewed(_) => "Renew",
            TaskStep::RenewalLost => "RenewLost",
            TaskStep::RenewalFailed(_) => "RenewFails",
            TaskStep::Finished { .. } => "Finish",
            TaskStep::Abandoned(_) => "Abandon",
            TaskStep::PortCallFailed(_) => return,
        };
        let t = self.spec_time(at);
        if actor == PLANNER {
            let observation = format!("[a |-> \"{action}\", id |-> {id}, t |-> {t}");
            self.push(PLANNER, observation);
        } else {
            let pod = spec_pod(actor);
            let observation = format!(
                "[a |-> \"{action}\", p |-> {}, id |-> {id}, t |-> {t}",
                pod.to_uppercase()
            );
            self.push(pod, observation);
        }
    }

    fn spec_time(&self, at: SystemTime) -> u128 {
        let elapsed = at.duration_since(self.origin).unwrap_or_default();
        elapsed.as_millis() / SPEC_TIME_UNIT.as_millis()
    }

    fn push(&mut self, actor: &'static str, observation: String) {
        let observation = format!("{observation}, w |-> {}]", self.wipes);
        self.actors.entry(actor).or_default().push(observation);
    }

    fn format_module(&self) -> String {
        let mut module =
            "---- MODULE NatsTrace ----\nCONSTANTS P1, P2\n\nObserved == [\n".to_owned();
        let actors: Vec<&str> = [PLANNER].into_iter().chain(SPEC_PODS).collect();
        for (index, actor) in actors.iter().enumerate() {
            let steps = self
                .actors
                .get(actor)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let separator = if index + 1 == actors.len() { "" } else { "," };
            let _ = writeln!(
                module,
                "  {actor} |-> <<\n    {}\n  >>{separator}",
                steps.join(",\n    ")
            );
        }
        module.push_str("]\n====\n");
        module
    }
}

fn spec_pod(actor: &str) -> &'static str {
    match actor {
        "p2" => "p2",
        _ => "p1",
    }
}
