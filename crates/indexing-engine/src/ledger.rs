use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobSummary {
    status: JobStatus,
    tasks: u32,
    rows: BTreeMap<&'static str, u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JobStatus {
    Running,
    Done,
    Failed,
}

impl JobSummary {
    pub(crate) fn new(status: JobStatus, tasks: u32, rows: BTreeMap<&'static str, u64>) -> Self {
        Self {
            status,
            tasks,
            rows,
        }
    }

    pub fn is_running(&self) -> bool {
        self.status == JobStatus::Running
    }

    pub fn is_done(&self) -> bool {
        self.status == JobStatus::Done
    }

    pub fn is_failed(&self) -> bool {
        self.status == JobStatus::Failed
    }

    pub fn count_tasks(&self) -> u32 {
        self.tasks
    }

    pub fn count_rows(&self, label: &str) -> u64 {
        self.rows.get(label).copied().unwrap_or(0)
    }
}
