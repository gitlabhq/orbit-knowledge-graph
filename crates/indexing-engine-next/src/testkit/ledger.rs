use std::collections::BTreeMap;

use uuid::Uuid;

use crate::lane::Lane;
use crate::ledger::{JobStatus, JobSummary};
use crate::payload::{Key, Tenant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskSummary {
    pub key: Key,
    pub lane: Lane,
    pub attempts: u16,
    pub error_class: Option<&'static str>,
    pub error_message: Option<String>,
}

#[derive(Default)]
pub(super) struct MemoryLedger {
    jobs: Vec<JobRecord>,
    tasks: Vec<(&'static str, TaskSummary)>,
}

struct JobRecord {
    job_id: Uuid,
    tenant: Tenant,
    worker: &'static str,
    key: Key,
    open_tasks: u32,
    tasks: u32,
    failed: bool,
    rows: BTreeMap<&'static str, u64>,
}

impl MemoryLedger {
    pub(super) fn open_job(&mut self, tenant: &Tenant, worker: &'static str, key: &Key) -> Uuid {
        let job_id = Uuid::now_v7();
        self.jobs.push(JobRecord {
            job_id,
            tenant: tenant.clone(),
            worker,
            key: key.clone(),
            open_tasks: 0,
            tasks: 0,
            failed: false,
            rows: BTreeMap::new(),
        });
        job_id
    }

    pub(super) fn add_task(&mut self, job_id: Uuid) {
        if let Some(job) = self.find_job(job_id) {
            job.open_tasks += 1;
            job.tasks += 1;
        }
    }

    pub(super) fn finish_task(&mut self, job_id: Uuid, rows: &BTreeMap<&'static str, u64>) {
        if let Some(job) = self.find_job(job_id) {
            job.open_tasks -= 1;
            for (label, count) in rows {
                *job.rows.entry(label).or_default() += count;
            }
        }
    }

    pub(super) fn fail_task(&mut self, job_id: Uuid) {
        if let Some(job) = self.find_job(job_id) {
            job.open_tasks -= 1;
            job.failed = true;
        }
    }

    pub(super) fn record_task(&mut self, worker: &'static str, task: TaskSummary) {
        self.tasks.push((worker, task));
    }

    pub(super) fn find_last_job(
        &self,
        tenant: &Tenant,
        worker: &str,
        key: &Key,
    ) -> Option<JobSummary> {
        self.jobs
            .iter()
            .rev()
            .find(|job| &job.tenant == tenant && job.worker == worker && &job.key == key)
            .map(JobRecord::summarize)
    }

    pub(super) fn find_tasks(&self, worker: &str) -> Vec<TaskSummary> {
        self.tasks
            .iter()
            .filter(|(task_worker, _)| *task_worker == worker)
            .map(|(_, task)| task.clone())
            .collect()
    }

    fn find_job(&mut self, job_id: Uuid) -> Option<&mut JobRecord> {
        self.jobs.iter_mut().find(|job| job.job_id == job_id)
    }
}

impl JobRecord {
    fn summarize(&self) -> JobSummary {
        let status = match (self.open_tasks, self.failed) {
            (0, false) => JobStatus::Done,
            (0, true) => JobStatus::Failed,
            _ => JobStatus::Running,
        };
        JobSummary::new(status, self.tasks, self.rows.clone())
    }
}
