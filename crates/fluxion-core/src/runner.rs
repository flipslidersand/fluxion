use std::time::Duration;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub run_id: String,
    pub workflow_name: String,
    pub jobs: Vec<JobResult>,
    pub total_elapsed_ms: u64,
    pub succeeded: usize,
    pub total: usize,
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct JobResult {
    pub job_id: String,
    pub status: String,
    pub elapsed_ms: u64,
    pub reason: Option<String>,
    pub skipped: bool,
}

impl RunResult {
    pub fn summary(&self) -> String {
        format!(
            "Run {} — {}/{} jobs succeeded in {:.2}s",
            self.run_id,
            self.succeeded,
            self.total,
            self.total_elapsed_ms as f64 / 1000.0
        )
    }
}

impl JobResult {
    pub fn from_succeeded(job_id: String, elapsed: Duration, skipped: bool) -> Self {
        Self {
            job_id,
            status: "succeeded".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: None,
            skipped,
        }
    }

    pub fn from_failed(job_id: String, elapsed: Duration, reason: String) -> Self {
        Self {
            job_id,
            status: "failed".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: Some(reason),
            skipped: false,
        }
    }
}
