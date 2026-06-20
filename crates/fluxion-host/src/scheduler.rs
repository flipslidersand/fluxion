use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use fluxion_core::{dag::Dag, state::JobStatus, workflow::{PermissionSet, Workflow}};
use tokio::sync::mpsc;

use crate::FluxionHost;

pub async fn run(wf: &Workflow, host: Arc<FluxionHost>) -> Result<()> {
    let dag = Dag::build(wf)?;
    let pad = wf.jobs.keys().map(|k| k.len()).max().unwrap_or(0);

    let mut statuses: HashMap<String, JobStatus> = wf
        .jobs
        .keys()
        .map(|k| (k.clone(), JobStatus::Pending))
        .collect();

    let (tx, mut rx) = mpsc::unbounded_channel::<JobEvent>();

    let roots = dag.roots();
    let mut in_flight = roots.len();
    for job_id in roots {
        print_running(&job_id, pad);
        launch(&job_id, wf, host.clone(), tx.clone());
        statuses.insert(job_id, JobStatus::Running);
    }

    let workflow_start = Instant::now();

    while in_flight > 0 {
        let Some(event) = rx.recv().await else { break };

        print_result(&event, pad);
        statuses.insert(event.job_id.clone(), event.status.clone());
        in_flight -= 1;

        if let JobStatus::Failed { reason, .. } = &event.status {
            eprintln!(
                "\nReason:\n  {}\n\nRetry:\n  fluxion retry <run-id> --from {}",
                reason, event.job_id
            );
            return Ok(());
        }

        for dep in dag.dependents.get(&event.job_id).into_iter().flatten() {
            let all_done = dag.deps[dep]
                .iter()
                .all(|d| matches!(statuses[d], JobStatus::Succeeded { .. }));
            if all_done {
                print_running(dep, pad);
                launch(dep, wf, host.clone(), tx.clone());
                statuses.insert(dep.clone(), JobStatus::Running);
                in_flight += 1;
            }
        }
    }

    let total = workflow_start.elapsed();
    let succeeded = statuses
        .values()
        .filter(|s| matches!(s, JobStatus::Succeeded { .. }))
        .count();
    println!(
        "\nCompleted {}/{} jobs in {:.2}s",
        succeeded,
        dag.topo_order.len(),
        total.as_secs_f64()
    );

    Ok(())
}

struct JobEvent {
    job_id: String,
    status: JobStatus,
}

fn launch(job_id: &str, wf: &Workflow, host: Arc<FluxionHost>, tx: mpsc::UnboundedSender<JobEvent>) {
    let job_id = job_id.to_string();
    let component = wf.jobs[&job_id].component.clone();
    let input = wf.jobs[&job_id].input.clone().unwrap_or_default().into_bytes();
    let perms = wf.jobs[&job_id].permissions.clone();
    let timeout_secs = perms.limits.timeout_secs;

    tokio::spawn(async move {
        let start = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            tokio::task::spawn_blocking(move || host.run_component(&component, input, &perms)),
        )
        .await;

        let elapsed = start.elapsed();
        let status = match result {
            Err(_) => JobStatus::Failed {
                elapsed,
                reason: format!("Timeout after {}s", timeout_secs),
            },
            Ok(Ok(Ok(_))) => JobStatus::Succeeded { elapsed },
            Ok(Ok(Err(e))) => JobStatus::Failed { elapsed, reason: e.to_string() },
            Ok(Err(e)) => JobStatus::Failed { elapsed, reason: e.to_string() },
        };
        let _ = tx.send(JobEvent { job_id, status });
    });
}

fn timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

fn print_running(job_id: &str, pad: usize) {
    println!("[{}] {:<pad$}  RUNNING", timestamp(), job_id, pad = pad);
}

fn print_result(event: &JobEvent, pad: usize) {
    match &event.status {
        JobStatus::Succeeded { elapsed } => println!(
            "[{}] {:<pad$}  SUCCESS  {:.2}s",
            timestamp(),
            event.job_id,
            elapsed.as_secs_f64(),
            pad = pad
        ),
        JobStatus::Failed { elapsed, reason: _ } => println!(
            "[{}] {:<pad$}  FAILED   {:.2}s",
            timestamp(),
            event.job_id,
            elapsed.as_secs_f64(),
            pad = pad
        ),
        _ => {}
    }
}

// Keep PermissionSet in scope for the import
#[allow(unused_imports)]
use PermissionSet as _;
