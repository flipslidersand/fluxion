use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use fluxion_core::{
    dag::Dag,
    state::JobStatus,
    store::RunStore,
    workflow::Workflow,
};
use tokio::sync::mpsc;

use crate::FluxionHost;

/// Run a workflow from scratch.
pub async fn run(wf: &Workflow, workflow_path: &Path, host: Arc<FluxionHost>) -> Result<()> {
    let store = RunStore::open()?;
    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, &wf.name, workflow_path)?;
    println!("Run ID: {run_id}");

    let success = execute(wf, host, &store, &run_id, HashMap::new()).await?;
    store.complete_run(&run_id, success)?;
    Ok(())
}

/// Retry a previous run, re-executing `from_job` and all its downstream dependents.
pub async fn retry(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
) -> Result<()> {
    let store = RunStore::open()?;

    let (_, prev_states) = store.load_run(prev_run_id)?;
    let dag = Dag::build(wf)?;
    let replay_set = downstream_inclusive(&dag, from_job);

    // Jobs that succeeded before and are NOT being replayed → skip them
    let pre_succeeded: HashMap<String, JobStatus> = prev_states
        .into_iter()
        .filter(|(id, status)| {
            matches!(status, JobStatus::Succeeded { .. }) && !replay_set.contains(id.as_str())
        })
        .collect();

    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, &wf.name, workflow_path)?;
    println!(
        "Retry run ID: {run_id}  (from '{from_job}', skipping {} pre-succeeded jobs)",
        pre_succeeded.len()
    );

    let success = execute(wf, host, &store, &run_id, pre_succeeded).await?;
    store.complete_run(&run_id, success)?;
    Ok(())
}

/// Core execution loop. `pre_succeeded` jobs are treated as already done.
async fn execute(
    wf: &Workflow,
    host: Arc<FluxionHost>,
    store: &RunStore,
    run_id: &str,
    pre_succeeded: HashMap<String, JobStatus>,
) -> Result<bool> {
    let dag = Dag::build(wf)?;
    let pad = wf.jobs.keys().map(|k| k.len()).max().unwrap_or(0);

    let mut statuses: HashMap<String, JobStatus> = wf
        .jobs
        .keys()
        .map(|k| (k.clone(), JobStatus::Pending))
        .collect();

    // Seed pre-succeeded jobs
    for (id, status) in &pre_succeeded {
        store.upsert_job(run_id, id, status)?;
        statuses.insert(id.clone(), status.clone());
        if let JobStatus::Succeeded { elapsed } = status {
            println!(
                "[skip] {:<pad$}  SUCCESS  {:.2}s  (previous run)",
                id,
                elapsed.as_secs_f64(),
                pad = pad
            );
        }
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<JobEvent>();

    // Launch roots not already pre-succeeded
    let mut in_flight = 0usize;
    for job_id in dag.roots() {
        if pre_succeeded.contains_key(&job_id) {
            continue;
        }
        print_running(&job_id, pad);
        store.upsert_job(run_id, &job_id, &JobStatus::Running)?;
        launch(&job_id, wf, host.clone(), tx.clone());
        statuses.insert(job_id, JobStatus::Running);
        in_flight += 1;
    }

    // Unlock non-root jobs whose deps are all pre-succeeded
    for (job_id, job) in &wf.jobs {
        if pre_succeeded.contains_key(job_id) || dag.roots().contains(job_id) {
            continue;
        }
        if job.depends_on.iter().all(|d| pre_succeeded.contains_key(d)) {
            print_running(job_id, pad);
            store.upsert_job(run_id, job_id, &JobStatus::Running)?;
            launch(job_id, wf, host.clone(), tx.clone());
            statuses.insert(job_id.clone(), JobStatus::Running);
            in_flight += 1;
        }
    }

    let workflow_start = Instant::now();
    let mut overall_success = true;

    while in_flight > 0 {
        let Some(event) = rx.recv().await else { break };

        print_result(&event, pad);
        store.upsert_job(run_id, &event.job_id, &event.status)?;
        statuses.insert(event.job_id.clone(), event.status.clone());
        in_flight -= 1;

        if let JobStatus::Failed { reason, .. } = &event.status {
            overall_success = false;
            eprintln!(
                "\nReason:\n  {}\n\nRetry:\n  fluxion retry {} --from {}",
                reason, run_id, event.job_id
            );
            break;
        }

        for dep in dag.dependents.get(&event.job_id).into_iter().flatten() {
            if pre_succeeded.contains_key(dep) {
                continue;
            }
            let all_done = dag.deps[dep]
                .iter()
                .all(|d| matches!(statuses[d], JobStatus::Succeeded { .. }));
            if all_done {
                print_running(dep, pad);
                store.upsert_job(run_id, dep, &JobStatus::Running)?;
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

    Ok(overall_success)
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

/// Collect `start` and all jobs reachable downstream from it (inclusive).
fn downstream_inclusive<'a>(dag: &'a Dag, start: &'a str) -> std::collections::HashSet<&'a str> {
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(start);
    while let Some(node) = queue.pop_front() {
        if visited.insert(node) {
            for dep in dag.dependents.get(node).into_iter().flatten() {
                queue.push_back(dep.as_str());
            }
        }
    }
    visited
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
