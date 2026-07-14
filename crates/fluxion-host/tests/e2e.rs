use std::path::PathBuf;
use std::sync::Arc;

use fluxion_core::workflow::Workflow;
use fluxion_host::{scheduler, FluxionHost};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn wasm(component: &str) -> String {
    let bin = component.replace('-', "_");
    workspace_root()
        .join("components")
        .join(component)
        .join(format!("target/wasm32-wasip1/debug/{bin}.wasm"))
        .to_string_lossy()
        .into_owned()
}

// Load a YAML workflow and patch all component paths to absolute.
fn load_wf(yaml: &str, component: &str) -> (Workflow, PathBuf) {
    let wf_path = workspace_root().join("examples").join(yaml);
    let mut wf = Workflow::from_file(&wf_path).expect("load yaml");
    let abs = wasm(component);
    for job in wf.jobs.values_mut() {
        job.component = abs.clone();
    }
    (wf, wf_path)
}

// Patch filesystem paths in vehicle-pipeline to use a custom tmp dir.
fn patch_pipeline_paths(wf: &mut Workflow, data_dir: &str, out_dir: &str) {
    for job in wf.jobs.values_mut() {
        if let Some(input) = &job.input {
            job.input = Some(
                input
                    .replace("/tmp/fluxion-pipeline", data_dir)
                    .replace("/tmp/fluxion-output", out_dir),
            );
        }
        for p in job.permissions.filesystem.read.iter_mut() {
            let s = p.to_string_lossy().into_owned();
            *p = PathBuf::from(s.replace("/tmp/fluxion-pipeline", data_dir));
        }
        for p in job.permissions.filesystem.write.iter_mut() {
            let s = p.to_string_lossy().into_owned();
            *p = PathBuf::from(
                s.replace("/tmp/fluxion-pipeline", data_dir)
                    .replace("/tmp/fluxion-output", out_dir),
            );
        }
    }
}

/// vehicle-pipeline: first run fails at validate (row 184 has year=1999),
/// then we fix normalized.csv and retry from validate — expect full success.
#[tokio::test]
#[ignore = "requires pre-built Wasm components"]
async fn vehicle_pipeline_validate_retry() {
    let data_dir = format!("/tmp/fluxion-e2e-{}-pipeline", std::process::id());
    let out_dir = format!("/tmp/fluxion-e2e-{}-output", std::process::id());
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();

    let host = Arc::new(FluxionHost::new().unwrap());
    let (mut wf, wf_path) = load_wf("vehicle-pipeline.yaml", "pipeline-stage");
    patch_pipeline_paths(&mut wf, &data_dir, &out_dir);

    // First run: fetch and normalize succeed, validate fails (year=1999 in row 184).
    let r1 = scheduler::run_silent(&wf, &wf_path, host.clone())
        .await
        .unwrap();
    assert!(!r1.success, "first run should fail at validate");
    let failed = r1.jobs.iter().find(|j| j.status == "failed").expect("failed job");
    assert_eq!(failed.job_id, "validate");
    assert!(
        failed.reason.as_deref().unwrap_or("").contains("1999"),
        "reason should mention the bad year"
    );

    // Fix: replace the invalid year in normalized.csv.
    let norm = format!("{}/normalized.csv", data_dir);
    let content = std::fs::read_to_string(&norm).unwrap();
    std::fs::write(&norm, content.replacen(",1999,", ",2019,", 1)).unwrap();

    // Retry from validate: only validate + export re-run.
    let r2 = scheduler::retry_silent(&wf, &wf_path, host, &r1.run_id, "validate")
        .await
        .unwrap();
    assert!(r2.success, "retry should succeed after fixing the year: {:?}", r2);

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}

/// resource-limits-demo: spin-forever is killed by epoch timeout after 2s;
/// fast-sum (short run) is expected to have been in-flight or succeeded.
#[tokio::test]
#[ignore = "requires pre-built Wasm components"]
async fn resource_limits_spin_timeout() {
    let host = Arc::new(FluxionHost::new().unwrap());
    let (wf, wf_path) = load_wf("resource-limits-demo.yaml", "spin");

    let result = scheduler::run_silent(&wf, &wf_path, host).await.unwrap();

    assert!(!result.success, "workflow should fail due to spin-forever timeout");
    let failed = result
        .jobs
        .iter()
        .find(|j| j.status == "failed")
        .expect("at least one failed job");
    assert_eq!(failed.job_id, "spin-forever");
    // Elapsed should be close to the 2s timeout, not several minutes.
    assert!(
        failed.elapsed_ms < 5_000,
        "epoch interruption should kill the job well under 5s, got {}ms",
        failed.elapsed_ms
    );
}

/// memory-limits-demo: ok-job (alloc 1MB within 16MB limit) succeeds,
/// then oom-job (alloc 10MB within 1MB limit) is rejected by StoreLimits.
#[tokio::test]
#[ignore = "requires pre-built Wasm components"]
async fn memory_limits_oom_enforcement() {
    let host = Arc::new(FluxionHost::new().unwrap());
    let (wf, wf_path) = load_wf("memory-limits-demo.yaml", "alloc-bomb");

    let result = scheduler::run_silent(&wf, &wf_path, host).await.unwrap();

    assert!(!result.success, "workflow should fail due to oom-job OOM");

    let ok = result
        .jobs
        .iter()
        .find(|j| j.job_id == "ok-job")
        .expect("ok-job result");
    assert_eq!(ok.status, "succeeded", "ok-job should succeed (1MB within 16MB limit)");

    let oom = result
        .jobs
        .iter()
        .find(|j| j.job_id == "oom-job")
        .expect("oom-job result");
    assert_eq!(oom.status, "failed", "oom-job should fail (10MB exceeds 1MB limit)");
    assert!(
        oom.reason.as_deref().unwrap_or("").to_lowercase().contains("oom")
            || oom.reason.as_deref().unwrap_or("").to_lowercase().contains("memory"),
        "failure reason should mention OOM/memory: {:?}",
        oom.reason
    );
}
