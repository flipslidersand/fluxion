mod telemetry;

use anyhow::Result;
use clap::{Parser, Subcommand};
use fluxion_core::{
    store::RunStore,
    workflow::{PermissionSet, Workflow},
};
use fluxion_host::{scheduler, FluxionHost};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "fluxion", about = "Safe Wasm-based job execution engine")]
struct Cli {
    /// Emit OpenTelemetry spans to stdout
    #[arg(long, global = true)]
    trace: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a YAML workflow (DAG of Wasm components)
    Run {
        /// Path to the workflow YAML file
        path: String,
    },
    /// Retry a previous run from a specific job
    Retry {
        /// Run ID from the previous execution
        run_id: String,
        /// Re-execute this job and all its downstream dependents
        #[arg(long)]
        from: String,
    },
    /// Execute a single Wasm component
    Component {
        #[command(subcommand)]
        action: ComponentCommands,
    },
    /// Manage run history
    Runs {
        #[command(subcommand)]
        action: RunsCommands,
    },
}

#[derive(Subcommand)]
enum ComponentCommands {
    /// Run a Wasm component with optional input
    Run {
        /// Path to the .wasm component file
        path: String,
        #[arg(long, default_value = "")]
        input: String,
    },
}

#[derive(Subcommand)]
enum RunsCommands {
    /// List recent runs
    List {
        #[arg(long, default_value = "20")]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let provider = telemetry::init(cli.trace);

    let result = run(cli.command).await;

    telemetry::shutdown(provider);
    result
}

async fn run(command: Commands) -> Result<()> {
    match command {
        Commands::Run { path } => {
            let wf = Workflow::from_file(&path)
                .map_err(|e| anyhow::anyhow!("Failed to load '{}': {}", path, e))?;
            let workflow_path = PathBuf::from(&path).canonicalize().unwrap_or(PathBuf::from(&path));
            let host = Arc::new(FluxionHost::new()?);
            scheduler::run(&wf, &workflow_path, host).await?;
        }

        Commands::Retry { run_id, from } => {
            let store = RunStore::open()?;
            let (workflow_path, _) = store.load_run(&run_id)?;
            let wf = Workflow::from_file(&workflow_path)
                .map_err(|e| anyhow::anyhow!("Failed to load workflow from '{}': {}", workflow_path, e))?;
            let wp = PathBuf::from(&workflow_path);
            let host = Arc::new(FluxionHost::new()?);
            scheduler::retry(&wf, &wp, host, &run_id, &from).await?;
        }

        Commands::Component { action } => match action {
            ComponentCommands::Run { path, input } => {
                let host = FluxionHost::new()?;
                let output = host
                    .run_component(&path, input.into_bytes(), &PermissionSet::default())
                    .map_err(|e| anyhow::anyhow!("Failed to run '{}': {}", path, e))?;
                println!("{}", String::from_utf8_lossy(&output));
            }
        },

        Commands::Runs { action } => match action {
            RunsCommands::List { limit } => {
                let store = RunStore::open()?;
                let runs = store.list_runs(limit)?;
                if runs.is_empty() {
                    println!("No runs found.");
                } else {
                    println!("{:<28}  {:<20}  {}", "RUN ID", "WORKFLOW", "STATUS");
                    println!("{}", "-".repeat(60));
                    for r in runs {
                        println!("{:<28}  {:<20}  {}", r.id, r.workflow_name, r.status);
                    }
                }
            }
        },
    }

    Ok(())
}
