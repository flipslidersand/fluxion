use anyhow::Result;
use clap::{Parser, Subcommand};
use fluxion_core::workflow::{PermissionSet, Workflow};
use fluxion_host::{scheduler, FluxionHost};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "fluxion", about = "Safe Wasm-based job execution engine")]
struct Cli {
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
    /// Execute a single Wasm component
    Component {
        #[command(subcommand)]
        action: ComponentCommands,
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { path } => {
            let wf = Workflow::from_file(&path)
                .map_err(|e| anyhow::anyhow!("Failed to load '{}': {}", path, e))?;
            let host = Arc::new(FluxionHost::new()?);
            scheduler::run(&wf, host).await?;
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
    }

    Ok(())
}
