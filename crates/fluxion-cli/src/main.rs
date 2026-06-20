use anyhow::Result;
use clap::{Parser, Subcommand};
use fluxion_host::FluxionHost;

#[derive(Parser)]
#[command(name = "fluxion", about = "Safe Wasm-based job execution engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
        /// Input data (UTF-8 string, passed as bytes)
        #[arg(long, default_value = "")]
        input: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Component { action } => match action {
            ComponentCommands::Run { path, input } => {
                let host = FluxionHost::new()?;
                let output = host.run_component(&path, input.into_bytes())?;
                println!("{}", String::from_utf8_lossy(&output));
            }
        },
    }

    Ok(())
}
