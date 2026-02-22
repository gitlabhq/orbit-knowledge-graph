use anyhow::Result;
use clap::{Parser, Subcommand};

/// GKG development task runner.
///
/// Automates common development workflows like E2E environment setup,
/// image building, and test execution.
#[derive(Parser)]
#[command(name = "xtask", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// E2E environment management.
    E2e {
        #[command(subcommand)]
        command: E2eCommand,
    },
}

#[derive(Subcommand)]
enum E2eCommand {
    /// Set up the full E2E environment (cluster, GitLab, GKG stack).
    Setup,
    /// Tear down the E2E environment.
    Teardown,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::E2e { command } => match command {
            E2eCommand::Setup => {
                println!("e2e setup: not yet implemented");
                Ok(())
            }
            E2eCommand::Teardown => {
                println!("e2e teardown: not yet implemented");
                Ok(())
            }
        },
    }
}
