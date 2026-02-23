use anyhow::Result;
use clap::{Parser, Subcommand};
use xshell::Shell;

mod e2e;

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
    ///
    /// By default, runs CNG + CNG-setup (cluster deploy then configure).
    /// Use --cng or --cng-setup to run a single phase.
    /// Use --gkg to run all phases including GKG stack.
    /// Use --gkg-only to run just the GKG stack (phases 1+2 must be done).
    Setup {
        /// Skip building CNG images (use previously built images).
        #[arg(long)]
        skip_build: bool,

        /// Run only CNG deploy (cluster + GitLab).
        #[arg(long)]
        cng: bool,

        /// Run only CNG setup (PG creds, JWT, migrate, test data).
        #[arg(long)]
        cng_setup: bool,

        /// Run all phases including GKG stack (ClickHouse + schema + tests).
        #[arg(long)]
        gkg: bool,

        /// Run only the GKG stack (steps 15-25). Assumes phases 1+2 are done.
        #[arg(long)]
        gkg_only: bool,
    },
    /// Tear down the E2E environment.
    ///
    /// By default, tears down everything (GKG + GitLab + Traefik + Colima).
    /// Use --gkg-only to tear down just the GKG/Tilt stack, keeping GitLab
    /// and Colima running (equivalent to the harness's --tilt-only).
    /// Use --keep-colima to remove everything except the Colima VM.
    Teardown {
        /// Keep the Colima VM running (only remove GitLab + Traefik).
        #[arg(long)]
        keep_colima: bool,

        /// Only tear down GKG/Tilt resources. Keeps GitLab and Colima running.
        #[arg(long)]
        gkg_only: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sh = Shell::new()?;

    match cli.command {
        Command::E2e { command } => match command {
            E2eCommand::Setup {
                skip_build,
                cng,
                cng_setup,
                gkg,
                gkg_only,
            } => {
                let cfg = e2e::config::Config::from_env();

                // Resolve which phases to run.
                // --gkg implies all. --gkg-only runs just phase 3.
                // Individual flags run just that phase.
                // No flags = CNG + CNG-setup (default).
                let explicit = cng || cng_setup || gkg || gkg_only;
                let run_cng = !explicit || cng || gkg;
                let run_cng_setup = !explicit || cng_setup || gkg;
                let run_gkg_stack = gkg || gkg_only;

                if run_cng {
                    e2e::pipeline::cng::run(&sh, &cfg, skip_build)?;
                }
                if run_cng_setup {
                    e2e::pipeline::cngsetup::run(&sh, &cfg)?;
                }
                if run_gkg_stack {
                    e2e::pipeline::gkg::run(&sh, &cfg)?;
                }

                Ok(())
            }
            E2eCommand::Teardown {
                keep_colima,
                gkg_only,
            } => {
                let cfg = e2e::config::Config::from_env();
                e2e::teardown::run(&sh, &cfg, keep_colima, gkg_only)
            }
        },
    }
}
