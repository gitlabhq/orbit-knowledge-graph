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

        /// Skip webpack asset compilation.
        ///
        /// The base CNG image's pre-built webpack assets are kept unchanged.
        /// Use this for faster Ruby-only rebuilds when JS hasn't changed.
        #[arg(long)]
        skip_webpack: bool,

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
    /// Run E2E tests against the running environment (~10s).
    ///
    /// Copies test scripts to the toolbox pod and runs the redaction test
    /// suite. Assumes setup --gkg has been run.
    Test,
    /// Rebuild images and restart pods.
    ///
    /// Use --gkg to rebuild the GKG server image and rollout restart (~2-3min).
    /// Use --rails to rebuild CNG images from GITLAB_SRC and helm upgrade (~5-8min).
    /// Both flags can be combined. At least one is required.
    /// Migrations and test data persist across restarts.
    Rebuild {
        /// Rebuild the GKG server image and rollout restart all GKG deployments.
        #[arg(long)]
        gkg: bool,

        /// Rebuild Rails CNG images from GITLAB_SRC and helm upgrade GitLab.
        #[arg(long)]
        rails: bool,

        /// Skip webpack asset compilation during Rails rebuild.
        #[arg(long)]
        skip_webpack: bool,
    },
    /// Port-forward GitLab and GKG services to localhost.
    ///
    /// Runs in the foreground. Ctrl+C to stop.
    Serve,
    /// Generate Ruby test scripts from scenarios.yaml.
    ///
    /// Reads e2e/tests/scenarios.yaml and generates create_test_data.rb
    /// and redaction_test.rb. Use --check to verify committed files match
    /// (for CI).
    Codegen {
        /// Verify generated files match without writing (for CI).
        #[arg(long)]
        check: bool,
    },
    /// Tear down the E2E environment.
    ///
    /// By default, tears down everything (GKG + GitLab + Traefik + Colima).
    /// Use --gkg-only to tear down just the GKG stack, keeping GitLab
    /// and Colima running.
    /// Use --keep-colima to remove everything except the Colima VM.
    Teardown {
        /// Keep the Colima VM running (only remove GitLab + Traefik).
        #[arg(long)]
        keep_colima: bool,

        /// Only tear down GKG resources. Keeps GitLab and Colima running.
        #[arg(long)]
        gkg_only: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let sh = Shell::new()?;

    match cli.command {
        Command::E2e { command } => {
            // Codegen is purely local — skip preflight tool checks.
            if let E2eCommand::Codegen { check } = command {
                let root = e2e::env::workspace_root();
                return e2e::codegen::run(&root, check);
            }

            for tool in e2e::constants::REQUIRED_TOOLS {
                if !e2e::cmd::exists(&sh, tool) {
                    anyhow::bail!("{tool} not found on PATH");
                }
            }

            match command {
                E2eCommand::Setup {
                    skip_build,
                    skip_webpack,
                    cng,
                    cng_setup,
                    gkg,
                    gkg_only,
                } => {
                    let cfg = e2e::config::Config::load()?;

                    let explicit = cng || cng_setup || gkg || gkg_only;
                    let run_cng = !explicit || cng || gkg;
                    let run_cng_setup = !explicit || cng_setup || gkg;
                    let run_gkg_stack = gkg || gkg_only;

                    if run_cng {
                        e2e::pipeline::cng::run(&sh, &cfg, skip_build, skip_webpack).await?;
                    }
                    if run_cng_setup {
                        e2e::pipeline::cngsetup::run(&cfg).await?;
                    }
                    if run_gkg_stack {
                        e2e::pipeline::gkg::run(&sh, &cfg).await?;
                    }

                    Ok(())
                }
                E2eCommand::Test => {
                    let cfg = e2e::config::Config::load()?;
                    e2e::pipeline::test::run(&cfg).await
                }
                E2eCommand::Serve => {
                    let cfg = e2e::config::Config::load()?;
                    e2e::pipeline::serve::run(&cfg).await
                }
                E2eCommand::Rebuild {
                    gkg,
                    rails,
                    skip_webpack,
                } => {
                    if !gkg && !rails {
                        anyhow::bail!("rebuild requires at least one of --gkg or --rails");
                    }
                    let cfg = e2e::config::Config::load()?;
                    e2e::pipeline::rebuild::run(&sh, &cfg, gkg, rails, skip_webpack).await
                }
                E2eCommand::Codegen { .. } => unreachable!("handled above"),
                E2eCommand::Teardown {
                    keep_colima,
                    gkg_only,
                } => {
                    let cfg = e2e::config::Config::load()?;
                    e2e::pipeline::teardown::run(&sh, &cfg, keep_colima, gkg_only).await
                }
            }
        }
    }
}
