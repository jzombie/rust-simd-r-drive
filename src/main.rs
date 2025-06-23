use clap::Parser;
use simd_r_drive::*;
mod cli;
use cli::{Cli, execute_command};

fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let cli = Cli::parse();

    execute_command(&cli);
}
