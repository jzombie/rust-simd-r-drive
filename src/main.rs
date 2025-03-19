use clap::Parser;
use simd_r_drive::*;
mod cli;
use cli::{send_output, Cli};
mod utils;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    send_output(&cli);
}
