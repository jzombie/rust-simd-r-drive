use crate::cli::{Commands, HELP_TEMPLATE};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
   // Obtain during build time, not runtime
    name = env!("CARGO_PKG_NAME"),
    version = env!("CARGO_PKG_VERSION"),
    about = env!("CARGO_PKG_DESCRIPTION"),
    long_about = None
)]
#[command(    
    after_help = HELP_TEMPLATE.replace("%BINARY_NAME%", env!("CARGO_PKG_NAME"))
)]
pub struct Cli {
    /// The file where data is stored (automatically created if it does not exist).
    #[arg(
        value_name = "storage",
        help = "Path to the storage file. If the file does not exist, it will be created automatically."
    )]
    pub storage: PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}
