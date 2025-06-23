use crate::cli::HELP_TEMPLATE;
use clap::CommandFactory;
use clap::Parser;
use clap::error::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;

/// CLI for starting the SIMD R Drive RPC server
#[derive(Parser, Debug)]
#[command(
    name = env!("CARGO_PKG_NAME"),
    version = env!("CARGO_PKG_VERSION"),
    about = env!("CARGO_PKG_DESCRIPTION"),
    long_about = None,
    after_help = HELP_TEMPLATE.replace("%BINARY_NAME%", env!("CARGO_PKG_NAME"))
)]
pub struct Cli {
    /// Path to the storage file. If the file does not exist, it will be created automatically.
    #[arg(
        value_name = "storage",
        help = "The file where data is stored (created if it does not exist)."
    )]
    pub storage: PathBuf,

    /// Address to bind the RPC server to. Defaults to 127.0.0.1 with a random port.
    #[arg(
        long,
        value_name = "ADDR",
        default_value = "127.0.0.1:0",
        help = "Socket address to listen on, e.g. 127.0.0.1:7000"
    )]
    pub listen: SocketAddr,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::try_parse().unwrap_or_else(|e| {
            // If it's a missing argument error, show full help instead of short usage
            if e.kind() == ErrorKind::MissingRequiredArgument {
                let mut cmd = Cli::command();
                let full_help = crate::cli::HELP_TEMPLATE.replace("%BINARY_NAME%", cmd.get_name());
                cmd = cmd
                    .override_usage("<storage> [OPTIONS]")
                    .after_help(full_help);
                cmd.print_help().unwrap();
                println!();
                std::process::exit(1);
            } else {
                e.exit(); // All other errors remain unchanged
            }
        })
    }
}
