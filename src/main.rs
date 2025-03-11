use clap::{Parser, Subcommand};
use env_logger;
use indoc::indoc;
use log::{error, info, warn};
use simd_r_drive::AppendStorage;
use std::path::PathBuf;

/// Append-Only Storage Engine CLI
#[derive(Parser)]
#[command(
    name = env!("CARGO_PKG_NAME"),
    version = env!("CARGO_PKG_VERSION"),
    about = env!("CARGO_PKG_DESCRIPTION"),
    long_about = None
)]
#[command(
    // TODO: Document `storage` (open `help` and view `storage` section)

    // TODO: string-replace-all with binary name
    after_help = indoc! {r#"
    Examples:
      simd-r-drive data.bin write mykey "Hello, world!"
      simd-r-drive data.bin read mykey
      simd-r-drive data.bin delete mykey
    "#}
)]
struct Cli {
    #[arg(value_name = "storage")]
    storage: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Read the value associated with a key
    Read {
        /// The key to read
        key: String,
    },

    /// Write a value for a given key
    Write {
        /// The key to write
        key: String,
        /// The value to store
        value: String,
    },

    /// Delete a key
    Delete {
        /// The key to delete
        key: String,
    },

    /// Compact the storage file to remove old entries
    Compact,
}

// TODO: Enable stdin to write
// TODO: Try "nesting" the .bin by reading it in and saving it as a key (does it work?)
fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Read { key } => {
            let storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
            match storage.get_entry_by_key(key.as_bytes()) {
                Some(value) => println!("{}", String::from_utf8_lossy(value)),
                None => {
                    error!("Error: Key '{}' not found", key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Write { key, value } => {
            let mut storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
            storage
                .append_entry(key.as_bytes(), value.as_bytes())
                .expect("Failed to write entry");
            info!("Stored '{}' -> '{}'", key, value);
        }

        Commands::Delete { key } => {
            let mut storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
            storage
                .delete_entry(key.as_bytes())
                .expect("Failed to delete entry");
            warn!("Deleted key '{}'", key);
        }

        Commands::Compact => {
            let mut storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
            info!("Starting compaction...");
            if let Err(e) = storage.compact() {
                error!("Compaction failed: {}", e);
                std::process::exit(1);
            }
            info!("Compaction completed successfully.");
        }
    }
}
