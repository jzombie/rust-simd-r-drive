use clap::{CommandFactory, Parser, Subcommand};
use simd_r_drive::AppendStorage;
use std::path::PathBuf;

/// Append-Only Storage Engine CLI
#[derive(Parser)]
#[command(
    name = "simd-r-drive",
    version = "1.0",
    about = "Append-Only Storage Engine"
)]
#[command(disable_help_flag = true)] // We will manually format help output
struct Cli {
    /// Path to the storage file (e.g., data.bin)
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
}

fn print_custom_help() {
    println!(
        r#"Append-Only Storage Engine CLI

A simple key-value store with an append-only storage engine. Data is stored in a binary file and can be read, written, or deleted.

Usage:
    simd-r-drive <storage> <command> [arguments]

Commands:
    read    <key>           Read the value associated with a key
    write   <key> <value>   Write a value for a given key
    delete  <key>           Delete a key

Arguments:
    <storage>    Path to the storage file (e.g., data.bin)

Examples:
    simd-r-drive data.bin write mykey "Hello, world!"
    simd-r-drive data.bin read mykey
    simd-r-drive data.bin delete mykey

Options:
    -h, --help      Show this help message and exit
    -V, --version   Show version information
"#
    );
}

fn main() {
    let cli = Cli::try_parse();

    match cli {
        Ok(cli) => match &cli.command {
            Commands::Read { key } => {
                let storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
                match storage.get_entry_by_key(key.as_bytes()) {
                    Some(value) => println!("{}", String::from_utf8_lossy(value)),
                    None => eprintln!("Error: Key '{}' not found", key),
                }
            }
            Commands::Write { key, value } => {
                let mut storage =
                    AppendStorage::open(&cli.storage).expect("Failed to open storage");
                storage
                    .append_entry(key.as_bytes(), value.as_bytes())
                    .expect("Failed to write entry");
                println!("Stored '{}' -> '{}'", key, value);
            }
            Commands::Delete { key } => {
                let mut storage =
                    AppendStorage::open(&cli.storage).expect("Failed to open storage");
                storage
                    .delete_entry(key.as_bytes())
                    .expect("Failed to delete entry");
                println!("Deleted key '{}'", key);
            }
        },
        Err(_) => {
            print_custom_help();
            std::process::exit(1);
        }
    }
}
