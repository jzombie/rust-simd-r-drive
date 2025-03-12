use indoc::indoc;
use clap::{Parser, Subcommand};
use env_logger;
use log::{error, info, warn};
use simd_r_drive::AppendStorage;
mod format_bytes;
use format_bytes::format_bytes;
use std::path::PathBuf;
use stdin_nonblocking::get_stdin_or_default;
use std::io::{self, IsTerminal, Write};


// Help text template with placeholder
const HELP_TEMPLATE: &str = indoc! {r#"
    Examples:
      %BINARY_NAME% data.bin write mykey "Hello, world!"
      %BINARY_NAME% data.bin read mykey
      %BINARY_NAME% data.bin delete mykey
"#};

/// Append-Only Storage Engine CLI
#[derive(Parser)]
#[command(
    name = env!("CARGO_PKG_NAME"),
    version = env!("CARGO_PKG_VERSION"),
    about = env!("CARGO_PKG_DESCRIPTION"),
    long_about = None
)]
#[command(    
    after_help = HELP_TEMPLATE.replace("%BINARY_NAME%", env!("CARGO_PKG_NAME"))
)]

struct Cli {
    /// The file where data is stored (automatically created if it does not exist).
    #[arg(value_name = "storage", help = "Path to the storage file. If the file does not exist, it will be created automatically.")]
    storage: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

// TODO: Implement copy and move commands (for single entries)
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

        /// The value to store (optional; reads from stdin if not provided)
        value: Option<String>,
    },

    /// Delete a key
    Delete {
        /// The key to delete
        key: String,
    },

    /// Compact the storage file to remove old entries
    Compact,

    /// Get current state of storage file
    Info,

    /// Access the metadata of a key
    Metadata {
        // The key to query
        key: String
    }
}

fn main() {
    let stdin_input = get_stdin_or_default(None);

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Read { key } => {
            let storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
            
            match storage.get_entry_by_key(key.as_bytes()) {
                Some(value) => {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();

                    if stdout.is_terminal() {
                        // If writing to a terminal, use UTF-8 safe string output
                        writeln!(handle, "{}", String::from_utf8_lossy(&value.as_slice()))
                            .expect("Failed to write output");
                    } else {
                        // If redirected, output raw binary
                        handle.write_all(&value.as_slice()).expect("Failed to write binary output");
                        handle.flush().expect("Failed to flush output");
                    }
                }
                None => {
                    error!("Error: Key '{}' not found", key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Write { key, value } => {
            let mut storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
        
            // Convert `Option<String>` to `Option<Vec<u8>>` (binary format)
            let value_bytes = value.as_ref().map(|s| s.as_bytes().to_vec());
        
            // `stdin_input` is already `Option<Vec<u8>>`, so merge them properly
            let final_value = value_bytes.or_else(|| stdin_input.clone());
        
            // Check if the final value is `None` or an empty binary array
            if final_value.as_deref().map_or(true, |v| v.is_empty()) {
                error!("Error: No value provided and stdin is empty.");
                std::process::exit(1);
            }
        
            // Unwrap safely since we checked for `None`
            let final_value = final_value.unwrap();
        
            storage
                .append_entry(key.as_bytes(), &final_value)
                .expect("Failed to write entry");
        
            info!(
                "Stored '{}'",
                key,
            );

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

        Commands::Metadata { key } => {
            let storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");
            
            match storage.get_entry_by_key(key.as_bytes()) {
                Some(entry) => {
                    println!(
                        "Metadata:\n\
                        --------------------------------\n\
                        Payload Size:               {} bytes\n\
                        Payload Size with Metadata: {} bytes\n\
                        Payload Offset Range:       {:?}\n\
                        Address Range:              {:?}\n\
                        Key Hash:                   {}\n\
                        Checksum:                   {:?}\n\
                        Checksum Validity:          {}\n\
                        --------------------------------",
                        entry.size(),
                        entry.size_with_metadata(),
                        entry.offset_range(),
                        entry.address_range(),
                        entry.key_hash(),
                        entry.checksum(),
                        if entry.is_valid_checksum() { "VALID" } else { "INVALID" }
                    );
                }
                None => {
                    error!("Error: Key '{}' not found", key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Info => {
            let storage = AppendStorage::open(&cli.storage).expect("Failed to open storage");

            // Retrieve storage file size
            let storage_size = storage.get_storage_size().unwrap_or(0);

            // Get compaction savings estimate
            let savings_estimate = storage.estimate_compaction_savings();

            // Count active entries
            let entry_count = storage.count();

            println!("Storage Info:");
            println!("--------------------------------");
            println!("File Path:       {:?}", cli.storage);
            println!("Total Size:      {}", format_bytes(storage_size));
            println!("Active Entries:  {}", entry_count);
            println!("Compaction Savings Estimate: {}", format_bytes(savings_estimate));
            println!("--------------------------------");
        }

    }
}
