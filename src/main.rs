use clap::{Parser, Subcommand};
use indoc::indoc;
use log::{error, info, warn};
use simd_r_drive::{DataStore, EntryStream};
mod utils;
use std::io::{self, IsTerminal, Read, Write};
use std::path::PathBuf;
use utils::format_bytes;

// Help text template with placeholder
const HELP_TEMPLATE: &str = indoc! {r#"
    Examples:
      # Writing a value explicitly
      %BINARY_NAME% data.bin write mykey "Hello, world!"

      # Writing a value from stdin
      echo "Hello, world!" | %BINARY_NAME% data.bin write mykey
      cat file.txt | %BINARY_NAME% data.bin write mykey

      # Reading a value
      %BINARY_NAME% data.bin read mykey

      # Deleting a key
      %BINARY_NAME% data.bin delete mykey

      # Copying a key to another storage file
      %BINARY_NAME% data.bin copy mykey target_data.bin

      # Moving a key to another storage file (deletes from source)
      %BINARY_NAME% data.bin move mykey target_data.bin

      # Compacting the storage file
      %BINARY_NAME% data.bin compact

      # Displaying storage file info
      %BINARY_NAME% data.bin info
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
    #[arg(
        value_name = "storage",
        help = "Path to the storage file. If the file does not exist, it will be created automatically."
    )]
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

        /// Buffer size for reading data (default: 64KB)
        #[arg(short = 'b', long = "buffer-size", value_name = "SIZE")]
        buffer_size: Option<String>,
    },

    /// Write a value for a given key
    Write {
        /// The key to write
        key: String,

        /// The value to store (optional; reads from stdin if not provided)
        value: Option<String>,
    },

    /// Copy an entry from one storage file to another
    Copy {
        /// The key to copy
        key: String,

        /// Target storage file
        #[arg(value_name = "target")]
        target: PathBuf,
    },

    /// Move an entry from one storage file to another
    Move {
        /// The key to move
        key: String,

        /// Target storage file
        #[arg(value_name = "target")]
        target: PathBuf,
    },

    /// Renames an entry
    Rename {
        old_key: String,

        new_key: String
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
        key: String,
    },
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Read { key, buffer_size } => {
            let storage = DataStore::open(&cli.storage).expect("Failed to open storage");

            // Default to 64KB if no buffer size is provided
            let buffer_size = buffer_size
                .as_deref()
                .map(utils::parse_buffer_size)
                .transpose() // Convert `Result<Option<T>, E>` to `Result<Option<T>, E>`
                .unwrap_or_else(|err| {
                    error!("{}", err);
                    std::process::exit(1);
                })
                .expect("Buffer size must be provided."); // Ensure it's required

            match storage.read(key.as_bytes()) {
                Some(entry_handle) => {
                    let stdout = io::stdout();
                    let mut stdout_handle = stdout.lock();
                    let mut entry_stream = EntryStream::from(entry_handle);
                    let mut buffer = vec![0u8; buffer_size];
            
                    loop {
                        let bytes_read = entry_stream.read(&mut buffer).expect("Failed to read entry");
                        if bytes_read == 0 {
                            break; // End of stream
                        }
                        stdout_handle.write_all(&buffer[..bytes_read]).expect("Failed to write output");
                        stdout_handle.flush().expect("Failed to flush output");
                    }
                }
                None => {
                    eprintln!("Error: Key '{}' not found", key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Write { key, value } => {
            let storage = DataStore::open(&cli.storage).expect("Failed to open storage");
            let key_as_bytes = key.as_bytes();

            if let Some(value) = value {
                // If a direct value is provided, write it normally
                storage
                    .write(key_as_bytes, value.as_bytes())
                    .expect("Failed to write entry");
            } else if !io::stdin().is_terminal() {
                // If stdin is piped, use a streaming approach
                let mut stdin_reader = io::stdin().lock();

                if let Err(err) = storage.write_stream(key_as_bytes, &mut stdin_reader) {
                    error!("Failed to write streamed stdin data: {}", err);
                    std::process::exit(1);
                }
            } else {
                // If neither a value nor piped stdin is provided, return an error
                error!("Error: No value provided and stdin is empty.");
                std::process::exit(1);
            }

            info!("Stored '{}'", key);
        }

        Commands::Copy { key, target } => {
            let source_storage =
                DataStore::open(&cli.storage).expect("Failed to open source storage");
            let mut target_storage =
                DataStore::open(target).expect("Failed to open target storage");

            source_storage
                .copy_entry(key.as_bytes(), &mut target_storage)
                .map_err(|err| {
                    error!("Could not copy entry. Received error: {}", err.to_string());
                    std::process::exit(1);
                })
                .ok(); // Ignore the success case

            info!("Copied key '{}' to {:?}", key, target);
        }

        Commands::Move { key, target } => {
            let source_storage =
                DataStore::open(&cli.storage).expect("Failed to open source storage");
            let mut target_storage =
                DataStore::open(target).expect("Failed to open target storage");

            source_storage
                .move_entry(key.as_bytes(), &mut target_storage)
                .map_err(|err| {
                    error!("Could not copy entry. Received error: {}", err.to_string());
                    std::process::exit(1);
                })
                .ok(); // Ignore the success case

            info!("Moved key '{}' to {:?}", key, target);
        }

        Commands::Rename { old_key, new_key } => {
            let storage =
                DataStore::open(&cli.storage).expect("Failed to open source storage");

                storage
                .rename_entry(old_key.as_bytes(), new_key.as_bytes())
                .map_err(|err| {
                    error!("Could not rename entry. Received error: {}", err.to_string());
                    std::process::exit(1);
                })
                .ok(); // Ignore the success case

            info!("Renamed key '{}' to {}", old_key, new_key);
        }

        Commands::Delete { key } => {
            let storage = DataStore::open(&cli.storage).expect("Failed to open storage");
            storage
                .delete_entry(key.as_bytes())
                .expect("Failed to delete entry");
            warn!("Deleted key '{}'", key);
        }

        Commands::Compact => {
            let mut storage = DataStore::open(&cli.storage).expect("Failed to open storage");
            info!("Starting compaction...");
            if let Err(e) = storage.compact() {
                error!("Compaction failed: {}", e);
                std::process::exit(1);
            }
            info!("Compaction completed successfully.");
        }

        Commands::Metadata { key } => {
            let storage = DataStore::open(&cli.storage).expect("Failed to open storage");

            match storage.read(key.as_bytes()) {
                Some(entry) => {
                    println!(
                        "\n{:=^50}\n\
                        {:<25} \"{}\"\n\
                        {:-<50}\n\
                        {:<25} {} bytes\n\
                        {:<25} {} bytes\n\
                        {:<25} {:?}\n\
                        {:<25} {:?}\n\
                        {:<25} {}\n\
                        {:<25} {}\n\
                        {:<25} {}\n\
                        {:-<50}\n\
                        {:<25} {:?}\n\
                        {:=<50}",
                        " METADATA SUMMARY ", // Centered Header
                        "ENTRY FOR:",
                        key, // Key Name
                        "",  // Separator
                        "PAYLOAD SIZE:",
                        entry.size(),
                        "TOTAL SIZE (W/ METADATA):",
                        entry.size_with_metadata(),
                        "OFFSET RANGE:",
                        entry.offset_range(),
                        "MEMORY ADDRESS:",
                        entry.address_range(),
                        "KEY HASH:",
                        entry.key_hash(),
                        "CHECKSUM:",
                        entry.checksum(),
                        "CHECKSUM VALIDITY:",
                        if entry.is_valid_checksum() {
                            "VALID"
                        } else {
                            "INVALID"
                        },
                        "", // Separator
                        "STORED METADATA:",
                        entry.metadata(),
                        "=" // Footer Line
                    );
                }
                None => {
                    error!("Error: Key '{}' not found", key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Info => {
            let storage = DataStore::open(&cli.storage).expect("Failed to open storage");

            // Retrieve storage file size
            let storage_size = storage.get_storage_size().unwrap_or(0);

            // Get compaction savings estimate
            let savings_estimate = storage.estimate_compaction_savings();

            // Count active entries
            let entry_count = storage.count();

            println!(
                "\n{:=^50}\n\
                {:<25} {:?}\n\
                {:-<50}\n\
                {:<25} {}\n\
                {:<25} {}\n\
                {:<25} {}\n\
                {:=<50}",
                " STORAGE INFO ", // Centered Header
                "STORAGE FILE:",
                cli.storage, // File Path
                "",          // Separator
                "TOTAL SIZE:",
                format_bytes(storage_size),
                "ACTIVE ENTRIES:",
                entry_count,
                "COMPACTION SAVINGS:",
                format_bytes(savings_estimate),
                "=" // Footer
            );
        }
    }
}
