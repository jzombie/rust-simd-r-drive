use clap::{Parser, Subcommand};
use indoc::indoc;
use log::{error, info, warn};
use simd_r_drive::DataStore;
mod utils;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use stdin_nonblocking::get_stdin_or_default;
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

    /// Move an entry from one storage file to another (copy and delete)
    Move {
        /// The key to move
        key: String,

        /// Target storage file
        #[arg(value_name = "target")]
        target: PathBuf,
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
    // TODO: Replace `stdin_input` with:
    // let stdin_stream = spawn_stdin_stream();

    // while let Ok(data) = stdin_stream.recv() {
    //     let mut reader = Cursor::new(data); // Convert received data into a readable stream
    //     storage
    //         .append_large_entry_from_reader(key, &mut reader)
    //         .expect("Failed to append streamed stdin data");
    // }
    let stdin_input = get_stdin_or_default(None);

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Read { key } => {
            let storage = DataStore::open(&cli.storage).expect("Failed to open storage");

            match storage.get_entry_by_key(key.as_bytes()) {
                Some(value) => {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();

                    if stdout.is_terminal() {
                        // If writing to a terminal, use UTF-8 safe string output
                        writeln!(handle, "{}", String::from_utf8_lossy(value.as_slice()))
                            .expect("Failed to write output");
                    } else {
                        // If redirected, output raw binary
                        handle
                            .write_all(value.as_slice())
                            .expect("Failed to write binary output");
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
            let storage = DataStore::open(&cli.storage).expect("Failed to open storage");

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

            info!("Stored '{}'", key,);
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
            let mut source_storage =
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

        Commands::Delete { key } => {
            let mut storage = DataStore::open(&cli.storage).expect("Failed to open storage");
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

            match storage.get_entry_by_key(key.as_bytes()) {
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
