use crate::cli::{Cli, Commands};
use crate::storage_engine::{
    DataStore, EntryStream,
    traits::{DataStoreReader, DataStoreWriter},
};
use crate::utils::{format_bytes, parse_buffer_size};
use std::io::{self, IsTerminal, Read, Write};

/// Executes commands from the CLI and interacts with the storage engine.
///
/// This function processes user-provided commands (`Commands` enum) and
/// delegates operations to the `DataStore`. It handles reading, writing,
/// copying, moving, renaming, deleting, compacting, and retrieving metadata
/// or storage information. The function ensures proper input validation,
/// handles errors, and manages both text and binary data streaming.
///
/// # Command Handling
/// - Reads and writes data entries, supporting both direct input and piped streams.
/// - Copies, moves, renames, and deletes keys in the storage engine.
/// - Provides metadata and storage information.
///
/// # References
/// - [`Commands`](crate::cli::Commands): Defines the available CLI commands.
/// - [`DataStore`](crate::storage_engine::DataStore): The underlying storage engine.
pub fn execute_command(cli: &Cli) {
    match &cli.command {
        Commands::Read { key, buffer_size } => {
            let storage = DataStore::open_existing(&cli.storage).expect("Failed to open storage");

            // Default to 64KB if no buffer size is provided
            let buffer_size = buffer_size
                .as_deref()
                .map(parse_buffer_size)
                .transpose()
                .unwrap_or_else(|err| {
                    eprintln!("{}", err);
                    std::process::exit(1);
                })
                .unwrap_or(64 * 1024); // Default to 64KB

            match storage.read(key.as_bytes()) {
                Ok(Some(entry_handle)) => {
                    let stdout = io::stdout();
                    let mut stdout_handle = stdout.lock();
                    let mut entry_stream = EntryStream::from(entry_handle);
                    let mut buffer = vec![0u8; buffer_size];

                    let is_terminal = io::stdout().is_terminal();

                    loop {
                        let bytes_read = entry_stream
                            .read(&mut buffer)
                            .expect("Failed to read entry");
                        if bytes_read == 0 {
                            break; // End of stream
                        }

                        if is_terminal {
                            // Convert bytes to a UTF-8 string (assuming text data)
                            match std::str::from_utf8(&buffer[..bytes_read]) {
                                Ok(text) => stdout_handle.write_all(text.as_bytes()).unwrap(),
                                Err(_) => stdout_handle.write_all(&buffer[..bytes_read]).unwrap(),
                            }
                        } else {
                            // Output raw binary data
                            stdout_handle.write_all(&buffer[..bytes_read]).unwrap();
                        }
                        stdout_handle.flush().unwrap();
                    }

                    // Ensure a newline at the end if it's a terminal
                    if is_terminal {
                        stdout_handle.write_all(b"\n").unwrap();
                    }
                }
                Ok(None) => {
                    eprintln!("Error: Key '{}' not found", key);
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
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
            } else if !io::stdin().is_terminal() && std::env::var("FORCE_NO_TTY").is_err() {
                // If stdin is piped, use a streaming approach
                let mut stdin_reader = io::stdin().lock();

                if let Err(err) = storage.write_stream(key_as_bytes, &mut stdin_reader) {
                    eprintln!("Failed to write streamed stdin data: {}", err);
                    std::process::exit(1);
                }
            } else {
                // If neither a value nor piped stdin is provided, return an error
                eprintln!("Error: No value provided and stdin is empty.");
                std::process::exit(1);
            }

            println!("Stored '{}'", key);
        }

        Commands::Copy { key, target } => {
            let source_storage =
                DataStore::open_existing(&cli.storage).expect("Failed to open source storage");

            let target_storage = DataStore::open(target).expect("Failed to open target storage");

            source_storage
                .copy_entry(key.as_bytes(), &target_storage)
                .map_err(|err| {
                    eprintln!("Could not copy entry. Received error: {}", err);
                    std::process::exit(1);
                })
                .ok(); // Ignore the success case

            println!("Copied key '{}' to {:?}", key, target);
        }

        Commands::Move { key, target } => {
            let source_storage =
                DataStore::open_existing(&cli.storage).expect("Failed to open source storage");

            let target_storage = DataStore::open(target).expect("Failed to open target storage");

            source_storage
                .move_entry(key.as_bytes(), &target_storage)
                .map_err(|err| {
                    eprintln!("Could not copy entry. Received error: {}", err);
                    std::process::exit(1);
                })
                .ok(); // Ignore the success case

            println!("Moved key '{}' to {:?}", key, target);
        }

        Commands::Rename { old_key, new_key } => {
            let storage =
                DataStore::open_existing(&cli.storage).expect("Failed to open source storage");

            storage
                .rename_entry(old_key.as_bytes(), new_key.as_bytes())
                .map_err(|err| {
                    eprintln!("Could not rename entry. Received error: {}", err);
                    std::process::exit(1);
                })
                .ok(); // Ignore the success case

            println!("Renamed key '{}' to {}", old_key, new_key);
        }

        Commands::Delete { key } => {
            let storage = DataStore::open_existing(&cli.storage).expect("Failed to open storage");

            storage
                .delete_entry(key.as_bytes())
                .expect("Failed to delete entry");
            eprintln!("Deleted key '{}'", key);
        }

        Commands::Compact => {
            let mut storage =
                DataStore::open_existing(&cli.storage).expect("Failed to open storage");
            println!("Starting compaction...");
            if let Err(e) = storage.compact() {
                eprintln!("Compaction failed: {}", e);
                std::process::exit(1);
            }
            println!("Compaction completed successfully.");
        }

        Commands::Metadata { key } => {
            let storage = DataStore::open_existing(&cli.storage).expect("Failed to open storage");

            match storage.read(key.as_bytes()) {
                Ok(Some(entry)) => {
                    println!("\n{:=^50}", " METADATA SUMMARY ");
                    println!("{:<25} \"{}\"", "ENTRY FOR:", key);
                    println!("{:-<50}", ""); // Separator

                    println!("{:<25} {} bytes", "PAYLOAD SIZE:", entry.size());
                    println!(
                        "{:<25} {} bytes",
                        "TOTAL SIZE (W/ METADATA):",
                        entry.size_with_metadata()
                    );
                    println!("{:<25} {:?}", "OFFSET RANGE:", entry.offset_range());
                    println!("{:<25} {:?}", "MEMORY ADDRESS:", entry.address_range());

                    println!("{:<25} {}", "KEY HASH:", entry.key_hash());
                    println!("{:<25} {}", "CHECKSUM:", entry.checksum());
                    println!(
                        "{:<25} {}",
                        "CHECKSUM VALIDITY:",
                        if entry.is_valid_checksum() {
                            "VALID"
                        } else {
                            "INVALID"
                        }
                    );

                    println!("{:-<50}", ""); // Separator
                    println!("{:<25} {:?}", "STORED METADATA:", entry.metadata());
                    println!("{:=<50}", ""); // Footer Line
                }
                Ok(None) => {
                    eprintln!("Error: Key '{}' not found", key);
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Info => {
            let storage = DataStore::open_existing(&cli.storage).expect("Failed to open storage");

            // Retrieve storage file size
            let storage_size = storage.get_storage_size().unwrap_or(0);

            // Get compaction savings estimate
            let savings_estimate = storage.estimate_compaction_savings();

            // Count active entries
            let entry_count = storage.count();

            println!("\n{:=^50}", " STORAGE INFO ");
            println!("{:<25} {:?}", "STORAGE FILE:", cli.storage);
            println!("{:-<50}", ""); // Separator

            println!("{:<25} {}", "TOTAL SIZE:", format_bytes(storage_size));
            println!("{:<25} {}", "ACTIVE ENTRIES:", entry_count.unwrap());
            println!(
                "{:<25} {}",
                "COMPACTION SAVINGS:",
                format_bytes(savings_estimate)
            );

            println!("{:=<50}", ""); // Footer
        }
    }
}
