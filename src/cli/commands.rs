use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub enum Commands {
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
    Rename { old_key: String, new_key: String },

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
