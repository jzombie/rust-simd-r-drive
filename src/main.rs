use clap::{Arg, Command};
use simd_r_drive::AppendStorage;
use std::path::PathBuf;

fn main() {
    let matches = Command::new("simd_r_drive")
        .version("1.0")
        .author("Your Name")
        .about("CLI for Append-Only Storage Engine")
        .arg(
            Arg::new("storage")
                .help("Path to the storage file")
                .required(true)
                .index(1),
        )
        .subcommand(
            Command::new("read")
                .about("Read the value associated with a key")
                .arg(Arg::new("key").required(true).help("Key to read")),
        )
        .subcommand(
            Command::new("write")
                .about("Write a value for a given key")
                .arg(Arg::new("key").required(true).help("Key to write"))
                .arg(Arg::new("value").required(true).help("Value to store")),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a key")
                .arg(Arg::new("key").required(true).help("Key to delete")),
        )
        .get_matches();

    let storage_path = PathBuf::from(matches.get_one::<String>("storage").unwrap());
    let mut storage = AppendStorage::open(&storage_path).expect("Failed to open storage");

    match matches.subcommand() {
        Some(("read", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().as_bytes();
            match storage.get_entry_by_key(key) {
                Some(value) => {
                    println!("{}", String::from_utf8_lossy(value));
                }
                None => {
                    eprintln!("Key not found.");
                    std::process::exit(1);
                }
            }
        }
        Some(("write", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().as_bytes();
            let value = sub_matches.get_one::<String>("value").unwrap().as_bytes();
            storage
                .append_entry(key, value)
                .expect("Failed to write entry");
            println!("Successfully written.");
        }
        Some(("delete", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().as_bytes();
            storage.delete_entry(key).expect("Failed to delete entry");
            println!("Successfully deleted.");
        }
        _ => {
            eprintln!("No valid command provided.");
            std::process::exit(1);
        }
    }
}
