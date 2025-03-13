use log::{debug, info, warn};
use memmap2::Mmap;
use std::collections::{HashMap, HashSet};
use std::convert::From;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Result, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Metadata structure (fixed 20 bytes at the end of each entry)
pub const METADATA_SIZE: usize = 20;
pub const KEY_HASH_RANGE: std::ops::Range<usize> = 0..8;
pub const PREV_OFFSET_RANGE: std::ops::Range<usize> = 8..16;
pub const CHECKSUM_RANGE: std::ops::Range<usize> = 16..20;

// Marker indicating a logically deleted entry in the storage
pub const NULL_BYTE: [u8; 1] = [0];

// Define checksum length explicitly since `CHECKSUM_RANGE.len()` isn't `const`
pub const CHECKSUM_LEN: usize = CHECKSUM_RANGE.end - CHECKSUM_RANGE.start;
