// TODO: Implement

use crate::storage_engine::EntryHandle;
use memmap2::Mmap;
use std::io::{self, Read};
use std::ops::Range;
use std::sync::Arc;

pub struct EntryStream {
    entry_handle: EntryHandle,
    position: usize, // Tracks how much has been read
}

impl EntryStream {
    pub fn new(entry_handle: EntryHandle) -> Self {
        Self {
            position: entry_handle.range.start,
            entry_handle,
        }
    }
}

impl Read for EntryStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let slice = self.entry_handle.as_slice(); // Get zero-copy reference

        let remaining = self.entry_handle.range.end - self.position;
        if remaining == 0 {
            return Ok(0); // EOF
        }

        let bytes_to_read = remaining.min(buf.len());
        let start_idx = self.position - self.entry_handle.range.start;

        buf[..bytes_to_read].copy_from_slice(&slice[start_idx..start_idx + bytes_to_read]);

        self.position += bytes_to_read;
        Ok(bytes_to_read)
    }
}
