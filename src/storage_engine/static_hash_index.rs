//! mmap-resident static hash table (key_hash → offset)
//! – 16 B per slot  (u64 key, u64 offset)
//! – open addressing, linear probe
//! – no allocations, thread-safe read-only

use memmap2::Mmap;
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
};

const FOOTER_MAGIC: u32 = 0x5348_5244; // "SHRD"
const FOOTER_SIZE: usize = 32;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IndexFooter {
    pub index_offset: u64,
    pub table_bytes: u64,
    pub indexed_up_to: u64,
    pub table_pow2: u32,
    pub magic: u32,
}

impl IndexFooter {
    pub fn read_from(mmap: &Mmap) -> Option<Self> {
        if mmap.len() < FOOTER_SIZE {
            return None;
        }
        let base = mmap.len() - FOOTER_SIZE;
        let mut buf = [0u8; FOOTER_SIZE];
        buf.copy_from_slice(&mmap[base..]);
        let magic = u32::from_le_bytes(buf[28..32].try_into().unwrap());
        if magic != FOOTER_MAGIC {
            return None;
        }
        Some(Self {
            index_offset: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            table_bytes: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            indexed_up_to: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            table_pow2: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            magic,
        })
    }

    // This function signature is changed to take a mutable reference to the writer.
    pub fn write<W: Write>(
        w: &mut W,
        off: u64,
        bytes: u64,
        upto: u64,
        pow2: u32,
    ) -> std::io::Result<()> {
        w.write_all(&off.to_le_bytes())?;
        w.write_all(&bytes.to_le_bytes())?;
        w.write_all(&upto.to_le_bytes())?;
        w.write_all(&pow2.to_le_bytes())?;
        w.write_all(&FOOTER_MAGIC.to_le_bytes())?;
        Ok(())
    }
}

/// read-only view over an mmap’d static table
#[derive(Debug)]
pub struct StaticHashIndex<'a> {
    mmap: &'a Mmap,
    footer: IndexFooter,
    slot_mask: u64,
}

impl<'a> StaticHashIndex<'a> {
    pub fn new(mmap: &'a Mmap, footer: IndexFooter) -> Self {
        let slot_cnt = footer.table_bytes / 16;
        Self {
            mmap,
            footer,
            slot_mask: slot_cnt as u64 - 1,
        }
    }

    #[inline]
    pub fn get(&self, key_hash: u64) -> Option<u64> {
        let mut idx = key_hash & self.slot_mask;
        loop {
            let base = (self.footer.index_offset + idx * 16) as usize;
            if base + 16 > self.mmap.len() {
                return None;
            } // Bounds check
            let stored = u64::from_le_bytes(self.mmap[base..base + 8].try_into().unwrap());
            if stored == 0 {
                return None;
            }
            if stored == key_hash {
                return Some(u64::from_le_bytes(
                    self.mmap[base + 8..base + 16].try_into().unwrap(),
                ));
            }
            idx = (idx + 1) & self.slot_mask;
        }
    }
}

/// build a packed table in RAM and append it (+footer) to `file`
pub fn flush_static_index(
    file: &mut File,
    pairs: &[(u64, u64)], // (key_hash, offset)
    indexed_up_to: u64,
) -> std::io::Result<()> {
    let need_slots = (pairs.len() as f64 / 0.7).ceil() as u64; // Target 70% load factor
    let pow2 = 64 - need_slots.leading_zeros();
    let slots = 1u64 << pow2;
    let mut table = vec![0u8; (slots * 16) as usize];

    for &(k, off) in pairs {
        let mut idx = xxhash_rust::xxh3::xxh3_64(&k.to_le_bytes()) & (slots - 1);
        loop {
            let base = (idx * 16) as usize;
            let stored = u64::from_le_bytes(table[base..base + 8].try_into().unwrap());
            if stored == 0 {
                table[base..base + 8].copy_from_slice(&k.to_le_bytes());
                table[base + 8..base + 16].copy_from_slice(&off.to_le_bytes());
                break;
            }
            idx = (idx + 1) & (slots - 1);
        }
    }

    let index_offset = file.seek(SeekFrom::End(0))?;
    file.write_all(&table)?;
    // The call to `IndexFooter::write` now passes `file` which is `&mut File`, matching the new signature.
    IndexFooter::write(
        file,
        index_offset,
        table.len() as u64,
        indexed_up_to,
        pow2 as u32,
    )?;
    file.flush()?;
    Ok(())
}
