//! Shared String Table (SST) builder and encoder.
//!
//! The SST maps unique strings → u32 index (insertion order).
//! `indexmap::IndexMap` gives O(1) lookup and preserves insertion order,
//! which is required because the BrtSst record must list strings in index order.

use indexmap::IndexMap;
use crate::biff12::{write_rec, write_r0};

pub struct Sst {
    map: IndexMap<String, u32>,
}

impl Sst {
    pub fn new() -> Self {
        Self { map: IndexMap::new() }
    }

    /// Insert a string and return its SST index (idempotent).
    pub fn intern(&mut self, s: &str) -> u32 {
        let next = self.map.len() as u32;
        *self.map.entry(s.to_owned()).or_insert(next)
    }

    /// Look up a string without inserting.
    pub fn get(&self, s: &str) -> Option<u32> {
        self.map.get(s).copied()
    }

    pub fn len(&self) -> u32 {
        self.map.len() as u32
    }

    /// Encode the complete sharedStrings.bin binary.
    ///
    /// Format:
    ///   BrtBeginSst(0x009F)  count(4) count(4)
    ///   for each string:
    ///     BrtSstItem(0x0013)  flags(1) cch(4) utf16le(cch*2)
    ///   BrtEndSst(0x00A0)
    pub fn encode(&self) -> Vec<u8> {
        let n = self.map.len() as u32;
        let mut buf = Vec::with_capacity(self.map.len() * 32);

        // BrtBeginSst: cstTotal(4) cstUnique(4)
        let hdr = {
            let mut h = [0u8; 8];
            h[0..4].copy_from_slice(&n.to_le_bytes());
            h[4..8].copy_from_slice(&n.to_le_bytes());
            h
        };
        write_rec(0x009F, &hdr, &mut buf);

        for s in self.map.keys() {
            // BrtSstItem: fHighByte(1) + XLWideString: cch(4) + utf16le
            let utf16: Vec<u16> = s.encode_utf16().collect();
            let cch = utf16.len() as u32;
            let mut pay = Vec::with_capacity(1 + 4 + utf16.len() * 2);
            pay.push(0x00); // fHighByte = 0 (UTF-16LE, not compressed)
            pay.extend_from_slice(&cch.to_le_bytes());
            for ch in &utf16 {
                pay.extend_from_slice(&ch.to_le_bytes());
            }
            write_rec(0x0013, &pay, &mut buf);
        }

        write_r0(0x00A0, &mut buf); // BrtEndSst
        buf
    }
}

impl Default for Sst {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_idempotent() {
        let mut sst = Sst::new();
        assert_eq!(sst.intern("hello"), 0);
        assert_eq!(sst.intern("world"), 1);
        assert_eq!(sst.intern("hello"), 0); // same index
        assert_eq!(sst.len(), 2);
    }

    #[test]
    fn test_encode_nonempty() {
        let mut sst = Sst::new();
        sst.intern("A");
        sst.intern("B");
        let bin = sst.encode();
        // Should start with BrtBeginSst varint (0x9F = 159 → varint 0x9F 0x01)
        assert_eq!(bin[0], 0x9F);
        assert_eq!(bin[1], 0x01);
        assert!(!bin.is_empty());
    }
}
