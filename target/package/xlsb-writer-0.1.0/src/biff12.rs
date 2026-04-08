//! BIFF12 encoding primitives — varints, records, cell types.
//!
//! All multi-byte integers are little-endian (LE) as per the MS-XLSB spec.

// ── Varint encoding ──────────────────────────────────────────────────────────

/// Encode a BIFF12 variable-length unsigned integer into `buf`.
/// Returns the number of bytes written (1–4).
#[inline]
pub fn write_vi(val: u32, buf: &mut Vec<u8>) {
    let mut v = val;
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(b);
            break;
        } else {
            buf.push(b | 0x80);
        }
    }
}

/// Encode a BIFF12 record: varint(rid) + varint(payload.len()) + payload.
#[inline]
pub fn write_rec(rid: u32, payload: &[u8], buf: &mut Vec<u8>) {
    write_vi(rid, buf);
    write_vi(payload.len() as u32, buf);
    buf.extend_from_slice(payload);
}

/// Encode a zero-length record.
#[inline]
pub fn write_r0(rid: u32, buf: &mut Vec<u8>) {
    write_vi(rid, buf);
    buf.push(0x00);
}

// ── XLWideString ─────────────────────────────────────────────────────────────

/// Encode an XLWideString: cch (u32 LE) + UTF-16LE bytes.
pub fn write_wstr(s: &str, buf: &mut Vec<u8>) {
    let encoded: Vec<u16> = s.encode_utf16().collect();
    buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    for ch in &encoded {
        buf.extend_from_slice(&ch.to_le_bytes());
    }
}

// ── RK number encoding ───────────────────────────────────────────────────────

/// Try to encode `v` as an RK (compact number). Returns `None` if the value
/// cannot be represented (e.g. NaN, Inf, or out of RK range).
pub fn encode_rk(v: f64) -> Option<u32> {
    if !v.is_finite() {
        return None;
    }
    // Integer form: fInt=1 (bit1), fX100=0 (bit0)
    let iv = v as i64;
    if iv as f64 == v && iv >= -(1 << 29) && iv < (1 << 29) {
        return Some(((iv << 2) | 2) as u32);
    }
    // ×100 integer form: fInt=1, fX100=1
    let v100 = v * 100.0;
    let iv100 = v100.round() as i64;
    if (v100 - iv100 as f64).abs() < 1e-6 && iv100 >= -(1 << 29) && iv100 < (1 << 29) {
        return Some(((iv100 << 2) | 3) as u32);
    }
    // Double-top form: fInt=0, fX100=0
    let bits = v.to_bits();
    let hi = (bits >> 32) as u32;
    // lower 2 bits of hi word must be 0 (they become the fInt/fX100 flags)
    if hi & 3 == 0 {
        return Some(hi);
    }
    None
}

// ── Cell record builders ──────────────────────────────────────────────────────
//
// Each cell record layout:
//   col(4) ixfe(2) grbitFmt(2) [value...]
//
// Note: grbitFmt is always 0x0000 for data cells (not 0x0100 like blank cells).
// BrtCellBlank uses 0x0100 for grbitFmt to match reference files.

pub const RID_CELL_BLANK: u32 = 0x0001;
pub const RID_CELL_RK:    u32 = 0x0002;
pub const RID_CELL_BOOL:  u32 = 0x0004;
pub const RID_CELL_REAL:  u32 = 0x0005;
pub const RID_CELL_ISST:  u32 = 0x0007;

/// Write a BrtCellBlank record (col, ixfe, grbitFmt=0x0100).
pub fn write_cell_blank(col: u32, ixfe: u16, buf: &mut Vec<u8>) {
    let mut pay = [0u8; 8];
    pay[0..4].copy_from_slice(&col.to_le_bytes());
    pay[4..6].copy_from_slice(&ixfe.to_le_bytes());
    pay[6..8].copy_from_slice(&0x0100u16.to_le_bytes());
    write_rec(RID_CELL_BLANK, &pay, buf);
}

/// Write a BrtCellRk record.
pub fn write_cell_rk(col: u32, ixfe: u16, rk: u32, buf: &mut Vec<u8>) {
    let mut pay = [0u8; 12];
    pay[0..4].copy_from_slice(&col.to_le_bytes());
    pay[4..6].copy_from_slice(&ixfe.to_le_bytes());
    // pay[6..8] = grbitFmt = 0x0000
    pay[8..12].copy_from_slice(&rk.to_le_bytes());
    write_rec(RID_CELL_RK, &pay, buf);
}

/// Write a BrtCellReal record (full f64).
pub fn write_cell_real(col: u32, ixfe: u16, v: f64, buf: &mut Vec<u8>) {
    let mut pay = [0u8; 16];
    pay[0..4].copy_from_slice(&col.to_le_bytes());
    pay[4..6].copy_from_slice(&ixfe.to_le_bytes());
    pay[8..16].copy_from_slice(&v.to_le_bytes());
    write_rec(RID_CELL_REAL, &pay, buf);
}

/// Write a BrtCellBool record.
pub fn write_cell_bool(col: u32, ixfe: u16, v: bool, buf: &mut Vec<u8>) {
    let mut pay = [0u8; 9];
    pay[0..4].copy_from_slice(&col.to_le_bytes());
    pay[4..6].copy_from_slice(&ixfe.to_le_bytes());
    pay[8] = v as u8;
    write_rec(RID_CELL_BOOL, &pay, buf);
}

/// Write a BrtCellIsst record (shared string index).
pub fn write_cell_isst(col: u32, ixfe: u16, isst: u32, buf: &mut Vec<u8>) {
    let mut pay = [0u8; 12];
    pay[0..4].copy_from_slice(&col.to_le_bytes());
    pay[4..6].copy_from_slice(&ixfe.to_le_bytes());
    pay[8..12].copy_from_slice(&isst.to_le_bytes());
    write_rec(RID_CELL_ISST, &pay, buf);
}

// ── Row header ────────────────────────────────────────────────────────────────

/// Write a BrtRowHdr record (25-byte payload).
///
/// Layout (verified byte-for-byte against reference xlsb):
/// ```text
/// [0:4]   rw        u32  — row index
/// [4:8]   ixfe      u32  — XF index (ignored when fGhostDirty=0)
/// [8:10]  miyRw     u16  — row height = 300 (15pt in 1/20pt units)
/// [10:12] flags     u16  — 0x0000
/// [12]    padding   u8   — 0x00
/// [13:17] ccolspan  u32  — 1 (one BrtColSpan entry)
/// [17:21] colFirst  u32  — 0
/// [21:25] colLast   u32  — last col index with a cell record in this row
/// ```
pub fn write_row_hdr(row_idx: u32, last_col: u32, buf: &mut Vec<u8>) {
    let mut pay = [0u8; 25];
    pay[0..4].copy_from_slice(&row_idx.to_le_bytes());
    // [4:8]  ixfe = 0
    // [8:10] miyRw = 300
    pay[8..10].copy_from_slice(&300u16.to_le_bytes());
    // [10:12] flags = 0
    // [12]    padding = 0
    // [13:17] ccolspan = 1
    pay[13..17].copy_from_slice(&1u32.to_le_bytes());
    // [17:21] colFirst = 0
    // [21:25] colLast
    pay[21..25].copy_from_slice(&last_col.to_le_bytes());
    write_rec(0x0000, &pay, buf);
}

// ── Row envelope (appears before every row, from row 1 onwards) ───────────────
// Row 0's envelope is provided by the sheet header.

/// The fixed 15-byte row envelope written before each row (rows 1+).
/// Equivalent to _ROW_PRE in the Python writer.
pub const ROW_PRE: &[u8] = &[
    0x25, 0x06, 0x01, 0x00, 0x02, 0x0e, 0x00, 0x80,  // BrtBeginList payload
    0x80, 0x08, 0x02, 0x05, 0x00,                      // 0x0400 record
    0x26, 0x00,                                         // BrtEndList
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vi() {
        let mut buf = Vec::new();
        write_vi(0, &mut buf);       assert_eq!(buf, [0x00]);
        buf.clear();
        write_vi(127, &mut buf);     assert_eq!(buf, [0x7F]);
        buf.clear();
        write_vi(128, &mut buf);     assert_eq!(buf, [0x80, 0x01]);
        buf.clear();
        write_vi(0x009C, &mut buf);  assert_eq!(buf, [0x9C, 0x01]); // BrtBundleSh
    }

    #[test]
    fn test_rk_integer() {
        assert_eq!(encode_rk(8129.0), Some(((8129i64 << 2) | 2) as u32));
        assert_eq!(encode_rk(0.0),    Some(2));
        assert_eq!(encode_rk(-1.0),   Some((((-1i64) << 2) | 2) as u32));
    }

    #[test]
    fn test_rk_x100() {
        // 9.99 → ×100 = 999 → RK integer form with fX100 set
        let rk = encode_rk(9.99).unwrap();
        assert_eq!(rk & 1, 1); // fX100 set
        assert_eq!(rk & 2, 2); // fInt set
        let iv = (rk >> 2) as i64;
        assert_eq!(iv, 999);
    }

    #[test]
    fn test_rk_nan_inf() {
        assert!(encode_rk(f64::NAN).is_none());
        assert!(encode_rk(f64::INFINITY).is_none());
    }
}
