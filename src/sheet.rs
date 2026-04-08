//! Sheet (worksheet) binary encoder.
//!
//! Processes Arrow RecordBatches one at a time (streaming) and writes
//! BrtRowHdr + cell records into an in-memory buffer, while simultaneously
//! building the SST.

use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use crate::biff12::*;
use crate::sst::Sst;

// ── Date epoch ───────────────────────────────────────────────────────────────
// Excel date serial: days since 1899-12-30
const EXCEL_EPOCH_DAYS: i32 = 25569; // unix epoch (1970-01-01) in Excel serial

fn unix_days_to_excel(days: i32) -> f64 {
    (days + EXCEL_EPOCH_DAYS) as f64
}

fn unix_ms_to_excel(ms: i64) -> f64 {
    ms as f64 / 86_400_000.0 + EXCEL_EPOCH_DAYS as f64
}

// ── Sheet header / footer (verified byte-for-byte against reference xlsb) ────

pub fn write_sheet_header(num_rows: u32, freeze_row: u32, buf: &mut Vec<u8>) {
    write_r0(0x0081, buf); // BrtBeginSheet
    // BrtWsDim (static — scroll position hint, same in all reference files)
    write_rec(0x0093, &[
        0xc9,0x04,0x02,0x00,0x40,0x00,0x00,0x00,0x00,0x00,0x00,0xff,
        0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
    ], buf);
    // BrtWsProp (static)
    write_rec(0x0094, &[
        0x00,0x00,0x00,0x00,0xa0,0x86,0x01,0x00,0x00,0x00,0x00,0x00,0x77,0x00,0x00,0x00,
    ], buf);

    // ColInfos wrapper (0x0085..0x0086) — contains BrtPane for freeze
    write_r0(0x0085, buf);
    write_rec(0x0089, &bci_payload(num_rows, freeze_row), buf);
    if freeze_row > 0 {
        // BrtPane (0x0097) — Excel's freeze mechanism inside ColInfos
        let mut pane = [0u8; 29];
        // xnumXSplit = 0.0
        pane[0..8].copy_from_slice(&0f64.to_le_bytes());
        // xnumYSplit = freeze_row as f64
        pane[8..16].copy_from_slice(&(freeze_row as f64).to_le_bytes());
        // rwTop = freeze_row
        pane[16..20].copy_from_slice(&freeze_row.to_le_bytes());
        // colLeft = 0
        // pnnAct = 2 (bottomLeft)
        pane[24..28].copy_from_slice(&2u32.to_le_bytes());
        // flags = 0x03 (fFrozen | fFrozenNoSplit)
        pane[28] = 0x03;
        write_rec(0x0097, &pane, buf);
        // Two BrtColInfo records when frozen
        let (ci0, ci1) = ci_payload_frozen(num_rows);
        write_rec(0x0098, &ci0, buf);
        write_rec(0x0098, &ci1, buf);
    } else {
        write_rec(0x0098, &ci_payload(num_rows), buf);
    }
    write_r0(0x008A, buf); // BrtEndColInfos
    write_r0(0x0086, buf);

    // Static preamble (verified from reference)
    write_rec(0x0025, &[0x01,0x00,0x02,0x0e,0x00,0x80], buf);
    write_rec(0x0415, &[0x05,0x00], buf);
    write_r0(0x0026, buf);
    write_rec(0x01E5, &[0xff,0xff,0xff,0xff,0x08,0x00,0x2c,0x01,0x00,0x00,0x00,0x00], buf);
    write_r0(0x0091, buf);

    // Row-0 envelope (provided by header; rows 1+ use ROW_PRE)
    write_rec(0x0025, &[0x01,0x00,0x02,0x0e,0x00,0x80], buf);
    write_rec(0x0400, &[0x05,0x00], buf);
    write_r0(0x0026, buf);
}

pub fn write_sheet_footer(buf: &mut Vec<u8>) {
    // 160 bytes verbatim from Python reference
    buf.extend_from_slice(&[
        0x92, 0x01, 0x00, 0x97, 0x04, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xdd, 0x03, 0x02, 0x10, 0x00, 0xdc, 0x03, 0x30,
        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xe6, 0x3f, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xe6, 0x3f,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x3f,
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0xd3, 0x3f, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0xd3, 0x3f,
        0x25, 0x06, 0x01, 0x00, 0x00, 0x10, 0x00, 0x80, 0x80, 0x18, 0x10, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x00, 0x82, 0x01, 0x00,
    ]);
}

// ── BrtBeginColInfos payload ──────────────────────────────────────────────────

fn bci_payload(num_rows: u32, freeze_row: u32) -> [u8; 30] {
    let mut p = [0u8; 30];
    p[0..4].copy_from_slice(&0x3DCu32.to_le_bytes());
    // bytes [4:6] = 0x0000
    let row_count_field = if freeze_row > 0 { 0 } else { num_rows.saturating_sub(7) };
    p[6..10].copy_from_slice(&row_count_field.to_le_bytes());
    p[14..18].copy_from_slice(&0x40u32.to_le_bytes());
    p[18..22].copy_from_slice(&0x64u32.to_le_bytes());
    p
}

// ── BrtColInfo payload(s) ─────────────────────────────────────────────────────

fn ci_payload(num_rows: u32) -> [u8; 36] {
    let mut p = [0u8; 36];
    p[0..4].copy_from_slice(&3u32.to_le_bytes());
    p[4..8].copy_from_slice(&num_rows.to_le_bytes());
    p[16..20].copy_from_slice(&1u32.to_le_bytes());
    p[20..24].copy_from_slice(&num_rows.to_le_bytes());
    p[24..28].copy_from_slice(&num_rows.to_le_bytes());
    p
}

fn ci_payload_frozen(num_rows: u32) -> ([u8; 36], [u8; 36]) {
    // Frozen top pane: field0=3, num_rows=0
    let mut p0 = [0u8; 36];
    p0[0..4].copy_from_slice(&3u32.to_le_bytes());
    p0[16..20].copy_from_slice(&1u32.to_le_bytes());
    // Live bottom pane: field0=2, num_rows=num_rows
    let mut p1 = [0u8; 36];
    p1[0..4].copy_from_slice(&2u32.to_le_bytes());
    p1[4..8].copy_from_slice(&num_rows.to_le_bytes());
    p1[16..20].copy_from_slice(&1u32.to_le_bytes());
    p1[20..24].copy_from_slice(&num_rows.to_le_bytes());
    p1[24..28].copy_from_slice(&num_rows.to_le_bytes());
    (p0, p1)
}

// ── Row encoder ───────────────────────────────────────────────────────────────

/// Encodes a single row from an Arrow RecordBatch into BIFF12 cell records.
/// Appends directly into `sheet_buf` and interns strings into `sst`.
pub fn encode_row(
    batch: &arrow::record_batch::RecordBatch,
    row_idx: usize,
    global_row: u32,
    col_xf: &[u16],       // per-column XF index
    bold_header_xf: u16,  // XF to use for row 0 (0 = no bold)
    sst: &mut Sst,
    sheet_buf: &mut Vec<u8>,
) {
    // Row envelope for rows 1+ (row 0 gets its envelope from the header)
    if global_row > 0 {
        sheet_buf.extend_from_slice(ROW_PRE);
    }

    // Collect cell records into a temp buffer so we know last_col
    let ncols = batch.num_columns();
    let mut cell_buf = Vec::with_capacity(ncols * 16);
    let mut last_col = 0u32;

    for col_idx in 0..ncols {
        let ixfe = if global_row == 0 && bold_header_xf > 0 {
            bold_header_xf
        } else {
            col_xf.get(col_idx).copied().unwrap_or(0)
        };

        let col = col_idx as u32;
        let col_arr = batch.column(col_idx);

        if col_arr.is_null(row_idx) {
            write_cell_blank(col, ixfe, &mut cell_buf);
            last_col = col;
            continue;
        }

        let written = encode_cell(col_arr, row_idx, col, ixfe, sst, &mut cell_buf);
        if written {
            last_col = col;
        }
    }

    write_row_hdr(global_row, last_col, sheet_buf);
    sheet_buf.extend_from_slice(&cell_buf);
}

/// Encode a single cell. Returns true if a record was written.
fn encode_cell(
    arr: &dyn Array,
    row: usize,
    col: u32,
    ixfe: u16,
    sst: &mut Sst,
    buf: &mut Vec<u8>,
) -> bool {
    use DataType::*;

    match arr.data_type() {
        Utf8 => {
            let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            let s = arr.value(row);
            if s.is_empty() {
                write_cell_blank(col, ixfe, buf);
            } else {
                let isst = sst.intern(s);
                write_cell_isst(col, ixfe, isst, buf);
            }
            true
        }
        LargeUtf8 => {
            let arr = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let s = arr.value(row);
            if s.is_empty() {
                write_cell_blank(col, ixfe, buf);
            } else {
                let isst = sst.intern(s);
                write_cell_isst(col, ixfe, isst, buf);
            }
            true
        }
        Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            write_cell_bool(col, ixfe, arr.value(row), buf);
            true
        }
        Int8  => encode_int(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(row) as i64, col, ixfe, buf),
        Int16 => encode_int(arr.as_any().downcast_ref::<Int16Array>().unwrap().value(row) as i64, col, ixfe, buf),
        Int32 => encode_int(arr.as_any().downcast_ref::<Int32Array>().unwrap().value(row) as i64, col, ixfe, buf),
        Int64 => encode_int(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row), col, ixfe, buf),
        UInt8  => encode_int(arr.as_any().downcast_ref::<UInt8Array>().unwrap().value(row) as i64, col, ixfe, buf),
        UInt16 => encode_int(arr.as_any().downcast_ref::<UInt16Array>().unwrap().value(row) as i64, col, ixfe, buf),
        UInt32 => encode_int(arr.as_any().downcast_ref::<UInt32Array>().unwrap().value(row) as i64, col, ixfe, buf),
        UInt64 => encode_int(arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(row) as i64, col, ixfe, buf),
        Float32 => {
            let v = arr.as_any().downcast_ref::<Float32Array>().unwrap().value(row) as f64;
            encode_float(v, col, ixfe, buf)
        }
        Float64 => {
            let v = arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            encode_float(v, col, ixfe, buf)
        }
        Date32 => {
            // Days since Unix epoch → Excel serial
            let days = arr.as_any().downcast_ref::<Date32Array>().unwrap().value(row);
            let excel = unix_days_to_excel(days);
            encode_float(excel, col, ixfe, buf)
        }
        Date64 => {
            // Milliseconds since Unix epoch
            let ms = arr.as_any().downcast_ref::<Date64Array>().unwrap().value(row);
            let excel = unix_ms_to_excel(ms);
            encode_float(excel, col, ixfe, buf)
        }
        Timestamp(TimeUnit::Millisecond, _) => {
            let ms = arr.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row);
            encode_float(unix_ms_to_excel(ms), col, ixfe, buf)
        }
        Timestamp(TimeUnit::Second, _) => {
            let s = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(row);
            encode_float(unix_ms_to_excel(s * 1000), col, ixfe, buf)
        }
        Decimal128(_, scale) => {
            let raw = arr.as_any().downcast_ref::<Decimal128Array>().unwrap().value(row);
            let v = raw as f64 / 10f64.powi(*scale as i32);
            encode_float(v, col, ixfe, buf)
        }
        _ => {
            // Unsupported type → blank
            write_cell_blank(col, ixfe, buf);
            true
        }
    }
}

#[inline]
fn encode_int(v: i64, col: u32, ixfe: u16, buf: &mut Vec<u8>) -> bool {
    let vf = v as f64;
    if let Some(rk) = encode_rk(vf) {
        write_cell_rk(col, ixfe, rk, buf);
    } else {
        write_cell_real(col, ixfe, vf, buf);
    }
    true
}

#[inline]
fn encode_float(v: f64, col: u32, ixfe: u16, buf: &mut Vec<u8>) -> bool {
    if !v.is_finite() {
        write_cell_blank(col, ixfe, buf);
        return true;
    }
    if let Some(rk) = encode_rk(v) {
        write_cell_rk(col, ixfe, rk, buf);
    } else {
        write_cell_real(col, ixfe, v, buf);
    }
    true
}

/// Write row 0 as column names from the Arrow schema field names.
/// This mirrors the Python writer where row[0] is always the header list.
pub fn encode_header_row(
    batch: &arrow::record_batch::RecordBatch,
    bold_xf: u16,
    sst: &mut crate::sst::Sst,
    sheet_buf: &mut Vec<u8>,
) {
    // Row 0 envelope is already in the sheet header — do NOT write ROW_PRE here.
    let ncols = batch.num_columns();
    let schema = batch.schema();
    let mut cell_buf = Vec::with_capacity(ncols * 16);
    let mut last_col = 0u32;

    for col_idx in 0..ncols {
        let col = col_idx as u32;
        let name = schema.field(col_idx).name();
        if name.is_empty() {
            write_cell_blank(col, bold_xf, &mut cell_buf);
        } else {
            let isst = sst.intern(name);
            write_cell_isst(col, bold_xf, isst, &mut cell_buf);
        }
        last_col = col;
    }

    write_row_hdr(0, last_col, sheet_buf);
    sheet_buf.extend_from_slice(&cell_buf);
}

