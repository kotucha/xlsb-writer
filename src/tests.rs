//! Integration tests — these validate that the Rust output matches
//! the Python writer's output for the same input data.
//!
//! Run with: cargo test

#[cfg(test)]
mod tests {
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::io::Cursor;

    use crate::{XlsbWriter, SheetOptions};
    use crate::biff12::*;
    use crate::sst::Sst;
    use crate::styles::StylesBuilder;
    use crate::workbook::build_workbook;

    fn parse_biff12(data: &[u8]) -> Vec<(u32, Vec<u8>)> {
        let mut recs = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let (rid, n) = read_vi(data, pos);
            pos += n;
            let (rlen, n) = read_vi(data, pos);
            pos += n;
            recs.push((rid, data[pos..pos + rlen as usize].to_vec()));
            pos += rlen as usize;
        }
        recs
    }

    fn read_vi(data: &[u8], mut pos: usize) -> (u32, usize) {
        let mut val = 0u32;
        let mut shift = 0;
        let start = pos;
        loop {
            let b = data[pos]; pos += 1;
            val |= ((b & 0x7F) as u32) << shift;
            shift += 7;
            if b & 0x80 == 0 { break; }
        }
        (val, pos - start)
    }

    // ── biff12 primitives ──────────────────────────────────────────────────────

    #[test]
    fn vi_roundtrip() {
        for v in [0u32, 1, 127, 128, 255, 0x3FFF, 0x1FFFFF, u32::MAX >> 4] {
            let mut buf = Vec::new();
            write_vi(v, &mut buf);
            let (decoded, _) = read_vi(&buf, 0);
            assert_eq!(decoded, v, "vi roundtrip failed for {v}");
        }
    }

    #[test]
    fn rk_integers() {
        for v in [0i64, 1, -1, 100, -100, 8129, (1 << 29) - 1] {
            let rk = encode_rk(v as f64).expect("integer should encode as RK");
            // Decode: fInt=bit1, fX100=bit0
            assert_eq!(rk & 2, 2, "fInt should be set for integer {v}");
            let decoded = ((rk as i32) >> 2) as f64;
            assert!((decoded - v as f64).abs() < 1e-9);
        }
    }

    #[test]
    fn rk_large_int_falls_back() {
        // Value too large for RK integer form → double-top or None
        let v = (1i64 << 30) as f64;
        // encode_rk may return Some (double-top) or None; should not panic
        let _ = encode_rk(v);
    }

    // ── SST ────────────────────────────────────────────────────────────────────

    #[test]
    fn sst_intern_and_encode() {
        let mut sst = Sst::new();
        assert_eq!(sst.intern("hello"), 0);
        assert_eq!(sst.intern("world"), 1);
        assert_eq!(sst.intern("hello"), 0);
        let bin = sst.encode();
        let recs = parse_biff12(&bin);
        // BrtBeginSst + 2 items + BrtEndSst = 4 records
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[0].0, 0x009F); // BrtBeginSst
        assert_eq!(recs[3].0, 0x00A0); // BrtEndSst
    }

    // ── Styles ─────────────────────────────────────────────────────────────────

    #[test]
    fn styles_base_is_650_bytes() {
        let sb = StylesBuilder::new();
        assert_eq!(sb.build(false).len(), 650);
    }

    #[test]
    fn styles_bold_adds_font() {
        let sb = StylesBuilder::new();
        let out = sb.build(true);
        // Bold bls=700 → 0xBC02 LE somewhere in output
        assert!(out.windows(2).any(|w| w == [0xbc, 0x02]),
                "bold font bls=700 not found in styles");
    }

    #[test]
    fn styles_custom_fmt() {
        let mut sb = StylesBuilder::new();
        let ifmt = sb.resolve_fmt("float2"); // "#,##0.00" → builtin 4
        assert_eq!(ifmt, 4);
        let _xf = sb.xf_for_ifmt(ifmt);
        let out = sb.build(false);
        assert!(out.len() > 650, "custom XF should grow output");
    }

    // ── Workbook ───────────────────────────────────────────────────────────────

    #[test]
    fn workbook_single_sheet1_is_531_bytes() {
        let wb = build_workbook(&["Sheet1"]);
        assert_eq!(wb.len(), 531);
    }

    #[test]
    fn workbook_contains_sheet_names() {
        let wb = build_workbook(&["Alpha", "Beta"]);
        let data = wb.as_slice();
        // "Alpha" in UTF-16LE
        let alpha: Vec<u8> = "Alpha".encode_utf16().flat_map(|c| c.to_le_bytes()).collect();
        assert!(data.windows(alpha.len()).any(|w| w == alpha));
        let beta: Vec<u8> = "Beta".encode_utf16().flat_map(|c| c.to_le_bytes()).collect();
        assert!(data.windows(beta.len()).any(|w| w == beta));
    }

    // ── End-to-end writer ──────────────────────────────────────────────────────

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name",  DataType::Utf8,    false),
            Field::new("qty",   DataType::Int32,   false),
            Field::new("price", DataType::Float64, false),
            Field::new("flag",  DataType::Boolean, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["Alpha", "Beta", "Gamma"])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
            Arc::new(Float64Array::from(vec![9.99, 1234.56, 0.001])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ]).unwrap()
    }

    #[test]
    fn write_single_sheet_produces_valid_zip() {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut w = XlsbWriter::new(&mut buf);
            let batch = make_test_batch();
            let opts = SheetOptions {
                column_formats: vec![
                    None,
                    Some("int".to_string()),
                    Some("float2".to_string()),
                    None,
                ],
                ..Default::default()
            };
            w.write_sheet("Sheet1", std::iter::once(batch), opts).unwrap();
            w.finish().unwrap();
        }
        let bytes = buf.into_inner();
        // Should be a valid ZIP (starts with PK signature)
        assert_eq!(&bytes[..2], b"PK");
        // Check it contains sheet1.bin
        let cursor = Cursor::new(&bytes);
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        assert!(zip.by_name("xl/worksheets/sheet1.bin").is_ok());
        assert!(zip.by_name("xl/styles.bin").is_ok());
        assert!(zip.by_name("xl/sharedStrings.bin").is_ok());
        assert!(zip.by_name("xl/workbook.bin").is_ok());
    }

    #[test]
    fn write_multi_sheet() {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut w = XlsbWriter::new(&mut buf);
            let batch = make_test_batch();
            w.write_sheet("Sales",   std::iter::once(batch.clone()), SheetOptions::default()).unwrap();
            w.write_sheet("Finance", std::iter::once(batch),         SheetOptions::default()).unwrap();
            w.finish().unwrap();
        }
        let bytes = buf.into_inner();
        let cursor = Cursor::new(&bytes);
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        assert!(zip.by_name("xl/worksheets/sheet1.bin").is_ok());
        assert!(zip.by_name("xl/worksheets/sheet2.bin").is_ok());
    }

    /// Write a file to disk for comparison against the Python oracle.
    /// Run: cargo test write_oracle_file -- --nocapture
    /// Then: python verify_rust_output.py --rust <path shown in output>
    #[test]
    fn write_oracle_file() {
        use std::fs::File;
        use std::env;
        // Works on Windows, Linux and macOS
        let out_path = env::temp_dir().join("rust_output.xlsb");
        let file = File::create(&out_path).expect("could not create output file");
        let mut w = XlsbWriter::new(file);
        let opts = SheetOptions {
            column_formats: vec![
                None,                        // name   -> General
                Some("int".to_string()),     // qty    -> #,##0
                Some("float2".to_string()),  // price  -> #,##0.00
                None,                        // flag   -> General
            ],
            bold_header: true,
            freeze_header: true,
        };
        w.write_sheet("Sheet1", std::iter::once(make_test_batch()), opts).unwrap();
        w.finish().unwrap();
        println!("Written to: {}", out_path.display());
        println!("Now run:  python verify_rust_output.py --rust \"{}\"", out_path.display());
    }

    #[test]
    fn debug_styles_output() {
        use std::fs::File;
        use std::io::Write;
        let mut sb = StylesBuilder::new();
        // Simulate: column_formats = [None, int, float2, None]
        let ifmt_int   = sb.resolve_fmt("int");    // -> 3
        let ifmt_float = sb.resolve_fmt("float2"); // -> 4
        sb.xf_for_ifmt(ifmt_int);
        sb.xf_for_ifmt(ifmt_float);
        let bold_idx = sb.bold_xf_index();
        let out = sb.build(true);
        println!("styles.bin: {} bytes", out.len());
        println!("bold_xf_idx: {}", bold_idx);
        println!("hex: {}", out.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(""));
        // Write to temp file for comparison
        let path = std::env::temp_dir().join("rust_styles_debug.bin");
        let mut f = File::create(&path).unwrap();
        f.write_all(&out).unwrap();
        println!("Written to: {}", path.display());
    }
}
