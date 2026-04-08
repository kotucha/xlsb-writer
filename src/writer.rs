//! Public API: `XlsbWriter` — the main entry point.
//!
//! # Architecture
//!
//! Writing an XLSB file is a two-phase process:
//!
//! 1. **Encode phase** — stream Arrow RecordBatches row by row, building:
//!    - `sheet_buf`: raw BIFF12 cell records (in memory)
//!    - `sst`: shared string table (string → index map)
//!
//! 2. **Assemble phase** — once all rows are encoded, write the ZIP:
//!    - sharedStrings.bin  (SST, needed before sheet for index validity)
//!    - styles.bin
//!    - workbook.bin
//!    - xl/worksheets/sheetN.bin  (sheet_buf wrapped in header/footer)
//!    - [Content_Types].xml, .rels etc.

use std::io::{Write, Seek};
use zip::{ZipWriter, write::SimpleFileOptions, CompressionMethod};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, TimeUnit};

use crate::sst::Sst;
use crate::styles::StylesBuilder;
use crate::sheet::{write_sheet_header, write_sheet_footer, encode_row, encode_header_row};
use crate::workbook::build_workbook;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum WriteError {
    Io(std::io::Error),
    Zip(zip::result::ZipError),
}
impl From<std::io::Error> for WriteError { fn from(e: std::io::Error) -> Self { Self::Io(e) } }
impl From<zip::result::ZipError> for WriteError { fn from(e: zip::result::ZipError) -> Self { Self::Zip(e) } }
impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "{:?}", self) }
}

// ── Sheet options ─────────────────────────────────────────────────────────────

/// Per-sheet formatting options.
#[derive(Clone, Debug)]
pub struct SheetOptions {
    /// Bold the header row (row 0). Default: true.
    pub bold_header: bool,
    /// Freeze the header row. Default: true.
    pub freeze_header: bool,
    /// Column format strings, by column index.
    /// Use shorthand names: "int", "float2", "pct2", "date", "datetime", etc.
    /// Or raw Excel format strings: "#,##0.00", "YYYY-MM-DD", etc.
    pub column_formats: Vec<Option<String>>,
}

impl Default for SheetOptions {
    fn default() -> Self {
        Self {
            bold_header: true,
            freeze_header: true,
            column_formats: Vec::new(),
        }
    }
}

// ── Internal per-sheet state ──────────────────────────────────────────────────

#[allow(dead_code)]
struct SheetState {
    name: String,
    buf: Vec<u8>,    // raw BIFF12 bytes (header + rows, no footer yet)
    num_rows: u32,
    num_cols: u32,
    freeze_row: u32,
}

// ── XlsbWriter ────────────────────────────────────────────────────────────────

/// Streaming XLSB writer.
///
/// Call `write_sheet()` for each sheet (passing an iterator of RecordBatches),
/// then call `finish()` to assemble and write the ZIP file.
pub struct XlsbWriter<W: Write + Seek> {
    zip: ZipWriter<W>,
    sst: Sst,
    styles: StylesBuilder,
    sheets: Vec<SheetState>,
    bold_header: bool,  // global flag — if any sheet uses bold, styles needs it
}

impl<W: Write + Seek> XlsbWriter<W> {
    pub fn new(sink: W) -> Self {
        Self {
            zip: ZipWriter::new(sink),
            sst: Sst::new(),
            styles: StylesBuilder::new(),
            sheets: Vec::new(),
            bold_header: false,
        }
    }

    /// Encode one sheet from an iterator of RecordBatches.
    ///
    /// The first batch's schema is used to determine column names and types.
    /// All batches must have the same schema.
    pub fn write_sheet(
        &mut self,
        name: &str,
        batches: impl Iterator<Item = RecordBatch>,
        opts: SheetOptions,
    ) -> Result<(), WriteError> {
        if opts.bold_header { self.bold_header = true; }

        let _freeze_row = if opts.freeze_header { 1u32 } else { 0 };
        let mut sheet_buf: Vec<u8> = Vec::with_capacity(1 << 20); // 1 MB initial
        let mut num_rows = 0u32;
        let mut num_cols = 0u32;
        let mut col_xf: Vec<u16> = Vec::new();
        #[allow(unused_assignments)]
        let mut bold_xf_idx: u16 = 0;
        let mut schema_resolved = false;

        for batch in batches {
            if !schema_resolved {
                num_cols = batch.num_columns() as u32;
                col_xf = self.resolve_col_xf(&batch, &opts);
                // bold_xf_index() must be called AFTER resolve_col_xf() so all column
                // XFs are registered first — the bold XF is always appended last.
                bold_xf_idx = if opts.bold_header { self.styles.bold_xf_index() } else { 0 };
                schema_resolved = true;

                // Write header row (row 0) from schema field names
                encode_header_row(&batch, bold_xf_idx, &mut self.sst, &mut sheet_buf);
                num_rows += 1;
            }

            for row_idx in 0..batch.num_rows() {
                let bold_xf = 0; // data rows never bold
                encode_row(&batch, row_idx, num_rows, &col_xf, bold_xf, &mut self.sst, &mut sheet_buf);
                num_rows += 1;
            }
        }

        self.sheets.push(SheetState {
            name: name.to_owned(),
            buf: sheet_buf,
            num_rows,
            num_cols,
            freeze_row: if opts.freeze_header { 1 } else { 0 },
        });

        Ok(())
    }

    /// Resolve column XF indices for a batch's schema and the given options.
    fn resolve_col_xf(&mut self, batch: &RecordBatch, opts: &SheetOptions) -> Vec<u16> {
        let schema = batch.schema();
        (0..schema.fields().len()).map(|i| {
            // Explicit column format from opts?
            if let Some(Some(fmt)) = opts.column_formats.get(i) {
                let ifmt = self.styles.resolve_fmt(fmt);
                return self.styles.xf_for_ifmt(ifmt);
            }
            // Auto-format dates
            match schema.field(i).data_type() {
                DataType::Date32 | DataType::Date64 => {
                    self.styles.xf_for_ifmt(14) // m/d/yyyy
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) |
                DataType::Timestamp(TimeUnit::Second, _) => {
                    self.styles.xf_for_ifmt(14)
                }
                _ => 0, // General
            }
        }).collect()
    }

    /// Assemble and write the complete XLSB ZIP file.
    pub fn finish(mut self) -> Result<(), WriteError> {
        macro_rules! zo { () => {
            SimpleFileOptions::default()
                .compression_method(CompressionMethod::Deflated)
                .compression_level(Some(6))
        }}

        let sheet_names: Vec<&str> = self.sheets.iter().map(|s| s.name.as_str()).collect();
        let n = sheet_names.len();

        self.zip.start_file("[Content_Types].xml", zo!())?;
        self.zip.write_all(content_types(n).as_bytes())?;

        self.zip.start_file("_rels/.rels", zo!())?;
        self.zip.write_all(ROOT_RELS.as_bytes())?;

        self.zip.start_file("xl/workbook.bin", zo!())?;
        self.zip.write_all(&build_workbook(&sheet_names))?;

        self.zip.start_file("xl/_rels/workbook.bin.rels", zo!())?;
        self.zip.write_all(workbook_rels(n).as_bytes())?;

        self.zip.start_file("xl/sharedStrings.bin", zo!())?;
        self.zip.write_all(&self.sst.encode())?;

        self.zip.start_file("xl/styles.bin", zo!())?;
        self.zip.write_all(&self.styles.build(self.bold_header))?;

        self.zip.start_file("docProps/core.xml", zo!())?;
        self.zip.write_all(CORE_XML.as_bytes())?;
        self.zip.start_file("docProps/app.xml", zo!())?;
        self.zip.write_all(APP_XML.as_bytes())?;

        for (i, sheet) in self.sheets.into_iter().enumerate() {
            self.zip.start_file(format!("xl/worksheets/sheet{}.bin", i + 1), zo!())?;

            // Write header (now we know the true num_rows)
            let freeze_row = sheet.freeze_row;
            let mut header = Vec::new();
            write_sheet_header(sheet.num_rows, freeze_row, &mut header);
            self.zip.write_all(&header)?;

            // Write row data
            self.zip.write_all(&sheet.buf)?;

            // Write footer
            let mut footer = Vec::new();
            write_sheet_footer(&mut footer);
            self.zip.write_all(&footer)?;
        }

        self.zip.finish()?;
        Ok(())
    }
}

// ── XML boilerplate ───────────────────────────────────────────────────────────

fn content_types(n: usize) -> String {
    let mut s = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n\
         <Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">\
         <Default Extension=\"bin\" ContentType=\"application/vnd.ms-excel.sheet.binary.macroEnabled.main\"/>\
         <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>\
         <Default Extension=\"xml\" ContentType=\"application/xml\"/>\
         <Override PartName=\"/xl/workbook.bin\" ContentType=\"application/vnd.ms-excel.sheet.binary.macroEnabled.main\"/>"
    );
    for i in 1..=n {
        s.push_str(&format!(
            "<Override PartName=\"/xl/worksheets/sheet{i}.bin\" ContentType=\"application/vnd.ms-excel.worksheet\"/>"
        ));
    }
    s.push_str(
        "<Override PartName=\"/xl/styles.bin\" ContentType=\"application/vnd.ms-excel.styles\"/>\
         <Override PartName=\"/xl/sharedStrings.bin\" ContentType=\"application/vnd.ms-excel.sharedStrings\"/>\
         <Override PartName=\"/docProps/core.xml\" ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>\
         <Override PartName=\"/docProps/app.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>\
         </Types>"
    );
    s
}

fn workbook_rels(n: usize) -> String {
    let mut s = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n\
         <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
    );
    for i in 1..=n {
        s.push_str(&format!(
            "<Relationship Id=\"rId{i}\" \
             Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" \
             Target=\"worksheets/sheet{i}.bin\"/>"
        ));
    }
    let styles_id = n + 1;
    let sst_id    = n + 2;
    s.push_str(&format!(
        "<Relationship Id=\"rId{styles_id}\" \
         Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" \
         Target=\"styles.bin\"/>\
         <Relationship Id=\"rId{sst_id}\" \
         Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" \
         Target=\"sharedStrings.bin\"/>\
         </Relationships>"
    ));
    s
}

const ROOT_RELS: &str =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n\
     <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\
     <Relationship Id=\"rId1\" \
     Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" \
     Target=\"xl/workbook.bin\"/>\
     <Relationship Id=\"rId2\" \
     Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" \
     Target=\"docProps/app.xml\"/>\
     <Relationship Id=\"rId3\" \
     Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" \
     Target=\"docProps/core.xml\"/>\
     </Relationships>";

const CORE_XML: &str =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n\
     <cp:coreProperties \
     xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" \
     xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\
     <dc:creator>xlsb-writer</dc:creator></cp:coreProperties>";

const APP_XML: &str =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n\
     <Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\">\
     <Application>xlsb-writer</Application></Properties>";
