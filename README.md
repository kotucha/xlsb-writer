# xlsb-writer
# xlsb-writer
[![GitHub stars](https://img.shields.io/github/stars/kotucha/xlsb-writer.svg?style=social)](https://github.com/kotucha/xlsb-writer)

A Rust library for writing Excel Binary (XLSB) files from Arrow RecordBatches. First and only xlsb writer (to my knowledge).
## Demo Utility
A pre-compiled CLI tool for converting XLSX to XLSB using this library is available for download.

[Download the Converter Binary](https://github.com/kotucha/xlsb-writer/releases)

*Note: This repository contains the source code for the library only (for now).*

## Supported ✓

### Cell types
- String (via SST shared strings)
- Integer, float (RK encoding + BrtCellReal fallback)
- Boolean
- Date / datetime (auto-formatted as m/d/yyyy; explicit YYYY-MM-DD via column_formats)
- None / blank (BrtCellBlank)
- Number formats — via column_formats: general, int, int0, float1/2/3/4, pct, pct1/2, sci, date, datetime, time, text, accounting, currency, euro — plus any raw Excel format string

### Sheet features
- Bold header row (default True)
- Freeze header row (default True)
- Correct BrtRowHdr with per-row colLast (BrtColSpan)

### Workbook
- Single sheet
- Multiple sheets, any names
- Sheet tab names

### Styles
- Reference styles.bin verbatim for base (font, fills, borders, Normal style, TableStyles, ColorPalette)
- Custom number formats spliced in cleanly
- Bold font registered as Font[1]

### Input types
- Arrow `RecordBatch` (designed for zero-copy integration with DuckDB via its Arrow export, or to be wrapped in a DuckDB extension).

## Not implemented

### Cell features
- Formulas (BrtFmlaString / BrtFmlaNum / BrtFmlaBool / BrtFmlaError)
- Rich text / inline formatting within a cell (BrtCellRString)
- Error cells (written as blank)
- Hyperlinks (BrtHLink)

### Sheet features
- Column widths (all default width)
- Variable row heights (all 15pt fixed)
- Freeze columns (only freeze rows is implemented)
- Cell merges (BrtBeginMergeCells / BrtMergeCell)
- Conditional formatting
- Data validation
- Multiple frozen rows (only row 1 freeze tested)

### Cell formatting beyond number formats
- Background / fill colors
- Font size, italic, underline, strikethrough
- Cell borders
- Text alignment / wrap
- Per-cell font control (only header row bold; no per-cell styling API)

### Workbook features
- Named ranges (BrtName)
- binaryIndex seek-optimization file (omitted — no functional impact)
- Print settings / page setup
- Defined names / print areas
=======
A Rust library for writing Excel Binary (XLSB) files from Arrow RecordBatches. First and only xlsb writer (to my knowledge).
>>>>>>> 66e49684f991bdf8afe7f95c60ad2acc88fccf39
