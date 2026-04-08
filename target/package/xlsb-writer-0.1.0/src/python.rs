use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::PyArrowType;
use std::fs::File;

use crate::writer::{XlsbWriter, SheetOptions};

/// Writes an Arrow table or RecordBatch stream directly to an XLSB file.
/// 
/// Args:
///     path (str): The output file path (e.g. "output.xlsb")
///     sheet_name (str): The name of the sheet tab.
///     data (pyarrow.Table or PyCapsule stream): The Arrow data to write.
#[pyfunction]
#[pyo3(signature = (path, sheet_name, data))]
pub fn write_sheet(path: &str, sheet_name: &str, data: &Bound<'_, PyAny>) -> PyResult<()> {
    // Zero-copy extract the stream using the Arrow C Data Interface
    let stream: PyArrowType<ArrowArrayStreamReader> = data.extract().map_err(|e| {
        PyValueError::new_err(format!("Failed to extract Arrow stream from Python object. Please pass a PyArrow Table or RecordBatchReader: {}", e))
    })?;
    
    let reader = stream.0;

    let mut file = File::create(path).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let mut writer = XlsbWriter::new(&mut file);

    // Map Result<RecordBatch, _> to RecordBatch for the writer
    let batches = reader.map(|batch_res| batch_res.expect("Failed to read Arrow batch"));

    writer.write_sheet(sheet_name, batches, SheetOptions::default())
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        
    writer.finish()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(())
}

#[pymodule]
fn xlsb_writer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(write_sheet, m)?)?;
    Ok(())
}
