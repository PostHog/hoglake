//! The module `_variant_backend` imports: Arrow in and out through the C
//! Data Interface (pyo3-arrow), the GIL released for the whole encode.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field};
use pyo3::exceptions::PyTypeError;
use pyo3::panic::PanicException;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyBytes;
use pyo3_arrow::PyArray;

use crate::encode::{self, Detail, Options};

/// The calls' arguments and results `_variant_backend` expects; the version
/// pins the semantics.
const PROTOCOL: u32 = 1;

/// The running interpreter's json rules, probed once: CPython 3.13 added the
/// trailing-comma messages, and 3.14 and 3.13.14 or so dropped the character
/// a `\u` escape wanted after its four digits. A patch release can change
/// them, so the version does not say.
static JSON_RULES: PyOnceLock<(bool, bool)> = PyOnceLock::new();

fn json_rules(py: Python<'_>) -> PyResult<(bool, bool)> {
    let loads = py.import("json")?.getattr("loads")?;
    let message = |text: &str| -> PyResult<String> {
        loads.call1((text,)).err().map_or(Ok(String::new()), |error| error.value(py).getattr("msg")?.extract())
    };
    Ok((message("[1,]")?.starts_with("Illegal trailing comma"), message("\"\\u0041")? == "Invalid \\uXXXX escape"))
}

/// `(row, reason, path, detail)`.
type Invalid = (usize, &'static str, Option<String>, String);
/// `(chunks, rows, sql_null_rows, invalid)`.
type Encoded<'py> = (Vec<Bound<'py, PyAny>>, usize, usize, Vec<Invalid>);

/// Encodes JSON text, Arrow binary or null arrays, into unshredded storage.
/// With `raise_on_invalid` it stops at the first row not written.
#[pyfunction]
#[pyo3(signature = (chunks, *, nullable, raise_on_invalid, chunk_rows, chunk_bytes, max_int_digits))]
fn encode_json<'py>(
    py: Python<'py>,
    chunks: Vec<PyArray>,
    nullable: bool,
    raise_on_invalid: bool,
    chunk_rows: usize,
    chunk_bytes: usize,
    max_int_digits: usize,
) -> PyResult<Encoded<'py>> {
    let inputs: Vec<ArrayRef> = chunks.into_iter().map(|array| array.into_inner().0).collect();
    let (trailing_comma_message, escape_needs_tail) = *JSON_RULES.get_or_try_init(py, || json_rules(py))?;
    let options = Options {
        nullable,
        stop_at_invalid: raise_on_invalid,
        chunk_rows,
        chunk_bytes,
        max_int_digits,
        trailing_comma_message,
        escape_needs_tail,
    };
    // The inputs were imported holding the GIL, and are dropped holding it,
    // when this call returns.
    let encoded = py.detach(|| encode::encode_json(&inputs, &options)).map_err(PyTypeError::new_err)?;
    let field = Arc::new(Field::new("", DataType::Struct(encode::storage_fields()), true));
    let to_pyarrow = py.import("pyarrow")?.getattr("array")?;
    let export = |chunk| to_pyarrow.call1((PyArray::new(Arc::new(chunk), field.clone()),));
    let arrays = encoded.chunks.into_iter().map(export).collect::<PyResult<_>>()?;
    let invalid = encoded
        .invalid
        .into_iter()
        .map(|(row, fault)| {
            let path = fault.path();
            Ok((row, fault.reason, path, detail(py, fault.detail)?))
        })
        .collect::<PyResult<_>>()?;
    Ok((arrays, encoded.rows, encoded.sql_null_rows, invalid))
}

fn detail(py: Python<'_>, detail: Detail) -> PyResult<String> {
    match detail {
        Detail::Text(text) => Ok(text),
        // `_quoted`: repr follows the interpreter's Unicode version, so the
        // interpreter spells the key.
        Detail::DuplicateKey(key) => {
            let name = PyBytes::new(py, &key).call_method1("decode", ("utf-8", "surrogatepass"))?;
            let shown: String = name.repr()?.extract()?;
            Ok(format!("key {} appears twice in one object", encode::cut(&shown, 64)))
        }
    }
}

#[pymodule]
fn pyhoglake_variant_codec(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PROTOCOL", PROTOCOL)?;
    m.add("PanicException", m.py().get_type::<PanicException>())?;
    m.add_function(wrap_pyfunction!(encode_json, m)?)?;
    Ok(())
}
