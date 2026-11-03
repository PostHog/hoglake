//! The module `_variant_backend` imports: Arrow in and out through the C
//! Data Interface (pyo3-arrow), the GIL released for the whole encode or
//! decode.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::panic::PanicException;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyBytes;
use pyo3_arrow::{PyArray, PyDataType};

use crate::decode::{Part, Step};
use crate::encode::{self, Detail, Options};

/// The calls' arguments and results `_variant_backend` expects, and what
/// they mean. pyhoglake checks this, not the package version, so bump it on
/// any change a released pyhoglake would notice.
const PROTOCOL: u32 = 3;

/// A declaration's plan, from the write form of its storage (`Plan.write_type`):
/// a type it does not lay out as `compile_plan` does is a ValueError.
#[pyclass(frozen)]
struct Plan(encode::Plan);

#[pymethods]
impl Plan {
    #[new]
    fn new(write_type: PyDataType) -> PyResult<Self> {
        encode::Plan::new(write_type.as_ref()).map(Plan).map_err(PyValueError::new_err)
    }
}

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
/// `(chunks, rows, sql_null_rows, invalid, counts)`: counts are each typed
/// node's `(typed, fallback, variant_null)`, in declaration order.
type Encoded<'py> = (Vec<Bound<'py, PyAny>>, usize, usize, Vec<Invalid>, Vec<[usize; 3]>);

/// Encodes JSON text, Arrow binary or null arrays, into `plan`'s storage.
/// With `raise_on_invalid` it stops at the first row not written.
#[pyfunction]
#[pyo3(signature = (chunks, plan, *, nullable, raise_on_invalid, chunk_rows, chunk_bytes, max_int_digits))]
#[allow(clippy::too_many_arguments)]
fn encode_json<'py>(
    py: Python<'py>,
    chunks: Vec<PyArray>,
    plan: &Bound<'py, Plan>,
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
    let plan = &plan.get().0;
    let encoded = py.detach(|| encode::encode_json(&inputs, plan, &options)).map_err(PyTypeError::new_err)?;
    let field = Arc::new(Field::new("", DataType::Struct(plan.fields.clone()), true));
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
    Ok((arrays, encoded.rows, encoded.sql_null_rows, invalid, encoded.counts))
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

/// A fault's detail as `_variant_backend` spells it: text, or
/// `(kind, argument)` for the interpreter to spell.
fn parts(py: Python<'_>, parts: Vec<Part>) -> PyResult<Vec<Bound<'_, PyAny>>> {
    fn tagged<'py>(kind: &str, argument: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = argument.py();
        Ok((kind, argument).into_pyobject(py)?.into_any())
    }
    parts
        .into_iter()
        .map(|part| match part {
            Part::Text(text) => Ok(text.into_pyobject(py)?.into_any()),
            Part::Quoted(name) => tagged("quoted", name.into_pyobject(py)?.into_any()),
            Part::Column(names) => tagged("column", names.into_pyobject(py)?.into_any()),
            Part::Type(steps) => tagged("type", steps.into_pyobject(py)?.into_any()),
            Part::RawType(steps) => tagged("raw_type", steps.into_pyobject(py)?.into_any()),
        })
        .collect()
}

/// Whether the running CPython's `struct` keeps a float's signaling NaN
/// signaling as a double (3.14), probed once: the decoder reads as it does.
static SIGNALING_NAN: PyOnceLock<bool> = PyOnceLock::new();

fn signaling_nan(py: Python<'_>) -> PyResult<bool> {
    let nan = PyBytes::new(py, &[0x01, 0x00, 0x80, 0x7F]);
    let read: f64 = py.import("struct")?.call_method1("unpack", ("<f", nan))?.get_item(0)?.extract()?;
    Ok(read.to_bits() & 1 << 51 == 0)
}

/// `(row, reason, trail, detail)`: the trail innermost first, as
/// `_Invalid.trail`, or None where the fault has no path.
type DecodeFault<'py> = (Option<usize>, &'static str, Option<Vec<Bound<'py, PyAny>>>, Vec<Bound<'py, PyAny>>);

/// Reads storage chunks (`reassemble`) up to the first fault: each row as
/// tagged JSON when `tagged`, and the fault.
#[pyfunction]
#[pyo3(signature = (chunks, *, strict, canonical, nullable, tagged))]
fn decode<'py>(
    py: Python<'py>,
    chunks: Vec<PyArray>,
    strict: bool,
    canonical: bool,
    nullable: bool,
    tagged: bool,
) -> PyResult<(Vec<String>, Option<DecodeFault<'py>>)> {
    let inputs: Vec<_> = chunks.into_iter().map(PyArray::into_inner).collect();
    let signaling_nan = *SIGNALING_NAN.get_or_try_init(py, || signaling_nan(py))?;
    let options = crate::decode::Options { strict, canonical, nullable, tagged, signaling_nan };
    // Imported holding the GIL, and dropped holding it when this returns.
    let decoded = py.detach(|| crate::decode::decode(&inputs, &options));
    let Some((row, fault)) = decoded.fault else {
        return Ok((decoded.rows, None));
    };
    let step = |step| match step {
        Step::Key(key) => Ok(key.into_pyobject(py)?.into_any()),
        Step::Index(index) => Ok(index.into_pyobject(py)?.into_any()),
    };
    let trail = fault.trail.map(|trail| trail.into_iter().map(step).collect::<PyResult<_>>()).transpose()?;
    Ok((decoded.rows, Some((row, fault.reason, trail, parts(py, fault.detail)?))))
}

/// `structure_fault`: the detail of the first null a REQUIRED child holds
/// under a present parent, or None.
#[pyfunction]
fn structure_fault(py: Python<'_>, array: PyArray) -> PyResult<Option<Vec<Bound<'_, PyAny>>>> {
    let (array, _) = array.into_inner();
    py.detach(|| crate::decode::structure_fault(&array)).map(|found| parts(py, found)).transpose()
}

#[pymodule]
fn pyhoglake_variant_codec(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PROTOCOL", PROTOCOL)?;
    m.add("PanicException", m.py().get_type::<PanicException>())?;
    m.add_class::<Plan>()?;
    m.add_function(wrap_pyfunction!(encode_json, m)?)?;
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(structure_fault, m)?)?;
    Ok(())
}
