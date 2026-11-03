//! VARIANT storage back to values: `_variant_codec`'s decoder
//! (`decode_metadata`, `decode_value`), its reassembly of any conformant
//! storage (`_Reassembly`, `reassemble`) and `structure_fault`, check for
//! check and fault for fault. Written apart from the encoder, which it
//! checks: the two share `consts`, never logic.

mod storage;
mod tagged;
mod value;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;

pub use storage::structure_fault;
pub use tagged::tagged;
pub use value::Value;

/// How one call reads: `strict` holds storage to the spec's writer rules
/// (`verify`), `canonical` to pyhoglake's encoding; `nullable` false
/// refuses a SQL NULL row; `tagged` keeps each row's value as tagged JSON;
/// `signaling_nan` is the running CPython's `struct` rule for a float's
/// signaling NaN (3.14 keeps it signaling as a double; widening quiets it).
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub strict: bool,
    pub canonical: bool,
    pub nullable: bool,
    pub tagged: bool,
    pub signaling_nan: bool,
}

/// A piece of a fault's detail: text, or what the binding spells as the
/// Python codec does: a name quoted by the interpreter's `repr`
/// (`_quoted`), a storage column's path (`_column_path`), or the Arrow
/// type at a child path of the storage (`_type_text`, or `Raw` as Arrow
/// writes it).
#[derive(Clone, Debug, PartialEq)]
pub enum Part {
    Text(String),
    Quoted(String),
    Column(Vec<String>),
    Type(Vec<usize>),
    RawType(Vec<usize>),
}

/// A step of a row's path: a key, or an index.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    Key(String),
    Index(usize),
}

/// Why storage does not read (`_Invalid`): `trail` is innermost first,
/// None where the fault has no path (the storage's type, a SQL NULL row).
#[derive(Clone, Debug, PartialEq)]
pub struct Fault {
    pub reason: &'static str,
    pub detail: Vec<Part>,
    pub trail: Option<Vec<Step>>,
}

impl Fault {
    pub fn new(reason: &'static str, detail: Vec<Part>) -> Self {
        Fault { reason, detail, trail: Some(Vec::new()) }
    }

    pub fn step(mut self, step: Step) -> Self {
        if let Some(trail) = &mut self.trail {
            trail.push(step);
        }
        self
    }
}

/// `Err(fault())` unless `ok`.
fn ensure(ok: bool, fault: impl FnOnce() -> Fault) -> Result<(), Fault> {
    ok.then_some(()).ok_or_else(fault)
}

fn bad(detail: impl Into<String>) -> Fault {
    Fault::new("malformed_variant", vec![Part::Text(detail.into())])
}

fn odd(detail: impl Into<String>) -> Fault {
    Fault::new("non_canonical", vec![Part::Text(detail.into())])
}

fn shape(detail: impl Into<String>) -> Fault {
    Fault::new("invalid_shredding", vec![Part::Text(detail.into())])
}

/// What one call read: each row as tagged JSON, if asked, up to the first
/// fault, and that fault with its row (None: the storage's type).
#[derive(Debug, Default)]
pub struct Decoded {
    pub rows: Vec<String>,
    pub fault: Option<(Option<usize>, Fault)>,
}

/// One value from its metadata and value bytes (`decode_metadata`, then
/// `decode_value`).
pub fn unshredded<'a>(metadata: &'a [u8], value: &'a [u8], o: &Options) -> Result<Value<'a>, Fault> {
    value::value(value, &value::metadata(metadata, o.strict, o.canonical)?, o, 0)
}

/// Reads every row of storage `chunks`, each with its field (an extension
/// type is in the field's metadata), stopping at the first fault.
pub fn decode(chunks: &[(ArrayRef, FieldRef)], o: &Options) -> Decoded {
    let mut out = Decoded::default();
    out.fault = read(chunks, o, &mut out.rows).err();
    out
}

fn read(chunks: &[(ArrayRef, FieldRef)], o: &Options, rows: &mut Vec<String>) -> Result<(), (Option<usize>, Fault)> {
    let mut row = 0;
    for (chunk, field) in chunks {
        let storage = storage::Storage::new(chunk, field).map_err(|fault| (None, Fault { trail: None, ..fault }))?;
        for index in 0..chunk.len() {
            let detail = || vec![Part::Text("SQL NULL into a NOT NULL column".into())];
            let null = || Fault { reason: "sql_null_in_not_null", detail: detail(), trail: None };
            let value = ensure(o.nullable || chunk.is_valid(index), null).and_then(|_| storage.row(index, o));
            let value = value.map_err(|fault| (Some(row), fault))?;
            if o.tagged {
                rows.push(tagged(value.as_ref()));
            }
            row += 1;
        }
    }
    Ok(())
}
