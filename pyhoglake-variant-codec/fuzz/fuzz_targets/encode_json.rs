//! Any bytes as one row: no panic, one row out, and a row written reads
//! back through arrow-rs as serde_json reads the text (tests/oracle);
//! shredded, no panic and the same verdict. Both read back through the
//! codec's decoder as canonical storage of the same value.
#![no_main]

use std::sync::{Arc, LazyLock};

use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::{DataType, Field, Fields};
use libfuzzer_sys::fuzz_target;
use pyhoglake_variant_codec::decode;
use pyhoglake_variant_codec::encode::{encode_json, Options, Plan};

/// `{a: int64, b: string, c: array<double>, d: {e: boolean}, f: variant}`.
static SHREDDED: LazyLock<Plan> = LazyLock::new(|| {
    let group = |typed: Option<DataType>| {
        let value = Field::new("value", DataType::Binary, true);
        DataType::Struct(Fields::from_iter(
            [value].into_iter().chain(typed.map(|t| Field::new("typed_value", t, true))),
        ))
    };
    let object = |fields: Vec<(&str, DataType)>| {
        DataType::Struct(Fields::from_iter(fields.into_iter().map(|(name, kind)| Field::new(name, kind, false))))
    };
    let list = DataType::List(Arc::new(Field::new("element", group(Some(DataType::Float64)), false)));
    let root = group(Some(object(vec![
        ("a", group(Some(DataType::Int64))),
        ("b", group(Some(DataType::Utf8))),
        ("c", group(Some(list))),
        ("d", group(Some(object(vec![("e", group(Some(DataType::Boolean)))])))),
        ("f", group(None)),
    ])));
    let DataType::Struct(root) = root else { unreachable!() };
    let metadata = Arc::new(Field::new("metadata", DataType::Binary, false));
    Plan::new(&DataType::Struct(Fields::from_iter([metadata].into_iter().chain(root.iter().cloned())))).unwrap()
});

#[path = "../../tests/oracle/mod.rs"]
mod oracle;

fuzz_target!(|data: &[u8]| {
    let rows: ArrayRef = Arc::new(BinaryArray::from(vec![Some(data)]));
    let options = Options {
        nullable: true,
        stop_at_invalid: false,
        chunk_rows: 8192,
        chunk_bytes: 512 * 1024 * 1024,
        max_int_digits: 4300,
        trailing_comma_message: true,
        escape_needs_tail: true,
    };
    let out = encode_json(std::slice::from_ref(&rows), &Plan::default(), &options).expect("binary input");
    assert_eq!(out.chunks.iter().map(Array::len).sum::<usize>(), 1);
    let shredded = encode_json(&[rows], &SHREDDED, &options).expect("binary input");
    assert_eq!(shredded.invalid.is_empty(), out.invalid.is_empty());
    let o = decode::Options { strict: true, canonical: true, nullable: true, tagged: true, signaling_nan: false };
    let read = |chunk: &arrow_array::StructArray| {
        let field = Arc::new(Field::new("", chunk.data_type().clone(), true));
        decode::decode(&[(Arc::new(chunk.clone()) as ArrayRef, field)], &o)
    };
    let (plain, shredded) = (read(&out.chunks[0]), read(&shredded.chunks[0]));
    assert!(plain.fault.is_none() && shredded.fault.is_none(), "{:?} {:?}", plain.fault, shredded.fault);
    assert_eq!(plain.rows, shredded.rows);
    if out.invalid.is_empty() {
        let child = |i: usize| out.chunks[0].column(i).as_any().downcast_ref::<BinaryArray>().unwrap().value(0);
        oracle::check(child(0), child(1), data);
    }
});
