//! Any bytes as one row: no panic, one row out, and a row written reads
//! back through arrow-rs as serde_json reads the text (tests/oracle).
#![no_main]

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray};
use libfuzzer_sys::fuzz_target;
use pyhoglake_variant_codec::encode::{encode_json, Options};

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
    let out = encode_json(&[rows], &options).expect("binary input");
    assert_eq!(out.chunks.iter().map(Array::len).sum::<usize>(), 1);
    if out.invalid.is_empty() {
        let child = |i: usize| out.chunks[0].column(i).as_any().downcast_ref::<BinaryArray>().unwrap().value(0);
        oracle::check(child(0), child(1), data);
    }
});
