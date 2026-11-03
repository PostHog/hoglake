//! Any bytes as a metadata and a value (the first byte says where the
//! value starts): no panic; read in every mode alike where each reads,
//! canonical within strict within lenient; as arrow-rs reads them where it
//! reads them too (tests/oracle); tagged as JSON; and as one row of
//! unshredded storage, the same.
#![no_main]

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, StructArray};
use arrow_schema::{DataType, Field, Fields};
use libfuzzer_sys::fuzz_target;
use parquet_variant::Variant;
use pyhoglake_variant_codec::decode::{self, unshredded, Options};

#[path = "../../tests/oracle/mod.rs"]
mod oracle;

fn chrono_safe(value: &decode::Value) -> bool {
    match value {
        decode::Value::Date(days) => days.unsigned_abs() < 90_000_000,
        decode::Value::Object(fields) => fields.iter().all(|(_, value)| chrono_safe(value)),
        decode::Value::Array(items) => items.iter().all(chrono_safe),
        _ => true,
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&split, rest)) = data.split_first() else { return };
    let (metadata, value) = rest.split_at(usize::from(split).min(rest.len()));
    let modes = [(false, false), (true, false), (true, true)];
    let options = modes.map(|(strict, canonical)| Options {
        strict,
        canonical,
        nullable: true,
        tagged: true,
        signaling_nan: false,
    });
    let read = [0, 1, 2].map(|mode| unshredded(metadata, value, &options[mode]));
    assert!(read[2].is_err() || read[1].is_ok());
    assert!(read[1].is_err() || read[0].is_ok());
    if let Ok(lenient) = &read[0] {
        // As tags: floats by their bits, NaN too.
        for other in read.iter().flatten() {
            assert_eq!(decode::tagged(Some(other)), decode::tagged(Some(lenient)));
        }
        // arrow-rs 59 panics on a date beyond chrono's years (`decode_date`).
        let theirs = chrono_safe(lenient).then(|| Variant::try_new(metadata, value).ok()).flatten();
        if let Some(theirs) = theirs {
            assert!(oracle::same_value(lenient, &theirs), "{lenient:?} vs {theirs:?}");
        }
        let tagged = decode::tagged(Some(lenient));
        let mut json = serde_json::Deserializer::from_str(&tagged);
        json.disable_recursion_limit();
        let parsed: Vec<Result<serde_json::Value, _>> = json.into_iter().collect();
        assert!(parsed.len() == 1 && parsed[0].is_ok(), "{tagged}");
    }
    let fields = Fields::from(vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
    ]);
    let columns: Vec<ArrayRef> =
        vec![Arc::new(BinaryArray::from(vec![metadata])), Arc::new(BinaryArray::from(vec![value]))];
    let storage: ArrayRef = Arc::new(StructArray::new(fields.clone(), columns, None));
    let field = Arc::new(Field::new("", DataType::Struct(fields), true));
    for ((o, bytes), strict) in options.iter().zip(&read).zip([false, true, true]) {
        let row = decode::decode(&[(storage.clone(), field.clone())], o);
        match bytes {
            // Strict, a row also uses every key in its dictionary's keys.
            Ok(value) => assert!(row.fault.is_some() && strict || row.rows == [decode::tagged(Some(value))]),
            Err(fault) => assert_eq!(row.fault.map(|(_, f)| f), Some(fault.clone())),
        }
    }
});
