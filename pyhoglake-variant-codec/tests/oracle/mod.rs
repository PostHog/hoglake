//! arrow-rs `parquet-variant` as an independent decoder (design §4.6): a
//! written row reads back as serde_json reads its text. serde_json cannot
//! judge duplicate keys or lone surrogates, and a written row has neither.
//! Shared by `tests/encode.rs` and the fuzz target.

use parquet_variant::{Variant, VariantMetadata};
use serde_json::Value;

/// Panics unless `metadata` and `value` are a valid Variant of the JSON
/// `text`, its dictionary sorted and unique.
pub fn check(metadata: &[u8], value: &[u8], text: &[u8]) {
    let dictionary = VariantMetadata::try_new(metadata).expect("valid metadata");
    let names: Vec<&str> = dictionary.iter().collect();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]) && (names.is_empty() || dictionary.is_sorted()));
    let decoded = Variant::try_new(metadata, value).expect("a valid Variant");
    let mut reader = serde_json::Deserializer::from_slice(text);
    reader.disable_recursion_limit();
    let expected: Value = reader.into_iter().next().expect("a value").expect("text serde_json reads");
    same(&decoded, &expected, &String::from_utf8_lossy(text));
}

fn same(got: &Variant, want: &Value, text: &str) {
    match (got, want) {
        (Variant::Null, Value::Null)
        | (Variant::BooleanTrue, Value::Bool(true))
        | (Variant::BooleanFalse, Value::Bool(false)) => {}
        (Variant::String(s), Value::String(w)) => assert_eq!(s, w, "{text}"),
        (Variant::ShortString(s), Value::String(w)) => assert_eq!(s.as_str(), w, "{text}"),
        (Variant::Double(x), Value::Number(n)) => {
            assert_eq!(x.to_bits(), n.to_string().parse::<f64>().unwrap().to_bits(), "{text}")
        }
        (
            Variant::Int8(_) | Variant::Int16(_) | Variant::Int32(_) | Variant::Int64(_) | Variant::Decimal16(_),
            Value::Number(n),
        ) => {
            let got = match got {
                Variant::Int8(v) => i128::from(*v),
                Variant::Int16(v) => i128::from(*v),
                Variant::Int32(v) => i128::from(*v),
                Variant::Int64(v) => i128::from(*v),
                Variant::Decimal16(d) if d.scale() == 0 => d.integer(),
                _ => panic!("{got:?} for {n} in {text}"),
            };
            assert_eq!(got, n.to_string().parse::<i128>().unwrap(), "{text}");
        }
        (Variant::List(list), Value::Array(items)) => {
            assert_eq!(list.len(), items.len(), "{text}");
            list.iter().zip(items).for_each(|(g, w)| same(&g, w, text));
        }
        (Variant::Object(object), Value::Object(map)) => {
            assert_eq!(object.len(), map.len(), "{text}");
            for (name, g) in object.iter() {
                same(&g, &map[name], text);
            }
        }
        _ => panic!("{got:?} is not {want} in {text}"),
    }
}
