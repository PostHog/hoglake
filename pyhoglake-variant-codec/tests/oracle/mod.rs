//! arrow-rs `parquet-variant` as an independent decoder (design §4.6): a
//! written row reads back as serde_json reads its text. serde_json cannot
//! judge duplicate keys or lone surrogates, and a written row has neither.
//! And the codec's own decoder held to arrow-rs, value for value. Shared by
//! the tests and the fuzz targets, each of which uses part of it.
#![allow(dead_code)]

use chrono::{NaiveTime, Timelike};
use parquet_variant::{Variant, VariantMetadata};
use pyhoglake_variant_codec::decode;
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

/// Whether the codec's decoder read the value arrow-rs reads: types by
/// family (an int of any width, a float as its double), objects by name.
pub fn same_value(ours: &decode::Value, theirs: &Variant) -> bool {
    use decode::Value as V;
    let micros = |time: &NaiveTime| {
        i64::from(time.num_seconds_from_midnight()) * 1_000_000 + i64::from(time.nanosecond() / 1000)
    };
    match (ours, theirs) {
        (V::Null, Variant::Null) => true,
        (&V::Bool(b), Variant::BooleanTrue) => b,
        (&V::Bool(b), Variant::BooleanFalse) => !b,
        (&V::Int(n), Variant::Int8(x)) => n == i64::from(*x),
        (&V::Int(n), Variant::Int16(x)) => n == i64::from(*x),
        (&V::Int(n), Variant::Int32(x)) => n == i64::from(*x),
        (&V::Int(n), Variant::Int64(x)) => n == *x,
        (&V::Float(x), Variant::Double(y)) => x.to_bits() == y.to_bits(),
        (&V::Float(x), Variant::Float(y)) => x.to_bits() == f64::from(*y).to_bits(),
        (&V::Decimal(n, s), Variant::Decimal4(d)) => n == i128::from(d.integer()) && s == d.scale(),
        (&V::Decimal(n, s), Variant::Decimal8(d)) => n == i128::from(d.integer()) && s == d.scale(),
        (&V::Decimal(n, s), Variant::Decimal16(d)) => n == d.integer() && s == d.scale(),
        (&V::Date(days), Variant::Date(d)) => {
            Some(i64::from(days) * 86_400) == d.and_hms_opt(0, 0, 0).map(|t| t.and_utc().timestamp())
        }
        (&V::Timestamp(m, true), Variant::TimestampMicros(t)) => m == t.timestamp_micros(),
        (&V::Timestamp(m, false), Variant::TimestampNtzMicros(t)) => m == t.and_utc().timestamp_micros(),
        (&V::TimestampNanos(n, true), Variant::TimestampNanos(t)) => Some(n) == t.timestamp_nanos_opt(),
        (&V::TimestampNanos(n, false), Variant::TimestampNtzNanos(t)) => Some(n) == t.and_utc().timestamp_nanos_opt(),
        (&V::Time(m), Variant::Time(t)) => m == micros(t),
        (V::Str(s), Variant::String(t)) => s == t,
        (V::Str(s), Variant::ShortString(t)) => *s == t.as_str(),
        (V::Bytes(b), Variant::Binary(c)) => b == c,
        (V::Uuid(b), Variant::Uuid(u)) => *b == u.as_bytes(),
        (V::Object(fields), Variant::Object(object)) => {
            fields.len() == object.len()
                && fields.iter().all(|(name, value)| object.get(name).is_some_and(|theirs| same_value(value, &theirs)))
        }
        (V::Array(items), Variant::List(list)) => {
            items.len() == list.len() && items.iter().zip(list.iter()).all(|(value, theirs)| same_value(value, &theirs))
        }
        _ => false,
    }
}
