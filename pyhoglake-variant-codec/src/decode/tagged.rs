//! A row as tagged JSON (design §3, R5): one tag per Python type that
//! `to_python` returns, with the value exactly, so two decoders' rows
//! compare as data. SQL NULL is `null`, as is a Variant null inside a value;
//! at the top it is `["variant_null"]`. Then `["bool", b]`, `["int", n]`,
//! `["float", bits]` (as a double), `["decimal", unscaled, scale]`,
//! `["str", s]`, `["bytes", hex]`, `["uuid", hex]`, `["date", days]`,
//! `["time", micros]`, `["datetime", micros, aware]` (since the epoch),
//! `["dict", [[key, v], ...]]` in key order, `["list", [v, ...]]`, and a
//! pyarrow scalar as `["scalar", str(type), raw]`: a nanosecond timestamp,
//! or a date or timestamp outside the years 1 to 9999.

use super::value::{Value, DAYS, MICROS};

pub fn tagged(row: Option<&Value>) -> String {
    let mut out = String::new();
    match row {
        None => out.push_str("null"),
        Some(value) => write(value, true, &mut out),
    }
    out
}

fn write(value: &Value, top: bool, out: &mut String) {
    let hex = |bytes: &[u8]| format!("\"{}\"", bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>());
    let utc = |aware: bool| if aware { ", tz=UTC" } else { "" };
    let body = match *value {
        Value::Null => return out.push_str(if top { "[\"variant_null\"]" } else { "null" }),
        Value::Bool(b) => format!("\"bool\",{b}"),
        Value::Int(n) => format!("\"int\",{n}"),
        Value::Float(x) => format!("\"float\",{}", x.to_bits()),
        Value::Decimal(unscaled, scale) => format!("\"decimal\",{unscaled},{scale}"),
        Value::Str(s) => format!("\"str\",{}", string(s)),
        Value::Bytes(b) => format!("\"bytes\",{}", hex(b)),
        Value::Uuid(b) => format!("\"uuid\",{}", hex(b)),
        Value::Date(days) if DAYS.contains(&days) => format!("\"date\",{days}"),
        Value::Date(days) => format!("\"scalar\",\"date32[day]\",{days}"),
        Value::Time(micros) => format!("\"time\",{micros}"),
        Value::Timestamp(micros, aware) if MICROS.contains(&micros) => format!("\"datetime\",{micros},{aware}"),
        Value::Timestamp(micros, aware) => format!("\"scalar\",\"timestamp[us{}]\",{micros}", utc(aware)),
        Value::TimestampNanos(nanos, aware) => format!("\"scalar\",\"timestamp[ns{}]\",{nanos}", utc(aware)),
        Value::Object(ref fields) => {
            out.push_str("[\"dict\",[");
            for (i, (key, item)) in fields.iter().enumerate() {
                out.push_str(&format!("{}[{},", if i > 0 { "," } else { "" }, string(key)));
                write(item, false, out);
                out.push(']');
            }
            return out.push_str("]]");
        }
        Value::Array(ref items) => {
            out.push_str("[\"list\",[");
            for (i, item) in items.iter().enumerate() {
                out.push_str(if i > 0 { "," } else { "" });
                write(item, false, out);
            }
            return out.push_str("]]");
        }
    };
    out.push_str(&format!("[{body}]"));
}

/// A JSON string.
fn string(s: &str) -> String {
    let escaped = s.chars().map(|c| match c {
        '"' | '\\' => format!("\\{c}"),
        c if u32::from(c) < 0x20 => format!("\\u{:04x}", u32::from(c)),
        c => c.to_string(),
    });
    format!("\"{}\"", escaped.collect::<String>())
}
