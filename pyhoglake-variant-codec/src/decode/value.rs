//! The metadata and value decoders (`decode_metadata`, `decode_value`):
//! the spec's rules, `strict`'s writer rules and `canonical`'s encoding,
//! each fault where the Python decoder finds it.

use super::{bad, ensure, odd, Fault, Options, Part, Step};
use crate::consts::MAX_DEPTH;

/// A decoded value, as `to_python` gives it. A Variant null is `Null`
/// (`VARIANT_NULL` at the top); an integer of any width is `Int`, a float
/// or double `Float`; a date or timestamp outside the years 1 to 9999 is
/// a pyarrow scalar there, as nanosecond timestamps always are.
#[derive(Clone, Debug, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// Unscaled, and the scale: a `Decimal` whose exponent is minus it.
    Decimal(i128, u8),
    Date(i32),
    /// Microseconds since the epoch; whether adjusted to UTC.
    Timestamp(i64, bool),
    TimestampNanos(i64, bool),
    Time(i64),
    Str(&'a str),
    Bytes(&'a [u8]),
    Uuid(&'a [u8]),
    Object(Vec<(&'a str, Value<'a>)>),
    Array(Vec<Value<'a>>),
}

/// The days and microseconds since the epoch Python's years 1 to 9999 hold.
pub const DAYS: std::ops::RangeInclusive<i32> = -719_162..=2_932_896;
pub const MICROS: std::ops::RangeInclusive<i64> = -62_135_596_800_000_000..=253_402_300_799_999_999;

/// The fewest bytes, 1 to 4, of an unsigned field that holds `n`.
pub fn smallest(n: usize) -> usize {
    1 + usize::from(n > 0xFF) + usize::from(n > 0xFFFF) + usize::from(n > 0xFF_FFFF)
}

/// `width` little-endian bytes at `pos`, which must end by `end`.
fn uint(data: &[u8], pos: usize, width: usize, end: usize) -> Result<usize, Fault> {
    ensure(pos.checked_add(width).is_some_and(|stop| stop <= end), || bad("truncated"))?;
    Ok(data[pos..pos + width].iter().rev().fold(0, |n, &b| n << 8 | usize::from(b)))
}

/// `count + 1` fields of `width` bytes, saturating: positions past the
/// data are refused as truncated.
fn span(count: usize, width: usize) -> usize {
    count.saturating_add(1).saturating_mul(width)
}

fn signed(bytes: &[u8]) -> i128 {
    let negative = bytes.last().is_some_and(|&b| b & 0x80 != 0);
    bytes.iter().rev().fold(if negative { -1 } else { 0 }, |n, &b| n << 8 | i128::from(b))
}

pub fn text(raw: &[u8]) -> Result<&str, Fault> {
    std::str::from_utf8(raw).map_err(|_| bad("a string is not UTF-8"))
}

/// A time of day (VE:431); Trino shreds only those, too.
pub fn time_of_day<'a>(micros: i64) -> Result<Value<'a>, Fault> {
    let detail = || bad(format!("a time of {micros} microseconds is not a time of day"));
    ensure((0..86_400_000_000).contains(&micros), detail).map(|_| Value::Time(micros))
}

/// The encoder's bound over the whole value: an empty container may sit
/// at the deepest level, and nothing below it. It is the reader's limit
/// (`nesting_too_deep`), which keeps its recursion bounded.
pub fn check_depth(count: usize, depth: usize) -> Result<(), Fault> {
    let detail =
        || Fault::new("nesting_too_deep", vec![Part::Text(format!("nested more than {MAX_DEPTH} levels deep"))]);
    ensure(count == 0 || depth < MAX_DEPTH, detail)
}

/// The dictionary of a metadata buffer (VE:74-146).
pub fn metadata(data: &[u8], strict: bool, canonical: bool) -> Result<Vec<&str>, Fault> {
    let header = *data.first().ok_or_else(|| bad("the metadata is empty"))?;
    ensure(header & 0x0F == 1, || bad(format!("metadata version {}, not 1", header & 0x0F)))?;
    let (sorted, width, end) = (header & 0x10 != 0, usize::from(header >> 6) + 1, data.len());
    let count = uint(data, 1, width, end)?;
    ensure(span(count, width) <= end - (1 + width), || bad("the metadata is truncated"))?;
    let offsets = (0..=count).map(|i| uint(data, 1 + width + i * width, width, end)).collect::<Result<Vec<_>, _>>()?;
    let base = 1 + width + (count + 1) * width;
    let climbs = offsets[0] == 0 && offsets.windows(2).all(|pair| pair[0] <= pair[1]);
    ensure(climbs, || bad("the metadata offsets do not climb from 0"))?;
    ensure(base + offsets[count] == end, || bad("the metadata length does not match its offsets"))?;
    let raw: Vec<&[u8]> = offsets.windows(2).map(|pair| &data[base + pair[0]..base + pair[1]]).collect();
    let names = raw.iter().map(|name| std::str::from_utf8(name)).collect::<Result<Vec<_>, _>>();
    let names = names.map_err(|_| bad("a metadata string is not UTF-8"))?;
    // A writer's fault only: readers look names up by id, never by the flag.
    let unsorted = (strict || canonical) && sorted && raw.windows(2).any(|pair| pair[0] >= pair[1]);
    ensure(!unsorted, || bad("sorted_strings is set, but the strings are not sorted and unique"))?;
    if canonical {
        // Bit 5 is reserved and readers ignore it (VE:90): a second spelling.
        ensure(header & 0x20 == 0, || odd("the metadata header sets a reserved bit"))?;
        ensure(sorted == (count > 0), || odd("sorted_strings is not set exactly when there are strings"))?;
        let smallest = width == smallest(offsets[count].max(count));
        ensure(smallest, || odd("the metadata offset width is not the smallest"))?;
    }
    Ok(names)
}

/// One value buffer, wholly, at `depth` of the whole value.
pub fn value<'a>(data: &'a [u8], names: &[&'a str], o: &Options, depth: usize) -> Result<Value<'a>, Fault> {
    let (strict, canonical, signaling) = (o.strict || o.canonical, o.canonical, o.signaling_nan);
    let reader = Reader { data, names, strict, canonical, signaling };
    let (value, end) = reader.decode(0, data.len(), depth)?;
    ensure(end == data.len(), || bad(format!("{} bytes follow the value", data.len() - end)))?;
    Ok(value)
}

struct Reader<'a, 'n> {
    data: &'a [u8],
    names: &'n [&'a str],
    strict: bool,
    canonical: bool,
    signaling: bool,
}

impl<'a> Reader<'a, '_> {
    fn decode(&self, pos: usize, end: usize, depth: usize) -> Result<(Value<'a>, usize), Fault> {
        ensure(pos < end, || bad("truncated"))?;
        let head = usize::from(self.data[pos] >> 2);
        match self.data[pos] & 0x03 {
            1 => {
                let stop = pos + 1 + head;
                ensure(stop <= end, || bad("a short string is truncated"))?;
                Ok((Value::Str(text(&self.data[pos + 1..stop])?), stop))
            }
            0 => self.primitive(pos + 1, end, head),
            2 => self.object(pos, end, head, depth),
            _ => self.array(pos, end, head, depth),
        }
    }

    fn primitive(&self, body: usize, end: usize, type_id: usize) -> Result<(Value<'a>, usize), Fault> {
        let fixed = |size: usize, what: &str| {
            ensure(body + size <= end, || bad(format!("a {what} is truncated")))?;
            Ok((&self.data[body..body + size], body + size))
        };
        Ok(match type_id {
            0 => (Value::Null, body),
            1 | 2 => (Value::Bool(type_id == 1), body),
            3..=6 => {
                let (raw, stop) = fixed(1 << (type_id - 3), "primitive")?;
                let raw = signed(raw) as i64;
                let fits = |bytes: u32| (-1..=0).contains(&(raw >> (8 * bytes - 1)));
                let smallest = [1, 2, 4].iter().position(|&bytes| fits(bytes)).map_or(6, |at| at + 3);
                let detail = || odd(format!("the integer {raw} is not in its smallest width"));
                ensure(!self.canonical || smallest == type_id, detail)?;
                (Value::Int(raw), stop)
            }
            7 | 11..=14 | 17..=19 => {
                let (raw, stop) = fixed(if matches!(type_id, 11 | 14) { 4 } else { 8 }, "primitive")?;
                let never = self.canonical && matches!(type_id, 14 | 18 | 19);
                ensure(!never, || odd(format!("primitive type {type_id}, which pyhoglake never writes")))?;
                let raw = signed(raw);
                let value = match type_id {
                    7 => Value::Float(f64::from_bits(raw as u64)),
                    14 => {
                        // Widening quiets a signaling NaN; CPython 3.14's struct
                        // keeps it signaling (gh-130317), as `signaling` says.
                        let wide = f64::from(f32::from_bits(raw as u32));
                        let signals = self.signaling && wide.is_nan() && raw & 1 << 22 == 0;
                        Value::Float(if signals { f64::from_bits(wide.to_bits() & !(1 << 51)) } else { wide })
                    }
                    11 => Value::Date(raw as i32),
                    12 | 13 => Value::Timestamp(raw as i64, type_id == 12),
                    18 | 19 => Value::TimestampNanos(raw as i64, type_id == 18),
                    _ => time_of_day(raw as i64)?,
                };
                (value, stop)
            }
            8..=10 => {
                let (size, most) = [(4, 9), (8, 18), (16, 38)][type_id - 8];
                let (raw, stop) = fixed(size + 1, "decimal")?;
                // The scale is 0 to 38 at every width (VE:422-424), and each
                // width holds 9, 18 or 38 digits (VE:442-446): more is
                // malformed, as Spark's checkDecimal has it.
                ensure(raw[0] <= 38, || bad(format!("a decimal has scale {}", raw[0])))?;
                let unscaled = signed(&raw[1..]);
                let digits = unscaled.unsigned_abs().to_string().len();
                ensure(digits <= most, || bad(format!("a decimal{size} holds {digits} digits, beyond its {most}")))?;
                let precision = digits.max(usize::from(raw[0]));
                let width = 8 + usize::from(precision > 9) + usize::from(precision > 18);
                let detail = || odd(format!("a decimal of precision {precision} is not in its smallest width"));
                ensure(!self.canonical || type_id == width, detail)?;
                (Value::Decimal(unscaled, raw[0]), stop)
            }
            15 | 16 => {
                let size = uint(self.data, body, 4, end)?;
                let stop = body + 4 + size;
                ensure(stop <= end, || bad("a binary or string is truncated"))?;
                let raw = &self.data[body + 4..stop];
                if type_id == 15 {
                    return Ok((Value::Bytes(raw), stop));
                }
                ensure(!self.canonical || size >= 64, || odd("a string of under 64 bytes is not a short string"))?;
                (Value::Str(text(raw)?), stop)
            }
            20 => fixed(16, "uuid").map(|(raw, stop)| (Value::Uuid(raw), stop))?,
            _ => return Err(bad(format!("unknown primitive type {type_id}"))),
        })
    }

    /// The header past `at`: where the values start, and the `count + 1`
    /// offsets; the values must end by `end`.
    fn offsets(
        &self,
        at: usize,
        count: usize,
        width: usize,
        end: usize,
        what: &str,
    ) -> Result<(usize, Vec<usize>), Fault> {
        let base = at.saturating_add(span(count, width));
        ensure(base <= end, || bad(format!("an {what} header is truncated")))?;
        let offsets =
            (0..=count).map(|i| uint(self.data, at + i * width, width, end)).collect::<Result<Vec<_>, _>>()?;
        ensure(base + offsets[count] <= end, || bad(format!("an {what} is truncated")))?;
        Ok((base, offsets))
    }

    /// Reserved bits clear, `is_large` exactly above 255 elements, the
    /// smallest offset width (VE:210, 228).
    fn canonical_container(
        &self,
        reserved: usize,
        large: bool,
        count: usize,
        width: usize,
        last: usize,
    ) -> Result<(), Fault> {
        ensure(reserved == 0, || odd("a container header sets a reserved bit"))?;
        ensure(large == (count > 255), || odd("is_large is not set exactly above 255 elements"))?;
        ensure(width == smallest(last), || odd("the offset width is not the smallest"))
    }

    fn object(&self, pos: usize, end: usize, head: usize, depth: usize) -> Result<(Value<'a>, usize), Fault> {
        let (large, id_width, offset_width) = (head >> 4 & 1 == 1, (head >> 2 & 0x03) + 1, (head & 0x03) + 1);
        let count_width = if large { 4 } else { 1 };
        let count = uint(self.data, pos + 1, count_width, end)?;
        check_depth(count, depth)?;
        let ids_at = pos + 1 + count_width;
        let offsets_at = ids_at.saturating_add(count.saturating_mul(id_width));
        let (base, offsets) = self.offsets(offsets_at, count, offset_width, end, "object")?;
        let ids =
            (0..count).map(|i| uint(self.data, ids_at + i * id_width, id_width, end)).collect::<Result<Vec<_>, _>>()?;
        let stop = base + offsets[count];
        ensure(ids.iter().all(|&id| id < self.names.len()), || bad("an object field id is beyond the metadata"))?;
        let keys: Vec<&'a str> = ids.iter().map(|&id| self.names[id]).collect();
        // Sorted and unique by UTF-8 bytes (VE:456, 465); a reader takes
        // fields in any order (DuckDB 1.5.5 lists them as inserted), but
        // never one name twice.
        let in_order = keys.windows(2).all(|pair| pair[0] < pair[1]);
        if !in_order {
            ensure(!self.strict, || bad("object keys are not sorted and unique"))?;
            let mut seen = std::collections::HashSet::new();
            if let Some(repeated) = keys.iter().find(|key| !seen.insert(**key)) {
                let detail =
                    vec![Part::Text("an object has two fields named ".into()), Part::Quoted((*repeated).into())];
                return Err(Fault::new("malformed_variant", detail));
            }
        }
        // The values lie in any order, but cover the bytes from offset 0 to
        // the last offset, each ending where the next one up begins (VE:
        // 293-296): no value shared, no bytes hidden.
        let mut starts = offsets[..count].to_vec();
        starts.sort_unstable();
        ensure(starts.windows(2).all(|pair| pair[0] < pair[1]), || bad("two object fields start at one offset"))?;
        ensure(count > 0 || offsets[0] == 0, || bad("an empty object's last offset is not 0"))?;
        ensure(starts.first().is_none_or(|&first| first == 0), || bad("no object field starts at offset 0"))?;
        if self.canonical {
            self.canonical_container(head & 0x20, large, count, offset_width, offsets[count])?;
            let narrow = id_width == smallest(ids.iter().copied().max().unwrap_or(0));
            ensure(narrow, || odd("the field id width is not the smallest"))?;
        }
        let mut fields = Vec::with_capacity(count);
        for (index, &key) in keys.iter().enumerate() {
            let start = offsets[index];
            let field = || {
                let (value, value_end) = self.decode(base + start, stop, depth + 1)?;
                let next = starts.get(starts.partition_point(|&s| s <= start)).copied().unwrap_or(offsets[count]);
                ensure(value_end == base + next, || bad("an object field does not end where the next value begins"))?;
                let laid = !self.canonical || value_end == base + offsets[index + 1];
                ensure(laid, || odd("object values are not laid out in key order")).map(|_| value)
            };
            fields.push((key, field().map_err(|fault| fault.step(Step::Key(key.into())))?));
        }
        if !in_order {
            fields.sort_unstable_by(|a, b| a.0.cmp(b.0));
        }
        Ok((Value::Object(fields), stop))
    }

    fn array(&self, pos: usize, end: usize, head: usize, depth: usize) -> Result<(Value<'a>, usize), Fault> {
        let (large, offset_width) = (head >> 2 & 1 == 1, (head & 0x03) + 1);
        let count_width = if large { 4 } else { 1 };
        let count = uint(self.data, pos + 1, count_width, end)?;
        check_depth(count, depth)?;
        let (base, offsets) = self.offsets(pos + 1 + count_width, count, offset_width, end, "array")?;
        // Offsets count from the first byte of the first element (VE:349).
        let first = if count > 0 {
            "the first array element does not start at offset 0"
        } else {
            "an empty array's last offset is not 0"
        };
        ensure(offsets[0] == 0, || bad(first))?;
        if self.canonical {
            self.canonical_container(head & 0x38, large, count, offset_width, offsets[count])?;
        }
        let item = |index: usize| {
            let (value, value_end) = self.decode(base + offsets[index], base + offsets[count], depth + 1)?;
            let ends = value_end == base + offsets[index + 1];
            ensure(ends, || bad("an array element does not end at the next offset")).map(|_| value)
        };
        let items = (0..count).map(|index| item(index).map_err(|fault| fault.step(Step::Index(index))));
        Ok((Value::Array(items.collect::<Result<_, _>>()?), base + offsets[count]))
    }
}
