//! Storage read back (`_Reassembly`, VS:286-334): any conformant layout,
//! pyhoglake's, Trino's or DuckDB's (OPTIONAL field and element groups),
//! with any of pyarrow's binary, string and list layouts. Columns are found
//! by name. The readers are built, and the storage's shape checked, in the
//! order the Python codec builds its readers; then each row reads.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, Time64MicrosecondType, TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, FieldRef, Fields, TimeUnit};

use super::value::{self, Value, DAYS, MICROS};
use super::{bad, ensure, odd, shape, Fault, Options, Part, Step};
use crate::consts::MAX_DEPTH;

fn text(detail: &str) -> Part {
    Part::Text(detail.into())
}

fn with<T: Clone>(path: &[T], step: T) -> Vec<T> {
    [path, &[step]].concat()
}

fn refused(detail: Vec<Part>) -> Fault {
    Fault::new("invalid_shredding", detail)
}

/// An extension type is no plain type: pyarrow tests it as itself.
fn plain(field: &Field) -> bool {
    !field.metadata().contains_key("ARROW:extension:name")
}

/// A group's column names, refusing one it repeats: storage is read by name
/// (VS:54), and two shredded fields cannot share one.
fn unique_names(fields: &Fields) -> Result<(), Fault> {
    let mut names: Vec<&str> = fields.iter().map(|field| field.name().as_str()).collect();
    names.sort_unstable();
    match names.windows(2).find(|pair| pair[0] == pair[1]) {
        Some(pair) => Err(refused(vec![text("a group has two columns named "), Part::Quoted(pair[0].into())])),
        None => Ok(()),
    }
}

/// A list's offsets, values and element field, at either offset width.
fn list(array: &ArrayRef) -> Option<(Vec<usize>, &ArrayRef, &FieldRef)> {
    match array.data_type() {
        DataType::List(element) => {
            let list = array.as_list::<i32>();
            Some((list.value_offsets().iter().map(|&at| at as usize).collect(), list.values(), element))
        }
        DataType::LargeList(element) => {
            let list = array.as_list::<i64>();
            Some((list.value_offsets().iter().map(|&at| at as usize).collect(), list.values(), element))
        }
        _ => None,
    }
}

/// The bytes of a binary column in any of its layouts, or of a string
/// column read as binary (Arrow does not check that it is UTF-8).
enum Bin {
    Small(BinaryArray),
    Large(LargeBinaryArray),
    View(BinaryViewArray),
}

impl Bin {
    fn of(array: &ArrayRef, strings: bool) -> Option<Bin> {
        Some(match (array.data_type(), strings) {
            (DataType::Binary, false) => Bin::Small(array.as_binary::<i32>().clone()),
            (DataType::LargeBinary, false) => Bin::Large(array.as_binary::<i64>().clone()),
            (DataType::BinaryView, false) => Bin::View(array.as_binary_view().clone()),
            (DataType::Utf8, true) => Bin::Small(array.as_string::<i32>().clone().into()),
            (DataType::LargeUtf8, true) => Bin::Large(array.as_string::<i64>().clone().into()),
            (DataType::Utf8View, true) => Bin::View(array.as_string_view().clone().to_binary_view()),
            _ => return None,
        })
    }

    fn get(&self, i: usize) -> Option<&[u8]> {
        match self {
            Bin::Small(array) => array.is_valid(i).then(|| array.value(i)),
            Bin::Large(array) => array.is_valid(i).then(|| array.value(i)),
            Bin::View(array) => array.is_valid(i).then(|| array.value(i)),
        }
    }
}

/// `metadata` or `value`, which must be binary.
fn binary_column(array: &ArrayRef, field: &Field, steps: Vec<usize>) -> Result<Bin, Fault> {
    let detail = || vec![Part::Text(format!("{} is ", field.name())), Part::Type(steps), text(", not binary")];
    plain(field).then(|| Bin::of(array, false)).flatten().ok_or_else(|| refused(detail()))
}

pub struct Storage {
    rows: ArrayRef,
    metadata: Bin,
    root: Group,
}

impl Storage {
    pub fn new(chunk: &ArrayRef, field: &Field) -> Result<Storage, Fault> {
        let refused = || shape("VARIANT storage is a struct with a metadata column");
        let fields = match chunk.data_type() {
            DataType::Struct(fields) if plain(field) => fields,
            _ => return Err(refused()),
        };
        unique_names(fields)?;
        let (at, metadata) = fields.find("metadata").ok_or_else(refused)?;
        let metadata = binary_column(chunk.as_struct().column(at), metadata, vec![at])?;
        let root = Group::new(chunk, field, Vec::new(), Vec::new(), 0, true, false)?;
        Ok(Storage { rows: chunk.clone(), metadata, root })
    }

    /// A row: None for SQL NULL, `Null` for a Variant null.
    pub fn row(&self, i: usize, o: &Options) -> Result<Option<Value<'_>>, Fault> {
        if self.rows.is_null(i) {
            return Ok(None);
        }
        let raw = self.metadata.get(i).ok_or_else(|| shape("a present row has no metadata"))?;
        let mut names = value::metadata(raw, o.strict, o.canonical)?;
        // A missing value where one is required reads as Variant null (VS:78);
        // a writer must write 00.
        let missing = || shape("a present row has neither value nor typed_value");
        let value = match self.root.read(i, &names, o)? {
            None => ensure(!o.strict, missing).map(|_| Value::Null)?,
            Some(value) => value,
        };
        if o.strict {
            let mut used = Vec::new();
            keys(&value, &mut used);
            used.sort_unstable();
            used.dedup();
            names.sort_unstable();
            names.dedup();
            if let Some(missing) = used.iter().find(|key| names.binary_search(key).is_err()) {
                return Err(refused(vec![
                    text("key "),
                    Part::Quoted((*missing).into()),
                    text(" is not in the metadata"),
                ]));
            }
            let all = !o.canonical || used.len() == names.len();
            ensure(all, || odd("the metadata holds keys the value does not use"))?;
        }
        Ok(Some(value))
    }
}

fn keys<'a>(value: &Value<'a>, out: &mut Vec<&'a str>) {
    match value {
        Value::Object(fields) => fields.iter().for_each(|(key, item)| {
            out.push(key);
            keys(item, out);
        }),
        Value::Array(items) => items.iter().for_each(|item| keys(item, out)),
        _ => {}
    }
}

/// A `value`/`typed_value` group, whose value sits at `depth` of the whole
/// value: its own validity (the top's is the row's, SQL NULL), its value
/// bytes, and its `typed_value` column and how that reads.
struct Group {
    present: Option<ArrayRef>,
    value: Option<Bin>,
    typed: Option<(ArrayRef, Shape)>,
    depth: usize,
}

impl Group {
    fn new(
        group: &ArrayRef,
        field: &Field,
        steps: Vec<usize>,
        path: Vec<String>,
        depth: usize,
        top: bool,
        element: bool,
    ) -> Result<Group, Fault> {
        let fields = match group.data_type() {
            DataType::Struct(fields) if plain(field) => fields,
            _ => return Err(refused(vec![Part::Column(path), text(" is "), Part::Type(steps), text(", not a group")])),
        };
        unique_names(fields)?;
        let allowed = |name: &&str| matches!(*name, "value" | "typed_value") || top && *name == "metadata";
        if let Some(extra) = fields.iter().map(|field| field.name().as_str()).filter(|name| !allowed(name)).min() {
            return Err(refused(vec![text("a group has a column "), Part::Quoted(extra.into())]));
        }
        let columns = group.as_struct();
        // Arrays may drop value when elements are typed (VS:126).
        ensure(element || fields.find("value").is_some(), || shape("a group has no value column"))?;
        let value = fields.find("value").map(|(at, field)| binary_column(columns.column(at), field, with(&steps, at)));
        let value = value.transpose()?;
        let typed = fields.find("typed_value").map(|(at, field)| {
            let shape =
                Shape::new(columns.column(at), field, with(&steps, at), with(&path, "typed_value".into()), depth);
            shape.map(|shape| (columns.column(at).clone(), shape))
        });
        Ok(Group { present: (!top).then(|| group.clone()), value, typed: typed.transpose()?, depth })
    }

    /// The group's value at row `i`; None where it is missing.
    fn read<'r>(&'r self, i: usize, names: &[&'r str], o: &Options) -> Result<Option<Value<'r>>, Fault> {
        if self.present.as_ref().is_some_and(|present| present.is_null(i)) {
            return ensure(!o.strict, || shape("a field or element group is null")).map(|_| None);
        }
        let raw = self.value.as_ref().and_then(|value| value.get(i));
        if let Some(value) = self.typed.as_ref().map(|typed| self.typed(typed, i, names, raw, o)).transpose()?.flatten()
        {
            return Ok(Some(value));
        }
        let Some(raw) = raw else { return Ok(None) };
        let value = value::value(raw, names, o, self.depth)?;
        // In value, what the typed leaf takes is a second spelling of the row
        // pyhoglake writes in typed_value (D3, D10): canonical only.
        if let Some((_, Shape::Leaf(leaf))) = self.typed.as_ref().filter(|_| o.canonical) {
            if leaf.takes(&value) {
                let detail =
                    vec![text("value holds what its typed "), Part::Type(leaf.steps.clone()), text(" leaf takes")];
                return Err(Fault::new("non_canonical", detail));
            }
        }
        Ok(Some(value))
    }

    /// Whether row `i` holds the field: the group present and a column set.
    fn occupied(&self, i: usize) -> bool {
        self.present.as_ref().is_none_or(|present| present.is_valid(i))
            && (self.value.as_ref().is_some_and(|value| value.get(i).is_some())
                || self.typed.as_ref().is_some_and(|(typed, _)| typed.is_valid(i)))
    }
}

/// How a `typed_value` column reads.
enum Shape {
    /// Fields in storage order, and their names sorted.
    Object(Vec<(String, Group)>, Vec<String>),
    Array(Vec<usize>, Box<Group>),
    Leaf(Leaf),
}

impl Shape {
    fn new(
        array: &ArrayRef,
        field: &Field,
        steps: Vec<usize>,
        path: Vec<String>,
        depth: usize,
    ) -> Result<Shape, Fault> {
        let shredded = plain(field).then(|| array.data_type());
        let container = matches!(shredded, Some(DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_)));
        // Below the deepest level a shredded container holds nothing this
        // reads, and building its reader would recurse as deep as it goes.
        let detail = || format!("the storage shreds values nested more than {MAX_DEPTH} levels deep");
        ensure(!container || depth <= MAX_DEPTH, || Fault::new("nesting_too_deep", vec![Part::Text(detail())]))?;
        Ok(match (shredded, list(array).filter(|_| container)) {
            (Some(DataType::Struct(fields)), _) => {
                unique_names(fields)?;
                let groups = fields.iter().zip(array.as_struct().columns()).enumerate().map(|(at, (field, column))| {
                    let (steps, path) = (with(&steps, at), with(&path, field.name().clone()));
                    Ok((field.name().clone(), Group::new(column, field, steps, path, depth + 1, false, false)?))
                });
                let groups = groups.collect::<Result<Vec<_>, Fault>>()?;
                let mut names: Vec<String> = groups.iter().map(|(name, _)| name.clone()).collect();
                names.sort_unstable();
                Shape::Object(groups, names)
            }
            (_, Some((offsets, values, element))) => {
                let path = [path, vec!["list".into(), "element".into()]].concat();
                Shape::Array(
                    offsets,
                    Box::new(Group::new(values, element, with(&steps, 0), path, depth + 1, false, true)?),
                )
            }
            _ => Shape::Leaf(Leaf::new(array, field, steps)?),
        })
    }
}

impl Group {
    /// The value at row `i` given the group's `value` bytes; None where
    /// `typed_value` is null and the bytes say nothing (the group reads them).
    fn typed<'r>(
        &self,
        (array, how): &'r (ArrayRef, Shape),
        i: usize,
        names: &[&'r str],
        raw: Option<&'r [u8]>,
        o: &Options,
    ) -> Result<Option<Value<'r>>, Fault> {
        let decode = |raw| value::value(raw, names, o, self.depth);
        if array.is_null(i) {
            let (Some(raw), false) = (raw, matches!(how, Shape::Leaf(_))) else { return Ok(None) };
            return match (how, decode(raw)?) {
                // An object's typed_value is never null (VS:162, 218-220), and
                // an array goes to typed_value (VS:119-120).
                (Shape::Object(..), Value::Object(_)) => Err(shape("an object is in value while typed_value is null")),
                (Shape::Array(..), Value::Array(_)) if o.strict => {
                    Err(shape("an array is in value while typed_value is null"))
                }
                (_, value) => Ok(Some(value)),
            };
        }
        match how {
            Shape::Object(fields, declared) => {
                if self.depth >= MAX_DEPTH {
                    value::check_depth(fields.iter().filter(|(_, group)| group.occupied(i)).count(), self.depth)?;
                }
                let mut out = Vec::with_capacity(fields.len());
                for (name, group) in fields {
                    let field = group.read(i, names, o).map_err(|fault| fault.step(Step::Key(name.clone())))?;
                    out.extend(field.map(|value| (name.as_str(), value)));
                }
                if let Some(raw) = raw {
                    let Value::Object(residual) = decode(raw)? else {
                        return Err(shape("a non-object value has shredded fields"));
                    };
                    let empty = o.canonical && residual.is_empty();
                    ensure(!empty, || odd("a partially shredded object has an empty residual"))?;
                    // The residual's keys are sorted: the first clash is the least.
                    let declared = |key: &&str| declared.binary_search_by(|name| name.as_str().cmp(key)).is_ok();
                    if let Some((key, _)) = residual.iter().find(|(key, _)| declared(key)) {
                        return Err(refused(vec![
                            text("shredded field "),
                            Part::Quoted((*key).into()),
                            text(" is in value too"),
                        ]));
                    }
                    out.extend(residual);
                }
                out.sort_unstable_by(|a, b| a.0.cmp(b.0));
                Ok(Some(Value::Object(out)))
            }
            Shape::Array(offsets, elements) => {
                ensure(raw.is_none(), || shape("an array has a value as well as typed_value"))?;
                let (start, stop) = (offsets[i], offsets[i + 1]);
                value::check_depth(stop.saturating_sub(start), self.depth)?;
                let mut items = Vec::with_capacity(stop.saturating_sub(start));
                for at in start..stop {
                    // Elements are never missing (VS:145): a reader reads one
                    // as Variant null.
                    match elements.read(at, names, o).map_err(|fault| fault.step(Step::Index(at - start)))? {
                        Some(value) => items.push(value),
                        None => {
                            items.push(ensure(!o.strict, || shape("an array element is missing")).map(|_| Value::Null)?)
                        }
                    }
                }
                Ok(Some(Value::Array(items)))
            }
            Shape::Leaf(leaf) => {
                ensure(raw.is_none(), || shape("a primitive has both value and typed_value"))?;
                // Nor in a typed leaf does pyhoglake write a float or nanoseconds (D3).
                let never = o.canonical && matches!(leaf.kind, Kind::Float32 | Kind::Timestamp(_, true));
                let detail = || {
                    vec![
                        text("a typed "),
                        Part::RawType(leaf.steps.clone()),
                        text(" leaf holds a value, which pyhoglake never writes"),
                    ]
                };
                ensure(!never, || Fault::new("non_canonical", detail()))?;
                leaf.read(array, i).map(Some)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Bool,
    /// The leaf's range.
    Int(i64, i64),
    Float32,
    Float64,
    Bytes,
    Text,
    /// Precision and scale.
    Decimal(u8, u8),
    Date,
    Time,
    /// Whether adjusted to UTC; whether nanoseconds.
    Timestamp(bool, bool),
    Uuid,
}

/// A typed leaf (`_leaves`): its kind, where its type is in the storage,
/// and string or binary bytes.
struct Leaf {
    kind: Kind,
    steps: Vec<usize>,
    bytes: Option<Bin>,
}

impl Leaf {
    fn new(array: &ArrayRef, field: &Field, steps: Vec<usize>) -> Result<Leaf, Fault> {
        let refuse = |steps, how: &str| {
            refused(vec![text("typed_value of type "), steps, Part::Text(format!(" is not a {how}"))])
        };
        let kind = match (field.metadata().get("ARROW:extension:name").map(String::as_str), array.data_type()) {
            (Some("arrow.uuid"), DataType::FixedSizeBinary(16)) => Kind::Uuid,
            (None, DataType::Boolean) => Kind::Bool,
            (None, DataType::Int8) => Kind::Int(i8::MIN.into(), i8::MAX.into()),
            (None, DataType::Int16) => Kind::Int(i16::MIN.into(), i16::MAX.into()),
            (None, DataType::Int32) => Kind::Int(i32::MIN.into(), i32::MAX.into()),
            (None, DataType::Int64) => Kind::Int(i64::MIN, i64::MAX),
            (None, DataType::Float32) => Kind::Float32,
            (None, DataType::Float64) => Kind::Float64,
            (None, DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => Kind::Bytes,
            (
                None,
                &DataType::Decimal32(precision, scale)
                | &DataType::Decimal64(precision, scale)
                | &DataType::Decimal128(precision, scale)
                | &DataType::Decimal256(precision, scale),
            ) => {
                // A Variant decimal has 38 digits at most, at a scale of 0 to
                // 38 (VE:422-446).
                let variant = 0 <= scale && scale as u8 <= precision && precision <= 38;
                ensure(variant, || refuse(Part::RawType(steps.clone()), "Variant decimal"))?;
                Kind::Decimal(precision, scale as u8)
            }
            (None, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => Kind::Text,
            (None, DataType::Date32) => Kind::Date,
            (None, DataType::Time64(TimeUnit::Microsecond)) => Kind::Time,
            (None, DataType::Timestamp(unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond), zone)) => {
                Kind::Timestamp(zone.is_some(), *unit == TimeUnit::Nanosecond)
            }
            _ => return Err(refuse(Part::Type(steps), "shredded Variant type")),
        };
        Ok(Leaf { kind, steps, bytes: Bin::of(array, kind == Kind::Text) })
    }

    /// Row `i`, present, as the Python codec reads it: a value no Python
    /// type holds is refused at its row.
    fn read<'r>(&'r self, array: &'r ArrayRef, i: usize) -> Result<Value<'r>, Fault> {
        let bytes = || self.bytes.as_ref().and_then(|bytes| bytes.get(i)).unwrap_or_default();
        Ok(match self.kind {
            Kind::Bool => Value::Bool(array.as_boolean().value(i)),
            Kind::Int(..) => Value::Int(match array.data_type() {
                DataType::Int8 => array.as_primitive::<Int8Type>().value(i).into(),
                DataType::Int16 => array.as_primitive::<Int16Type>().value(i).into(),
                DataType::Int32 => array.as_primitive::<Int32Type>().value(i).into(),
                _ => array.as_primitive::<Int64Type>().value(i),
            }),
            Kind::Float32 => Value::Float(array.as_primitive::<Float32Type>().value(i).into()),
            Kind::Float64 => Value::Float(array.as_primitive::<Float64Type>().value(i)),
            Kind::Bytes => Value::Bytes(bytes()),
            Kind::Text => Value::Str(value::text(bytes())?),
            Kind::Decimal(precision, scale) => {
                let unscaled = match array.data_type() {
                    DataType::Decimal32(..) => i256::from(array.as_primitive::<Decimal32Type>().value(i)),
                    DataType::Decimal64(..) => i256::from(array.as_primitive::<Decimal64Type>().value(i)),
                    DataType::Decimal128(..) => i256::from(array.as_primitive::<Decimal128Type>().value(i)),
                    _ => array.as_primitive::<Decimal256Type>().value(i),
                };
                // The precision is the most digits the leaf holds, as a width's
                // is for a Variant decimal; nothing upstream checks it.
                let limit = 10i128.pow(precision.into());
                let Some(unscaled) = unscaled.to_i128().filter(|unscaled| -limit < *unscaled && *unscaled < limit)
                else {
                    let digits = unscaled.to_string().trim_start_matches('-').len();
                    return Err(bad(format!(
                        "a DECIMAL({precision},{scale}) leaf holds {digits} digits, beyond its {precision}"
                    )));
                };
                Value::Decimal(unscaled, scale)
            }
            Kind::Date => Value::Date(array.as_primitive::<Date32Type>().value(i)),
            Kind::Time => value::time_of_day(array.as_primitive::<Time64MicrosecondType>().value(i))?,
            Kind::Timestamp(aware, false) => {
                Value::Timestamp(array.as_primitive::<TimestampMicrosecondType>().value(i), aware)
            }
            Kind::Timestamp(aware, true) => {
                Value::TimestampNanos(array.as_primitive::<TimestampNanosecondType>().value(i), aware)
            }
            Kind::Uuid => Value::Uuid(array.as_fixed_size_binary().value(i)),
        })
    }

    /// Whether this leaf takes `value` (D3, `_taken_by`), as the decoder
    /// gives it: an integer of any width is an int, and a date or timestamp
    /// beyond Python's years a pyarrow scalar, which no leaf takes.
    fn takes(&self, value: &Value) -> bool {
        match (self.kind, value) {
            (Kind::Int(low, high), Value::Int(n)) => (low..=high).contains(n),
            (Kind::Decimal(precision, scale), &Value::Decimal(n, of)) => {
                of == scale && n.unsigned_abs() < 10u128.pow(precision.into())
            }
            (Kind::Date, Value::Date(days)) => DAYS.contains(days),
            (Kind::Timestamp(aware, false), &Value::Timestamp(micros, of)) => aware == of && MICROS.contains(&micros),
            (Kind::Bool, Value::Bool(_))
            | (Kind::Float64, Value::Float(_))
            | (Kind::Text, Value::Str(_))
            | (Kind::Bytes, Value::Bytes(_))
            | (Kind::Time, Value::Time(_))
            | (Kind::Uuid, Value::Uuid(_)) => true,
            _ => false,
        }
    }
}

/// The first null a REQUIRED child holds under a present parent, as its
/// column's path and the detail (`structure_fault`), or None.
pub fn structure_fault(array: &ArrayRef) -> Option<Vec<Part>> {
    fault(array, &vec![true; array.len()], &mut Vec::new())
}

fn fault(array: &ArrayRef, present: &[bool], path: &mut Vec<String>) -> Option<Vec<Part>> {
    let own = || present.iter().enumerate().map(|(i, &present)| present && array.is_valid(i)).collect::<Vec<_>>();
    // Whether `child` holds a null where `present` is true.
    let null_under = |child: &ArrayRef, present: &[bool]| {
        let nulls = child.logical_nulls();
        nulls.is_some_and(|nulls| nulls.iter().zip(present).any(|(valid, &present)| present && !valid))
    };
    let found = |path: &[String], what: &str| {
        Some(vec![Part::Column(path.to_vec()), Part::Text(format!(" is null under a present {what}"))])
    };
    if let DataType::Struct(fields) = array.data_type() {
        let own = own();
        for (child, field) in array.as_struct().columns().iter().zip(fields) {
            path.push(field.name().clone());
            if !field.is_nullable() && null_under(child, &own) {
                return found(path, "parent");
            }
            if let Some(fault) = fault(child, &own, path) {
                return Some(fault);
            }
            path.pop();
        }
    } else if let Some((offsets, values, element)) = list(array) {
        // A null list over a non-empty range holds elements no reader sees.
        let (own, start) = (own(), offsets[0]);
        let mut held = vec![false; offsets[offsets.len() - 1] - start];
        for (i, pair) in offsets.windows(2).enumerate() {
            held[pair[0] - start..pair[1] - start].fill(own[i]);
        }
        let values = values.slice(start, held.len());
        path.push(element.name().clone());
        if !element.is_nullable() && null_under(&values, &held) {
            return found(path, "list");
        }
        let below = fault(&values, &held, path);
        path.pop();
        return below;
    }
    None
}
