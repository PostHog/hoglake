//! Shredding: the `value` and `typed_value` columns of each declared node,
//! row for row as the Python codec's builders fill them (`_Builder` and its
//! kin, D3, D10), placeholders under null parents included. A leaf takes a
//! value by exact type only; a leaf of a type JSON never gives (a float, a
//! date, binary...) takes nothing and is a null column of its type.

use std::sync::Arc;

use arrow_array::{
    make_array, new_null_array, ArrayRef, BinaryArray, BooleanArray, ListArray, StringArray, StructArray,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Fields, TimeUnit};

use super::parse::{Scratch, Tok};
use super::{write, Fault, Step};

/// The plan of a declaration, read off its write form: the storage fields,
/// and the root group (None for unshredded storage).
#[derive(Clone)]
pub struct Plan {
    pub fields: Fields,
    pub(super) root: Option<Group>,
}

impl Default for Plan {
    fn default() -> Self {
        Plan { fields: super::storage_fields(), root: None }
    }
}

impl Plan {
    /// The plan of `write_type`, as `compile_plan` lays it out (D11, VS:83-122).
    pub fn new(write_type: &DataType) -> Result<Plan, String> {
        let DataType::Struct(fields) = write_type else { return Err(format!("storage is a struct, not {write_type}")) };
        if *fields == super::storage_fields() {
            return Ok(Plan::default());
        }
        let metadata = fields.first().filter(|f| f.name() == "metadata" && !f.is_nullable());
        if metadata.is_none_or(|f| *f.data_type() != DataType::Binary) {
            return Err("storage starts with a REQUIRED binary metadata".to_owned());
        }
        let root = Group::new(fields.iter().skip(1).cloned().collect())?;
        Ok(Plan { fields: fields.clone(), root: Some(root) })
    }
}

/// What a leaf takes (`_matcher`): an integer its range holds, a decimal16
/// of scale 0 an integer beyond int64 its precision holds (D1), a double,
/// a boolean, a string; or nothing JSON gives.
#[derive(Clone, Copy)]
enum Take {
    Int(i64, i64),
    Wide(u128),
    Double,
    Bool,
    Str,
    Nothing,
}

/// A binary column's bytes and row ends.
#[derive(Clone)]
struct Bytes {
    data: Vec<u8>,
    ends: Vec<i32>,
}

impl Bytes {
    fn new() -> Self {
        Bytes { data: Vec::new(), ends: vec![0] }
    }

    fn end(&mut self) {
        self.ends.push(i32::try_from(self.data.len()).expect("a chunk's binary child fits int32 offsets"));
    }

    fn row(&self, row: usize) -> &[u8] {
        &self.data[self.ends[row] as usize..self.ends[row + 1] as usize]
    }

    fn truncate(&mut self, rows: usize) {
        self.data.truncate(self.ends[rows] as usize);
        self.ends.truncate(rows + 1);
    }

    fn take(&mut self) -> (OffsetBuffer<i32>, Buffer) {
        let Bytes { data, ends } = std::mem::replace(self, Bytes::new());
        (OffsetBuffer::new(ScalarBuffer::from(ends)), Buffer::from_vec(data))
    }
}

/// A primitive leaf's typed values: fixed-width little-endian, or a string's
/// bytes, a row each, nulls zeroed.
#[derive(Clone)]
struct Leaf {
    take: Take,
    width: usize,
    out: Bytes,
}

#[derive(Clone)]
enum Data {
    Variant,
    Leaf(Leaf),
    /// Field names in declared order, their indices sorted by name, and
    /// per row the key of each and the residual keys (`_ObjectBuilder`).
    Object {
        names: Vec<String>,
        sorted: Vec<usize>,
        found: Vec<Option<u32>>,
        rest: Vec<u32>,
        fields: Vec<Group>,
    },
    List(Vec<i32>, Box<Group>),
}

/// One row being shredded: its tape, the encoder's buffers, and the chunk's
/// byte budget, which counts what each column was given (`_Budget`).
pub struct Row<'a> {
    pub s: &'a Scratch,
    pub text: &'a [u8],
    pub b: &'a mut write::Bufs,
    pub used: &'a mut usize,
}

/// A group's `value` column, and which rows have one and a typed_value.
#[derive(Clone)]
struct Columns {
    value: Bytes,
    present: Vec<bool>,
    typed: Vec<bool>,
}

impl Columns {
    /// Ends a row: its value written from `start` and counted, or none.
    fn end(&mut self, used: &mut usize, start: Option<usize>, typed: bool) {
        if let Some(start) = start {
            *used += self.value.data.len() - start;
        }
        self.value.end();
        self.present.push(start.is_some());
        self.typed.push(typed);
    }
}

/// The columns of one declared group for the chunk being built, and its
/// typed, fallback and Variant-null counts so far.
#[derive(Clone)]
pub struct Group {
    fields: Fields,
    columns: Columns,
    data: Data,
    counts: [usize; 3],
}

fn nulls(valid: Vec<bool>) -> Option<NullBuffer> {
    Some(NullBuffer::from(valid)).filter(|nulls| nulls.null_count() > 0)
}

impl Data {
    /// The placeholders of a row without a typed_value: child groups absent.
    fn untyped(&mut self) {
        match self {
            Data::Variant => {}
            Data::Leaf(leaf) => {
                leaf.out.data.resize(leaf.out.data.len() + leaf.width, 0);
                leaf.out.end();
            }
            Data::Object { fields, .. } => fields.iter_mut().for_each(Group::absent),
            Data::List(offsets, _) => offsets.push(offsets[offsets.len() - 1]),
        }
    }
}

impl Group {
    /// The group of `fields`: `value`, then a `typed_value` unless the node
    /// is a variant.
    fn new(fields: Fields) -> Result<Group, String> {
        let value = fields.first().filter(|f| f.name() == "value" && f.is_nullable());
        if value.is_none_or(|f| *f.data_type() != DataType::Binary) || fields.len() > 2 {
            return Err(format!("a declared group is an optional binary value and a typed_value, not {fields:?}"));
        }
        let leaf = |take, width| Data::Leaf(Leaf { take, width, out: Bytes::new() });
        let kind = fields.get(1).filter(|f| f.name() == "typed_value").map(|f| f.data_type());
        let data = match kind {
            None if fields.len() == 1 => Data::Variant,
            Some(DataType::Struct(declared)) => {
                let groups = declared.iter().map(|f| match f.data_type() {
                    DataType::Struct(group) if !f.is_nullable() => Group::new(group.clone()),
                    _ => Err(format!("field group {} is a REQUIRED struct", f.name())),
                });
                let fields = groups.collect::<Result<Vec<_>, _>>()?;
                let names: Vec<String> = declared.iter().map(|f| f.name().clone()).collect();
                let mut sorted: Vec<usize> = (0..names.len()).collect();
                sorted.sort_by(|&a, &b| names[a].cmp(&names[b]));
                Data::Object { found: vec![None; names.len()], names, sorted, rest: Vec::new(), fields }
            }
            Some(DataType::List(element)) => match element.data_type() {
                DataType::Struct(group) if !element.is_nullable() => {
                    Data::List(vec![0], Box::new(Group::new(group.clone())?))
                }
                _ => return Err("a list element is a REQUIRED struct".to_owned()),
            },
            Some(DataType::Int8) => leaf(Take::Int(i8::MIN.into(), i8::MAX.into()), 1),
            Some(DataType::Int16) => leaf(Take::Int(i16::MIN.into(), i16::MAX.into()), 2),
            Some(DataType::Int32) => leaf(Take::Int(i32::MIN.into(), i32::MAX.into()), 4),
            Some(DataType::Int64) => leaf(Take::Int(i64::MIN, i64::MAX), 8),
            Some(DataType::Decimal128(precision, 0)) => {
                leaf(Take::Wide(10u128.saturating_pow((*precision).into())), 16)
            }
            Some(DataType::Float64) => leaf(Take::Double, 8),
            Some(DataType::Boolean) => leaf(Take::Bool, 1),
            Some(DataType::Utf8) => leaf(Take::Str, 0),
            Some(
                DataType::Float32
                | DataType::Date32
                | DataType::Time64(TimeUnit::Microsecond)
                | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
                | DataType::Binary
                | DataType::FixedSizeBinary(16)
                | DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..),
            ) => leaf(Take::Nothing, 0),
            Some(other) => return Err(format!("no declared node has a typed_value of {other}")),
            None => return Err(format!("a declared group's second column is its typed_value: {fields:?}")),
        };
        let columns = Columns { value: Bytes::new(), present: Vec::new(), typed: Vec::new() };
        Ok(Group { fields, columns, data, counts: [0; 3] })
    }

    /// Places the value at `at`: in `typed_value` where it fits, else in
    /// `value`, its faults in the Python codec's order (an object's
    /// residual first, then its declared fields in declared order).
    pub fn add(&mut self, r: &mut Row, at: u32) -> Result<(), Fault> {
        let (s, text, tok) = (r.s, r.text, r.s.toks[at as usize]);
        let columns = &mut self.columns;
        match (&mut self.data, tok) {
            (Data::Leaf(leaf), _) if takes(leaf.take, tok, s) => {
                let out = &mut leaf.out;
                match tok {
                    Tok::Int(n) => out.data.extend_from_slice(&n.to_le_bytes()[..leaf.width]),
                    Tok::Wide(index) => out.data.extend_from_slice(&s.wide[index as usize].to_le_bytes()),
                    Tok::Float(x) => out.data.extend_from_slice(&x.to_le_bytes()),
                    Tok::Str(span) => {
                        // A string counts its bytes; exactly one column is set (VS:113).
                        out.data.extend_from_slice(s.bytes(text, span));
                        *r.used += span.len as usize;
                    }
                    _ => out.data.push(u8::from(matches!(tok, Tok::True))),
                }
                out.end();
                columns.end(r.used, None, true);
            }
            (Data::Object { names, sorted, found, rest, fields }, Tok::Object(len, _, slot)) => {
                // Keys and names both in byte order: a merge finds the
                // declared ones by exact name (D6); the rest is the residual.
                found.fill(None);
                rest.clear();
                let mut next = sorted.iter().peekable();
                for &key in &s.slots[(slot - len) as usize..slot as usize] {
                    let name = s.key(text, key);
                    while next.next_if(|&&i| names[i].as_bytes() < name).is_some() {}
                    match next.next_if(|&&i| names[i].as_bytes() == name) {
                        Some(&i) => found[i] = Some(key),
                        None => rest.push(key),
                    }
                }
                let start = columns.value.data.len();
                if !rest.is_empty() {
                    write::fields(s, text, r.b, rest, &mut columns.value.data)?;
                }
                columns.end(r.used, (!rest.is_empty()).then_some(start), true);
                for (field, &key) in fields.iter_mut().zip(found.iter()) {
                    match key {
                        None => field.absent(),
                        Some(key) => field
                            .add(r, s.keys[key as usize].tok + 1)
                            .map_err(|fault| fault.step(Step::Key(s.key(text, key).to_vec())))?,
                    }
                }
            }
            (Data::List(offsets, element), Tok::Array(len, _)) => {
                let mut child = at + 1;
                for index in 0..len {
                    element.add(r, child).map_err(|fault| fault.step(Step::Index(index)))?;
                    child = write::next(s, child);
                }
                offsets.push(element.columns.present.len() as i32);
                // Elements count against the budget: list offsets are int32 too.
                *r.used += len as usize;
                columns.end(r.used, None, true);
            }
            (data, _) => {
                let start = columns.value.data.len();
                write::value(s, text, r.b, at, &mut columns.value.data)?;
                columns.end(r.used, Some(start), false);
                data.untyped();
            }
        }
        Ok(())
    }

    /// A missing field, a SQL NULL or invalid row, or the placeholder under
    /// a null parent: both columns null.
    pub fn absent(&mut self) {
        self.columns.end(&mut 0, None, false);
        self.data.untyped();
    }

    /// Cuts every column back to `rows` rows, a failed row's part with it.
    pub fn truncate(&mut self, rows: usize) {
        self.columns.value.truncate(rows);
        self.columns.present.truncate(rows);
        self.columns.typed.truncate(rows);
        match &mut self.data {
            Data::Variant => {}
            Data::Leaf(leaf) => leaf.out.truncate(rows),
            Data::Object { fields, .. } => fields.iter_mut().for_each(|field| field.truncate(rows)),
            Data::List(offsets, element) => {
                offsets.truncate(rows + 1);
                element.truncate(offsets[rows] as usize);
            }
        }
    }

    /// This chunk's `value` and `typed_value`, their counts added (`count`);
    /// the group is left empty for the next chunk.
    pub fn build(&mut self) -> Vec<ArrayRef> {
        let Columns { value, present, typed } = &mut self.columns;
        let (present, typed) = (std::mem::take(present), std::mem::take(typed));
        if !matches!(self.data, Data::Variant) {
            for (row, (&present, &typed)) in present.iter().zip(&typed).enumerate() {
                let slot = match (typed, value.row(row)) {
                    (true, _) => 0,
                    (false, [0]) => 2,
                    _ => 1,
                };
                self.counts[slot] += usize::from(typed || present);
            }
        }
        let rows = present.len();
        let (offsets, data) = value.take();
        let mut columns: Vec<ArrayRef> = vec![Arc::new(BinaryArray::new(offsets, data, nulls(present)))];
        let Some(kind) = self.fields.get(1).map(|f| f.data_type().clone()) else { return columns };
        let nulls = nulls(typed);
        columns.push(match (&mut self.data, &kind) {
            (Data::Object { fields, .. }, DataType::Struct(declared)) => {
                Arc::new(StructArray::new(declared.clone(), fields.iter_mut().map(Group::array).collect(), nulls))
            }
            (Data::List(offsets, element), DataType::List(field)) => {
                let offsets = OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(offsets, vec![0])));
                Arc::new(ListArray::new(field.clone(), offsets, element.array(), nulls))
            }
            (Data::Leaf(leaf), _) => match leaf.take {
                Take::Nothing => new_null_array(&kind, rows),
                Take::Str => {
                    let (offsets, data) = leaf.out.take();
                    Arc::new(StringArray::new(offsets, data, nulls))
                }
                Take::Bool => Arc::new(BooleanArray::new(leaf.out.take().1.iter().map(|&b| b == 1).collect(), nulls)),
                _ => {
                    let data = ArrayData::builder(kind).len(rows).nulls(nulls).add_buffer(leaf.out.take().1);
                    make_array(data.align_buffers(true).build().expect("a leaf of its width"))
                }
            },
            _ => unreachable!("Group::new pairs the data with its type"),
        });
        columns
    }

    /// This chunk's group struct: a field group or a list element, never null.
    fn array(&mut self) -> ArrayRef {
        Arc::new(StructArray::new(self.fields.clone(), self.build(), None))
    }

    /// The counts of every node with a type, in declaration order (`count`).
    pub fn counts(&self, out: &mut Vec<[usize; 3]>) {
        match &self.data {
            Data::Variant => {}
            Data::Leaf(_) => out.push(self.counts),
            Data::Object { fields, .. } => {
                out.push(self.counts);
                fields.iter().for_each(|field| field.counts(out));
            }
            Data::List(_, element) => {
                out.push(self.counts);
                element.counts(out);
            }
        }
    }
}

/// Whether a leaf takes `tok` (`_matcher`, over the types JSON gives). A
/// string with an unpaired surrogate is no string: `value` refuses it.
fn takes(take: Take, tok: Tok, s: &Scratch) -> bool {
    match (take, tok) {
        (Take::Int(low, high), Tok::Int(n)) => (low..=high).contains(&n),
        (Take::Wide(limit), Tok::Wide(index)) => s.wide[index as usize].unsigned_abs() < limit,
        (Take::Double, Tok::Float(_)) | (Take::Bool, Tok::True | Tok::False) => true,
        (Take::Str, Tok::Str(span)) => !span.lone,
        _ => false,
    }
}
