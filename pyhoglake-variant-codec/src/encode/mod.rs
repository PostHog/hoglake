//! JSON text to unshredded VARIANT storage (no declaration, or a root
//! `variant`): `_variant_codec.encode` with `json_text=True`, row for row,
//! chunk for chunk, fault for fault. A row's faults come in its order: bad
//! UTF-8; the parse's, in text order; depth; a key with an unpaired
//! surrogate; the values', in field-id order. A failed row adds no bytes.

mod parse;
mod write;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayAccessor, ArrayRef, BinaryArray, StructArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};

use crate::consts::{MAX_DEPTH, PATH_CAP};
pub use parse::PARSE_DEPTH;

/// How one call encodes. `stop_at_invalid` is `on_invalid="raise"`;
/// `max_int_digits` is `sys.get_int_max_str_digits()`, 0 for none; the last
/// two are CPython's: 3.13 and later, and before 3.14.
#[derive(Clone, Debug)]
pub struct Options {
    pub nullable: bool,
    pub stop_at_invalid: bool,
    pub chunk_rows: usize,
    pub chunk_bytes: usize,
    pub max_int_digits: usize,
    pub trailing_comma_message: bool,
    pub escape_needs_tail: bool,
}

/// A duplicate key is WTF-8 bytes: the binding spells it with `repr`.
#[derive(Debug, PartialEq)]
pub enum Detail {
    Text(String),
    DuplicateKey(Vec<u8>),
}

/// A key (WTF-8) or an index.
#[derive(Debug)]
pub enum Step {
    Key(Vec<u8>),
    Index(u32),
}

/// Why a row cannot be written (`_Invalid`), and where: `trail` is
/// innermost first, and a fault of the text as a whole has no path.
#[derive(Debug)]
pub struct Fault {
    pub reason: &'static str,
    pub detail: Detail,
    located: bool,
    trail: Vec<Step>,
}

impl Fault {
    pub fn unlocated(reason: &'static str, detail: impl Into<String>) -> Self {
        Fault { reason, detail: Detail::Text(detail.into()), located: false, trail: Vec::new() }
    }

    pub fn at(reason: &'static str, detail: impl Into<String>) -> Self {
        Fault { located: true, ..Fault::unlocated(reason, detail) }
    }

    fn along(mut outermost_first: Vec<Step>, reason: &'static str, detail: impl Into<String>) -> Self {
        outermost_first.reverse();
        Fault { trail: outermost_first, ..Fault::at(reason, detail) }
    }

    fn step(mut self, step: Step) -> Self {
        self.trail.push(step);
        self
    }

    /// The row's path as `_Invalid.path` spells it, or None.
    pub fn path(&self) -> Option<String> {
        if !self.located {
            return None;
        }
        let mut path = String::from("$");
        for step in self.trail.iter().rev() {
            match step {
                Step::Index(index) => path.push_str(&format!("[{index}]")),
                Step::Key(name) => member(&mut path, name),
            }
        }
        Some(cut(&path, PATH_CAP))
    }
}

/// `text` cut to `limit` code points, `...` marking the cut (`_cut`).
pub fn cut(text: &str, limit: usize) -> String {
    match (text.chars().nth(limit), text.char_indices().nth(limit - 3)) {
        (Some(_), Some((end, _))) => format!("{}...", &text[..end]),
        _ => text.to_owned(),
    }
}

fn code_points(wtf8: &[u8]) -> Vec<u32> {
    let (mut out, mut i) = (Vec::new(), 0);
    while i < wtf8.len() {
        let (n, lead) = match wtf8[i] {
            byte @ 0..=0x7F => (1, u32::from(byte)),
            byte @ 0xC0..=0xDF => (2, u32::from(byte & 0x1F)),
            byte @ 0xE0..=0xEF => (3, u32::from(byte & 0x0F)),
            byte => (4, u32::from(byte & 0x07)),
        };
        out.push(wtf8[i + 1..i + n].iter().fold(lead, |c, &byte| (c << 6) | u32::from(byte & 0x3F)));
        i += n;
    }
    out
}

/// Appends the path step of a key (`member`): `.name`, or `["name"]` as
/// JSON when it would read as more than one step, with `\u` escapes when
/// it holds a control character, a surrogate or a line separator.
fn member(path: &mut String, name: &[u8]) {
    let chars = code_points(name);
    let unsafe_char = |c: u32| {
        c <= 0x1F || (0x7F..=0x9F).contains(&c) || (0xD800..=0xDFFF).contains(&c) || c == 0x2028 || c == 0x2029
    };
    if chars.iter().any(|&c| unsafe_char(c)) {
        // json.dumps(name, ensure_ascii=True)
        path.push_str("[\"");
        for &c in &chars {
            match c {
                0x22 => path.push_str("\\\""),
                0x5C => path.push_str("\\\\"),
                0x0A => path.push_str("\\n"),
                0x0D => path.push_str("\\r"),
                0x09 => path.push_str("\\t"),
                0x08 => path.push_str("\\b"),
                0x0C => path.push_str("\\f"),
                0x20..=0x7E => path.push(c as u8 as char),
                0x1_0000.. => {
                    let c = c - 0x1_0000;
                    path.push_str(&format!("\\u{:04x}\\u{:04x}", 0xD800 | (c >> 10), 0xDC00 | (c & 0x3FF)));
                }
                _ => path.push_str(&format!("\\u{c:04x}")),
            }
        }
        path.push_str("\"]");
    } else if chars.is_empty() || chars.iter().any(|&c| matches!(c, 0x2E | 0x5B | 0x5D | 0x22 | 0x5C)) {
        // json.dumps(name, ensure_ascii=False): only " and \ to escape.
        path.push_str("[\"");
        for &c in &chars {
            if c == 0x22 || c == 0x5C {
                path.push('\\');
            }
            path.push(char::from_u32(c).expect("no surrogate here"));
        }
        path.push_str("\"]");
    } else {
        path.push('.');
        path.push_str(std::str::from_utf8(name).expect("no surrogate here"));
    }
}

/// The storage struct of an unshredded column: REQUIRED `metadata` and
/// REQUIRED `value` (D11).
pub fn storage_fields() -> Fields {
    Fields::from(vec![Field::new("metadata", DataType::Binary, false), Field::new("value", DataType::Binary, false)])
}

/// One chunk being built: each row's metadata and value bytes, and whether
/// it is present, a SQL NULL row being a null group over empty bytes; and
/// the variable-length bytes so far (`_Budget.used`).
#[derive(Default)]
struct Chunk {
    metadata: Vec<u8>,
    metadata_ends: Vec<i32>,
    value: Vec<u8>,
    value_ends: Vec<i32>,
    present: Vec<bool>,
    nulls: usize,
    used: usize,
}

impl Chunk {
    fn new() -> Self {
        Chunk { metadata_ends: vec![0], value_ends: vec![0], ..Chunk::default() }
    }

    /// Ends the row, present, or not and rid of what it left.
    fn end_row(&mut self, present: bool) {
        let (metadata, value) = (self.metadata_ends[self.present.len()], self.value_ends[self.present.len()]);
        if present {
            self.used += self.metadata.len() + self.value.len() - (metadata + value) as usize;
        } else {
            self.metadata.truncate(metadata as usize);
            self.value.truncate(value as usize);
            self.nulls += 1;
        }
        let end = |bytes: &[u8]| i32::try_from(bytes.len()).expect("a chunk's binary child fits int32 offsets");
        self.metadata_ends.push(end(&self.metadata));
        self.value_ends.push(end(&self.value));
        self.present.push(present);
    }

    fn build(self) -> StructArray {
        let binary = |ends: Vec<i32>, bytes: Vec<u8>| -> ArrayRef {
            Arc::new(BinaryArray::new(OffsetBuffer::new(ScalarBuffer::from(ends)), Buffer::from_vec(bytes), None))
        };
        let nulls = (self.nulls > 0).then(|| NullBuffer::from(self.present));
        let children = vec![binary(self.metadata_ends, self.metadata), binary(self.value_ends, self.value)];
        StructArray::new(storage_fields(), children, nulls)
    }
}

/// What one call wrote, and the rows it could not.
#[derive(Default)]
pub struct Encoded {
    pub chunks: Vec<StructArray>,
    pub rows: usize,
    pub sql_null_rows: usize,
    /// Each row not written, and why. With `stop_at_invalid` the call ends
    /// at the first, a SQL NULL into a NOT NULL target among them.
    pub invalid: Vec<(usize, Fault)>,
}

struct Encoder<'o> {
    o: &'o Options,
    tape: parse::Scratch,
    bufs: write::Bufs,
    chunk: Chunk,
    out: Encoded,
}

impl Encoder<'_> {
    /// One row; false when the call stops here.
    fn row(&mut self, raw: Option<&[u8]>) -> bool {
        let row = self.out.rows;
        self.out.rows += 1;
        match raw {
            None if !self.o.nullable => {
                let fault = Fault::unlocated("sql_null_in_not_null", "SQL NULL into a NOT NULL column");
                self.out.invalid.push((row, fault));
                return false;
            }
            None => {
                self.chunk.end_row(false);
                self.out.sql_null_rows += 1;
            }
            Some(text) => {
                let result = self.encode_row(text);
                self.chunk.end_row(result.is_ok());
                if let Err(fault) = result {
                    self.out.invalid.push((row, fault));
                    if self.o.stop_at_invalid {
                        return false;
                    }
                }
            }
        }
        if self.chunk.present.len() >= self.o.chunk_rows || self.chunk.used >= self.o.chunk_bytes {
            let full = std::mem::replace(&mut self.chunk, Chunk::new());
            self.out.chunks.push(full.build());
        }
        true
    }

    fn encode_row(&mut self, text: &[u8]) -> Result<(), Fault> {
        if let Err(error) = std::str::from_utf8(text) {
            // The reasons of Python's UTF-8 decoder.
            let reason = match error.error_len() {
                None => "unexpected end of data",
                Some(_) if (0xC2..=0xF4).contains(&text[error.valid_up_to()]) => "invalid continuation byte",
                Some(_) => "invalid start byte",
            };
            return Err(Fault::unlocated("invalid_unicode", format!("the text is not UTF-8 ({reason})")));
        }
        parse::parse(&mut self.tape, text, self.o)?;
        if let Some(container) = self.tape.too_deep {
            let detail = format!("nested more than {MAX_DEPTH} levels deep");
            return Err(Fault::along(write::path_to(&self.tape, text, container), "nesting_too_deep", detail));
        }
        write::metadata(&self.tape, text, &mut self.bufs, &mut self.chunk.metadata)?;
        write::value(&self.tape, text, &mut self.bufs, 0, &mut self.chunk.value)
    }

    fn rows<'a>(&mut self, array: impl ArrayAccessor<Item = &'a [u8]>) -> bool {
        (0..array.len()).all(|i| self.row(array.is_valid(i).then(|| array.value(i))))
    }
}

/// Encodes the rows of `inputs`, Arrow binary (`binary`, `large_binary`,
/// `binary_view`) or null arrays, into chunks of unshredded storage.
pub fn encode_json(inputs: &[ArrayRef], o: &Options) -> Result<Encoded, String> {
    let (tape, bufs, chunk, out) =
        (parse::Scratch::default(), write::Bufs::default(), Chunk::new(), Encoded::default());
    let mut e = Encoder { o, tape, bufs, chunk, out };
    for array in inputs {
        let go = match array.data_type() {
            DataType::Binary => e.rows(array.as_binary::<i32>()),
            DataType::LargeBinary => e.rows(array.as_binary::<i64>()),
            DataType::BinaryView => e.rows(array.as_binary_view()),
            DataType::Null => (0..array.len()).all(|_| e.row(None)),
            other => return Err(format!("JSON text comes as Arrow binary, not {other}")),
        };
        if !go {
            return Ok(e.out);
        }
    }
    if !e.chunk.present.is_empty() || e.out.chunks.is_empty() {
        e.out.chunks.push(e.chunk.build());
    }
    Ok(e.out)
}
