//! A strict JSON parser to a per-row tape, as the Python codec's parse
//! (CPython's Modules/_json.c): its checks in its order, its messages,
//! positions in code points.

use super::{Detail, Fault, Options};
use crate::consts::{MAX_DECIMAL16_DIGITS, MAX_DEPTH};

/// The 10,000th nested container is too deep to parse, as CPython 3.12 and
/// 3.13's C recursion limit has it. They stop a few sooner by their caller's
/// C frames (9,994 at a script's top level) and 3.14 later; for a row in
/// between, `_variant_backend` takes Python's verdict.
pub const PARSE_DEPTH: usize = 10_000;

/// A string's bytes, in the text or unescaped in the arena; `lone` when it
/// holds an unpaired surrogate (WTF-8) from an escape.
#[derive(Clone, Copy)]
pub struct Span {
    pub start: u32,
    pub len: u32,
    pub arena: bool,
    pub lone: bool,
}

/// Wide: an integer beyond int64 of at most 38 digits, in `wide`; Big: the
/// text of a longer one; Key: in `keys`, before its value. Containers: len,
/// and the index after their last token (0 while open); an Object's keys in
/// field-id order end at its third, in `slots`.
#[derive(Clone, Copy)]
pub enum Tok {
    Null,
    True,
    False,
    Int(i64),
    Wide(u32),
    Big(u32, u32),
    Float(f64),
    Str(Span),
    Key(u32),
    Array(u32, u32),
    Object(u32, u32, u32),
}

/// A key: its first 8 bytes big-endian, to sort by, the tape indices of its
/// object and itself, and its field id.
pub struct Key {
    pub span: Span,
    pub prefix: u64,
    pub object: u32,
    pub tok: u32,
    pub id: u32,
}

/// A row's tape, kept for its allocations. `too_deep`: the first container
/// at MAX_DEPTH that holds anything.
#[derive(Default)]
pub struct Scratch {
    pub toks: Vec<Tok>,
    pub keys: Vec<Key>,
    pub wide: Vec<i128>,
    pub arena: Vec<u8>,
    pub too_deep: Option<u32>,
    pub order: Vec<u32>,
    pub slots: Vec<u32>,
    slotted: u32,
    open: Vec<u32>,
}

impl Scratch {
    pub fn bytes<'a>(&'a self, text: &'a [u8], span: Span) -> &'a [u8] {
        let source = if span.arena { &self.arena[..] } else { text };
        &source[span.start as usize..(span.start + span.len) as usize]
    }

    pub fn key<'a>(&'a self, text: &'a [u8], key: u32) -> &'a [u8] {
        self.bytes(text, self.keys[key as usize].span)
    }

    fn end(&self, at: u32) -> u32 {
        match self.toks[at as usize] {
            Tok::Array(_, end) | Tok::Object(_, end, _) => end,
            _ => at + 1,
        }
    }

    /// Sorts the keys by bytes, then object, gives them field ids, and finds
    /// the duplicate the pairs hook refuses first: in the first object to
    /// close (`closed`: the parse stopped, only closed ones count), the first
    /// key seen twice. Without one, lays out `slots`.
    fn dictionary(&mut self, text: &[u8], closed: bool) -> Option<Vec<u8>> {
        let mut order = std::mem::take(&mut self.order);
        order.clear();
        order.extend((0..self.keys.len() as u32).filter(|&k| !closed || self.end(self.keys[k as usize].object) > 0));
        order.sort_unstable_by(|&a, &b| {
            let (x, y) = (&self.keys[a as usize], &self.keys[b as usize]);
            (x.prefix.cmp(&y.prefix))
                .then_with(|| self.key(text, a).cmp(self.key(text, b)))
                .then(x.object.cmp(&y.object))
        });
        let (mut id, mut first) = (0, None);
        for i in 0..order.len() {
            let b = order[i] as usize;
            if i > 0 {
                let (x, y) = (&self.keys[order[i - 1] as usize], &self.keys[b]);
                let same = x.prefix == y.prefix && self.bytes(text, x.span) == self.bytes(text, y.span);
                let closes = |o: u32| (self.end(o), u32::MAX - o);
                if same && x.object == y.object && first.is_none_or(|f| closes(x.object) < closes(f)) {
                    first = Some(x.object);
                }
                id += u32::from(!same);
            }
            self.keys[b].id = id;
        }
        if first.is_none() && !closed {
            self.slots.resize(self.keys.len(), 0);
            for &k in &order {
                if let Tok::Object(_, _, slot) = &mut self.toks[self.keys[k as usize].object as usize] {
                    self.slots[*slot as usize] = k;
                    *slot += 1;
                }
            }
        }
        self.order = order;
        let mine: Vec<u32> =
            (0..self.keys.len() as u32).filter(|&k| Some(self.keys[k as usize].object) == first).collect();
        mine.iter().enumerate().find_map(|(i, &k)| {
            let name = self.key(text, k);
            mine[..i].iter().any(|&j| self.key(text, j) == name).then(|| name.to_vec())
        })
    }
}

/// Parses one row of UTF-8 `text` into `s`.
pub fn parse(s: &mut Scratch, text: &[u8], o: &Options) -> Result<(), Fault> {
    s.toks.clear();
    s.keys.clear();
    s.wide.clear();
    s.arena.clear();
    s.open.clear();
    s.too_deep = None;
    s.slotted = 0;
    let parsed = Parser { t: text, s, o }.run();
    match s.dictionary(text, parsed.is_err()) {
        Some(key) => Err(Fault { detail: Detail::DuplicateKey(key), ..Fault::unlocated("duplicate_key", "") }),
        None => parsed,
    }
}

fn json(t: &[u8], message: &str, at: usize) -> Fault {
    let chars = t[..at].iter().filter(|&&b| (b as i8) >= -0x40).count();
    Fault::unlocated("invalid_json", format!("{message} at character {chars}"))
}

fn constant(name: &str) -> Fault {
    Fault::unlocated("non_finite_number", format!("{name} is not a JSON number"))
}

struct Parser<'a> {
    t: &'a [u8],
    s: &'a mut Scratch,
    o: &'a Options,
}

impl Parser<'_> {
    fn skip(&self, mut i: usize, set: &[u8]) -> usize {
        while self.t.get(i).is_some_and(|b| set.contains(b)) {
            i += 1;
        }
        i
    }

    fn ws(&self, i: usize) -> usize {
        self.skip(i, b" \t\n\r")
    }

    fn push(&mut self, tok: Tok, end: usize) -> usize {
        self.s.toks.push(tok);
        end
    }

    fn run(&mut self) -> Result<(), Fault> {
        let t = self.t;
        let mut i = self.ws(0);
        loop {
            // A value starts at i.
            let depth = self.s.open.len();
            if let Some(&parent) = self.s.open.last() {
                if let Tok::Array(len, _) | Tok::Object(len, _, _) = &mut self.s.toks[parent as usize] {
                    *len += 1;
                }
                if depth > MAX_DEPTH && self.s.too_deep.is_none() {
                    self.s.too_deep = Some(self.s.open[MAX_DEPTH]);
                }
            }
            let at = self.s.toks.len() as u32;
            let rest = &t[i..];
            match t.get(i) {
                Some(&open @ (b'{' | b'[')) => {
                    if depth + 1 >= PARSE_DEPTH {
                        let detail = "the text nests deeper than the JSON parser recurses";
                        return Err(Fault::unlocated("nesting_too_deep", detail));
                    }
                    let (object, close) = if open == b'{' { (true, b'}') } else { (false, b']') };
                    self.s.open.push(at);
                    i = self.push(if object { Tok::Object(0, 0, 0) } else { Tok::Array(0, 0) }, self.ws(i + 1));
                    if t.get(i) != Some(&close) {
                        i = if object { self.key(i, at)? } else { i };
                        continue;
                    }
                }
                Some(b'"') => {
                    let (span, end) = self.string(i + 1)?;
                    i = self.push(Tok::Str(span), end);
                }
                Some(b'n') if rest.starts_with(b"null") => i = self.push(Tok::Null, i + 4),
                Some(b't') if rest.starts_with(b"true") => i = self.push(Tok::True, i + 4),
                Some(b'f') if rest.starts_with(b"false") => i = self.push(Tok::False, i + 5),
                Some(b'N') if rest.starts_with(b"NaN") => return Err(constant("NaN")),
                Some(b'I') if rest.starts_with(b"Infinity") => return Err(constant("Infinity")),
                Some(b'-') if rest.starts_with(b"-Infinity") => return Err(constant("-Infinity")),
                _ => i = self.number(i)?.ok_or_else(|| json(t, "Expecting value", i))?,
            }
            // After a value: close containers, or find the next value.
            loop {
                i = self.ws(i);
                let Some(&top) = self.s.open.last() else {
                    return if i == t.len() { Ok(()) } else { Err(json(t, "Extra data", i)) };
                };
                let object = matches!(self.s.toks[top as usize], Tok::Object(..));
                let close = if object { b'}' } else { b']' };
                match t.get(i) {
                    Some(&c) if c == close => {
                        i += 1;
                        self.s.open.pop();
                        let (end, slotted) = (self.s.toks.len() as u32, self.s.slotted);
                        match &mut self.s.toks[top as usize] {
                            Tok::Array(_, e) => *e = end,
                            Tok::Object(len, e, slot) => (*e, *slot, self.s.slotted) = (end, slotted, slotted + *len),
                            _ => unreachable!("a container"),
                        }
                    }
                    Some(b',') => {
                        let comma = i;
                        i = self.ws(i + 1);
                        if self.o.trailing_comma_message && t.get(i) == Some(&close) {
                            let kind = if object { "object" } else { "array" };
                            return Err(json(t, &format!("Illegal trailing comma before end of {kind}"), comma));
                        }
                        i = if object { self.key(i, top)? } else { i };
                        break;
                    }
                    _ => return Err(json(t, "Expecting ',' delimiter", i)),
                }
            }
        }
    }

    /// A key and its colon; returns where the value starts.
    fn key(&mut self, i: usize, object: u32) -> Result<usize, Fault> {
        if self.t.get(i) != Some(&b'"') {
            return Err(json(self.t, "Expecting property name enclosed in double quotes", i));
        }
        let (span, end) = self.string(i + 1)?;
        let bytes = self.s.bytes(self.t, span);
        let prefix = (0..8).fold(0, |p, i| p << 8 | u64::from(bytes.get(i).copied().unwrap_or(0)));
        let tok = self.s.toks.len() as u32;
        self.s.toks.push(Tok::Key(self.s.keys.len() as u32));
        self.s.keys.push(Key { span, prefix, object, tok, id: 0 });
        let i = self.ws(end);
        if self.t.get(i) != Some(&b':') {
            return Err(json(self.t, "Expecting ':' delimiter", i));
        }
        Ok(self.ws(i + 1))
    }

    /// The string from `start`, after its quote, and the index after it.
    fn string(&mut self, start: usize) -> Result<(Span, usize), Fault> {
        let (t, len) = (self.t, self.t.len());
        let (mut end, mut copied, mut lone) = (start, None, false);
        loop {
            let mut next = special(t, end);
            if next < len && t[next] <= 0x1F {
                return Err(json(t, "Invalid control character at", next));
            }
            if next == len || (t[next] == b'\\' && next + 1 == len) {
                return Err(json(t, "Unterminated string starting at", start - 1));
            }
            let arena = &mut self.s.arena;
            if t[next] == b'"' {
                let span = match copied {
                    None => Span { start: start as u32, len: (next - start) as u32, arena: false, lone },
                    Some(from) => {
                        arena.extend_from_slice(&t[end..next]);
                        Span { start: from as u32, len: (arena.len() - from) as u32, arena: true, lone }
                    }
                };
                return Ok((span, next + 1));
            }
            copied.get_or_insert(arena.len());
            arena.extend_from_slice(&t[end..next]);
            next += 1;
            if t[next] != b'u' {
                end = next + 1;
                arena.push(match t[next] {
                    b'"' | b'\\' | b'/' => t[next],
                    b'b' => 0x08,
                    b'f' => 0x0C,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    _ => return Err(json(t, "Invalid \\escape", end - 2)),
                });
                continue;
            }
            next += 1;
            end = next + 4;
            // Older CPython wants a character after the four digits.
            if end > len || (self.o.escape_needs_tail && end == len) {
                return Err(json(t, "Invalid \\uXXXX escape", next - 1));
            }
            let mut c = hex4(&t[next..end]).ok_or_else(|| json(t, "Invalid \\uXXXX escape", end - 5))?;
            if (0xD800..0xDC00).contains(&c) && end + 6 < len && t[end] == b'\\' && t[end + 1] == b'u' {
                let low = hex4(&t[end + 2..end + 6]).ok_or_else(|| json(t, "Invalid \\uXXXX escape", end + 1))?;
                if (0xDC00..0xE000).contains(&low) {
                    (c, end) = (0x10000 + ((c - 0xD800) << 10) + (low - 0xDC00), end + 6);
                }
            }
            lone |= (0xD800..0xE000).contains(&c);
            match char::from_u32(c) {
                Some(c) => arena.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes()),
                // WTF-8, which keeps code-point order: the order Python sorts keys in.
                None => arena.extend([0xED, 0x80 | ((c >> 6) & 0x3F) as u8, 0x80 | (c & 0x3F) as u8]),
            }
        }
    }

    /// The number at `start`, and the index after it, or None for none.
    fn number(&mut self, start: usize) -> Result<Option<usize>, Fault> {
        const DIGITS: &[u8] = b"0123456789";
        let t = self.t;
        let negative = usize::from(t.get(start) == Some(&b'-'));
        let mut i = match t.get(start + negative) {
            Some(b'0') => start + negative + 1,
            Some(b'1'..=b'9') => self.skip(start + negative, DIGITS),
            _ => return Ok(None),
        };
        let digits = i - start - negative;
        let mut float = false;
        if t.get(i) == Some(&b'.') && t.get(i + 1).is_some_and(u8::is_ascii_digit) {
            (float, i) = (true, self.skip(i + 1, DIGITS));
        }
        if matches!(t.get(i), Some(b'e' | b'E')) {
            let sign = i + 1 + usize::from(matches!(t.get(i + 1), Some(b'+' | b'-')));
            if t.get(sign).is_some_and(u8::is_ascii_digit) {
                (float, i) = (true, self.skip(sign, DIGITS));
            }
        }
        // The scan above admits only ASCII digits, signs, `.` and `e`.
        let text = std::str::from_utf8(&t[start..i]).unwrap_or_default();
        let tok = if float {
            // Unreachable: the scan above admits only JSON number grammar.
            let value: f64 =
                text.parse().map_err(|_| Fault::unlocated("invalid_json", format!("{text} is no number")))?;
            if value.is_infinite() {
                let detail = format!("{} is beyond the range of a double", &text[..text.len().min(40)]);
                return Err(Fault::unlocated("non_finite_number", detail));
            }
            Tok::Float(value)
        } else if self.o.max_int_digits > 0 && digits > self.o.max_int_digits {
            // CPython's int() refusal, the sign not counted.
            let detail = format!(
                "Exceeds the limit ({} digits) for integer string conversion: value has {digits} digits; \
                 use sys.set_int_max_str_digits() to increase the limit",
                self.o.max_int_digits
            );
            return Err(Fault::unlocated("integer_out_of_range", detail));
        } else if digits > MAX_DECIMAL16_DIGITS {
            Tok::Big(start as u32, (i - start) as u32)
        } else {
            let magnitude = t[start + negative..i].iter().fold(0i128, |n, d| n * 10 + i128::from(d - b'0'));
            let value = if negative == 1 { -magnitude } else { magnitude };
            i64::try_from(value).map(Tok::Int).unwrap_or_else(|_| {
                self.s.wide.push(value);
                Tok::Wide(self.s.wide.len() as u32 - 1)
            })
        };
        Ok(Some(self.push(tok, i)))
    }
}

/// The first `"`, `\\` or control character from `i`, eight bytes at a
/// time: each test flags its lowest hit exactly.
fn special(t: &[u8], mut i: usize) -> usize {
    const ONES: u64 = 0x0101_0101_0101_0101;
    while let Some(word) = t.get(i..i + 8) {
        let w = u64::from_le_bytes(word.try_into().expect("8 bytes"));
        let zero = |x: u64| x.wrapping_sub(ONES) & !x & (ONES << 7);
        let hits = zero(w ^ (ONES * 0x22)) | zero(w ^ (ONES * 0x5C)) | (w.wrapping_sub(ONES * 0x20) & !w & (ONES << 7));
        if hits != 0 {
            return i + hits.trailing_zeros() as usize / 8;
        }
        i += 8;
    }
    i + t[i..].iter().position(|&b| b == b'"' || b == b'\\' || b <= 0x1F).unwrap_or(t.len() - i)
}

fn hex4(digits: &[u8]) -> Option<u32> {
    digits.iter().try_fold(0u32, |n, &d| Some((n << 4) | (d as char).to_digit(16)?))
}
