//! A parsed row's dictionary and value bytes: the Python codec's
//! `_metadata` and `_encode`, byte for byte (D8, D9).

use super::parse::{Scratch, Tok};
use super::{Fault, Step};
use crate::consts::{
    ARRAY, DECIMAL16, DOUBLE, EMPTY_METADATA, FALSE, INT16, INT32, INT64, INT8, LONG_STRING, MAX_SHORT_STRING,
    MAX_SMALL_COUNT, METADATA_SORTED, NULL, OBJECT, SHORT_STRING, TRUE,
};

/// What a row's encode reuses: the dictionary's keys, a stack of the
/// open containers' offsets, and a header.
#[derive(Default)]
pub struct Bufs {
    names: Vec<u32>,
    offsets: Vec<usize>,
    head: Vec<u8>,
}

/// The fewest bytes, 1 to 4, of an unsigned field that holds `n`.
fn width(n: usize) -> usize {
    1 + usize::from(n > 0xFF) + usize::from(n > 0xFFFF) + usize::from(n > 0xFF_FFFF)
}

/// `n` in `width` little-endian bytes: four written, the rest cut, so the
/// copy is of a known size.
fn put(out: &mut Vec<u8>, width: usize, n: usize) {
    out.extend_from_slice(&u32::try_from(n).expect("a Variant offset fits 32 bits").to_le_bytes());
    out.truncate(out.len() - 4 + width);
}

/// The index after the value at `at`.
fn next(s: &Scratch, at: u32) -> u32 {
    match s.toks[at as usize] {
        Tok::Array(_, end) | Tok::Object(_, end, _) => end,
        _ => at + 1,
    }
}

/// The steps from the root to the container at `target`, outermost first.
pub fn path_to(s: &Scratch, text: &[u8], target: u32) -> Vec<Step> {
    let (mut steps, mut at) = (Vec::new(), 0);
    while at != target {
        let (Tok::Array(len, _) | Tok::Object(len, _, _)) = s.toks[at as usize] else { unreachable!("a container") };
        let mut child = at + 1;
        for index in 0..len {
            let (step, value) = match s.toks[child as usize] {
                Tok::Key(key) => (Step::Key(s.key(text, key).to_vec()), child + 1),
                _ => (Step::Index(index), child),
            };
            child = next(s, value);
            if (value..child).contains(&target) {
                steps.push(step);
                at = value;
                break;
            }
        }
    }
    steps
}

/// Writes the row's metadata: every key once, sorted by its bytes
/// (`s.order` and the ids, from the parse), `sorted_strings` set, one
/// offset width for the count and the total length.
pub fn metadata(s: &Scratch, text: &[u8], b: &mut Bufs, out: &mut Vec<u8>) -> Result<(), Fault> {
    b.names.clear();
    b.offsets.clear();
    if s.keys.is_empty() {
        out.extend_from_slice(&EMPTY_METADATA);
        return Ok(());
    }
    for &key in &s.order {
        if b.names.last().is_none_or(|&last| s.keys[last as usize].id != s.keys[key as usize].id) {
            b.names.push(key);
        }
    }
    // Python fails on the first such name, at the first object holding it
    // (`_key_trail`): the first of its run, as the order breaks ties by object.
    if let Some(&key) = b.names.iter().find(|&&key| s.keys[key as usize].span.lone) {
        let mut steps = path_to(s, text, s.keys[key as usize].object);
        steps.push(Step::Key(s.key(text, key).to_vec()));
        return Err(Fault::along(steps, "invalid_unicode", "an object key holds an unpaired surrogate"));
    }
    let total: usize = b.names.iter().map(|&key| s.keys[key as usize].span.len as usize).sum();
    let w = width(total.max(b.names.len()));
    out.push(METADATA_SORTED | ((w as u8 - 1) << 6));
    put(out, w, b.names.len());
    put(out, w, 0);
    let mut offset = 0;
    for &key in &b.names {
        offset += s.keys[key as usize].span.len as usize;
        put(out, w, offset);
    }
    for &key in &b.names {
        out.extend_from_slice(s.key(text, key));
    }
    Ok(())
}

/// Appends the bytes of the value at `at`. Its containers add their steps
/// to a fault's path on the way out.
pub fn value(s: &Scratch, text: &[u8], b: &mut Bufs, at: u32, out: &mut Vec<u8>) -> Result<(), Fault> {
    match s.toks[at as usize] {
        Tok::Null => out.push(NULL),
        Tok::True => out.push(TRUE),
        Tok::False => out.push(FALSE),
        Tok::Int(n) => {
            // The smallest integer type that holds it (D1).
            let (header, w) = match n {
                -0x80..=0x7F => (INT8, 1),
                -0x8000..=0x7FFF => (INT16, 2),
                -0x8000_0000..=0x7FFF_FFFF => (INT32, 4),
                _ => (INT64, 8),
            };
            out.push(header);
            out.extend_from_slice(&n.to_le_bytes()[..w]);
        }
        Tok::Wide(index) => {
            out.extend_from_slice(&[DECIMAL16, 0]);
            out.extend_from_slice(&s.wide[index as usize].to_le_bytes());
        }
        Tok::Big(start, len) => {
            let bits = bit_length(&text[start as usize..(start + len) as usize]);
            return Err(Fault::at(
                "integer_out_of_range",
                format!("an integer of {bits} bits has more than 38 digits"),
            ));
        }
        Tok::Float(x) => {
            out.push(DOUBLE);
            out.extend_from_slice(&x.to_le_bytes());
        }
        Tok::Str(span) if span.lone => {
            return Err(Fault::at("invalid_unicode", "a string holds an unpaired surrogate"))
        }
        Tok::Str(span) => {
            let bytes = s.bytes(text, span);
            if bytes.len() <= MAX_SHORT_STRING {
                out.push(((bytes.len() as u8) << 2) | SHORT_STRING);
            } else {
                out.push(LONG_STRING);
                put(out, 4, bytes.len());
            }
            out.extend_from_slice(bytes);
        }
        Tok::Array(len, _) => {
            let (start, offsets) = (out.len(), b.offsets.len());
            let mut child = at + 1;
            for index in 0..len {
                b.offsets.push(out.len() - start);
                value(s, text, b, child, out).map_err(|fault| fault.step(Step::Index(index)))?;
                child = next(s, child);
            }
            header(s, b, out, start, offsets, None);
        }
        Tok::Object(len, _, slot) => {
            // Values in field-id order, which is key order (VE:454-465).
            let (start, offsets, keys) = (out.len(), b.offsets.len(), &s.slots[(slot - len) as usize..slot as usize]);
            for &key in keys {
                b.offsets.push(out.len() - start);
                let child = s.keys[key as usize].tok + 1;
                value(s, text, b, child, out).map_err(|fault| fault.step(Step::Key(s.key(text, key).to_vec())))?;
            }
            header(s, b, out, start, offsets, Some(keys));
        }
        Tok::Key(_) => unreachable!("a key is read with its object"),
    }
    Ok(())
}

/// Puts the header of the container whose values were written from
/// `start` in front of them: an array's, or with its `keys` an object's.
fn header(s: &Scratch, b: &mut Bufs, out: &mut Vec<u8>, start: usize, offsets: usize, keys: Option<&[u32]>) {
    let (total, n) = (out.len() - start, b.offsets.len() - offsets);
    let (ow, large) = (width(total), n > MAX_SMALL_COUNT);
    let id = |key: &u32| s.keys[*key as usize].id as usize;
    let iw = keys.map(|keys| width(keys.last().map_or(0, id)));
    b.head.clear();
    b.head.push(match iw {
        None => ARRAY | ((ow as u8 - 1) << 2) | (u8::from(large) << 4),
        Some(iw) => OBJECT | ((ow as u8 - 1) << 2) | ((iw as u8 - 1) << 4) | (u8::from(large) << 6),
    });
    put(&mut b.head, if large { 4 } else { 1 }, n);
    for key in keys.unwrap_or_default() {
        put(&mut b.head, iw.unwrap_or_default(), id(key));
    }
    for offset in b.offsets.drain(offsets..).chain([total]) {
        put(&mut b.head, ow, offset);
    }
    out.splice(start..start, b.head.iter().copied());
}

/// `int.bit_length()` of a JSON integer's text, sign aside.
fn bit_length(text: &[u8]) -> u64 {
    let mut limbs: Vec<u32> = Vec::new();
    for chunk in text.strip_prefix(b"-").unwrap_or(text).chunks(9) {
        let mut carry = chunk.iter().fold(0u64, |n, d| n * 10 + u64::from(d - b'0'));
        for limb in &mut limbs {
            let product = u64::from(*limb) * 10u64.pow(chunk.len() as u32) + carry;
            (*limb, carry) = (product as u32, product >> 32);
        }
        if carry > 0 {
            limbs.push(carry as u32);
        }
    }
    limbs.last().map_or(0, |top| 32 * (limbs.len() as u64 - 1) + u64::from(32 - top.leading_zeros()))
}
