//! The decoder against apache/parquet-testing's vectors through arrow-rs
//! `parquet-variant` (an independent decoder), hand-written faults, tags
//! and storage. The Python decoder is its reference in pyhoglake's suites.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, ListArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use parquet_variant::Variant;
use pyhoglake_variant_codec::decode::{self, unshredded, Fault, Options, Part, Step};

mod oracle;

#[test]
fn decode_and_encode_share_constants_only() {
    // The decoder is the encoder's independent check (design R4).
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let read = |dir: &str| -> Vec<(String, String)> {
        let entries = std::fs::read_dir(src.join(dir)).unwrap();
        entries
            .map(|e| e.unwrap().path())
            .map(|p| (p.display().to_string(), std::fs::read_to_string(p).unwrap()))
            .collect()
    };
    for (name, text) in read("decode") {
        assert!(!text.contains("encode::") && !text.contains("crate::encode"), "{name} uses the encoder");
    }
    for (name, text) in read("encode") {
        assert!(!text.contains("decode::") && !text.contains("crate::decode"), "{name} uses the decoder");
    }
}

#[test]
fn the_vectors_read_as_arrow_rs_reads_them() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../pyhoglake/tests/data/parquet-testing/variant");
    let mut read = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "value") {
            continue;
        }
        let (metadata, value) =
            (std::fs::read(path.with_extension("metadata")).unwrap(), std::fs::read(&path).unwrap());
        let ours = unshredded(&metadata, &value, &mode(true, false)).unwrap();
        let theirs = Variant::try_new(&metadata, &value).unwrap();
        assert!(oracle::same_value(&ours, &theirs), "{}: {ours:?} vs {theirs:?}", path.display());
        read += 1;
    }
    assert_eq!(read, 29);
}

fn text(fault: Fault) -> (&'static str, String) {
    let detail = fault.detail.into_iter().map(|part| match part {
        Part::Text(text) => text,
        other => format!("{other:?}"),
    });
    (fault.reason, detail.collect())
}

fn fault(metadata: &[u8], value: &[u8], strict: bool, canonical: bool) -> (&'static str, String) {
    text(unshredded(metadata, value, &mode(strict, canonical)).unwrap_err())
}

const EMPTY: &[u8] = b"\x01\x00\x00";

fn mode(strict: bool, canonical: bool) -> Options {
    Options { strict, canonical, ..Options::default() }
}

#[test]
fn faults_read_as_the_python_decoder_words_them() {
    let bad = |d: &str| ("malformed_variant", d.to_owned());
    let odd = |d: &str| ("non_canonical", d.to_owned());
    assert_eq!(fault(b"", b"\x00", false, false), bad("the metadata is empty"));
    assert_eq!(fault(b"\x02\x00\x00", b"\x00", false, false), bad("metadata version 2, not 1"));
    assert_eq!(fault(b"\x01\x05\x00", b"\x00", false, false), bad("the metadata is truncated"));
    assert_eq!(fault(b"\x01\x01\x01\x00a", b"\x00", false, false), bad("the metadata offsets do not climb from 0"));
    assert_eq!(
        fault(b"\x11\x02\x00\x01\x02ba", b"\x00", true, false),
        bad("sorted_strings is set, but the strings are not sorted and unique")
    );
    assert_eq!(fault(b"\x21\x00\x00", b"\x00", true, true), odd("the metadata header sets a reserved bit"));
    assert_eq!(
        fault(b"\x41\x00\x00\x00\x00", b"\x00", true, true),
        odd("the metadata offset width is not the smallest")
    );
    assert_eq!(fault(EMPTY, b"", false, false), bad("truncated"));
    assert_eq!(fault(EMPTY, b"\x00\x00", false, false), bad("1 bytes follow the value"));
    assert_eq!(fault(EMPTY, b"\x0c", false, false), bad("a primitive is truncated"));
    assert_eq!(fault(EMPTY, b"\x10\x05\x00", true, true), odd("the integer 5 is not in its smallest width"));
    assert_eq!(fault(EMPTY, b"\x20\x27\x01\x00\x00\x00", false, false), bad("a decimal has scale 39"));
    assert_eq!(
        fault(EMPTY, b"\x20\x00\x00\xca\x9a\x3b", false, false),
        bad("a decimal4 holds 10 digits, beyond its 9")
    );
    let day = [&[0x44][..], &86_400_000_000i64.to_le_bytes()].concat();
    assert_eq!(fault(EMPTY, &day, false, false), bad("a time of 86400000000 microseconds is not a time of day"));
    assert_eq!(fault(EMPTY, b"\x54", false, false), bad("unknown primitive type 21"));
    assert_eq!(fault(EMPTY, b"\x05\xff", false, false), bad("a string is not UTF-8"));
    assert_eq!(fault(EMPTY, b"\x03\x01\x00\x05\x00", false, false), bad("an array is truncated"));
    assert_eq!(
        fault(EMPTY, b"\x03\x02\x00\x02\x02\x00\x00", false, false),
        bad("an array element does not end at the next offset")
    );
    assert_eq!(
        fault(EMPTY, b"\x02\x01\x00\x00\x01\x00", false, false),
        bad("an object field id is beyond the metadata")
    );
    // An unsorted object: refused when strict, sorted otherwise; one name
    // twice is refused, quoted by the interpreter (the binding).
    let keyed = b"\x01\x02\x00\x01\x02ba";
    assert_eq!(
        fault(keyed, b"\x02\x02\x00\x01\x00\x01\x02\x00\x00", true, false),
        bad("object keys are not sorted and unique")
    );
    let read = unshredded(keyed, b"\x02\x02\x00\x01\x00\x01\x02\x00\x00", &mode(false, false)).unwrap();
    assert_eq!(decode::tagged(Some(&read)), r#"["dict",[["a",null],["b",null]]]"#);
    let twice = unshredded(keyed, b"\x02\x02\x00\x00\x00\x01\x02\x00\x00", &mode(false, false)).unwrap_err();
    assert_eq!(twice.detail, [Part::Text("an object has two fields named ".into()), Part::Quoted("b".into())]);
    // Paths: the trail, innermost first.
    let nested =
        unshredded(b"\x11\x01\x00\x01k", b"\x02\x01\x00\x00\x05\x03\x01\x00\x01\x0c", &mode(false, false)).unwrap_err();
    assert_eq!(nested.trail, Some(vec![Step::Index(0), Step::Key("k".into())]));
    // Depth: an empty array 128 levels down, and not a full one.
    let wrap =
        |inner: Vec<u8>| [vec![0x0F, 0x01, 0, 0, 0, 0], (inner.len() as u32).to_le_bytes().to_vec(), inner].concat();
    let deep = |inner: &[u8]| (0..128).fold(inner.to_vec(), |inner, _| wrap(inner));
    assert!(unshredded(EMPTY, &deep(b"\x03\x00\x00"), &mode(true, false)).is_ok());
    let too_deep = unshredded(EMPTY, &deep(b"\x03\x01\x00\x01\x00"), &mode(false, false)).unwrap_err();
    assert_eq!(text(too_deep.clone()), ("nesting_too_deep", "nested more than 128 levels deep".into()));
    assert_eq!(too_deep.trail, Some(vec![Step::Index(0); 128]));
}

#[test]
fn every_type_is_tagged_as_python_spells_it() {
    let tag = |value: &[u8]| decode::tagged(Some(&unshredded(EMPTY, value, &mode(false, false)).unwrap()));
    let with = |header: u8, body: &[u8]| tag(&[&[header][..], body].concat());
    assert_eq!(tag(b"\x00"), r#"["variant_null"]"#);
    assert_eq!(tag(b"\x04"), r#"["bool",true]"#);
    assert_eq!(with(0x18, &i64::MIN.to_le_bytes()), format!(r#"["int",{}]"#, i64::MIN));
    assert_eq!(with(0x1c, &(-0.0f64).to_le_bytes()), format!(r#"["float",{}]"#, 1u64 << 63));
    assert_eq!(with(0x38, &f32::INFINITY.to_le_bytes()), format!(r#"["float",{}]"#, f64::INFINITY.to_bits()));
    assert_eq!(tag(b"\x20\x02\x2e\xfb\xff\xff"), r#"["decimal",-1234,2]"#);
    let decimal16 = [&[0x28, 38][..], &(-(10i128.pow(38) - 1)).to_le_bytes()].concat();
    assert_eq!(tag(&decimal16), format!(r#"["decimal",{},38]"#, -(10i128.pow(38) - 1)));
    assert_eq!(with(0x2c, &(-719_162i32).to_le_bytes()), r#"["date",-719162]"#);
    assert_eq!(with(0x2c, &2_932_897i32.to_le_bytes()), r#"["scalar","date32[day]",2932897]"#);
    assert_eq!(with(0x30, &(-1i64).to_le_bytes()), r#"["datetime",-1,true]"#);
    assert_eq!(with(0x34, &i64::MAX.to_le_bytes()), format!(r#"["scalar","timestamp[us]",{}]"#, i64::MAX));
    assert_eq!(with(0x48, &5i64.to_le_bytes()), r#"["scalar","timestamp[ns, tz=UTC]",5]"#);
    assert_eq!(with(0x4c, &5i64.to_le_bytes()), r#"["scalar","timestamp[ns]",5]"#);
    assert_eq!(with(0x44, &1i64.to_le_bytes()), r#"["time",1]"#);
    assert_eq!(tag(b"\x3c\x02\x00\x00\x00\x00\xff"), r#"["bytes","00ff"]"#);
    assert_eq!(tag(b"\x0d\"\x01\\"), r#"["str","\"\u0001\\"]"#);
    assert_eq!(with(0x50, &[0xab; 16]), format!(r#"["uuid","{}"]"#, "ab".repeat(16)));
    assert_eq!(decode::tagged(None), "null");
    // A float's signaling NaN, widened (quiet) or kept signaling (CPython 3.14).
    let nan = [0x38, 0x01, 0x00, 0x80, 0x7f];
    let signaling = Options { signaling_nan: true, ..Options::default() };
    assert_eq!(tag(&nan), format!(r#"["float",{}]"#, 0x7ff8_0000_2000_0000u64));
    let kept = decode::tagged(Some(&unshredded(EMPTY, &nan, &signaling).unwrap()));
    assert_eq!(kept, format!(r#"["float",{}]"#, 0x7ff0_0000_2000_0000u64));
}

fn binary(rows: &[Option<&[u8]>]) -> ArrayRef {
    Arc::new(BinaryArray::from(rows.to_vec()))
}

fn group(value: ArrayRef, typed: Option<ArrayRef>) -> ArrayRef {
    let mut fields = vec![Field::new("value", DataType::Binary, true)];
    let mut columns = vec![value];
    if let Some(typed) = typed {
        fields.push(Field::new("typed_value", typed.data_type().clone(), true));
        columns.push(typed);
    }
    Arc::new(StructArray::new(Fields::from(fields), columns, None))
}

fn read(storage: ArrayRef, o: &Options) -> decode::Decoded {
    let field: FieldRef = Arc::new(Field::new("", storage.data_type().clone(), true));
    decode::decode(&[(storage, field)], o)
}

#[test]
fn shredded_storage_reads_back() {
    // {"a": int64 typed or in value, "l": [int64]}, with a residual.
    let n = 3;
    let a = group(binary(&[None, Some(b"\x05x"), None]), Some(Arc::new(Int64Array::from(vec![Some(7), None, None]))));
    let elements =
        group(binary(&[None, None, Some(b"\x00")]), Some(Arc::new(Int64Array::from(vec![Some(1), Some(2), None]))));
    let element = Arc::new(Field::new("element", elements.data_type().clone(), false));
    let list = ListArray::new(
        element,
        OffsetBuffer::from_lengths([2, 0, 1]),
        elements,
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let l = group(binary(&[None, None, None]), Some(Arc::new(list)));
    let typed = StructArray::new(
        Fields::from(vec![
            Field::new("a", a.data_type().clone(), false),
            Field::new("l", l.data_type().clone(), false),
        ]),
        vec![a, l],
        None,
    );
    let metadata = binary(&[Some(&b"\x11\x03\x00\x01\x02\x03alz"[..]); 3]);
    let residual = binary(&[Some(b"\x02\x01\x02\x00\x02\x0c\x01"), None, None]);
    let root = StructArray::new(
        Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("typed_value", typed.data_type().clone(), true),
        ]),
        vec![metadata, residual, Arc::new(typed)],
        None,
    );
    let storage: ArrayRef = Arc::new(root);
    assert_eq!(storage.len(), n);
    let lenient = Options { nullable: true, tagged: true, ..Options::default() };
    let out = read(storage.clone(), &lenient);
    assert_eq!(out.fault, None);
    assert_eq!(
        out.rows,
        [
            r#"["dict",[["a",["int",7]],["l",["list",[["int",1],["int",2]]]],["z",["int",1]]]]"#,
            r#"["dict",[["a",["str","x"]]]]"#,
            r#"["dict",[["l",["list",[null]]]]]"#,
        ]
    );
    // verify: row 0 is canonical; row 1's dictionary holds keys it does not use.
    let canonical = Options { strict: true, canonical: true, nullable: true, ..Options::default() };
    let (row, fault) = read(storage.clone(), &canonical).fault.unwrap();
    assert_eq!(
        (row, text(fault)),
        (Some(1), ("non_canonical", "the metadata holds keys the value does not use".into()))
    );
    assert!(decode::structure_fault(&storage).is_none());
}

#[test]
fn the_shape_of_storage_is_checked_first() {
    let o = Options { nullable: true, ..Options::default() };
    let (row, fault) = read(binary(&[Some(b"x")]), &o).fault.unwrap();
    assert_eq!(
        (row, text(fault)),
        (None, ("invalid_shredding", "VARIANT storage is a struct with a metadata column".into()))
    );
    let storage: ArrayRef = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("metadata", DataType::Binary, false), Field::new("value", DataType::Int64, true)]),
        vec![binary(&[Some(EMPTY)]), Arc::new(Int64Array::from(vec![1]))],
        None,
    ));
    let (_, fault) = read(storage, &o).fault.unwrap();
    assert_eq!(fault.detail, [Part::Text("value is ".into()), Part::Type(vec![1]), Part::Text(", not binary".into())]);
    assert_eq!(fault.trail, None);
}
