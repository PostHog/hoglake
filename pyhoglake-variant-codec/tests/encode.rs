//! The encoder against hand-written bytes, the Python codec's faults and
//! messages, and arrow-rs `parquet-variant` as an independent decoder.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, NullArray, StructArray};
use pyhoglake_variant_codec::encode::{encode_json, Detail, Encoded, Options, Plan, PARSE_DEPTH};

mod oracle;

/// The Python codec's defaults, on 3.13.
fn defaults() -> Options {
    Options {
        nullable: true,
        stop_at_invalid: false,
        chunk_rows: 8192,
        chunk_bytes: 512 * 1024 * 1024,
        max_int_digits: 4300,
        trailing_comma_message: true,
        escape_needs_tail: true,
    }
}

fn run(rows: &[Option<&[u8]>], o: &Options) -> Encoded {
    let array: ArrayRef = Arc::new(BinaryArray::from(rows.to_vec()));
    encode_json(&[array], &Plan::default(), o).expect("binary input")
}

fn bytes(chunk: &StructArray, row: usize) -> (Vec<u8>, Vec<u8>) {
    let child = |i: usize| chunk.column(i).as_any().downcast_ref::<BinaryArray>().unwrap().value(row).to_vec();
    (child(0), child(1))
}

/// One row's (metadata, value), or its (reason, path, detail).
type Row = Result<(Vec<u8>, Vec<u8>), (String, Option<String>, String)>;

fn one_with(text: &str, o: &Options) -> Row {
    let mut out = run(&[Some(text.as_bytes())], o);
    match out.invalid.pop() {
        None => Ok(bytes(&out.chunks[0], 0)),
        Some((_, fault)) => {
            let path = fault.path();
            let detail = match fault.detail {
                Detail::Text(text) => text,
                Detail::DuplicateKey(key) => format!("duplicate {}", String::from_utf8_lossy(&key)),
            };
            Err((fault.reason.to_owned(), path, detail))
        }
    }
}

fn one(text: &str) -> Row {
    one_with(text, &defaults())
}

fn value(text: &str) -> Vec<u8> {
    one(text).unwrap().1
}

fn fault(text: &str) -> (String, Option<String>, String) {
    one(text).unwrap_err()
}

fn unlocated(reason: &str, detail: &str) -> (String, Option<String>, String) {
    (reason.to_owned(), None, detail.to_owned())
}

#[test]
fn scalars_take_the_smallest_type() {
    assert_eq!(value("null"), [0x00]);
    assert_eq!(value("true"), [0x04]);
    assert_eq!(value("false"), [0x08]);
    assert_eq!(value("-0"), [0x0C, 0]);
    assert_eq!(value("127"), [0x0C, 127]);
    assert_eq!(value("-128"), [0x0C, 0x80]);
    assert_eq!(value("128"), [0x10, 128, 0]);
    assert_eq!(value("-32769"), [0x14, 0xFF, 0x7F, 0xFF, 0xFF]);
    assert_eq!(value("2147483648"), [0x18, 0, 0, 0, 0x80, 0, 0, 0, 0]);
    let mut big = vec![0x28, 0];
    big.extend_from_slice(&(i128::from(i64::MAX) + 1).to_le_bytes());
    assert_eq!(value("9223372036854775808"), big);
    let mut most = vec![0x28, 0];
    most.extend_from_slice(&(-(10i128.pow(38) - 1)).to_le_bytes());
    assert_eq!(value(&format!("-{}", "9".repeat(38))), most);
    assert_eq!(value("-0.0"), [&[0x1Cu8][..], &(-0.0f64).to_le_bytes()].concat());
    assert_eq!(value("1E2"), [&[0x1Cu8][..], &100f64.to_le_bytes()].concat());
    assert_eq!(value("1e-400"), [&[0x1Cu8][..], &0f64.to_le_bytes()].concat());
    assert_eq!(one("1").unwrap().0, [0x01, 0x00, 0x00]);
}

#[test]
fn strings_are_short_up_to_63_bytes() {
    let text = |n: usize| format!("\"{}\"", "x".repeat(n));
    assert_eq!(value(&text(63))[0], 63 << 2 | 1);
    assert_eq!(value(&text(64))[..5], [0x40, 64, 0, 0, 0]);
    assert_eq!(value(r#""a\n\u00e9\ud83d\ude00\/""#), [&[9 << 2 | 1][..], "a\né😀/".as_bytes()].concat());
}

#[test]
fn objects_sort_the_dictionary_and_their_fields() {
    let (metadata, bytes) = one(r#"{"b":1,"a":{"b":true}}"#).unwrap();
    assert_eq!(metadata, [0x11, 2, 0, 1, 2, b'a', b'b']);
    // Fields in id order: a (id 0) then b; a's value is {"b": true}.
    assert_eq!(bytes, [0x02, 2, 0, 1, 0, 6, 8, 0x02, 1, 1, 0, 1, 0x04, 0x0C, 1]);
    assert_eq!(one("{}").unwrap(), (vec![1, 0, 0], vec![0x02, 0, 0]));
    assert_eq!(value("[]"), [0x03, 0, 0]);
    assert_eq!(value("[1,\"x\"]"), [0x03, 2, 0, 2, 4, 0x0C, 1, 0x05, b'x']);
}

#[test]
fn widths_grow_at_their_boundaries() {
    // 256 elements: a 4-byte count; offsets past 255 bytes: 2-byte offsets.
    let items = vec!["1"; 256].join(",");
    let v = value(&format!("[{items}]"));
    assert_eq!(v[0], 0x03 | (1 << 2) | (1 << 4));
    assert_eq!(v[1..5], [0, 1, 0, 0]);
    let v = value(&format!("[{}]", vec!["1"; 255].join(",")));
    assert_eq!(v[..2], [0x03 | (1 << 2), 255]);
    // 65,536 bytes of values: 3-byte offsets.
    let long = format!("[\"{}\"]", "y".repeat(65_531));
    assert_eq!(value(&long)[0] >> 2 & 3, 2);
    // 257 keys: field ids up to 256 take 2 bytes.
    let keys: Vec<String> = (0..257).map(|i| format!("\"k{i:03}\":0")).collect();
    let (metadata, v) = one(&format!("{{{}}}", keys.join(","))).unwrap();
    assert_eq!(v[0], 0x02 | (1 << 2) | (1 << 4) | (1 << 6));
    assert_eq!(metadata[0], 0x11 | (1 << 6));
}

#[test]
fn json_faults_carry_pythons_messages_and_positions() {
    let o312 = Options { trailing_comma_message: false, ..defaults() };
    let o314 = Options { escape_needs_tail: false, ..defaults() };
    let json = |detail: &str| unlocated("invalid_json", detail);
    for (text, detail) in [
        ("", "Expecting value at character 0"),
        (" ", "Expecting value at character 1"),
        ("[1,]", "Illegal trailing comma before end of array at character 2"),
        ("{\"a\":1, }", "Illegal trailing comma before end of object at character 6"),
        ("[1 2]", "Expecting ',' delimiter at character 3"),
        ("{\"a\" 1}", "Expecting ':' delimiter at character 5"),
        ("{", "Expecting property name enclosed in double quotes at character 1"),
        ("\"abc", "Unterminated string starting at at character 0"),
        ("\"a\\", "Unterminated string starting at at character 0"),
        ("\"\\x\"", "Invalid \\escape at character 1"),
        ("\"\u{1}\"", "Invalid control character at at character 1"),
        ("\"\\u0041", "Invalid \\uXXXX escape at character 2"),
        ("\"\\ud800\\uzz\"", "Invalid \\uXXXX escape at character 8"),
        ("01", "Extra data at character 1"),
        ("1.", "Extra data at character 1"),
        ("-", "Expecting value at character 0"),
        ("-NaN", "Expecting value at character 0"),
        ("nul", "Expecting value at character 0"),
        ("\u{feff}1", "Expecting value at character 0"),
        ("[\"é\" x]", "Expecting ',' delimiter at character 5"),
    ] {
        assert_eq!(fault(text), json(detail), "{text:?}");
    }
    assert_eq!(one_with("[1,]", &o312).unwrap_err(), json("Expecting value at character 3"));
    let property = "Expecting property name enclosed in double quotes at character 7";
    assert_eq!(one_with("{\"a\":1,}", &o312).unwrap_err(), json(property));
    let tail = "Unterminated string starting at at character 0";
    assert_eq!(one_with("\"\\u0041", &o314).unwrap_err(), json(tail));
}

#[test]
fn other_parse_faults_come_in_text_order() {
    let non_finite = |detail: &str| unlocated("non_finite_number", detail);
    assert_eq!(fault("NaN"), non_finite("NaN is not a JSON number"));
    assert_eq!(fault("[-Infinity]"), non_finite("-Infinity is not a JSON number"));
    let wide = format!("1{}e400", "0".repeat(50));
    assert_eq!(fault(&wide), non_finite(&format!("{} is beyond the range of a double", &wide[..40])));
    // A duplicate is refused when its object closes: before a later fault,
    // not before an earlier one, and never in an object left open.
    let duplicate = || ("duplicate_key".to_owned(), None, "duplicate a".to_owned());
    assert_eq!(fault(r#"{"a":1,"\u0061":2}"#), duplicate());
    assert_eq!(fault(r#"[{"a":1,"a":2}, NaN"#), duplicate());
    assert_eq!(fault(r#"{"a":1,"a":2,"b":NaN}"#), non_finite("NaN is not a JSON number"));
    assert_eq!(fault(r#"{"x":{"a":1,"a":2},"x":3}"#), duplicate());
    assert_eq!(fault(r#"{"a":{"b":1,"b":2},"a":3}"#).2, "duplicate b");
    assert_eq!(fault(r#"{"a":1,"a":2"#).0, "invalid_json");
    assert_eq!(fault(r#"{"a":1,"a":2} x"#), duplicate());
}

#[test]
fn integers_beyond_38_digits_fault_when_encoded() {
    let limit = |digits: usize| {
        let detail = format!(
            "Exceeds the limit (4300 digits) for integer string conversion: value has {digits} digits; \
             use sys.set_int_max_str_digits() to increase the limit"
        );
        unlocated("integer_out_of_range", &detail)
    };
    assert_eq!(fault(&format!("-{}", "9".repeat(4301))), limit(4301));
    let located = |bits: u64, path: &str| {
        (
            "integer_out_of_range".to_owned(),
            Some(path.to_owned()),
            format!("an integer of {bits} bits has more than 38 digits"),
        )
    };
    assert_eq!(fault(&format!("[1,1{}]", "0".repeat(38))), located(127, "$[1]"));
    assert_eq!(fault(&"9".repeat(4300)), located(14285, "$"));
    let unlimited = Options { max_int_digits: 0, ..defaults() };
    assert_eq!(one_with(&"9".repeat(5000), &unlimited).unwrap_err(), located(16610, "$"));
    // Fields fault in key order, not text order.
    assert_eq!(fault(&format!(r#"{{"b":1{0},"a":1{0}}}"#, "0".repeat(40))).1.as_deref(), Some("$.a"));
}

#[test]
fn depth_is_bounded_at_128() {
    let nest = |n: usize| format!("{}1{}", "[".repeat(n), "]".repeat(n));
    assert!(one(&nest(128)).is_ok());
    let path = format!("${}", "[0]".repeat(128));
    let detail = "nested more than 128 levels deep".to_owned();
    assert_eq!(fault(&nest(129)), ("nesting_too_deep".to_owned(), Some(path), detail));
    // Text errors win over depth; the parser's own bound is unlocated.
    assert_eq!(fault(&format!("{}x", nest(129))).0, "invalid_json");
    let deep = "[".repeat(PARSE_DEPTH);
    let recursion = "the text nests deeper than the JSON parser recurses";
    assert_eq!(fault(&deep), unlocated("nesting_too_deep", recursion));
    assert_eq!(fault(&"[".repeat(PARSE_DEPTH - 1)).0, "invalid_json");
}

#[test]
fn unpaired_surrogates_fault_with_their_paths() {
    let key = (
        "invalid_unicode".to_owned(),
        Some(r#"$.x["\ud800"]"#.to_owned()),
        "an object key holds an unpaired surrogate".to_owned(),
    );
    assert_eq!(fault(r#"{"x":{"\ud800":1},"y":{"\ud800":2}}"#), key);
    // The first object holding the key, not the key seen first.
    assert_eq!(fault(r#"{"x":{"\udfff":1},"\udfff":2}"#).1.as_deref(), Some(r#"$["\udfff"]"#));
    let string = |path: &str| {
        ("invalid_unicode".to_owned(), Some(path.to_owned()), "a string holds an unpaired surrogate".to_owned())
    };
    assert_eq!(fault(r#"{"a.b":["ok","\udc00\ud800"]}"#), string(r#"$["a.b"][1]"#));
    assert_eq!(fault(r#"{"z":"\ud800","a":"\ud800"}"#), string("$.a"));
    assert_eq!(fault(r#""\ud800\u0041""#), string("$"));
}

#[test]
fn paths_spell_keys_as_member_does() {
    let at = |key: &str| fault(&format!("{{{key}:\"\\ud800\"}}")).1.unwrap();
    assert_eq!(at(r#""a""#), "$.a");
    assert_eq!(at(r#""""#), r#"$[""]"#);
    assert_eq!(at(r#""q\"\\""#), r#"$["q\"\\"]"#);
    assert_eq!(at(r#""é😀""#), "$.é😀");
    assert_eq!(at(r#""\u2028\t\u007f😀""#), r#"$["\u2028\t\u007f\ud83d\ude00"]"#);
    let long = at(&format!("\"{}\"", "k".repeat(2000)));
    assert_eq!((long.chars().count(), &long[1018..]), (1024, "kkk..."));
}

#[test]
fn utf8_faults_name_pythons_reason() {
    for (text, reason) in [
        (&b"\xff"[..], "invalid start byte"),
        (b"\"\xc0\x80\"", "invalid start byte"),
        (b"\"\xe0\x80\x80\"", "invalid continuation byte"),
        (b"\"\xed\xa0\x80\"", "invalid continuation byte"),
        (b"\"\xf0\x9f", "unexpected end of data"),
    ] {
        let mut out = run(&[Some(text)], &defaults());
        let (_, fault) = out.invalid.pop().unwrap();
        assert_eq!(fault.detail, Detail::Text(format!("the text is not UTF-8 ({reason})")));
    }
}

#[test]
fn chunks_close_on_rows_or_bytes() {
    let row =
        &b"\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\""[..];
    let rows = vec![Some(row); 7];
    // 105 bytes of value and 3 of metadata a row: a chunk closes on reaching the cap.
    let sizes = |o: Options| run(&rows, &o).chunks.iter().map(Array::len).collect::<Vec<_>>();
    assert_eq!(sizes(Options { chunk_bytes: 250, ..defaults() }), [3, 3, 1]);
    assert_eq!(sizes(Options { chunk_bytes: 216, ..defaults() }), [2, 2, 2, 1]);
    assert_eq!(sizes(Options { chunk_rows: 7, ..defaults() }), [7]);
    let out = run(&[None, Some(b"x"), Some(b"1"), None], &Options { chunk_rows: 2, ..defaults() });
    assert_eq!((out.rows, out.sql_null_rows, out.invalid.len()), (4, 2, 1));
    assert_eq!(out.chunks.iter().map(|c| c.null_count()).collect::<Vec<_>>(), [2, 1]);
    let (metadata, value) = bytes(&out.chunks[0], 1);
    assert!(metadata.is_empty() && value.is_empty());
    let empty: ArrayRef = Arc::new(NullArray::new(0));
    assert_eq!(encode_json(&[empty], &Plan::default(), &defaults()).unwrap().chunks.len(), 1);
}

#[test]
fn raise_mode_and_not_null_stop_at_the_first_row() {
    let stop = Options { stop_at_invalid: true, ..defaults() };
    let out = run(&[Some(b"1"), Some(b"[1,"), Some(b"x")], &stop);
    assert_eq!((out.rows, out.invalid.len(), out.invalid[0].0), (2, 1, 1));
    let not_null = Options { nullable: false, ..defaults() };
    let out = run(&[Some(b"x"), None, Some(b"y")], &not_null);
    let reasons: Vec<_> = out.invalid.iter().map(|(row, f)| (*row, f.reason)).collect();
    assert_eq!(reasons, [(0, "invalid_json"), (1, "sql_null_in_not_null")]);
}

/// arrow-rs reads every value back as serde_json reads its text.
#[test]
fn an_independent_decoder_agrees() {
    let texts = [
        r#"{"$browser":"Chrome","$set":{"email":"a@b.c","n":[1,2.5,-0.0,null]},"ok":true,"":{}}"#,
        r#"[1e22, 123456789012345678901234567890, -9223372036854775809, 5e-324, "é", [[[]]]]"#,
        r#"{"b":{"b":{"b":[false]}},"a":"x","B":"\u00e9\ud83d\ude00"}"#,
    ];
    let wide: Vec<String> = (0..300).map(|i| format!("\"key{i}\":{}", "1".repeat(i % 20 + 1))).collect();
    let wide = format!("{{{}}}", wide.join(","));
    let deep = format!("{}{}", "[".repeat(128), "]".repeat(128));
    for text in texts.iter().copied().chain([wide.as_str(), deep.as_str()]) {
        let (metadata, value) = one(text).unwrap();
        oracle::check(&metadata, &value, text.as_bytes());
    }
}
