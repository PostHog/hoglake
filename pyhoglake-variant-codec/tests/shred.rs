//! Shredding against hand-written columns: placement by exact type, the
//! placeholders under null parents, the counts, the fault order and the
//! budget of a failed row, and the plans refused.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float64Type, Int64Type, Int8Type};
use arrow_array::{Array, ArrayRef, BinaryArray, StructArray};
use arrow_schema::{DataType, Field, Fields};
use pyhoglake_variant_codec::encode::{encode_json, Encoded, Options, Plan};

fn options() -> Options {
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

/// A declared group: `value`, and a `typed_value` of `typed` unless None.
fn group(typed: Option<DataType>) -> DataType {
    let mut fields = vec![Field::new("value", DataType::Binary, true)];
    fields.extend(typed.map(|kind| Field::new("typed_value", kind, true)));
    DataType::Struct(Fields::from(fields))
}

fn object(fields: &[(&str, DataType)]) -> DataType {
    let fields = fields.iter().map(|(name, kind)| Field::new(*name, kind.clone(), false));
    DataType::Struct(Fields::from_iter(fields))
}

fn storage(root: DataType) -> DataType {
    let DataType::Struct(root) = root else { unreachable!() };
    let metadata = Arc::new(Field::new("metadata", DataType::Binary, false));
    DataType::Struct(Fields::from_iter([metadata].into_iter().chain(root.iter().cloned())))
}

/// `{s: string, n: int8, l: array<int64>, o: {b: boolean}, v: variant,
/// d: decimal16(38, 0), u: uuid, x: double}`.
fn declared() -> Plan {
    let list = DataType::List(Arc::new(Field::new("element", group(Some(DataType::Int64)), false)));
    let o = object(&[("b", group(Some(DataType::Boolean)))]);
    let root = object(&[
        ("s", group(Some(DataType::Utf8))),
        ("n", group(Some(DataType::Int8))),
        ("l", group(Some(list))),
        ("o", group(Some(o))),
        ("v", group(None)),
        ("d", group(Some(DataType::Decimal128(38, 0)))),
        ("u", group(Some(DataType::FixedSizeBinary(16)))),
        ("x", group(Some(DataType::Float64))),
    ]);
    Plan::new(&storage(group(Some(root)))).unwrap()
}

fn run(plan: &Plan, rows: &[Option<&str>], o: &Options) -> Encoded {
    let array: ArrayRef = Arc::new(BinaryArray::from_iter(rows.iter().map(|row| row.map(str::as_bytes))));
    encode_json(&[array], plan, o).unwrap()
}

fn child<'a>(array: &'a StructArray, name: &str) -> &'a StructArray {
    array.column_by_name(name).unwrap().as_struct()
}

#[test]
fn values_go_to_typed_value_by_exact_type_only() {
    let row = r#"{"s":"x","n":1,"l":[2,"a"],"o":{"b":true},"v":[1],"d":12345678901234567890,"x":-0.0,"r":2.5}"#;
    let other = r#"{"s":1,"n":128,"o":{"b":0},"d":1,"u":"x","x":1}"#;
    let out = run(&declared(), &[Some(row), Some(other)], &options());
    let chunk = &out.chunks[0];
    // The residual holds r alone; every declared field is typed.
    let residual = chunk.column(1).as_binary::<i32>();
    assert_eq!(residual.value(0), [0x02, 0x01, 0x05, 0x00, 0x09, 0x1C, 0, 0, 0, 0, 0, 0, 4, 0x40]);
    let fields = chunk.column(2).as_struct();
    let typed = |name: &str| child(fields, name).column(1).clone();
    assert_eq!(typed("s").as_string::<i32>().value(0), "x");
    assert_eq!(typed("n").as_primitive::<Int8Type>().value(0), 1);
    let elements = typed("l").as_list::<i32>().value(0);
    let elements = elements.as_struct();
    assert_eq!(elements.column(1).as_primitive::<Int64Type>().value(0), 2);
    assert!(elements.column(1).is_null(1) && elements.column(0).as_binary::<i32>().value(1) == [0x05, b'a']);
    assert!(typed("o").as_struct().column(0).as_struct().column(1).as_boolean().value(0));
    assert_eq!(child(fields, "v").column(0).as_binary::<i32>().value(0), [0x03, 0x01, 0x00, 0x02, 0x0C, 0x01]);
    assert_eq!(typed("d").as_primitive::<Decimal128Type>().value(0), 12345678901234567890);
    assert_eq!(typed("x").as_primitive::<Float64Type>().value(0).to_bits(), (-0.0f64).to_bits());
    // Across classes and out of range, nothing is typed: an int is no
    // string, 128 no int8, 0 no boolean, a small int no decimal16, 1 no
    // double; a uuid takes nothing JSON gives.
    for name in ["s", "n", "d", "u", "x"] {
        assert!(typed(name).is_null(1) && child(fields, name).column(0).is_valid(1), "{name}");
    }
    assert!(typed("o").as_struct().column(0).as_struct().column(1).is_null(1));
    // (typed, fallback, variant_null) per typed node, in declaration order:
    // $, s, n, l, l[*], o, o.b, d, u, x.
    let counts =
        [[2, 0, 0], [1, 1, 0], [1, 1, 0], [1, 0, 0], [1, 1, 0], [2, 0, 0], [1, 1, 0], [1, 1, 0], [0, 1, 0], [1, 1, 0]];
    assert_eq!(out.counts, counts);
}

#[test]
fn rows_without_a_typed_value_hold_placeholders() {
    let rows = [Some(r#""text""#), Some("null"), None, Some("[1,"), Some("{}")];
    let out = run(&declared(), &rows, &options());
    let chunk = &out.chunks[0];
    assert_eq!(chunk.nulls().map(|n| n.null_count()), Some(2));
    let (value, fields) = (chunk.column(1).as_binary::<i32>(), chunk.column(2).as_struct());
    assert_eq!((value.value(0), value.value(1)), (&[0x11, b't', b'e', b'x', b't'][..], &[0x00][..]));
    assert!(value.is_null(2) && value.is_null(3) && value.is_null(4));
    assert_eq!(fields.nulls().map(|n| n.null_count()), Some(4));
    // Field groups are never null; under a null typed_value, and for a
    // missing field, both their columns are, and a list is empty.
    for (index, column) in fields.columns().iter().enumerate() {
        let group = column.as_struct();
        assert_eq!(group.null_count(), 0);
        assert!(group.columns().iter().all(|c| c.null_count() == 5), "{index}");
    }
    let list = child(fields, "l").column(1).as_list::<i32>();
    assert_eq!(list.value_offsets(), [0; 6]);
    assert_eq!(out.counts[0], [1, 1, 1]);
}

#[test]
fn faults_come_residual_first_then_declared_order() {
    let rows = [
        r#"{"z":"\ud800","s":"\ud800"}"#,
        r#"{"n":99999999999999999999999999999999999999999,"s":"\ud800"}"#,
        r#"{"l":[1,[2,"\udc00"]],"o":{"b":"\ud800"}}"#,
    ];
    let out = run(&declared(), &rows.map(Some), &options());
    let paths: Vec<_> = out.invalid.iter().map(|(_, fault)| fault.path().unwrap()).collect();
    assert_eq!(paths, ["$.z", "$.s", "$.l[1][1]"]);
    assert_eq!(out.chunks[0].null_count(), 3);
}

#[test]
fn a_failed_row_keeps_the_bytes_it_placed() {
    // The residual (207 bytes) is counted before n fails, as Python's
    // builders count it, so the chunk closes on the failed row.
    let row = format!(r#"{{"r":"{}","n":{}}}"#, "y".repeat(200), "9".repeat(39));
    let o = Options { chunk_bytes: 200, ..options() };
    let out = run(&declared(), &[Some(&row), Some(&row), Some("1")], &o);
    assert_eq!(out.chunks.iter().map(Array::len).collect::<Vec<_>>(), [1, 1, 1]);
    assert_eq!(out.invalid.len(), 2);
}

#[test]
fn plans_not_laid_out_as_compile_plan_does_are_refused() {
    let metadata = Field::new("metadata", DataType::Binary, false);
    let mut refused = vec![DataType::Int64, storage(group(Some(DataType::LargeUtf8)))];
    refused.push(DataType::Struct(Fields::from(vec![Field::new("value", DataType::Binary, true)])));
    let nullable_group = Field::new("a", group(Some(DataType::Int8)), true);
    refused.push(storage(group(Some(DataType::Struct(Fields::from(vec![nullable_group]))))));
    let element = Field::new("element", group(Some(DataType::Int8)), true);
    refused.push(storage(group(Some(DataType::List(Arc::new(element))))));
    let required_value = Field::new("value", DataType::Binary, false);
    let int8 = Field::new("typed_value", DataType::Int8, true);
    refused.push(DataType::Struct(Fields::from(vec![metadata, required_value, int8])));
    for kind in refused {
        assert!(Plan::new(&kind).is_err(), "{kind}");
    }
}
