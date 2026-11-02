//! What the encoder and the decoder share: constants, never logic (VE is
//! parquet-format's VariantEncoding.md at bf0993925ccf41b1fb4b1ae241af4a66e8adf1fe).

/// How deeply a value may nest (`MAX_VARIANT_DEPTH`), and how many
/// characters of a row's path a fault keeps (`PATH_CAP`).
pub const MAX_DEPTH: usize = 128;
pub const PATH_CAP: usize = 1024;

/// The metadata of a value without objects (VE:118-121), and the header of
/// one with: version 1, `sorted_strings`.
pub const EMPTY_METADATA: [u8; 3] = [0x01, 0x00, 0x00];
pub const METADATA_SORTED: u8 = 0x11;

/// Value headers: basic types (VE:149-160), then primitive type ids shifted
/// over basic type 0 (VE:414-434).
pub const SHORT_STRING: u8 = 0x01;
pub const OBJECT: u8 = 0x02;
pub const ARRAY: u8 = 0x03;
pub const NULL: u8 = 0x00;
pub const TRUE: u8 = 0x04;
pub const FALSE: u8 = 0x08;
pub const INT8: u8 = 0x0C;
pub const INT16: u8 = 0x10;
pub const INT32: u8 = 0x14;
pub const INT64: u8 = 0x18;
pub const DOUBLE: u8 = 0x1C;
pub const DECIMAL16: u8 = 0x28;
pub const LONG_STRING: u8 = 0x40;

/// The longest short string, the most elements before a 4-byte count, and
/// the most digits of an integer as decimal16 of scale 0.
pub const MAX_SHORT_STRING: usize = 63;
pub const MAX_SMALL_COUNT: usize = 255;
pub const MAX_DECIMAL16_DIGITS: usize = 38;
