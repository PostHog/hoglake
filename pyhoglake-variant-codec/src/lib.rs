//! pyhoglake's VARIANT value codec. Private to pyhoglake, whose
//! `_variant_backend` calls it; the Python codec (`_variant_codec.py`) is
//! the reference it is held to byte for byte until it is removed.
//!
//! N1 is the encoder of JSON text into unshredded storage (`encode`); the
//! binding (`python`, the `python` feature) moves Arrow in and out.
#![forbid(unsafe_code)]

pub mod consts;
pub mod encode;
#[cfg(feature = "python")]
mod python;
