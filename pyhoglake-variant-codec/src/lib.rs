//! pyhoglake's VARIANT value codec. Private to pyhoglake, whose
//! `_variant_backend` calls it; the Python codec (`_variant_codec.py`) is
//! the reference it is held to byte for byte until it is removed.
//!
//! It encodes JSON text into storage, shredded or not (`encode`), and reads
//! any conformant storage back (`decode`); the binding (`python`, the
//! `python` feature) moves Arrow in and out.
#![forbid(unsafe_code)]

pub mod consts;
pub mod decode;
pub mod encode;
#[cfg(feature = "python")]
mod python;
