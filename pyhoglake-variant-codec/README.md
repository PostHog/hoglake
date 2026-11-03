# pyhoglake-variant-codec

pyhoglake's VARIANT value codec, in Rust. It encodes JSON text as the
Parquet VARIANT storage that [pyhoglake](https://pypi.org/project/pyhoglake/)
writes, byte for byte as pyhoglake's Python codec does, and releases the
GIL while it works. It is private to pyhoglake and has no API of its own:
install it beside pyhoglake of the same version, and
`pyhoglake.variant.encode_json` uses it.

`PYHOGLAKE_VARIANT_CODEC`, read when pyhoglake is imported, picks the codec:

- `auto` (the default) uses this package when it is installed with
  pyhoglake's version, and the Python codec otherwise;
- `native` requires it, and raises where it is missing or of another
  version;
- `python` never uses it.

## Wheels

abi3 wheels for CPython 3.12 and later:

- Linux x86_64 and aarch64, glibc 2.28 or newer (`manylinux_2_28`) and
  musl 1.2 or newer (`musllinux_1_2`);
- macOS arm64 (11.0 or newer) and x86_64 (10.12 or newer);
- Windows x86_64.

Free-threaded CPython and PyPy are not supported: abi3 is CPython with
the GIL.

## Building from the sdist

Elsewhere, pip or uv builds the sdist through maturin, which needs Rust
1.91 or newer (<https://rustup.rs>). With rustup, the sdist's
`rust-toolchain.toml` selects the Rust the wheels are built with.

    pip install --no-binary pyhoglake-variant-codec pyhoglake-variant-codec==<pyhoglake's version>

In a hoglake checkout, `uv sync --group variant-codec` in `pyhoglake/`
builds it into the workspace environment.

The release wheels are built by `.github/workflows/variant-codec-wheels.yml`
with maturin, the Linux ones through zig against the oldest glibc and musl
above. Each is tested on its own platform before the publish workflow
uploads it, on a `pyhoglake-variant-codec-v<version>` tag.
