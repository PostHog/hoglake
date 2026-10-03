from .halts import UnsupportedFormatError


def require_parquet(file_format: str) -> None:
    if file_format != "parquet":
        raise UnsupportedFormatError(
            f"Hedgerow cannot replicate data format {file_format!r}. "
            "Use Parquet source and destination tables."
        )
