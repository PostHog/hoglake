"""Strict configuration for the opt-in buffered event ingestion service."""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .buffering import BufferPolicy
from .config import ConfigError, _check_keys, _int, _str
from .duckdb_writer import DuckDBWriterOptions


@dataclass(frozen=True)
class BufferedConfig:
    state_path: str
    spill_directory: str
    policy: BufferPolicy
    writer: DuckDBWriterOptions
    json_columns: tuple[str, ...] | None = None
    max_failures: int = 3

    @classmethod
    def parse(cls, value):
        if not isinstance(value, Mapping):
            raise ConfigError("buffered must be a mapping")
        _check_keys(
            value,
            {
                "state_path",
                "spill_directory",
                "policy",
                "writer",
                "json_columns",
                "max_failures",
            },
            "buffered",
        )
        state = _str(value, "state_path", "buffered")
        spill = _str(value, "spill_directory", "buffered")
        if not Path(state).is_absolute() or not Path(spill).is_absolute():
            raise ConfigError(
                "buffered state_path and spill_directory must be absolute paths"
            )
        columns = value.get("json_columns")
        if "json_columns" in value and (
            not isinstance(columns, list)
            or any(not isinstance(c, str) or not c for c in columns)
            or len(set(columns)) != len(columns)
        ):
            raise ConfigError(
                "buffered.json_columns must be a list of unique column names"
            )
        writer = value.get("writer", {})
        if not isinstance(writer, Mapping):
            raise ConfigError("buffered.writer must be a mapping")
        _check_keys(
            writer,
            {"memory_bytes", "scratch_bytes", "row_group_rows"},
            "buffered.writer",
        )
        return cls(
            state,
            spill,
            BufferPolicy.parse(value.get("policy", {})),
            DuckDBWriterOptions(
                memory_bytes=_int(
                    writer, "memory_bytes", "buffered.writer", 512 * 1024**2, 1
                ),
                scratch_bytes=_int(
                    writer, "scratch_bytes", "buffered.writer", 8 * 1024**3, 1
                ),
                row_group_rows=_int(
                    writer, "row_group_rows", "buffered.writer", 8192, 2048
                ),
            ),
            None if columns is None else tuple(columns),
            _int(value, "max_failures", "buffered", 3, 0),
        )
