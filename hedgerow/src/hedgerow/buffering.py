"""Buffer policy shared by discovery and the durable flush scheduler."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from .config import ConfigError, _check_keys, _int


@dataclass(frozen=True)
class BufferPolicy:
    target_file_bytes: int = 256 * 1024 * 1024
    max_age_s: int = 24 * 60 * 60
    team_max_age_s: dict[int, int] = field(default_factory=dict)
    workers: int = 4
    # Hard bound on routing expansion in one discovery window. Fail without
    # checkpointing rather than silently omit routes or exhaust memory.
    max_fragments_per_window: int = 1_000_000

    @classmethod
    def parse(cls, value: object) -> BufferPolicy:
        if not isinstance(value, Mapping):
            raise ConfigError("buffering must be a mapping")
        _check_keys(
            value,
            {
                "target_file_bytes",
                "max_age_s",
                "team_max_age_s",
                "workers",
                "max_fragments_per_window",
            },
            "buffering",
        )
        overrides = value.get("team_max_age_s", {})
        if not isinstance(overrides, Mapping):
            raise ConfigError("buffering.team_max_age_s must be a mapping")
        parsed = {}
        for team, age in overrides.items():
            try:
                team_id = int(team)
            except (ValueError, TypeError):
                raise ConfigError(
                    "team override keys must be integer team ids"
                ) from None
            if isinstance(team, bool) or str(team_id) != str(team) or team_id < 0:
                raise ConfigError(
                    "team override keys must be non-negative integer team ids"
                )
            parsed[team_id] = _int(
                {"age": age}, "age", "buffering.team_max_age_s", 86400, 1
            )
        return cls(
            target_file_bytes=_int(
                value, "target_file_bytes", "buffering", 256 * 1024 * 1024, 1
            ),
            max_age_s=_int(value, "max_age_s", "buffering", 86400, 1),
            team_max_age_s=parsed,
            workers=_int(value, "workers", "buffering", 4, 1),
            max_fragments_per_window=_int(
                value, "max_fragments_per_window", "buffering", 1_000_000, 1
            ),
        )
