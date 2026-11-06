"""Fetch upstream SQL separately from Hoglake's MIT-licensed source tree."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import httpx

from . import CLICKBENCH_REVISION
from .client import table_name

QUERY_URL = (
    "https://raw.githubusercontent.com/ClickHouse/ClickBench/"
    f"{CLICKBENCH_REVISION}/trino-datalake-partitioned/queries.sql"
)
QUERY_SHA256 = "950d640b365703ac264fff2f7e377939510fde5b9a1c148776c96e8ac318c2c4"
QUERY_LICENSE = "CC-BY-NC-SA-4.0"
QUERY_COUNT = 43


def load_queries(cache: Path, catalog: str, schema: str, table: str) -> list[str]:
    """Only the pinned bytes are accepted, including when supplied from a cache.

    Queries remain a separately downloaded upstream input, with the source and
    its license recorded in every run. We only replace the target table name.
    The upstream adapter already supplies the legacy workload's Trino syntax.
    """
    payload = (
        cache.read_bytes()
        if cache.exists()
        else httpx.get(QUERY_URL, timeout=60).raise_for_status().content
    )
    if hashlib.sha256(payload).hexdigest() != QUERY_SHA256:
        raise ValueError("query file checksum differs from the pinned upstream SQL")
    if not cache.exists():
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(payload)
    queries = [
        line.strip().removesuffix(";")
        for line in payload.decode().splitlines()
        if line.strip()
    ]
    if len(queries) != QUERY_COUNT:
        raise ValueError(f"expected {QUERY_COUNT} upstream queries")
    target = table_name(catalog, schema, table)
    return [
        re.sub(r"\bFROM hits\b", lambda _: f"FROM {target}", query) for query in queries
    ]
