"""Real parquet registrations for scenarios that need many real objects.

``Table.append`` is the honest path for ordinary writes, but it is one
commit per call. A scenario that wants a BATCH of real files registered
in one commit (expiry-throughput's cleanup probes) builds registrations
here instead: the bytes are real parquet with the catalog's field ids,
and every stat in the registration is extracted from the written footer
by pyhoglake's own ``extract_column_stats`` — the same numbers
``Table.append`` would ship. Nothing is invented.

The caller uploads the returned payload to ``reg["path"]`` (via
``Bench.put_object``) before committing the registration; registering
first would present the server a path that does not resolve yet.
"""

from __future__ import annotations

import io
import struct
import uuid
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyhoglake.models import Column
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import columns_to_arrow_schema


def build_real_registration(
    data: pa.Table,
    columns: Sequence[Column],
    *,
    data_path: str,
    namespace: str,
    table: str,
) -> tuple[dict[str, Any], bytes]:
    """Encode ``data`` as one real parquet object for ``columns``.

    Returns ``(registration, payload)``: a FileRegistration dict whose
    ``column_stats`` are footer-derived, and the parquet bytes the caller
    must upload to ``registration["path"]`` before committing.
    """
    target = columns_to_arrow_schema(tuple(columns))
    aligned = data.select(target.names).cast(target)
    sink = io.BytesIO()
    pq.write_table(aligned, sink)
    payload = sink.getvalue()
    metadata = pq.ParquetFile(io.BytesIO(payload)).metadata
    # The 8-byte trailer is <footer_size:uint32le> + b"PAR1".
    footer_size = struct.unpack("<I", payload[-8:-4])[0]
    stats = extract_column_stats(metadata, tuple(columns))
    path = f"{data_path}data/{namespace}/{table}/{uuid.uuid4().hex}.parquet"
    registration: dict[str, Any] = {
        "path": path,
        "record_count": metadata.num_rows,
        "file_size_bytes": len(payload),
        "footer_size": footer_size,
        "column_stats": [s.to_wire() for s in stats],
    }
    return registration, payload
