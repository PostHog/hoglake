"""pyhoglake — Python client for the hoglake control plane."""

import importlib.metadata
import logging

from . import ops, transforms, upload
from .bounds import decode_bound, encode_bound
from .client import (
    UNGUARDED,
    Catalog,
    HoglakeClient,
    Namespace,
    S3Config,
    Table,
    View,
)
from .errors import (
    AlreadyExistsError,
    CommitConflictError,
    DdlSinceReadSnapshotError,
    ExpiredError,
    HoglakeError,
    IncarnationChangedError,
    MalformedResponseError,
    NotFoundError,
    OffsetRegressionError,
    ReadSnapshotExpiredError,
    ReconciliationRequiredError,
    UnsupportedTypeError,
    ValidationError,
)
from .models import (
    AppendedFile,
    AppendResult,
    CatalogInfo,
    CatalogOptions,
    ChangesPlan,
    CleanupResult,
    Column,
    ColumnStats,
    CommitResult,
    ConsumerOffset,
    DataFile,
    DeleteFile,
    ExpiryResult,
    PartitionField,
    PartitionSpec,
    ScanFile,
    Snapshot,
    SnapshotChange,
    TableInfo,
    TableSummary,
    ViewInfo,
)
from .ops import AlterOp
from .types import arrow_type_to_coltype, coltype_to_arrow

# Read from the installed metadata so it cannot drift from pyproject.toml.
__version__ = importlib.metadata.version("pyhoglake")

# A library must not configure logging for its host, but this one does
# warn on the writer path (a single-request upload path that is not
# available costs 3x the S3 requests, and nothing else would say so).
# The NullHandler is the library idiom: it keeps pyhoglake from writing
# to stderr through logging's handler of last resort, at the cost of a
# host that configures no logging at all hearing nothing — which is the
# host's choice to make, not ours.
logging.getLogger("pyhoglake").addHandler(logging.NullHandler())

__all__ = [
    "UNGUARDED",
    "AlreadyExistsError",
    "AlterOp",
    "AppendResult",
    "AppendedFile",
    "Catalog",
    "CatalogInfo",
    "CatalogOptions",
    "ChangesPlan",
    "CleanupResult",
    "Column",
    "ColumnStats",
    "CommitConflictError",
    "CommitResult",
    "ConsumerOffset",
    "DataFile",
    "DdlSinceReadSnapshotError",
    "DeleteFile",
    "ExpiredError",
    "ExpiryResult",
    "HoglakeClient",
    "HoglakeError",
    "IncarnationChangedError",
    "MalformedResponseError",
    "Namespace",
    "NotFoundError",
    "OffsetRegressionError",
    "PartitionField",
    "PartitionSpec",
    "ReadSnapshotExpiredError",
    "ReconciliationRequiredError",
    "S3Config",
    "ScanFile",
    "Snapshot",
    "SnapshotChange",
    "Table",
    "TableInfo",
    "TableSummary",
    "UnsupportedTypeError",
    "ValidationError",
    "View",
    "ViewInfo",
    "__version__",
    "arrow_type_to_coltype",
    "coltype_to_arrow",
    "decode_bound",
    "encode_bound",
    "ops",
    "transforms",
    "upload",
]
