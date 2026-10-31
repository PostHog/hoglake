"""pyhoglake — Python client for the hoglake control plane."""

import importlib.metadata
import logging

from . import object_prefix, ops, transforms, upload, variant, writer
from .auth import BearerToken, ClientCredentials
from .bounds import decode_bound, encode_bound
from .client import (
    UNGUARDED,
    Catalog,
    HoglakeClient,
    Namespace,
    S3Config,
    Table,
    View,
    parquet_write_options,
)
from .errors import (
    AlreadyExistsError,
    CatalogFilesPendingExpiryError,
    CatalogHasAbandonedUploadsError,
    CatalogHasActiveCompactionClaimsError,
    CatalogHasActiveUploadsError,
    CatalogHasConsumerOffsetsError,
    CatalogHasLiveNamespacesError,
    CatalogNotEmptyError,
    CatalogRemovalsPendingError,
    CatalogTablesPendingRetirementError,
    CommitConflictError,
    DdlSinceReadSnapshotError,
    ExpiredError,
    HoglakeError,
    IdlessFilesPresentError,
    IncarnationChangedError,
    MalformedResponseError,
    NotFoundError,
    OffsetRegressionError,
    ReadSnapshotExpiredError,
    ReconciliationRequiredError,
    UnsupportedShreddingError,
    UnsupportedTypeError,
    ValidationError,
    VariantEncodingError,
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
    DroppedCatalog,
    ExpiryResult,
    PartitionField,
    PartitionSpec,
    ScanFile,
    Snapshot,
    SnapshotChange,
    SnapshotResolution,
    SortField,
    SortSpec,
    TableInfo,
    TableSummary,
    ViewInfo,
)
from .ops import AlterOp
from .types import arrow_type_to_coltype, coltype_to_arrow
from .variant import VARIANT_NULL, VariantReport, variant_field
from .writer import PreparedPart, write_part

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
    "VARIANT_NULL",
    "AlreadyExistsError",
    "AlterOp",
    "AppendResult",
    "AppendedFile",
    "BearerToken",
    "Catalog",
    "CatalogFilesPendingExpiryError",
    "CatalogHasAbandonedUploadsError",
    "CatalogHasActiveCompactionClaimsError",
    "CatalogHasActiveUploadsError",
    "CatalogHasConsumerOffsetsError",
    "CatalogHasLiveNamespacesError",
    "CatalogInfo",
    "CatalogNotEmptyError",
    "CatalogOptions",
    "CatalogRemovalsPendingError",
    "CatalogTablesPendingRetirementError",
    "ChangesPlan",
    "CleanupResult",
    "ClientCredentials",
    "Column",
    "ColumnStats",
    "CommitConflictError",
    "CommitResult",
    "ConsumerOffset",
    "DataFile",
    "DdlSinceReadSnapshotError",
    "DeleteFile",
    "DroppedCatalog",
    "ExpiredError",
    "ExpiryResult",
    "HoglakeClient",
    "HoglakeError",
    "IdlessFilesPresentError",
    "IncarnationChangedError",
    "MalformedResponseError",
    "Namespace",
    "NotFoundError",
    "OffsetRegressionError",
    "PartitionField",
    "PartitionSpec",
    "PreparedPart",
    "ReadSnapshotExpiredError",
    "ReconciliationRequiredError",
    "S3Config",
    "ScanFile",
    "Snapshot",
    "SnapshotChange",
    "SnapshotResolution",
    "SortField",
    "SortSpec",
    "Table",
    "TableInfo",
    "TableSummary",
    "UnsupportedShreddingError",
    "UnsupportedTypeError",
    "ValidationError",
    "VariantEncodingError",
    "VariantReport",
    "View",
    "ViewInfo",
    "__version__",
    "arrow_type_to_coltype",
    "coltype_to_arrow",
    "decode_bound",
    "encode_bound",
    "object_prefix",
    "ops",
    "parquet_write_options",
    "transforms",
    "upload",
    "variant",
    "variant_field",
    "write_part",
    "writer",
]
