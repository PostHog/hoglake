"""Create the destination from configuration at startup, millpond's way.

Millrace's staging key is the destination's partition tuple and it drops
unknown payload keys with no schema evolution, so the spec and every
column it reads must exist before the first record is staged. Hence the
destination is created HERE, before the flusher resolves and pins it
(``main.py`` → :func:`ensure_destination` → ``HoglakeFlusher.resolve``),
from a complete configured shape — never inferred from a first batch.

Three objects, each create-if-missing with the replica race tolerated
(a StatefulSet's pods boot together; the loser's create answers
``AlreadyExistsError`` and it re-reads what the winner made):

1. the catalog — created with ``MILLRACE_CATALOG_DATA_PATH``; unset, a
   missing catalog is a refusal naming the knob (exit 2). A catalog's
   data path is immutable, so a configured value that differs from an
   existing catalog's is a WARNING and the live one wins;
2. the namespace;
3. the table — created ATOMICALLY with ``MILLRACE_TABLE_COLUMNS``, the
   ``MILLRACE_PARTITION_BY`` spec and the ``MILLRACE_SORT_BY`` order in
   one request (pyhoglake's ``create_table`` fields, through the
   server's table-creations operation). When ``MILLRACE_PARTITION_BY``
   is set millrace REQUIRES the catalog to advertise
   ``atomic-partitioned-table-creation-v1``, and when ``MILLRACE_SORT_BY``
   is set ``atomic-sorted-table-creation-v1``, and refuses to start
   otherwise (:class:`DestinationError`, exit 1, the capability named):
   pyhoglake's create + alter fallback is never taken from here, because
   a sibling replica reading the table between the two requests would
   stage under an empty spec for a shape-cache TTL. A columns-only
   table has no spec to race and takes the plain create. Unset columns
   make a missing table a refusal naming the knob (exit 2).

An existing object is NEVER altered: when the table exists, its live
columns (name, type, nullability), partition spec and sort order are
compared with whatever is configured and every difference goes into
ONE WARNING carrying both sides in the knobs' own grammar (pasteable).
The live table wins — the flusher then resolves it exactly as before.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pyarrow as pa
from pyhoglake import (
    AlreadyExistsError,
    HoglakeClient,
    HoglakeError,
    NotFoundError,
    TableInfo,
)
from pyhoglake.client import (
    ATOMIC_PARTITIONED_CREATION,
    ATOMIC_SORTED_CREATION,
    Catalog,
    Namespace,
)
from pyhoglake.ops import NamedPartitionField, NamedSortField

from .config import Config, ConfigError
from .flush import FlushStartupError, read_table_identity
from .grouping import column_chains

log = logging.getLogger("millrace.destination")


class DestinationConfigError(ConfigError):
    """The destination is missing and the configuration does not say
    how to create it — a configuration refusal (exit 2), not a server
    failure, so the message names the knob to set."""


class DestinationError(Exception):
    """The server could not be asked (transport failure, a 5xx, a 403),
    answered something this writer cannot act on, or lacks a capability
    this writer requires: a startup failure logged in one line (exit 1),
    the same posture as an unresolvable destination."""


def ensure_destination(client: HoglakeClient, cfg: Config) -> None:
    """Create the catalog, namespace and table named by ``cfg`` when they
    do not exist (module docstring). Blocking I/O: run it before the
    loop starts, or through an executor. Raises
    :class:`DestinationConfigError` for a missing object the config
    cannot create, :class:`DestinationError` for a server failure."""
    try:
        catalog = _ensure_catalog(client, cfg)
        namespace = _ensure_namespace(catalog, cfg)
        _ensure_table(client, catalog, namespace, cfg)
    except (HoglakeError, FlushStartupError, httpx.HTTPError, OSError) as e:
        raise DestinationError(
            f"cannot ensure the hoglake destination "
            f"{cfg.catalog}/{cfg.namespace}/{cfg.table}: {e}"
        ) from e


def _ensure_catalog(client: HoglakeClient, cfg: Config) -> Catalog:
    try:
        catalog = client.catalog(cfg.catalog)
    except NotFoundError:
        if cfg.catalog_data_path is None:
            raise DestinationConfigError(
                [
                    (
                        f"catalog {cfg.catalog!r} does not exist and "
                        f"MILLRACE_CATALOG_DATA_PATH is not set: set it (the "
                        f"catalog's data path, e.g. s3://bucket/lake/) to have "
                        f"millrace create the catalog, or create it first"
                    )
                ]
            ) from None
        try:
            catalog = client.create_catalog(cfg.catalog, cfg.catalog_data_path)
        except AlreadyExistsError:
            # Another replica won the race between our read and our
            # create; what it made is what we wanted.
            log.info(
                "catalog %s was created by another replica meanwhile; re-reading",
                cfg.catalog,
            )
            catalog = client.catalog(cfg.catalog)
        else:
            log.info(
                "millrace created catalog %s (data_path=%s)",
                cfg.catalog,
                catalog.data_path,
            )
            return catalog
    if cfg.catalog_data_path is not None and not _same_path(
        catalog.data_path, cfg.catalog_data_path
    ):
        log.warning(
            "catalog %s exists with data_path=%s, which differs from "
            "MILLRACE_CATALOG_DATA_PATH=%s; a catalog's data path is "
            "immutable, so the live one wins (the knob is only used to "
            "create a missing catalog)",
            cfg.catalog,
            catalog.data_path,
            cfg.catalog_data_path,
        )
    return catalog


def _same_path(live: str, configured: str) -> bool:
    return live.rstrip("/") == configured.rstrip("/")


def _ensure_namespace(catalog: Catalog, cfg: Config) -> Namespace:
    try:
        return catalog.namespace(cfg.namespace)
    except NotFoundError:
        pass
    try:
        namespace = catalog.create_namespace(cfg.namespace)
    except AlreadyExistsError:
        log.info(
            "namespace %s.%s was created by another replica meanwhile",
            cfg.catalog,
            cfg.namespace,
        )
        return Namespace(catalog, cfg.namespace)
    log.info("millrace created namespace %s.%s", cfg.catalog, cfg.namespace)
    return namespace


def _read_table(client: HoglakeClient, cfg: Config) -> TableInfo | None:
    """The table's identity read (flush.py ``read_table_identity``, the
    one read every table read goes through), or None when the table
    does not exist."""
    try:
        return read_table_identity(client, cfg.catalog, cfg.namespace, cfg.table)
    except NotFoundError:
        return None


def _require_atomic_creation(catalog: Catalog, cfg: Config) -> None:
    """Refuse to create through anything but the atomic path (module
    docstring, point 3): the partitioned capability when a partition
    spec is configured, the sorted one when a sort order is. A
    columns-only table has no spec to race and takes the plain create."""
    caps = set(catalog.info.capabilities)
    required = []
    if cfg.partition_by:
        required.append(ATOMIC_PARTITIONED_CREATION)
    if cfg.sort_by:
        required.append(ATOMIC_SORTED_CREATION)
    missing = [c for c in required if c not in caps]
    if missing:
        raise DestinationError(
            f"catalog {cfg.catalog!r} does not advertise "
            f"{', '.join(missing)}: millrace creates a table only atomically "
            f"with its partition spec and sort order (a create followed by an "
            f"alter would let a sibling replica stage under an empty spec); "
            f"upgrade the hoglake server, or create "
            f"{cfg.namespace}.{cfg.table} first"
        )


def _ensure_table(
    client: HoglakeClient, catalog: Catalog, namespace: Namespace, cfg: Config
) -> None:
    info = _read_table(client, cfg)
    if info is None:
        if cfg.table_columns is None:
            raise DestinationConfigError(
                [
                    (
                        f"table {cfg.catalog}/{cfg.namespace}/{cfg.table} does not "
                        f"exist and MILLRACE_TABLE_COLUMNS is not set: set it (the "
                        f"complete typed column list, name:type,...) plus "
                        f"MILLRACE_PARTITION_BY / MILLRACE_SORT_BY to have millrace "
                        f"create the table, or create it first"
                    )
                ]
            )
        if cfg.partition_by or cfg.sort_by:
            _require_atomic_creation(catalog, cfg)
        schema = pa.schema(
            [pa.field(c.name, c.arrow_type(), nullable=True) for c in cfg.table_columns]
        )
        partition_fields = [
            NamedPartitionField(f.column, f.transform, f.param)
            for f in (cfg.partition_by or ())
        ]
        sort_fields = [
            NamedSortField(f.column, f.direction, f.null_order)
            for f in (cfg.sort_by or ())
        ]
        try:
            table = namespace.create_table(
                cfg.table,
                schema,
                partition_fields=partition_fields,
                sort_fields=sort_fields,
            )
        except AlreadyExistsError:
            log.info(
                "table %s.%s was created by another replica meanwhile; re-reading",
                cfg.namespace,
                cfg.table,
            )
            info = _read_table(client, cfg)
            if info is None:
                raise DestinationError(
                    f"table {cfg.namespace}.{cfg.table} answered already-exists "
                    f"to a create and not-found to the read that followed"
                ) from None
        else:
            log.info(
                "millrace created table %s/%s/%s table_uuid=%s (%d columns, "
                "partition by [%s], sort by [%s])",
                cfg.catalog,
                cfg.namespace,
                cfg.table,
                table.table_uuid,
                len(cfg.table_columns),
                ", ".join(f.describe() for f in (cfg.partition_by or ())),
                ", ".join(f.describe() for f in (cfg.sort_by or ())),
            )
            return
    differences = _differences(cfg, info)
    if not differences:
        # Only when there is a configured shape to agree with: nothing
        # configured means nothing was compared, and silence is right.
        if cfg.table_columns is not None or cfg.partition_by or cfg.sort_by:
            log.info(
                "table %s/%s/%s exists and agrees with the configured shape",
                cfg.catalog,
                cfg.namespace,
                cfg.table,
            )
        return
    if differences:
        log.warning(
            "table %s/%s/%s exists and differs from the configured shape; "
            "millrace never alters an existing table, so the live table wins "
            "(update the knobs to match, or recreate the table): %s",
            cfg.catalog,
            cfg.namespace,
            cfg.table,
            "; ".join(differences),
        )


# -- the agreement check ------------------------------------------------------


def _live_paths(info: TableInfo) -> dict[int, str]:
    """field id → dotted column path, every level."""
    return {
        fid: ".".join(c.name for c in chain)
        for fid, chain in column_chains(info.columns).items()
    }


def _type_name(col_type: str, params: dict[str, Any] | None) -> str:
    if col_type == "decimal" and params:
        return f"decimal({params.get('precision')},{params.get('scale')})"
    return col_type


def _differences(cfg: Config, info: TableInfo) -> list[str]:
    """Every way the live table differs from what is CONFIGURED (a knob
    left unset is not compared), each in the knob's own grammar.

    Nullability is compared for top-level columns only; a NOT NULL
    struct child is not reported (millrace creates nullable leaves too).
    """
    out: list[str] = []
    if cfg.table_columns is not None:
        live = {c.name: _type_name(c.type, c.type_params) for c in info.columns}
        want = {c.name: _type_name(c.col_type, c.params()) for c in cfg.table_columns}
        for name, want_type in want.items():
            live_type = live.get(name)
            if live_type is None:
                out.append(
                    f"MILLRACE_TABLE_COLUMNS names {name}:{want_type}, which the "
                    f"live table lacks"
                )
            elif live_type != want_type:
                out.append(
                    f"column {name} is {live_type} in the live table, "
                    f"{want_type} in MILLRACE_TABLE_COLUMNS"
                )
        # A variant column is an operator's (the grammar cannot declare
        # one) and millrace fills it through MILLRACE_VARIANT_SOURCES.
        extra = [
            f"{n}:{t}" for n, t in live.items() if n not in want and t != "variant"
        ]
        if extra:
            out.append(
                f"the live table carries column(s) MILLRACE_TABLE_COLUMNS does "
                f"not: {', '.join(extra)} (millrace never writes them)"
            )
        # Nullability: the configured columns are all nullable; a NOT
        # NULL live column that millrace does not fill on every row is a
        # record_validation refusal at the first flush with a null in it.
        for column in info.columns:
            if not column.nullable:
                out.append(
                    f"column {column.name} is NOT NULL in the live table; "
                    f"millrace creates nullable columns and cannot fill it"
                )
    if cfg.partition_by is not None:
        paths = _live_paths(info)
        spec = info.partition_spec
        live_fields = tuple(
            (
                paths.get(f.source_field_id, f"field_id={f.source_field_id}"),
                f.transform,
                f.transform_param,
            )
            for f in (spec.fields if spec is not None else ())
        )
        want_fields = tuple((f.column, f.transform, f.param) for f in cfg.partition_by)
        if live_fields != want_fields:
            out.append(
                f"live partition spec [{_describe_partition(live_fields)}] != "
                f"MILLRACE_PARTITION_BY [{_describe_partition(want_fields)}]"
            )
    if cfg.sort_by is not None:
        paths = _live_paths(info)
        sort = info.sort_spec
        live_sort = tuple(
            (
                paths.get(f.source_field_id, f"field_id={f.source_field_id}"),
                f.direction,
                f.null_order,
            )
            for f in (sort.fields if sort is not None else ())
        )
        want_sort = tuple((f.column, f.direction, f.null_order) for f in cfg.sort_by)
        if live_sort != want_sort:
            out.append(
                f"live sort order [{_describe_sort(live_sort)}] != "
                f"MILLRACE_SORT_BY [{_describe_sort(want_sort)}]"
            )
    return out


def _describe_partition(fields: tuple[tuple[str, str, int | None], ...]) -> str:
    return ", ".join(
        f"{t}({c}, {p})" if p is not None else f"{t}({c})" for c, t, p in fields
    )


def _describe_sort(fields: tuple[tuple[str, str, str], ...]) -> str:
    return ", ".join(f"{c} {d} {n}" for c, d, n in fields)
