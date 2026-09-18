"""CLI lifecycle for durable buffered ingestion; raw files are never retired."""

import logging
import time
from pathlib import Path
from threading import Event
from urllib.parse import urlsplit

import duckdb
from pyhoglake import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeClient,
    HoglakeError,
    NotFoundError,
    OffsetRegressionError,
    ValidationError,
)
from pyhoglake import IncarnationChangedError as ClientIncarnationChangedError

from .config import ConfigError
from .halts import (
    FeedExpiredError,
    HaltError,
    IncarnationChangedError,
    PersistentFailureError,
    SplitBrainError,
)
from .ingestion import BufferedIngestion

log = logging.getLogger("hedgerow")


def resolve_json_columns(source, destination, configured):
    src = {c.name: c.type for c in source.columns}
    dst = {c.name: c.type for c in destination.columns}
    if configured is None:
        return (
            ("properties",)
            if dst.get("properties") == "variant"
            and src.get("properties") in ("json", "string")
            else ()
        )
    # Explicit native VARIANT columns need no conversion either.
    return tuple(c for c in configured if not (src.get(c) == dst.get(c) == "variant"))


def configure_s3(settings):
    """Create a connection-local secret; never persist or log credentials."""
    if bool(settings.access_key) != bool(settings.secret_key):
        raise ConfigError(
            "buffered source S3 requires both access_key and secret_key, or neither"
        )

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    options = [
        "TYPE S3",
        "URL_STYLE " + literal("path" if settings.path_style else "vhost"),
    ]
    if settings.access_key:
        options += [
            "KEY_ID " + literal(settings.access_key),
            "SECRET " + literal(settings.secret_key),
        ]
    else:
        options.append("PROVIDER CREDENTIAL_CHAIN")
    if settings.endpoint:
        endpoint = urlsplit(
            settings.endpoint
            if "://" in settings.endpoint
            else "https://" + settings.endpoint
        )
        if (
            endpoint.scheme not in ("http", "https")
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.path not in ("", "/")
            or endpoint.query
            or endpoint.fragment
        ):
            raise ConfigError("buffered source S3 endpoint must be an HTTP(S) origin")
        options += [
            "ENDPOINT " + literal(endpoint.netloc),
            "USE_SSL " + str(endpoint.scheme == "https").lower(),
            "REGION " + literal(settings.region or "us-east-1"),
        ]
    if settings.region and not settings.endpoint:
        options.append("REGION " + literal(settings.region))
    statement = "CREATE SECRET buffered_source (" + ", ".join(options) + ")"

    def configure(connection):
        try:
            connection.execute(statement)
        except Exception:  # noqa: BLE001 -- redact errors that can echo credential SQL
            # DuckDB errors can echo SQL, including secret values.
            raise ConfigError(
                "could not configure DuckDB S3 access; check httpfs/aws extensions and source S3 settings"
            ) from None

    return configure


class BufferedService:
    def __init__(self, config):
        self.config = config
        self.stop = Event()
        self.clients = []
        self.coordinator = None
        self.failures = 0

    def start(self):
        if self.coordinator is not None:
            return
        cfg = self.config
        buffered = cfg.buffered
        configure = configure_s3(cfg.source.s3)
        with duckdb.connect() as connection:
            configure(connection)
        Path(buffered.state_path).parent.mkdir(parents=True, exist_ok=True)
        Path(buffered.spill_directory).mkdir(parents=True, exist_ok=True)
        for side in (cfg.source, cfg.destination):
            self.clients.append(HoglakeClient(side.url, s3=side.s3.to_pyhoglake()))
        source_catalog = self.clients[0].catalog(cfg.source.catalog)
        destination_catalog = self.clients[1].catalog(cfg.destination.catalog)
        source = source_catalog.namespace(cfg.source.namespace).table(cfg.source.table)
        destination = destination_catalog.namespace(cfg.destination.namespace).table(
            cfg.destination.table
        )
        columns = resolve_json_columns(
            source.info(), destination.info(), buffered.json_columns
        )
        self.coordinator = BufferedIngestion(
            source,
            destination,
            source_catalog=source_catalog,
            destination_catalog=destination_catalog,
            consumer_id=cfg.source.consumer_id,
            state_path=buffered.state_path,
            filesystem=cfg.source.s3.to_pyhoglake().filesystem(),
            spill_directory=buffered.spill_directory,
            policy=buffered.policy,
            start_snapshot=cfg.source.start_snapshot,
            max_snapshot_window=cfg.replication.max_snapshot_window,
            json_columns=columns,
            writer_options=buffered.writer,
            configure_duckdb=configure,
        )
        log.info(
            "buffered_started json_columns=%s workers=%d",
            columns,
            buffered.policy.workers,
        )

    def _cycle(self, discover=True):
        try:
            if discover:
                result = self.coordinator.run_once()
                if result is not None:
                    log.info(
                        "buffered_discovery files=%d fragments=%d published_through=%d",
                        result.files,
                        result.fragments,
                        self.coordinator.store.published_through,
                    )
            else:
                self.coordinator._guard()
                self.coordinator.scheduler.tick(time.time())
                self.coordinator._offset()
        except ExpiredError as error:
            raise FeedExpiredError(str(error)) from error
        except OffsetRegressionError as error:
            raise SplitBrainError(str(error)) from error
        except ClientIncarnationChangedError as error:
            raise IncarnationChangedError(str(error)) from error
        except CommitConflictError as error:
            # Prepared requests are immutable. Refreshing read_snapshot would
            # change the receipt payload, so even a generic retryable conflict
            # requires reconciliation here.
            raise PersistentFailureError(
                "prepared commit conflict requires reconciliation; request retained"
            ) from error
        except (ValidationError, NotFoundError, AlreadyExistsError) as error:
            raise PersistentFailureError(
                "permanent catalog/schema error; pending work retained"
            ) from error
        except HoglakeError as error:
            if (
                error.status_code is not None
                and 400 <= error.status_code < 500
                and error.status_code not in (408, 429)
            ):
                raise PersistentFailureError(
                    "permanent catalog error; pending work retained"
                ) from error
            raise

    def run_once(self):
        self.start()
        self._cycle()
        # One discovery window; settle ready work, without forcing young buffers.
        while self.coordinator.scheduler.active and not self.stop.wait(0.05):
            self._cycle(discover=False)

    def run_forever(self):
        self.start()
        while not self.stop.is_set():
            try:
                self._cycle()
            except (HaltError, ConfigError):
                raise
            except Exception as error:
                self.failures += 1
                if self.failures > self.config.buffered.max_failures:
                    raise PersistentFailureError(
                        "buffered retry budget exhausted; pending work retained"
                    ) from error
                log.warning(
                    "buffered_retry attempt=%d error_type=%s",
                    self.failures,
                    type(error).__name__,
                )
            else:
                # Idle worker polls aren't successful retries. Keep the budget
                # until the failed durable work has actually been completed.
                if not self.coordinator.store.recover():
                    self.failures = 0
            self.stop.wait(self.config.replication.poll_interval_s)

    def close(self):
        try:
            if self.coordinator is not None:
                self.coordinator.close()  # finish workers before releasing state lock
                self.coordinator = None
        finally:
            for client in self.clients:
                client.close()
            self.clients.clear()
