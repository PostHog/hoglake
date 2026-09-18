"""``hedgerow --config path/to/config.yaml`` — the console entry point.

Exit codes: 0 clean (only with ``--once``), 1 config error, and the
per-halt codes from :mod:`hedgerow.halts` (3 incarnation, 4 expired
feed, 5 deletes present, 6 schema mismatch, 7 split-brain offset,
8 data integrity, 9 persistent failure) so supervisors can tell a
crash-loopable failure from an operator-required HALT.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys

from .buffered_service import BufferedService
from .config import ConfigError, load_config
from .daemon import Hedgerow
from .halts import HaltError

log = logging.getLogger("hedgerow")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hedgerow",
        description=(
            "hoglake replication or buffered event ingestion, selected by config mode"
        ),
    )
    parser.add_argument("--config", required=True, help="path to YAML config")
    parser.add_argument(
        "--once",
        action="store_true",
        help="run one discovery/replication window; buffered mode settles ready work without forcing young buffers",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        config = load_config(args.config)
    except ConfigError as e:
        log.critical("config error: %s", e)
        return 1

    daemon = BufferedService(config) if config.mode == "buffered" else Hedgerow(config)
    previous = None
    if config.mode == "buffered":
        previous = signal.signal(signal.SIGTERM, lambda *_: daemon.stop.set())
    try:
        if args.once:
            daemon.start()
            daemon.run_once()
            return 0
        daemon.run_forever()
        return 0
    except HaltError as e:
        log.critical("HALT: %s", e)
        return e.exit_code
    except ConfigError as e:
        log.critical("config error: %s", e)
        return 1
    except Exception as e:
        if config.mode != "buffered":
            raise
        log.critical("buffered failure: %s; pending work retained", type(e).__name__)
        return 9
    except KeyboardInterrupt:
        log.info("interrupted; exiting")
        return 0
    finally:
        try:
            daemon.close()
        finally:
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    sys.exit(main())
