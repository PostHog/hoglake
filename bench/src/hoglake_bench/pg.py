"""Direct-Postgres test-harness affordance: statistics settling.

The bench is an API-only client everywhere else. This one hook exists
because metadata scenarios seed thousands of rows and then measure
immediately — inside Postgres's autoanalyze lag window, where the
planner still holds pre-seed statistics and its plan choices reflect
staleness, not the schema. A scaling flag measured there is an
artifact (observed 2026-09-12: a 2.57x changefeed "regression" that an
index fix could not clear because autoanalyze landed after the run).

ANALYZE between seeding and measurement is deterministic and cheap;
sleeping past autovacuum_naptime is neither. When the DSN is absent or
unreachable the caller degrades to the old behavior and must say so on
any flag it raises.
"""

from __future__ import annotations

# The tables the metadata scenarios grow. ANALYZE on extras is cheap;
# missing one silently re-opens the staleness window, so keep the list
# generous.
SETTLE_TABLES = (
    "hog_snapshot",
    "hog_snapshot_change",
    "hog_data_file",
    "hog_delete_file",
    "hog_file_partition_value",
    "hog_consumer_offset",
)


def settle_stats(dsn: str) -> bool:
    """ANALYZE the bench-grown tables. True on success, False when the
    DSN is empty/unreachable (callers annotate their flags)."""
    if not dsn:
        return False
    try:
        import psycopg
    except ImportError:
        return False
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            conn.execute(f"ANALYZE {', '.join(SETTLE_TABLES)}")
            conn.commit()
        return True
    except Exception:
        return False
