"""Durable raw-file scheduling metadata; event payloads never enter this database.

One coordinator owns an OS lock for the lifetime of its worker pool. Reassignment
requires transferring the durable volume after the previous process exits. This
is deliberately not a lease: a paused old writer cannot wake after lease expiry
and publish concurrently with its replacement. SQLite is not supported on NFS.
"""

from __future__ import annotations

import fcntl
import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .halts import DataIntegrityError, SplitBrainError


@dataclass(frozen=True)
class Fragment:
    """A partition's rows within a committed source file, with row-group pruning.

    ``compressed_bytes`` is a readiness estimate, allocated from compressed source
    bytes, not a claim about the size of transformed/sorted destination output.
    """

    file_id: int
    team_id: int
    partition: tuple[str | None, ...]
    row_groups: tuple[int, ...]
    rows: int
    compressed_bytes: int


@dataclass(frozen=True)
class SourceFile:
    file_id: int
    snapshot: int
    committed_at: float
    path: str
    rows: int


@dataclass(frozen=True)
class Work:
    work_id: str
    team_id: int
    partition: tuple[str | None, ...]
    fragments: tuple[dict[str, Any], ...]
    request: dict[str, Any] | None


class PendingStore:
    def __init__(self, path: str, identity: dict[str, Any], start_snapshot: int = 0):
        path = str(Path(path).resolve())
        self._lock = open(Path(path).with_suffix(".lock"), "a+b")  # noqa: SIM115 -- held until close()
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock.close()
            raise SplitBrainError(
                "pending store already has an active coordinator"
            ) from None
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS job (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                identity TEXT NOT NULL, discovered INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY, snapshot INTEGER NOT NULL,
                committed_at REAL NOT NULL, path TEXT NOT NULL,
                rows INTEGER NOT NULL, cleaned INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS work (
                work_id TEXT PRIMARY KEY, team_id INTEGER NOT NULL UNIQUE,
                partition_key TEXT NOT NULL, request TEXT);
            CREATE TABLE IF NOT EXISTS pending (
                file_id INTEGER NOT NULL REFERENCES files(file_id),
                team_id INTEGER NOT NULL, partition_key TEXT NOT NULL,
                row_groups TEXT NOT NULL, rows INTEGER NOT NULL,
                compressed_bytes INTEGER NOT NULL,
                work_id TEXT REFERENCES work(work_id),
                PRIMARY KEY(file_id, team_id, partition_key));
            CREATE INDEX IF NOT EXISTS pending_partition
                ON pending(team_id, partition_key);
            CREATE INDEX IF NOT EXISTS pending_work ON pending(work_id);
            CREATE INDEX IF NOT EXISTS file_snapshot ON files(snapshot);
        """)
        identity_json = json.dumps(identity, sort_keys=True)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO job VALUES (1, ?, ?)",
                (identity_json, start_snapshot),
            )
        if self.db.execute("SELECT identity FROM job").fetchone()[0] != identity_json:
            self.close()
            raise DataIntegrityError(
                "pending store belongs to a different job/schema/incarnation"
            )

    @property
    def discovered(self) -> int:
        return self.db.execute("SELECT discovered FROM job").fetchone()[0]

    def discover(
        self,
        from_snapshot: int,
        to_snapshot: int,
        files: list[SourceFile],
        fragments: list[Fragment],
    ) -> None:
        """Atomically enqueue an entire validated window and advance discovery.

        Discovery must read each file's routing columns once, reconcile its row
        count, and provide ALL its partitions. An incomplete window never commits.
        """
        if from_snapshot != self.discovered or to_snapshot <= from_snapshot:
            raise DataIntegrityError("non-contiguous discovery window")
        by_id = {f.file_id: f for f in files}
        if len(by_id) != len(files):
            raise DataIntegrityError("duplicate source file identity")
        counts = dict.fromkeys(by_id, 0)
        for f in fragments:
            if f.file_id not in by_id or f.rows <= 0 or f.compressed_bytes < 0:
                raise DataIntegrityError("invalid source fragment")
            if not f.row_groups or any(g < 0 for g in f.row_groups):
                raise DataIntegrityError("invalid row-group selection")
            counts[f.file_id] += f.rows
        for f in files:
            if (
                not from_snapshot < f.snapshot <= to_snapshot
                or counts[f.file_id] != f.rows
            ):
                raise DataIntegrityError("source snapshot or row-count mismatch")
        with self.db:
            for f in files:
                self.db.execute(
                    "INSERT INTO files VALUES (?, ?, ?, ?, ?, 0)",
                    (f.file_id, f.snapshot, f.committed_at, f.path, f.rows),
                )
            for f in fragments:
                self.db.execute(
                    "INSERT INTO pending VALUES (?, ?, ?, ?, ?, ?, NULL)",
                    (
                        f.file_id,
                        f.team_id,
                        json.dumps(f.partition),
                        json.dumps(f.row_groups),
                        f.rows,
                        f.compressed_bytes,
                    ),
                )
            self.db.execute("UPDATE job SET discovered = ?", (to_snapshot,))

    def ready(
        self,
        now: float,
        target_bytes: int,
        max_age_s: float = 86400,
        team_max_age_s: dict[int, float] | None = None,
        limit: int = 4,
    ) -> list[tuple[int, str]]:
        """Partition-local byte thresholds and oldest still-pending commit age.

        Return at most one partition per team: all concurrent publication ownership
        for a team is represented by its unique ``work`` row.
        """
        overrides = team_max_age_s or {}
        result: list[tuple[int, str]] = []
        teams: set[int] = set()
        cursor = self.db.execute("""
            SELECT p.team_id, p.partition_key, SUM(p.compressed_bytes) AS bytes,
                   MIN(f.committed_at) AS oldest
            FROM pending p JOIN files f USING(file_id)
            WHERE p.work_id IS NULL AND NOT EXISTS
                (SELECT 1 FROM work w WHERE w.team_id = p.team_id)
            GROUP BY p.team_id, p.partition_key ORDER BY oldest, p.team_id, p.partition_key
        """)
        for row in cursor:
            team = row["team_id"]
            if team in teams:
                continue
            if row["bytes"] >= target_bytes or now >= row["oldest"] + overrides.get(
                team, max_age_s
            ):
                result.append((team, row["partition_key"]))
                teams.add(team)
                if len(result) == limit:
                    break
        return result

    def claim(self, team_id: int, partition_key: str) -> Work:
        """Freeze the current input set. Later arrivals belong to the next work."""
        work_id = str(uuid.uuid4())
        with self.db:
            self.db.execute(
                "INSERT INTO work VALUES (?, ?, ?, NULL)",
                (work_id, team_id, partition_key),
            )
            updated = self.db.execute(
                """UPDATE pending SET work_id = ?
                WHERE team_id = ? AND partition_key = ? AND work_id IS NULL""",
                (work_id, team_id, partition_key),
            ).rowcount
            if not updated:
                raise DataIntegrityError("cannot claim empty work")
        return self.work(work_id)

    def work(self, work_id: str) -> Work:
        row = self.db.execute(
            "SELECT * FROM work WHERE work_id = ?", (work_id,)
        ).fetchone()
        if row is None:
            raise DataIntegrityError("unknown work identity")
        fragments = []
        for f in self.db.execute(
            """SELECT p.*, f.path, f.snapshot, f.committed_at
            FROM pending p JOIN files f USING(file_id) WHERE work_id = ? ORDER BY file_id""",
            (work_id,),
        ):
            fragment = dict(f)
            fragment["row_groups"] = tuple(json.loads(fragment["row_groups"]))
            fragments.append(fragment)
        return Work(
            work_id,
            row["team_id"],
            tuple(json.loads(row["partition_key"])),
            tuple(fragments),
            json.loads(row["request"]) if row["request"] else None,
        )

    def recover(self) -> list[Work]:
        return [
            self.work(row[0])
            for row in self.db.execute("SELECT work_id FROM work ORDER BY work_id")
        ]

    def prepare(self, work_id: str, request: dict[str, Any]) -> Work:
        """Persist the EXACT request before the first network publication attempt."""
        if request.get("idempotency_key") != work_id:
            raise DataIntegrityError(
                "publication request must use its durable work identity"
            )
        encoded = json.dumps(request, sort_keys=True)
        with self.db:
            previous = self.db.execute(
                "SELECT request FROM work WHERE work_id = ?", (work_id,)
            ).fetchone()
            if previous is None or (previous[0] is not None and previous[0] != encoded):
                raise DataIntegrityError("cannot replace a prepared publication")
            self.db.execute(
                "UPDATE work SET request = ? WHERE work_id = ?", (encoded, work_id)
            )
        return self.work(work_id)

    def published(self, work_id: str) -> None:
        """Only call after a successful commit response (including receipt replay)."""
        with self.db:
            row = self.db.execute(
                "SELECT request FROM work WHERE work_id = ?", (work_id,)
            ).fetchone()
            if row is None or row[0] is None:
                raise DataIntegrityError("publication was not prepared")
            self.db.execute("DELETE FROM pending WHERE work_id = ?", (work_id,))
            self.db.execute("DELETE FROM work WHERE work_id = ?", (work_id,))

    @property
    def published_through(self) -> int:
        """The sole source consumer offset: never passes ANY pending source input."""
        row = self.db.execute("""SELECT MIN(snapshot) FROM files f
            WHERE EXISTS (SELECT 1 FROM pending p WHERE p.file_id = f.file_id)""").fetchone()[
            0
        ]
        return self.discovered if row is None else row - 1

    def cleanup_candidates(self, limit: int = 1000) -> list[dict[str, Any]]:
        """Whole raw files with ALL dependencies published; not deletion authority.

        A lifecycle backend must retire catalog references first, respect other
        consumers and let Hoglake expiry/cleanup remove objects. Never S3 DELETE.
        """
        return [
            dict(r)
            for r in self.db.execute(
                """SELECT * FROM files f WHERE cleaned = 0
            AND NOT EXISTS (SELECT 1 FROM pending p WHERE p.file_id = f.file_id)
            ORDER BY snapshot, file_id LIMIT ?""",
                (limit,),
            )
        ]

    def close(self) -> None:
        self.db.close()
        self._lock.close()
