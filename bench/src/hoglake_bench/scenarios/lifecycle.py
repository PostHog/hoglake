"""Scenario 8: the analytics-engineering lifecycle, replayed from a seed.

Not uniform synthetic ops: a staging -> intermediate -> mart modeling
workflow — staged loads of real parquet, schema evolution over time
(column adds, type promotions per the server's documented promotion
matrix, nested-type introduction), CTAS-style derived tables, a
drop-and-recreate (preceded by variant-column archive DDL — the one
honest way bench can exercise variant, since its write path cannot
produce variant data), and changefeed consumption downstream with
offset commits.

The workload is a DESCRIPTION first: :func:`plan_workload` turns
(seed, scale, evolution-rate) into a tuple of step dataclasses, pure and
replayable — the executor then walks the plan against the API, deriving
every load's data from (seed, batch_key) and the table's CURRENT schema.
Same seed, same plan, same bytes.

Promotions follow the server's matrix (Model.kt PROMOTIONS — DuckLake's
documented table intersected with legal Iceberg facade evolutions):
int8 -> int16/int/long, int16 -> int/long, int -> long,
uint8 -> uint16/uint32, uint16 -> uint32, float -> double. The planner
tracks per-column type state so chained promotions stay legal.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
from pyhoglake import Table, ops
from pyhoglake.models import Column
from pyhoglake.types import (
    NESTED_TYPES,
    coltype_to_arrow,
    column_to_arrow_field,
    schema_to_column_defs,
)

from ..context import Bench
from ..datagen import to_arrow_schema
from ..datagen.arrays import make_array
from ..datagen.columns import ARROW_BY_SCALAR, ColSpec
from ..runner import FailureGuard
from ..stats import Metric, Recorder
from .common import ScenarioReport, assert_row_tiling, check, notice

#: End-to-end: every load writes real parquet through Table.append;
#: DDL, CTAS and changefeed reads run against the same catalog state.
IO_MODE = "end-to-end"

#: Same fixed-seed convention as the seed task and the typed writer.
DEFAULT_SEED = 4740871

#: The server's promotion matrix (Model.kt PROMOTIONS), restated for the
#: planner. Tested against an independent restatement in
#: tests/test_lifecycle.py so a drifted entry fails loudly.
PROMOTIONS: dict[str, tuple[str, ...]] = {
    "int8": ("int16", "int", "long"),
    "int16": ("int", "long"),
    "int": ("long",),
    "uint8": ("uint16", "uint32"),
    "uint16": ("uint32",),
    "float": ("double",),
}

#: Starting types for measure columns — all promotable, so evolution has
#: somewhere to go.
_PROMOTABLE_START: tuple[str, ...] = tuple(PROMOTIONS)

#: Extra (non-promotable) scalar variety for staging tables.
_EXTRA_SCALARS: tuple[str, ...] = (
    "string",
    "double",
    "long",
    "date",
    "json",
    "uint32",
    "timestamp_ms",
)

#: Nested shapes evolution can introduce, by token.
NESTED_SHAPES: dict[str, pa.DataType] = {
    "props": pa.map_(pa.string(), pa.string()),
    "tags": pa.list_(pa.string()),
    "codes": pa.map_(pa.int32(), pa.int64()),
    "spans": pa.list_(
        pa.struct(
            [
                pa.field("op", pa.string()),
                pa.field("dur_us", pa.int64()),
                pa.field("ok", pa.bool_()),
            ]
        )
    ),
}

CONSUMER = "bench-lifecycle"
_TEAMS = (1, 2, 3, 4, 5)


# -- the workload description ------------------------------------------------


@dataclass(frozen=True)
class CreateTable:
    table: str
    layer: str
    columns: tuple[tuple[str, str], ...]  # (name, coltype) — scalars


@dataclass(frozen=True)
class Load:
    table: str
    rows: int
    batch_key: int  # data = f(seed, batch_key, current schema)


@dataclass(frozen=True)
class AddColumn:
    table: str
    name: str
    coltype: str


@dataclass(frozen=True)
class PromoteColumn:
    table: str
    name: str
    from_: str
    to: str


@dataclass(frozen=True)
class AddNested:
    table: str
    name: str
    shape: str  # NESTED_SHAPES key


@dataclass(frozen=True)
class AddVariantDdl:
    table: str
    name: str


@dataclass(frozen=True)
class Ctas:
    table: str
    source: str
    layer: str


@dataclass(frozen=True)
class DropRecreate:
    table: str


@dataclass(frozen=True)
class ChangefeedRead:
    table: str


Step = (
    CreateTable
    | Load
    | AddColumn
    | PromoteColumn
    | AddNested
    | AddVariantDdl
    | Ctas
    | DropRecreate
    | ChangefeedRead
)


def plan_workload(
    seed: int,
    *,
    scale: int = 2,
    evolution_rate: float = 0.4,
    rows_per_load: int = 2_000,
) -> tuple[Step, ...]:
    """The whole workflow as data. Pure: equal arguments, equal plan."""
    if scale < 1:
        raise ValueError(f"scale must be >= 1, got {scale}")
    if not 0.0 <= evolution_rate <= 1.0:
        raise ValueError(f"evolution_rate must be in [0, 1], got {evolution_rate}")
    rng = np.random.default_rng([seed, scale, int(evolution_rate * 1000)])
    steps: list[Step] = []
    n_staging = 1 + scale
    n_cycles = 2 + 2 * scale
    staging = [f"stg_{i}" for i in range(n_staging)]
    state: dict[str, dict[str, str]] = {}
    shapes_used: dict[str, set[str]] = {}
    frozen: set[str] = set()  # recreated tables evolve no further
    next_col = 0
    next_batch = 0

    for name in staging:
        cols: list[tuple[str, str]] = [
            ("id", "long"),
            ("team_id", "long"),
            ("ts", "timestamptz"),
        ]
        for _ in range(2 + int(rng.integers(0, 3))):
            cols.append(
                (
                    f"x{next_col}",
                    _PROMOTABLE_START[int(rng.integers(0, len(_PROMOTABLE_START)))],
                )
            )
            next_col += 1
        for _ in range(1 + int(rng.integers(0, 2))):
            cols.append(
                (
                    f"x{next_col}",
                    _EXTRA_SCALARS[int(rng.integers(0, len(_EXTRA_SCALARS)))],
                )
            )
            next_col += 1
        steps.append(CreateTable(name, "staging", tuple(cols)))
        state[name] = dict(cols)
        shapes_used[name] = set()

    ctas_cycle = n_cycles // 2
    drop_cycle = max(ctas_cycle, (2 * n_cycles) // 3)
    intermediates: list[str] = []

    for cycle in range(n_cycles):
        for name in staging:
            steps.append(Load(name, rows_per_load, batch_key=next_batch))
            next_batch += 1
        for name in staging:
            if name in frozen or rng.random() >= evolution_rate:
                continue
            promotable = [c for c, t in state[name].items() if t in PROMOTIONS]
            unused_shapes = sorted(set(NESTED_SHAPES) - shapes_used[name])
            choices = ["add"]
            if promotable:
                choices.append("promote")
            if unused_shapes:
                choices.append("nested")
            pick = choices[int(rng.integers(0, len(choices)))]
            if pick == "promote":
                col = promotable[int(rng.integers(0, len(promotable)))]
                from_ = state[name][col]
                targets = PROMOTIONS[from_]
                to = targets[int(rng.integers(0, len(targets)))]
                steps.append(PromoteColumn(name, col, from_, to))
                state[name][col] = to
            elif pick == "nested":
                shape = unused_shapes[int(rng.integers(0, len(unused_shapes)))]
                steps.append(AddNested(name, f"x{next_col}", shape))
                shapes_used[name].add(shape)
                next_col += 1
            else:
                coltype = _EXTRA_SCALARS[int(rng.integers(0, len(_EXTRA_SCALARS)))]
                steps.append(AddColumn(name, f"x{next_col}", coltype))
                state[name][f"x{next_col}"] = coltype
                next_col += 1
        if cycle == ctas_cycle:
            for i, name in enumerate(staging):
                target = f"int_{i}"
                steps.append(Ctas(target, name, "intermediate"))
                intermediates.append(target)
        if cycle == drop_cycle:
            victim = staging[0]
            # The consumer drains the old incarnation FIRST — rows loaded
            # this cycle would otherwise vanish unconsumed with the drop
            # and the end-state books would not balance.
            steps.append(ChangefeedRead(victim))
            # Archive DDL then immediate drop/recreate: pyhoglake cannot
            # append while a variant column is live, so nothing may load
            # in between (pinned by a planner test).
            steps.append(AddVariantDdl(victim, "archived_attrs"))
            steps.append(DropRecreate(victim))
            frozen.add(victim)
        for name in staging:
            steps.append(ChangefeedRead(name))

    steps.append(Ctas("mart_activity", intermediates[0], "mart"))
    return tuple(steps)


# -- execution ---------------------------------------------------------------


def _gen_dtype(col: Column) -> pa.DataType:
    """The GENERATION arrow type for a catalog column: unsigned kinds
    keep their unsigned arrow type (so values stay in domain — the
    catalog's writer-contract schema widens uint32 to int64, which would
    otherwise let the generator mint negatives); containers come from
    the recursive field builder; the rest from the scalar mapping."""
    if col.type in ("uint8", "uint16", "uint32", "uint64"):
        return ARROW_BY_SCALAR[col.type]
    if col.type in NESTED_TYPES:
        return column_to_arrow_field(col).type
    return coltype_to_arrow(col.type, col.type_params)


def _load_batch(
    seed: int, batch_key: int, columns: tuple[Column, ...], rows: int
) -> pa.Table:
    """Deterministic rows for the table's CURRENT schema."""
    rng = np.random.default_rng([seed, batch_key])
    names, arrays = [], []
    for col in sorted(columns, key=lambda c: c.ordinal):
        names.append(col.name)
        if col.name == "team_id":
            arrays.append(
                pa.array(
                    np.asarray(_TEAMS, dtype=np.int64)[
                        rng.integers(0, len(_TEAMS), rows)
                    ]
                )
            )
            continue
        rate = 0.15 if col.nullable else 0.0
        arrays.append(make_array(rng, _gen_dtype(col), rows, rate))
    return pa.table(dict(zip(names, arrays)))


class _Executor:
    def __init__(self, bench: Bench, args: argparse.Namespace) -> None:
        self.bench = bench
        self.args = args
        self.catalog = bench.new_catalog("lc")
        self.ns = self.catalog.create_namespace("analytics")
        self.tables: dict[str, Table] = {}
        # per-incarnation accounting, keyed by (name, table_uuid)
        self.loaded_rows: dict[tuple[str, str], int] = {}
        self.consumed_rows: dict[tuple[str, str], int] = {}
        self.offsets: dict[str, int] = {}  # table name -> committed snapshot
        self.team_counts: dict[str, dict[int, int]] = {}
        self.recorders: dict[str, Recorder] = {}
        self.uuid_changes = 0

    def record(self, kind: str) -> Recorder:
        return self.recorders.setdefault(kind, Recorder())

    def _create(self, name: str, columns: tuple[tuple[str, str], ...]) -> None:
        schema = to_arrow_schema(
            [
                ColSpec(
                    n,
                    t,
                    ARROW_BY_SCALAR[t],
                    nullable=n not in ("id", "team_id", "ts"),
                )
                for n, t in columns
            ]
        )
        with self.record("create_table").measure():
            table = self.ns.create_table(name, schema)
        self.tables[name] = table
        self.team_counts[name] = {}
        self.offsets[name] = self.catalog.refresh().head_snapshot_id

    def step_create(self, s: CreateTable) -> None:
        self._create(s.table, s.columns)

    def step_load(self, s: Load) -> None:
        table = self.tables[s.table]
        data = _load_batch(self.args.seed, s.batch_key, tuple(table.columns), s.rows)
        with self.record("load").measure():
            table.append(
                data, author="hoglake-bench", message=f"lifecycle load {s.batch_key}"
            )
        key = (s.table, table.table_uuid)
        self.loaded_rows[key] = self.loaded_rows.get(key, 0) + s.rows
        counts = self.team_counts[s.table]
        for team in data.column("team_id").to_pylist():
            counts[team] = counts.get(team, 0) + 1

    def step_add_column(self, s: AddColumn) -> None:
        with self.record("evolve").measure():
            self.tables[s.table].alter([ops.add_column(s.name, s.coltype)])

    def step_promote(self, s: PromoteColumn) -> None:
        with self.record("evolve").measure():
            self.tables[s.table].alter([ops.promote_column(s.name, s.to)])

    def step_add_nested(self, s: AddNested) -> None:
        # ops.add_column has no children support; build the full column
        # def (children included) the way create_table does.
        field = pa.field(s.name, NESTED_SHAPES[s.shape])
        col_def = schema_to_column_defs(pa.schema([field]))[0]
        with self.record("evolve").measure():
            self.tables[s.table].alter([ops.AlterOp("add_column", {"column": col_def})])

    def step_add_variant(self, s: AddVariantDdl) -> None:
        # variant DDL only — no data is (or can honestly be) written.
        with self.record("evolve").measure():
            self.tables[s.table].alter([ops.add_column(s.name, "variant")])

    def step_ctas(self, s: Ctas) -> None:
        """CTAS-style derived table: schema + rows derived from what the
        workflow actually loaded into the source (per-team counts)."""
        counts = dict(self.team_counts[s.source])
        source = self.tables[s.source]
        source_key = (s.source, source.table_uuid)
        if source_key in self.loaded_rows:
            # staging source: the derived counts must cover every row the
            # workflow loaded into this incarnation
            check(
                sum(counts.values()) == self.loaded_rows[source_key],
                f"ctas {s.table}: derived counts cover {sum(counts.values())} "
                f"rows, source incarnation loaded "
                f"{self.loaded_rows[source_key]}",
            )
        with self.record("ctas").measure():
            table = self.ns.create_table(
                s.table,
                pa.schema(
                    [
                        pa.field("team_id", pa.int64(), nullable=False),
                        pa.field("row_count", pa.int64(), nullable=False),
                    ]
                ),
            )
            teams = sorted(counts)
            table.append(
                pa.table(
                    {
                        "team_id": pa.array(teams, pa.int64()),
                        "row_count": pa.array([counts[t] for t in teams], pa.int64()),
                    }
                ),
                author="hoglake-bench",
                message=f"ctas from {s.source}",
            )
        self.tables[s.table] = table
        self.team_counts[s.table] = dict(counts)

    def step_drop_recreate(self, s: DropRecreate) -> None:
        table = self.tables[s.table]
        old_uuid = table.table_uuid
        # recreate with the evolved SCALAR columns (a rebuilt report
        # table keeps its shape); nested additions and the variant
        # archive column do not carry over
        recreate = tuple(
            (c.name, c.type)
            for c in table.columns
            if c.type not in NESTED_TYPES and c.type != "variant"
        )
        with self.record("drop_recreate").measure():
            table.drop()
            self._create(s.table, recreate)
        new_uuid = self.tables[s.table].table_uuid
        check(
            new_uuid != old_uuid,
            f"{s.table}: table_uuid unchanged across drop+recreate",
        )
        self.uuid_changes += 1
        self.team_counts[s.table] = {}

    def step_changefeed(self, s: ChangefeedRead) -> None:
        table = self.tables[s.table]
        head = self.catalog.refresh().head_snapshot_id
        with self.record("changefeed").measure():
            plan = table.changes(from_snapshot=self.offsets[s.table], to_snapshot=head)
            self.catalog.commit_offset(CONSUMER, table.table_uuid, head)
        self.offsets[s.table] = head
        key = (s.table, plan.table_uuid)
        self.consumed_rows[key] = self.consumed_rows.get(key, 0) + sum(
            f.record_count for f in plan.files
        )

    def execute(self, step: Step) -> None:
        handlers = {
            CreateTable: self.step_create,
            Load: self.step_load,
            AddColumn: self.step_add_column,
            PromoteColumn: self.step_promote,
            AddNested: self.step_add_nested,
            AddVariantDdl: self.step_add_variant,
            Ctas: self.step_ctas,
            DropRecreate: self.step_drop_recreate,
            ChangefeedRead: self.step_changefeed,
        }
        handlers[type(step)](step)


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    plan = plan_workload(
        args.seed,
        scale=args.scale,
        evolution_rate=args.evolution_rate,
        rows_per_load=args.rows_per_load,
    )
    report = ScenarioReport(
        scenario="analytics-lifecycle",
        params={
            "seed": args.seed,
            "scale": args.scale,
            "evolution_rate": args.evolution_rate,
            "rows_per_load": args.rows_per_load,
            "steps": len(plan),
            "warmup": 0,
            "duration": args.duration,
            "url": args.url,
        },
    )
    bench.ensure_bucket()
    ex = _Executor(bench, args)
    guard = FailureGuard()
    deadline = time.monotonic() + args.duration if args.duration is not None else None
    done = 0
    t0 = time.monotonic()
    for step in plan:
        if deadline is not None and time.monotonic() > deadline:
            break
        try:
            ex.execute(step)
        except Exception as exc:
            if guard.is_server_failure(exc):
                guard.failure(exc)  # trips BenchAbort after the cap
                continue  # the step is lost; invariants below are gated
            raise
        guard.success()
        done += 1
    wall_s = time.monotonic() - t0
    complete = done == len(plan)

    for kind in (
        "create_table",
        "load",
        "evolve",
        "ctas",
        "drop_recreate",
        "changefeed",
    ):
        rec = ex.recorders.get(kind)
        if rec is not None and rec.count:
            report.add(Metric.from_recorder(f"lifecycle.{kind}", rec, wall_s))
    total_loaded = sum(ex.loaded_rows.values())
    report.add(
        Metric(
            name="lifecycle.summary",
            ops=done,
            wall_s=wall_s,
            extra={
                "steps_planned": len(plan),
                "steps_executed": done,
                "rows_loaded": total_loaded,
                "rows_s": total_loaded / wall_s if wall_s else 0.0,
                "incarnation_changes": ex.uuid_changes,
            },
        )
    )

    if not complete:
        notice(
            "analytics-lifecycle: --duration cut the plan at "
            f"{done}/{len(plan)} steps — end-state invariant checks SKIPPED"
        )
        return report

    # invariants: the workflow's books must balance
    check(ex.uuid_changes >= 1, "no drop+recreate incarnation change observed")
    for key, loaded in ex.loaded_rows.items():
        consumed = ex.consumed_rows.get(key, 0)
        check(
            consumed == loaded,
            f"changefeed consumed {consumed} rows of {key[0]} "
            f"(incarnation {key[1][:8]}), loaded {loaded}",
        )
    for table in ex.tables.values():
        assert_row_tiling(table)
    offsets = {o.table_uuid for o in ex.catalog.offsets(CONSUMER)}
    check(bool(offsets), "consumer committed no offsets")
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    def _rate(value: str) -> float:
        rate = float(value)
        if not 0.0 <= rate <= 1.0:
            raise argparse.ArgumentTypeError(
                f"--evolution-rate must be in [0, 1], got {value}"
            )
        return rate

    p.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="workload seed — the same seed replays the identical plan "
        f"and data (default {DEFAULT_SEED})",
    )
    p.add_argument(
        "--scale",
        type=int,
        default=2,
        help="workload size: 1+scale staging tables, 2+2*scale cycles",
    )
    p.add_argument(
        "--evolution-rate",
        type=_rate,
        default=0.4,
        help="per-table per-cycle probability of a schema-evolution step",
    )
    p.add_argument(
        "--rows-per-load", type=int, default=2_000, help="rows per staged load"
    )
