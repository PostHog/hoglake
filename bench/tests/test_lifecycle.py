"""The analytics-lifecycle workload planner: a seeded, replayable
description of a staging -> intermediate -> mart modeling workflow.
The plan is data; two runs with one seed must execute identically."""

import pytest

from hoglake_bench.cli import FULL_PROFILE, QUICK_PROFILE, SCENARIOS
from hoglake_bench.scenarios import lifecycle
from hoglake_bench.scenarios.lifecycle import (
    AddColumn,
    AddVariantDdl,
    ChangefeedRead,
    CreateTable,
    Ctas,
    DropRecreate,
    Load,
    PromoteColumn,
    plan_workload,
)

#: The server's documented promotion matrix (Model.kt PROMOTIONS —
#: DuckLake's table intersected with Iceberg facade evolutions),
#: restated independently so an illegal planner step fails loudly.
LEGAL_PROMOTIONS = {
    "int8": {"int16", "int", "long"},
    "int16": {"int", "long"},
    "int": {"long"},
    "uint8": {"uint16", "uint32"},
    "uint16": {"uint32"},
    "float": {"double"},
}


def _plan(seed=11, **kw):
    kw.setdefault("scale", 2)
    kw.setdefault("evolution_rate", 0.5)
    return plan_workload(seed, **kw)


class TestDeterminism:
    def test_same_seed_same_plan(self):
        assert _plan(7) == _plan(7)

    def test_different_seed_different_plan(self):
        assert _plan(7) != _plan(8)

    def test_scale_grows_the_plan(self):
        assert len(plan_workload(3, scale=2)) > len(plan_workload(3, scale=1))

    def test_zero_evolution_rate_plans_no_evolution(self):
        steps = plan_workload(5, scale=2, evolution_rate=0.0)
        assert not [s for s in steps if isinstance(s, (AddColumn, PromoteColumn))]


class TestPlanLegality:
    def test_every_promotion_is_in_the_documented_matrix(self):
        promoted = [s for s in _plan(seed=101) if isinstance(s, PromoteColumn)]
        assert promoted, "plan at evolution_rate=0.5 should promote something"
        for step in promoted:
            assert step.to in LEGAL_PROMOTIONS.get(step.from_, set()), (
                f"illegal promotion {step.from_} -> {step.to}"
            )

    def test_promotions_chain_from_tracked_state(self):
        """A second promotion of one column must start from the promoted
        type, not the original — the planner tracks schema state."""
        for seed in range(40):
            steps = plan_workload(seed, scale=3, evolution_rate=1.0)
            current: dict[tuple[str, str], str] = {}
            for s in steps:
                if isinstance(s, CreateTable):
                    for name, coltype in s.columns:
                        current[(s.table, name)] = coltype
                elif isinstance(s, AddColumn):
                    current[(s.table, s.name)] = s.coltype
                elif isinstance(s, PromoteColumn):
                    assert current[(s.table, s.name)] == s.from_
                    assert s.to in LEGAL_PROMOTIONS[s.from_]
                    current[(s.table, s.name)] = s.to

    def test_tables_exist_before_they_are_used(self):
        steps = _plan(seed=13)
        live: set[str] = set()
        for s in steps:
            if isinstance(s, CreateTable):
                live.add(s.table)
            elif isinstance(s, Ctas):
                assert s.source in live
                live.add(s.table)
            elif isinstance(s, DropRecreate):
                assert s.table in live
            else:
                assert s.table in live, f"{s} before create"

    def test_no_load_lands_while_a_variant_column_is_live(self):
        """pyhoglake cannot append to a table with a live variant column,
        so the archive DDL must be immediately followed by the
        drop/recreate, never by a load on that table."""
        steps = _plan(seed=13)
        variant_live: set[str] = set()
        saw_variant = False
        for s in steps:
            if isinstance(s, AddVariantDdl):
                variant_live.add(s.table)
                saw_variant = True
            elif isinstance(s, DropRecreate):
                variant_live.discard(s.table)
            elif isinstance(s, Load):
                assert s.table not in variant_live
        assert saw_variant, "the plan should exercise variant DDL"

    def test_lifecycle_covers_the_workflow_kinds(self):
        steps = _plan(seed=13)
        kinds = {type(s) for s in steps}
        assert {
            CreateTable,
            Load,
            Ctas,
            DropRecreate,
            ChangefeedRead,
            AddVariantDdl,
        } <= kinds

    def test_loads_carry_distinct_batch_keys(self):
        loads = [s for s in _plan(seed=13) if isinstance(s, Load)]
        keys = [s.batch_key for s in loads]
        assert len(set(keys)) == len(keys)


class TestScenarioRegistration:
    def test_registered_with_profiles_and_io_mode(self):
        assert "analytics-lifecycle" in SCENARIOS
        assert SCENARIOS["analytics-lifecycle"] is lifecycle
        assert lifecycle.IO_MODE == "end-to-end"
        assert "analytics-lifecycle" in QUICK_PROFILE
        assert "analytics-lifecycle" in FULL_PROFILE

    def test_args_parse_with_defaults(self):
        from hoglake_bench.cli import build_parser

        args = build_parser().parse_args(["analytics-lifecycle"])
        assert args.seed == lifecycle.DEFAULT_SEED
        assert args.scale >= 1
        assert 0.0 <= args.evolution_rate <= 1.0

    def test_evolution_rate_outside_unit_interval_is_refused(self):
        from hoglake_bench.cli import build_parser

        with pytest.raises(SystemExit):
            build_parser().parse_args(
                ["analytics-lifecycle", "--evolution-rate", "1.5"]
            )
