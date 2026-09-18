"""Every scenario declares how much real IO its numbers include, and the
results journal carries that declaration — so a metadata-only run can
never be mistaken for an end-to-end number after the fact."""

import json

from hoglake_bench.cli import SCENARIOS
from hoglake_bench.context import Bench, BenchConfig
from hoglake_bench.stats import Metric

IO_MODES = {"metadata-only", "mixed", "end-to-end"}


class TestScenarioIoModes:
    def test_every_scenario_declares_an_io_mode(self):
        for name, mod in SCENARIOS.items():
            assert getattr(mod, "IO_MODE", None) in IO_MODES, (
                f"scenario {name} must declare IO_MODE as one of {IO_MODES}"
            )

    def test_fabricating_scenarios_say_metadata_only(self):
        # These register files/stats/deletes that never exist in object
        # storage; their numbers exclude footer parse and object IO and
        # must be labeled so.
        for name in (
            "commit-throughput",
            "commit-contention",
            "delete-contention",
            "changefeed-scan",
            "ddl-churn",
        ):
            assert SCENARIOS[name].IO_MODE == "metadata-only"

    def test_real_write_scenarios_say_end_to_end(self):
        assert SCENARIOS["end-to-end-writer"].IO_MODE == "end-to-end"

    def test_expiry_is_mixed(self):
        # expiry drains fabricated registrations (metadata-only) AND
        # deletes real parquet objects; the label says both halves exist.
        assert SCENARIOS["expiry-throughput"].IO_MODE == "mixed"


class TestJournalCarriesIoMode:
    def test_append_result_records_io_mode(self, tmp_path):
        cfg = BenchConfig(results_path=str(tmp_path / "out.jsonl"))
        bench = Bench(cfg)
        try:
            bench.append_result(
                "unit-test",
                {},
                [Metric(name="m", ops=1, wall_s=1.0)],
                io_mode="metadata-only",
            )
        finally:
            bench.close()
        rec = json.loads((tmp_path / "out.jsonl").read_text())
        assert rec["io_mode"] == "metadata-only"

    def test_io_mode_field_is_always_present(self, tmp_path):
        cfg = BenchConfig(results_path=str(tmp_path / "out.jsonl"))
        bench = Bench(cfg)
        try:
            bench.append_result("unit-test", {}, [])
        finally:
            bench.close()
        rec = json.loads((tmp_path / "out.jsonl").read_text())
        assert "io_mode" in rec and rec["io_mode"] is None
