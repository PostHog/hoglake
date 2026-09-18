"""Buffered config, lifecycle, and credential-boundary regressions."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import yaml
from pyhoglake import (
    CommitConflictError,
    ExpiredError,
    OffsetRegressionError,
    ValidationError,
)
from test_config import VALID

from hedgerow import cli
from hedgerow.buffered_service import (
    BufferedService,
    configure_s3,
    resolve_json_columns,
)
from hedgerow.config import ConfigError, HedgerowConfig, S3Settings
from hedgerow.halts import FeedExpiredError, PersistentFailureError, SplitBrainError


def raw_config(tmp_path):
    raw = deepcopy(VALID)
    raw.pop("filter")
    raw.pop("metrics")
    raw["replication"] = {"poll_interval_s": 0.1}
    raw["mode"] = "buffered"
    raw["buffered"] = {
        "state_path": str(tmp_path / "state.sqlite"),
        "spill_directory": str(tmp_path / "scratch"),
    }
    return raw


def test_defaults_and_env_override(tmp_path):
    from hedgerow.config import load_config

    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw_config(tmp_path)))
    config = load_config(str(path), {"HEDGEROW__BUFFERED__JSON_COLUMNS": "[]"})
    assert config.buffered.json_columns == ()
    assert config.buffered.policy.max_age_s == 86400
    assert config.buffered.writer.memory_bytes == 512 * 1024**2


@pytest.mark.parametrize(
    "patch",
    [
        {"json_columns": None},
        {"json_columns": "properties"},
        {"json_columns": ["x", "x"]},
        {"state_path": "relative"},
        {"writer": {"threads": 8}},
        {"writer": {"row_group_rows": 1}},
        {"policy": {"workers": 0}},
        {"max_failures": True},
    ],
)
def test_invalid_buffered_config(tmp_path, patch):
    raw = raw_config(tmp_path)
    raw["buffered"].update(patch)
    with pytest.raises(ConfigError):
        HedgerowConfig.parse(raw)


def test_default_mapping_is_schema_aware():
    from pyhoglake.models import Column

    source = SimpleNamespace(columns=(Column("properties", "string", 1, 0),))
    destination = SimpleNamespace(columns=(Column("properties", "variant", 1, 0),))
    assert resolve_json_columns(source, destination, None) == ("properties",)
    assert resolve_json_columns(destination, destination, None) == ()
    assert resolve_json_columns(destination, destination, ("properties",)) == ()
    assert resolve_json_columns(source, source, None) == ()
    assert resolve_json_columns(source, destination, ()) == ()


def test_secret_is_escaped_and_errors_do_not_expose_credentials():
    statements = []
    configure = configure_s3(
        S3Settings("http://localhost:9000", "a'key", "private'value")
    )
    configure(SimpleNamespace(execute=statements.append))
    assert "KEY_ID 'a''key'" in statements[0]
    assert "SECRET 'private''value'" in statements[0]
    assert "USE_SSL false" in statements[0]

    def reject(sql):
        raise RuntimeError(sql)

    with pytest.raises(ConfigError) as error:
        configure(SimpleNamespace(execute=reject))
    assert "private" not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.parametrize(
    "settings",
    [
        S3Settings(access_key="key"),
        S3Settings(endpoint="http://user:password@host"),
        S3Settings(endpoint="http://host/bucket"),
    ],
)
def test_bad_s3_settings_rejected(settings):
    with pytest.raises(ConfigError):
        configure_s3(settings)


def test_cli_selects_buffered_and_closes_on_halt(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw_config(tmp_path)))
    events = []

    class Service:
        def __init__(self, config):
            self.stop = SimpleNamespace(set=lambda: None)

        def start(self):
            events.append("start")

        def run_once(self):
            raise FeedExpiredError("expired")

        def close(self):
            events.append("close")

    monkeypatch.setattr(cli, "BufferedService", Service)
    assert cli.main(["--config", str(path), "--once"]) == 4
    assert events == ["start", "close"]


def test_async_idle_polls_do_not_reset_retry_budget(tmp_path, monkeypatch):
    config = HedgerowConfig.parse(raw_config(tmp_path))
    service = BufferedService(config)
    service.coordinator = SimpleNamespace(
        store=SimpleNamespace(recover=lambda: ["frozen work"])
    )
    outcomes = iter([TimeoutError(), None] * 4)

    def cycle():
        outcome = next(outcomes)
        if outcome:
            raise outcome

    monkeypatch.setattr(service, "_cycle", cycle)
    monkeypatch.setattr(service.stop, "wait", lambda _: False)
    with pytest.raises(PersistentFailureError, match="budget"):
        service.run_forever()
    assert service.failures == 4


@pytest.mark.parametrize(
    "error, expected",
    [
        (ExpiredError("expired"), FeedExpiredError),
        (OffsetRegressionError("offset"), SplitBrainError),
    ],
)
def test_client_halts_are_translated(tmp_path, error, expected):
    service = BufferedService(HedgerowConfig.parse(raw_config(tmp_path)))

    def cycle():
        raise error

    service.coordinator = SimpleNamespace(run_once=cycle)
    with pytest.raises(expected):
        service._cycle()


def test_once_harvests_workers_without_another_discovery_window(tmp_path):
    service = BufferedService(HedgerowConfig.parse(raw_config(tmp_path)))
    calls = []
    active = {"work": object()}

    def tick(now):
        calls.append("tick")
        active.clear()

    service.coordinator = SimpleNamespace(
        run_once=lambda: calls.append("discover"),
        _guard=lambda: None,
        _offset=lambda: calls.append("offset"),
        scheduler=SimpleNamespace(active=active, tick=tick),
    )
    service.run_once()
    assert calls == ["discover", "tick", "offset"]


@pytest.mark.parametrize(
    "error", [CommitConflictError("DDL conflict"), ValidationError("bad footer")]
)
def test_immutable_commit_conflicts_and_local_validation_halt(tmp_path, error):
    service = BufferedService(HedgerowConfig.parse(raw_config(tmp_path)))

    def fail():
        raise error

    service.coordinator = SimpleNamespace(run_once=fail)
    with pytest.raises(PersistentFailureError):
        service._cycle()
    assert service.failures == 0


def test_buffered_sigterm_stops_and_restores_handler(tmp_path, monkeypatch):
    import signal
    from threading import Event

    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw_config(tmp_path)))
    stopped = Event()
    calls = []
    previous = signal.getsignal(signal.SIGTERM)

    class Service:
        def __init__(self, config):
            self.stop = stopped

        def run_forever(self):
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            assert stopped.is_set()

        def close(self):
            calls.append("closed")

    monkeypatch.setattr(cli, "BufferedService", Service)
    assert cli.main(["--config", str(path)]) == 0
    assert calls == ["closed"]
    assert signal.getsignal(signal.SIGTERM) is previous


def test_region_is_shared_between_arrow_and_duckdb():
    settings = S3Settings(access_key="key", secret_key="secret", region="eu-west-1")
    assert settings.to_pyhoglake().region == "eu-west-1"
    statements = []
    configure_s3(settings)(SimpleNamespace(execute=statements.append))
    assert "REGION 'eu-west-1'" in statements[0]
