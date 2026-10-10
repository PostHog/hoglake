"""Config validation: every knob parses, defaults, and refuses with a
message naming the knob and the problem — and ALL problems are reported
in one refusal, not one per restart."""

from __future__ import annotations

from typing import Any

import pytest

from millrace.config import (
    AssignmentMode,
    AutoOffsetReset,
    BackpressureConfig,
    Config,
    ConfigError,
    EventTimePolicy,
    PoisonConfig,
    TeamKeyCodec,
    load_config,
)

MINIMAL_ENV = {
    "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
    "MILLRACE_KAFKA_TOPIC": "events",
    "MILLRACE_CATALOG": "prod",
    "MILLRACE_NAMESPACE": "default",
    "MILLRACE_TABLE": "events",
    "MILLRACE_STAGE_URL": "s3://data-bucket/millrace",
    "MILLRACE_KAFKA_PARTITIONS": "0,1,2",
}


def load(env: dict[str, str]) -> Config:
    return load_config(env)


def problems(env: dict[str, str]) -> tuple[str, ...]:
    with pytest.raises(ConfigError) as excinfo:
        load_config(env)
    return excinfo.value.problems


def test_minimal_env_yields_the_documented_defaults():
    cfg = load(MINIMAL_ENV)
    assert cfg.kafka_bootstrap_servers == "broker-1:9092,broker-2:9092"
    assert cfg.kafka_topic == "events"
    assert cfg.kafka_group_id == "millrace-events"
    assert cfg.assignment_mode is AssignmentMode.STATIC
    assert cfg.static_partitions == (0, 1, 2)
    assert cfg.team_key_codec is TeamKeyCodec.UTF8_DECIMAL
    assert cfg.catalog == "prod"
    assert cfg.namespace == "default"
    assert cfg.table == "events"
    assert cfg.stage_store_url == "s3://data-bucket"
    assert cfg.stage_base_path == "millrace"
    assert cfg.target_output_bytes == 500 * 1024 * 1024
    assert cfg.flush_deadline_s == 900
    assert cfg.slow_lane_deadline_s == 21600
    assert cfg.min_flush_bytes == 1024 * 1024
    assert cfg.max_files_per_commit == 512
    assert cfg.backpressure == BackpressureConfig(
        pause_staged_bytes=8 * 1024**3,
        resume_staged_bytes=4 * 1024**3,
        pause_oldest_age_s=3600,
        resume_oldest_age_s=1800,
    )
    assert cfg.poison == PoisonConfig(
        max_records_per_run=1000, value_max_bytes=1024 * 1024
    )
    assert cfg.consume_batch_size == 65536
    assert cfg.consume_batch_max_bytes == 256 * 1024 * 1024
    assert cfg.poll_timeout_ms == 500
    assert cfg.event_time_policy is EventTimePolicy.QUARANTINE
    assert cfg.metrics_port == 8000
    assert cfg.receipt_horizon_s == 6 * 86400  # below the server's 7 d default
    # The fleet policy default: latest (millpond's whole-retention
    # replay incident), earliest still selectable.
    assert cfg.kafka_auto_offset_reset is AutoOffsetReset.LATEST
    assert cfg.team_id_field is None
    # The whale-shape fetch tuning (millpond's hot-partition ceiling).
    assert cfg.kafka_max_partition_fetch_bytes == 32 * 1024 * 1024
    assert cfg.kafka_fetch_max_bytes == 256 * 1024 * 1024
    assert cfg.kafka_queued_max_messages_kbytes == 262144


def test_planner_knobs_projection():
    cfg = load(
        MINIMAL_ENV
        | {
            "MILLRACE_TARGET_OUTPUT_BYTES": "4096",
            "MILLRACE_FLUSH_DEADLINE_S": "60",
            "MILLRACE_SLOW_LANE_DEADLINE_S": "25200",
            "MILLRACE_MIN_FLUSH_BYTES": "64",
            "MILLRACE_MAX_FILES_PER_COMMIT": "17",
        }
    )
    knobs = cfg.planner_knobs()
    assert knobs.target_output_bytes == 4096
    assert knobs.flush_deadline_s == 60
    assert knobs.slow_lane_deadline_s == 25200
    assert knobs.min_flush_bytes == 64
    assert knobs.max_files_per_commit == 17


# -- renamed (pre-three-lane) knobs ----------------------------------------------------
#
# MILLRACE_TARGET_FILE_BYTES / MILLRACE_REAP_DEADLINE_S are REFUSED —
# refused, not aliased: the rename narrowed the semantics (the size
# trigger now sizes the estimated parquet output; the churn reaper
# became the bounded slow lane), so a silent alias would deploy a policy
# the value did not ask for. There is no deprecation window: no
# deployment predates the rename.


@pytest.mark.parametrize(
    "old,new",
    [
        ("MILLRACE_TARGET_FILE_BYTES", "MILLRACE_TARGET_OUTPUT_BYTES"),
        ("MILLRACE_REAP_DEADLINE_S", "MILLRACE_SLOW_LANE_DEADLINE_S"),
    ],
)
def test_renamed_knobs_are_refused_naming_the_successor(old, new):
    (p,) = problems(MINIMAL_ENV | {old: "1024"})
    assert old in p and new in p and "renamed" in p


def test_renamed_knobs_refused_even_alongside_the_successor():
    probs = problems(
        MINIMAL_ENV
        | {
            "MILLRACE_TARGET_FILE_BYTES": "1024",
            "MILLRACE_TARGET_OUTPUT_BYTES": "2048",
        }
    )
    assert len(probs) == 1
    assert "MILLRACE_TARGET_FILE_BYTES" in probs[0]


def test_renamed_knob_with_an_empty_value_is_unset_not_refused():
    # Consistent with every other knob: whitespace-only reads as absent.
    cfg = load(MINIMAL_ENV | {"MILLRACE_REAP_DEADLINE_S": "  "})
    assert cfg.slow_lane_deadline_s == 21600


# -- the slow lane's 6-24 h range ------------------------------------------------------


@pytest.mark.parametrize("raw", ["21599", "86401", "3600", "172800"])
def test_slow_lane_deadline_outside_6_to_24_h_is_refused(raw):
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_SLOW_LANE_DEADLINE_S": raw})
    assert "MILLRACE_SLOW_LANE_DEADLINE_S" in p and raw in p


@pytest.mark.parametrize("raw", ["21600", "43200", "86400"])
def test_slow_lane_deadline_inside_6_to_24_h_is_accepted(raw):
    cfg = load(MINIMAL_ENV | {"MILLRACE_SLOW_LANE_DEADLINE_S": raw})
    assert cfg.slow_lane_deadline_s == int(raw)


# -- required knobs and the all-problems-at-once refusal ---------------------------


def test_every_missing_required_knob_is_named_in_one_refusal():
    probs = problems({})
    # The six outright-required knobs plus MILLRACE_KAFKA_PARTITIONS,
    # which the DEFAULT static assignment mode requires.
    assert len(probs) == 7
    for knob in (
        "MILLRACE_KAFKA_BOOTSTRAP_SERVERS",
        "MILLRACE_KAFKA_TOPIC",
        "MILLRACE_CATALOG",
        "MILLRACE_NAMESPACE",
        "MILLRACE_TABLE",
        "MILLRACE_STAGE_URL",
        "MILLRACE_KAFKA_PARTITIONS",
    ):
        assert any(knob in p and "required" in p for p in probs), knob


@pytest.mark.parametrize(
    "knob",
    [
        "MILLRACE_KAFKA_BOOTSTRAP_SERVERS",
        "MILLRACE_KAFKA_TOPIC",
        "MILLRACE_CATALOG",
        "MILLRACE_NAMESPACE",
        "MILLRACE_TABLE",
        "MILLRACE_STAGE_URL",
    ],
)
def test_each_required_knob_individually(knob):
    probs = problems({k: v for k, v in MINIMAL_ENV.items() if k != knob})
    assert len(probs) == 1
    assert knob in probs[0]


def test_invalid_topic_name_is_named():
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_KAFKA_TOPIC": "bad topic!"})
    assert "MILLRACE_KAFKA_TOPIC" in p and "bad topic!" in p


@pytest.mark.parametrize(
    "knob,value",
    [
        ("MILLRACE_CATALOG", "0Bad"),
        ("MILLRACE_NAMESPACE", "1ns"),
        ("MILLRACE_TABLE", "has space"),
    ],
)
def test_hoglake_identifier_shapes(knob, value):
    (p,) = problems(MINIMAL_ENV | {knob: value})
    assert knob in p and value in p


def test_group_id_default_and_override():
    assert load(MINIMAL_ENV).kafka_group_id == "millrace-events"
    assert (
        load(MINIMAL_ENV | {"MILLRACE_KAFKA_GROUP_ID": "custom-g"}).kafka_group_id
        == "custom-g"
    )


# -- assignment ------------------------------------------------------------------------


def test_static_partitions_parse_sorted():
    cfg = load(MINIMAL_ENV | {"MILLRACE_KAFKA_PARTITIONS": "4, 0,2"})
    assert cfg.static_partitions == (0, 2, 4)


def test_static_requires_partitions():
    env = {k: v for k, v in MINIMAL_ENV.items() if k != "MILLRACE_KAFKA_PARTITIONS"}
    (p,) = problems(env)
    assert "MILLRACE_KAFKA_PARTITIONS" in p and "static" in p


@pytest.mark.parametrize("raw", ["0,,1", "0,x", "-1,2", "", "0,0"])
def test_bad_partition_lists_are_refused(raw):
    probs = problems(MINIMAL_ENV | {"MILLRACE_KAFKA_PARTITIONS": raw})
    assert probs and all("MILLRACE_KAFKA_PARTITIONS" in p for p in probs)


def test_cooperative_forbids_an_explicit_partition_list():
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_KAFKA_ASSIGNMENT": "cooperative"})
    assert "MILLRACE_KAFKA_PARTITIONS" in p and "cooperative" in p


def test_cooperative_without_partitions_is_valid():
    env = {k: v for k, v in MINIMAL_ENV.items() if k != "MILLRACE_KAFKA_PARTITIONS"}
    cfg = load(env | {"MILLRACE_KAFKA_ASSIGNMENT": "cooperative"})
    assert cfg.assignment_mode is AssignmentMode.COOPERATIVE
    assert cfg.static_partitions is None


def test_assignment_mode_garbage_is_refused():
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_KAFKA_ASSIGNMENT": "eager"})
    assert "MILLRACE_KAFKA_ASSIGNMENT" in p and "static" in p and "cooperative" in p


# -- team key codec -----------------------------------------------------------------


def test_team_key_codec_choices():
    assert load(MINIMAL_ENV).team_key_codec is TeamKeyCodec.UTF8_DECIMAL
    assert (
        load(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": "be64"}).team_key_codec
        is TeamKeyCodec.BE64
    )
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": "ascii"})
    assert "MILLRACE_TEAM_KEY_CODEC" in p and "utf8-decimal" in p and "be64" in p


def test_value_json_codec_parses_its_field():
    cfg = load(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": "value-json:team_id"})
    assert cfg.team_key_codec is TeamKeyCodec.VALUE_JSON
    assert cfg.team_id_field == "team_id"


@pytest.mark.parametrize(
    "raw",
    [
        "value-json",  # no field at all
        "value-json:",  # empty field
        "value-json: team id",  # whitespace is a typo, not a key
        "value-json:a.b",  # dots imply the nesting we do not support
        "value-json:1field",  # must start alpha/underscore
    ],
)
def test_value_json_codec_refuses_a_bad_field(raw):
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": raw})
    assert "MILLRACE_TEAM_KEY_CODEC" in p
    assert "value-json" in p


def test_config_constructor_guards_the_codec_field_pair():
    kwargs: dict[str, Any] = dict(load(MINIMAL_ENV).__dict__)
    with pytest.raises(ValueError, match="value-json"):
        Config(**(kwargs | {"team_key_codec": TeamKeyCodec.VALUE_JSON}))
    with pytest.raises(ValueError, match="team_id_field"):
        Config(**(kwargs | {"team_id_field": "team_id"}))


# -- auto.offset.reset (B6: the fleet policy is latest) ------------------------------


def test_auto_offset_reset_defaults_to_latest_and_accepts_earliest():
    assert load(MINIMAL_ENV).kafka_auto_offset_reset is AutoOffsetReset.LATEST
    cfg = load(MINIMAL_ENV | {"MILLRACE_KAFKA_AUTO_OFFSET_RESET": "earliest"})
    assert cfg.kafka_auto_offset_reset is AutoOffsetReset.EARLIEST
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_KAFKA_AUTO_OFFSET_RESET": "smallest"})
    assert "MILLRACE_KAFKA_AUTO_OFFSET_RESET" in p and "latest" in p


# -- fetch tuning (T8) -----------------------------------------------------------------


def test_fetch_tuning_knobs_parse():
    cfg = load(
        MINIMAL_ENV
        | {
            "MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES": "1048576",
            "MILLRACE_KAFKA_FETCH_MAX_BYTES": "52428800",
            "MILLRACE_KAFKA_QUEUED_MAX_MESSAGES_KBYTES": "65536",
        }
    )
    assert cfg.kafka_max_partition_fetch_bytes == 1048576
    assert cfg.kafka_fetch_max_bytes == 52428800
    assert cfg.kafka_queued_max_messages_kbytes == 65536


def test_fetch_max_bytes_must_fit_a_partition_response():
    (p,) = problems(
        MINIMAL_ENV
        | {
            "MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES": "33554432",
            "MILLRACE_KAFKA_FETCH_MAX_BYTES": "1048576",
        }
    )
    assert "MILLRACE_KAFKA_FETCH_MAX_BYTES" in p
    assert "MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES" in p


# -- stage URL ----------------------------------------------------------------------


def test_stage_url_s3():
    cfg = load(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "s3://bkt/millrace/"})
    assert (cfg.stage_store_url, cfg.stage_base_path) == ("s3://bkt", "millrace")


def test_stage_url_s3_requires_a_prefix():
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "s3://bkt"})
    assert "MILLRACE_STAGE_URL" in p and "prefix" in p


def test_stage_url_s3_bucket_shape():
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "s3://Bad_Bucket/millrace"})
    assert "MILLRACE_STAGE_URL" in p and "bucket" in p


def test_stage_url_file():
    cfg = load(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "file:///var/lib/millrace"})
    assert (cfg.stage_store_url, cfg.stage_base_path) == (
        "file:///",
        "/var/lib/millrace",
    )


def test_stage_url_file_requires_a_path():
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "file:///"})
    assert "MILLRACE_STAGE_URL" in p and "directory" in p


def test_stage_url_memory():
    cfg = load(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "memory:///"})
    assert (cfg.stage_store_url, cfg.stage_base_path) == ("memory:///", "millrace")
    cfg = load(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "memory:///suite"})
    assert (cfg.stage_store_url, cfg.stage_base_path) == ("memory:///", "suite")


def test_stage_url_unknown_scheme():
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_STAGE_URL": "gs://bkt/millrace"})
    assert "MILLRACE_STAGE_URL" in p and "gs://bkt/millrace" in p


# -- positive integer knobs -------------------------------------------------------------


@pytest.mark.parametrize(
    "knob",
    [
        "MILLRACE_TARGET_OUTPUT_BYTES",
        "MILLRACE_FLUSH_DEADLINE_S",
        "MILLRACE_SLOW_LANE_DEADLINE_S",
        "MILLRACE_MIN_FLUSH_BYTES",
        "MILLRACE_MAX_FILES_PER_COMMIT",
        "MILLRACE_BACKPRESSURE_PAUSE_BYTES",
        "MILLRACE_BACKPRESSURE_RESUME_BYTES",
        "MILLRACE_BACKPRESSURE_PAUSE_AGE_S",
        "MILLRACE_BACKPRESSURE_RESUME_AGE_S",
        "MILLRACE_POISON_MAX_RECORDS",
        "MILLRACE_POISON_VALUE_MAX_BYTES",
        "MILLRACE_POISON_RETENTION_S",
        "MILLRACE_POISON_SWEEP_S",
        "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES",
        "MILLRACE_SLATEDB_L0_MAX_SSTS",
        "MILLRACE_CONSUME_BATCH_SIZE",
        "MILLRACE_CONSUME_BATCH_MAX_BYTES",
        "MILLRACE_POLL_TIMEOUT_MS",
        "MILLRACE_RECEIPT_HORIZON_S",
        "MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES",
        "MILLRACE_KAFKA_FETCH_MAX_BYTES",
        "MILLRACE_KAFKA_QUEUED_MAX_MESSAGES_KBYTES",
    ],
)
@pytest.mark.parametrize("raw", ["abc", "0", "-3", "1.5"])
def test_positive_int_knobs_refuse_garbage(knob, raw):
    probs = problems(MINIMAL_ENV | {knob: raw})
    assert probs and all(knob in p for p in probs)


def test_positive_int_knobs_accept_valid_values():
    cfg = load(
        MINIMAL_ENV
        | {
            "MILLRACE_TARGET_OUTPUT_BYTES": "1024",
            "MILLRACE_FLUSH_DEADLINE_S": "5",
            "MILLRACE_SLOW_LANE_DEADLINE_S": "43200",
            "MILLRACE_MIN_FLUSH_BYTES": "32",
            "MILLRACE_MAX_FILES_PER_COMMIT": "3",
            "MILLRACE_BACKPRESSURE_PAUSE_BYTES": "1000",
            "MILLRACE_BACKPRESSURE_RESUME_BYTES": "500",
            "MILLRACE_BACKPRESSURE_PAUSE_AGE_S": "100",
            "MILLRACE_BACKPRESSURE_RESUME_AGE_S": "50",
            "MILLRACE_POISON_MAX_RECORDS": "7",
            "MILLRACE_POISON_VALUE_MAX_BYTES": "64",
            "MILLRACE_CONSUME_BATCH_SIZE": "11",
            "MILLRACE_CONSUME_BATCH_MAX_BYTES": "4096",
            "MILLRACE_POLL_TIMEOUT_MS": "42",
            "MILLRACE_RECEIPT_HORIZON_S": "3600",
        }
    )
    assert cfg.target_output_bytes == 1024
    assert cfg.flush_deadline_s == 5
    assert cfg.slow_lane_deadline_s == 43200
    assert cfg.min_flush_bytes == 32
    assert cfg.max_files_per_commit == 3
    assert cfg.backpressure.pause_staged_bytes == 1000
    assert cfg.backpressure.resume_staged_bytes == 500
    assert cfg.backpressure.pause_oldest_age_s == 100
    assert cfg.backpressure.resume_oldest_age_s == 50
    assert cfg.poison.max_records_per_run == 7
    assert cfg.poison.value_max_bytes == 64
    assert cfg.consume_batch_size == 11
    assert cfg.consume_batch_max_bytes == 4096
    assert cfg.poll_timeout_ms == 42
    assert cfg.receipt_horizon_s == 3600


# -- hysteresis cross-checks --------------------------------------------------------------


def test_backpressure_byte_thresholds_must_not_touch():
    (p,) = problems(
        MINIMAL_ENV
        | {
            "MILLRACE_BACKPRESSURE_PAUSE_BYTES": "500",
            "MILLRACE_BACKPRESSURE_RESUME_BYTES": "500",
        }
    )
    assert "MILLRACE_BACKPRESSURE_PAUSE_BYTES" in p
    assert "MILLRACE_BACKPRESSURE_RESUME_BYTES" in p
    assert "flapping" in p


def test_backpressure_age_thresholds_must_not_invert():
    (p,) = problems(
        MINIMAL_ENV
        | {
            "MILLRACE_BACKPRESSURE_PAUSE_AGE_S": "50",
            "MILLRACE_BACKPRESSURE_RESUME_AGE_S": "100",
        }
    )
    assert "MILLRACE_BACKPRESSURE_PAUSE_AGE_S" in p
    assert "MILLRACE_BACKPRESSURE_RESUME_AGE_S" in p


def test_backpressure_config_guards_direct_construction():
    with pytest.raises(ValueError, match="MILLRACE_BACKPRESSURE_PAUSE_BYTES"):
        BackpressureConfig(
            pause_staged_bytes=10,
            resume_staged_bytes=10,
            pause_oldest_age_s=2,
            resume_oldest_age_s=1,
        )
    with pytest.raises(ValueError, match="MILLRACE_BACKPRESSURE_RESUME_AGE_S"):
        BackpressureConfig(
            pause_staged_bytes=10,
            resume_staged_bytes=5,
            pause_oldest_age_s=1,
            resume_oldest_age_s=0,
        )


def test_poison_config_guards_direct_construction():
    with pytest.raises(ValueError, match="MILLRACE_POISON_MAX_RECORDS"):
        PoisonConfig(max_records_per_run=0, value_max_bytes=1)
    with pytest.raises(ValueError, match="MILLRACE_POISON_VALUE_MAX_BYTES"):
        PoisonConfig(max_records_per_run=1, value_max_bytes=0)
    with pytest.raises(ValueError, match="MILLRACE_POISON_RETENTION_S"):
        PoisonConfig(max_records_per_run=1, value_max_bytes=1, retention_s=0)
    with pytest.raises(ValueError, match="MILLRACE_POISON_SWEEP_S"):
        PoisonConfig(max_records_per_run=1, value_max_bytes=1, sweep_s=-5)


# -- SlateDB writer tuning (T9) ------------------------------------------------------


def test_slatedb_defaults_match_the_documented_deployment():
    """GC OFF by default (the external maintenance service owns
    collection), a per-instance unflushed bound sized for K-instances-
    per-pod, and SlateDB's own L0 stall depth."""
    cfg = load(MINIMAL_ENV)
    assert cfg.slatedb.gc_enabled is False
    assert cfg.slatedb.max_unflushed_bytes == 256 * 1024 * 1024
    assert cfg.slatedb.l0_max_ssts == 8


def test_slatedb_knobs_parse_and_validate():
    cfg = load(
        MINIMAL_ENV
        | {
            "MILLRACE_SLATEDB_GC_ENABLED": "true",
            "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES": "134217728",
            "MILLRACE_SLATEDB_L0_MAX_SSTS": "32",
        }
    )
    assert cfg.slatedb.gc_enabled is True
    assert cfg.slatedb.max_unflushed_bytes == 128 * 1024 * 1024
    assert cfg.slatedb.l0_max_ssts == 32

    (p,) = problems(MINIMAL_ENV | {"MILLRACE_SLATEDB_GC_ENABLED": "maybe"})
    assert "MILLRACE_SLATEDB_GC_ENABLED" in p and "boolean" in p
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES": "0"})
    assert "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES" in p
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_SLATEDB_L0_MAX_SSTS": "-2"})
    assert "MILLRACE_SLATEDB_L0_MAX_SSTS" in p
    from millrace.config import SlateDbConfig

    with pytest.raises(ValueError, match="MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES"):
        SlateDbConfig(max_unflushed_bytes=0)
    with pytest.raises(ValueError, match="MILLRACE_SLATEDB_L0_MAX_SSTS"):
        SlateDbConfig(l0_max_ssts=0)
    # The binding's own rule, encoded at parse time rather than at first
    # DB open: the unflushed bound must exceed the (never overridden)
    # 64 MiB l0_sst_size_bytes library default.
    (p,) = problems(
        MINIMAL_ENV | {"MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES": str(64 * 1024 * 1024)}
    )
    assert "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES" in p and "l0_sst_size_bytes" in p


def test_poison_retention_knobs_parse():
    cfg = load(MINIMAL_ENV)
    assert cfg.poison.retention_s == 7 * 86400
    assert cfg.poison.sweep_s == 3600
    cfg = load(
        MINIMAL_ENV
        | {"MILLRACE_POISON_RETENTION_S": "86400", "MILLRACE_POISON_SWEEP_S": "60"}
    )
    assert cfg.poison.retention_s == 86400
    assert cfg.poison.sweep_s == 60
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_POISON_RETENTION_S": "0"})
    assert "MILLRACE_POISON_RETENTION_S" in p


def test_team_key_codec_env_is_the_parsers_inverse():
    """The env dump of the codec must reproduce the config — a bare
    ``value-json`` (no ``:<field>``) is refused at boot, which is exactly
    the bug this property exists to prevent (the live harness dumps a
    Config to a child's env)."""
    assert load(MINIMAL_ENV).team_key_codec_env == "utf8-decimal"
    assert (
        load(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": "be64"}).team_key_codec_env
        == "be64"
    )
    value_json = load(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": "value-json:team_id"})
    assert value_json.team_key_codec_env == "value-json:team_id"
    # Round-trip: the rendered spec parses back to the same config.
    assert (
        load(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": value_json.team_key_codec_env})
        == value_json
    )


# -- remaining knobs ----------------------------------------------------------------------


def test_event_time_policy():
    assert load(MINIMAL_ENV).event_time_policy is EventTimePolicy.QUARANTINE
    assert (
        load(MINIMAL_ENV | {"MILLRACE_EVENT_TIME_POLICY": "clamp"}).event_time_policy
        is EventTimePolicy.CLAMP
    )
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_EVENT_TIME_POLICY": "drop"})
    assert "MILLRACE_EVENT_TIME_POLICY" in p and "clamp" in p and "quarantine" in p


@pytest.mark.parametrize("raw", ["abc", "-1", "65536"])
def test_metrics_port_refused(raw):
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_METRICS_PORT": raw})
    assert "MILLRACE_METRICS_PORT" in p


def test_metrics_port_zero_is_ephemeral_and_legal():
    assert load(MINIMAL_ENV | {"MILLRACE_METRICS_PORT": "0"}).metrics_port == 0


def test_config_error_message_lists_every_problem():
    with pytest.raises(ConfigError) as excinfo:
        load_config({"MILLRACE_KAFKA_TOPIC": "events"})
    text = str(excinfo.value)
    assert "problem(s)" in text
    for problem in excinfo.value.problems:
        assert problem.removeprefix("  - ") in text
