"""Config validation: every knob parses, defaults, and refuses with a
message naming the knob and the problem — and ALL problems are reported
in one refusal, not one per restart."""

from __future__ import annotations

import pytest

from millrace.config import (
    AutoOffsetReset,
    Config,
    ConfigError,
    EventTimePolicy,
    load_config,
)

MINIMAL_ENV = {
    "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
    "MILLRACE_KAFKA_TOPIC": "events",
    "MILLRACE_CATALOG": "prod",
    "MILLRACE_NAMESPACE": "default",
    "MILLRACE_TABLE": "events",
    "MILLRACE_STAGE_URL": "s3://data-bucket/millrace",
    "MILLRACE_REPLICA_COUNT": "3",
    "POD_NAME": "millrace-events-1",
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
    assert cfg.replica_count == 3
    assert cfg.ordinal == 1
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
    assert cfg.consume_batch_size == 65536
    assert cfg.consume_batch_max_bytes == 256 * 1024 * 1024
    assert cfg.poll_timeout_ms == 500
    assert cfg.event_time_policy is EventTimePolicy.DROP
    assert cfg.metrics_port == 8000
    # The fleet policy default: latest (millpond's whole-retention
    # replay incident), earliest still selectable.
    assert cfg.kafka_auto_offset_reset is AutoOffsetReset.LATEST
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


# -- the slow lane sits at or above the age lane -------------------------------------


@pytest.mark.parametrize(
    "slow,flush", [("899", "900"), ("60", "900"), ("1", "2"), ("3599", "3600")]
)
def test_slow_lane_below_the_flush_deadline_is_refused_naming_both(slow, flush):
    (p,) = problems(
        MINIMAL_ENV
        | {"MILLRACE_SLOW_LANE_DEADLINE_S": slow, "MILLRACE_FLUSH_DEADLINE_S": flush}
    )
    assert "MILLRACE_SLOW_LANE_DEADLINE_S" in p and "MILLRACE_FLUSH_DEADLINE_S" in p
    assert slow in p and flush in p


@pytest.mark.parametrize(
    "slow,flush",
    [
        ("900", "900"),  # equal: the slow lane IS the age lane for tiny keys
        ("120", "60"),  # dev shape: minutes, not hours
        ("1", "1"),
        ("21600", "900"),  # the production default
        ("172800", "900"),  # no upper bound
    ],
)
def test_slow_lane_at_or_above_the_flush_deadline_is_accepted(slow, flush):
    cfg = load(
        MINIMAL_ENV
        | {"MILLRACE_SLOW_LANE_DEADLINE_S": slow, "MILLRACE_FLUSH_DEADLINE_S": flush}
    )
    assert cfg.slow_lane_deadline_s == int(slow)
    assert cfg.flush_deadline_s == int(flush)


def test_slow_lane_relation_is_not_checked_when_either_side_is_already_invalid():
    # One problem per knob at fault, never a derived one on top: a
    # refused value is None and the relation stays quiet about it.
    probs = problems(
        MINIMAL_ENV
        | {"MILLRACE_SLOW_LANE_DEADLINE_S": "60", "MILLRACE_FLUSH_DEADLINE_S": "x"}
    )
    assert len(probs) == 1 and "MILLRACE_FLUSH_DEADLINE_S" in probs[0]


# -- required knobs and the all-problems-at-once refusal ---------------------------


def test_every_missing_required_knob_is_named_in_one_refusal():
    probs = problems({})
    # The six outright-required knobs plus the two halves of the
    # assignment: the replica count and the pod name the ordinal comes from.
    assert len(probs) == 8
    for knob in (
        "MILLRACE_KAFKA_BOOTSTRAP_SERVERS",
        "MILLRACE_KAFKA_TOPIC",
        "MILLRACE_CATALOG",
        "MILLRACE_NAMESPACE",
        "MILLRACE_TABLE",
        "MILLRACE_STAGE_URL",
        "MILLRACE_REPLICA_COUNT",
        "POD_NAME",
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


# -- assignment: replica count + pod ordinal (millpond's model) -------------------------


def _without(env: dict[str, str], *names: str) -> dict[str, str]:
    return {k: v for k, v in env.items() if k not in names}


def test_ordinal_comes_from_pod_name():
    cfg = load(MINIMAL_ENV | {"POD_NAME": "millrace-events-2"})
    assert (cfg.replica_count, cfg.ordinal) == (3, 2)


def test_ordinal_falls_back_to_hostname():
    env = _without(MINIMAL_ENV, "POD_NAME") | {"HOSTNAME": "millrace-events-0"}
    assert load(env).ordinal == 0


def test_a_blank_pod_name_falls_back_to_hostname():
    env = MINIMAL_ENV | {"POD_NAME": " ", "HOSTNAME": "millrace-events-1"}
    assert load(env).ordinal == 1


@pytest.mark.parametrize(
    "hostname", ["laptop", "millrace-events-7"]
)  # no ordinal; ordinal >= replicas
def test_a_refused_hostname_fallback_says_to_set_pod_name(hostname):
    """Either refusal of the FALLBACK names the fix: POD_NAME from the
    downward API, with HOSTNAME named as what was read instead."""
    env = _without(MINIMAL_ENV, "POD_NAME") | {"HOSTNAME": hostname}
    (p,) = problems(env)
    assert p.startswith(f"HOSTNAME={hostname!r}")
    assert "fallback" in p and "POD_NAME" in p and "downward API" in p
    # Read through POD_NAME itself, the message does not mention a fallback.
    (p2,) = problems(MINIMAL_ENV | {"POD_NAME": hostname})
    assert p2.startswith(f"POD_NAME={hostname!r}") and "fallback" not in p2


def test_pod_name_wins_over_hostname():
    env = MINIMAL_ENV | {"POD_NAME": "millrace-events-2", "HOSTNAME": "laptop-0"}
    assert load(env).ordinal == 2


def test_ordinal_is_the_trailing_number_only():
    """``-<n>$``: a numeric token in the middle is not the ordinal, and
    the digits are read as one decimal number."""
    assert (
        load(
            MINIMAL_ENV
            | {"POD_NAME": "millrace-v2-events-12", "MILLRACE_REPLICA_COUNT": "13"}
        ).ordinal
        == 12
    )


def test_missing_pod_name_and_hostname_is_refused():
    (p,) = problems(_without(MINIMAL_ENV, "POD_NAME"))
    assert "POD_NAME" in p and "HOSTNAME" in p and "required" in p


@pytest.mark.parametrize("var", ["POD_NAME", "HOSTNAME"])
@pytest.mark.parametrize(
    "name", ["millrace", "millrace-", "millrace-a", "3-millrace", "millrace-3x"]
)
def test_a_pod_name_without_a_trailing_ordinal_is_refused(var, name):
    env = _without(MINIMAL_ENV, "POD_NAME") | {var: name}
    (p,) = problems(env)
    assert var in p and repr(name) in p and "ordinal" in p


def test_ordinal_at_or_past_the_replica_count_is_refused():
    (p,) = problems(MINIMAL_ENV | {"POD_NAME": "millrace-events-3"})
    assert "POD_NAME" in p and "3 >= MILLRACE_REPLICA_COUNT=3" in p
    (p,) = problems(MINIMAL_ENV | {"POD_NAME": "millrace-events-7"})
    assert "7 >= MILLRACE_REPLICA_COUNT=3" in p
    assert load(MINIMAL_ENV | {"POD_NAME": "millrace-events-2"}).ordinal == 2


def test_missing_replica_count_is_refused():
    (p,) = problems(_without(MINIMAL_ENV, "MILLRACE_REPLICA_COUNT"))
    assert "MILLRACE_REPLICA_COUNT" in p and "required" in p


@pytest.mark.parametrize("raw", ["0", "-1", "two", "1.5"])
def test_a_non_positive_replica_count_is_refused(raw):
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_REPLICA_COUNT": raw})
    assert "MILLRACE_REPLICA_COUNT" in p and "positive" in p


# -- MILLRACE_KAFKA_CONSUMER_* librdkafka passthrough -----------------------------------


def test_no_passthrough_means_no_overrides():
    assert load(MINIMAL_ENV).kafka_consumer_overrides == ()


def test_passthrough_maps_key_lowercase_with_dots_and_keeps_the_value_verbatim():
    cfg = load(
        MINIMAL_ENV
        | {
            "MILLRACE_KAFKA_CONSUMER_SECURITY_PROTOCOL": "SSL",
            "MILLRACE_KAFKA_CONSUMER_CLIENT_ID": "ws_az=use1-az1,millrace-events-0",
            "MILLRACE_KAFKA_CONSUMER_CLIENT_RACK": "use1-az1",
            "MILLRACE_KAFKA_CONSUMER_SASL_MECHANISM": " PLAIN ",
        }
    )
    assert cfg.kafka_consumer_overrides == (
        ("client.id", "ws_az=use1-az1,millrace-events-0"),
        ("client.rack", "use1-az1"),
        ("sasl.mechanism", " PLAIN "),
        ("security.protocol", "SSL"),
    )


@pytest.mark.parametrize(
    ("suffix", "knob"),
    [
        ("BOOTSTRAP_SERVERS", "MILLRACE_KAFKA_BOOTSTRAP_SERVERS"),
        ("GROUP_ID", "MILLRACE_KAFKA_GROUP_ID"),
        ("AUTO_OFFSET_RESET", "MILLRACE_KAFKA_AUTO_OFFSET_RESET"),
        ("ENABLE_AUTO_COMMIT", None),
        ("ENABLE_AUTO_OFFSET_STORE", None),
        ("PARTITION_ASSIGNMENT_STRATEGY", None),
    ],
)
def test_passthrough_refuses_the_keys_millrace_owns(suffix, knob):
    name = f"MILLRACE_KAFKA_CONSUMER_{suffix}"
    (p,) = problems(MINIMAL_ENV | {name: "x"})
    assert p.startswith(f"{name} is refused")
    assert suffix.lower().replace("_", ".") in p
    if knob is not None:
        assert f"set {knob} instead" in p
    else:
        assert "no knob on purpose" in p


def test_passthrough_refuses_an_empty_value_and_an_empty_key():
    probs = problems(
        MINIMAL_ENV
        | {"MILLRACE_KAFKA_CONSUMER_CLIENT_RACK": "  ", "MILLRACE_KAFKA_CONSUMER_": "x"}
    )
    assert len(probs) == 2
    assert any("MILLRACE_KAFKA_CONSUMER_CLIENT_RACK is empty" in p for p in probs)
    assert any("MILLRACE_KAFKA_CONSUMER_ names no librdkafka key" in p for p in probs)


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
        "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES",
        "MILLRACE_SLATEDB_L0_MAX_SSTS",
        "MILLRACE_CONSUME_BATCH_SIZE",
        "MILLRACE_CONSUME_BATCH_MAX_BYTES",
        "MILLRACE_POLL_TIMEOUT_MS",
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
            "MILLRACE_CONSUME_BATCH_SIZE": "11",
            "MILLRACE_CONSUME_BATCH_MAX_BYTES": "4096",
            "MILLRACE_POLL_TIMEOUT_MS": "42",
        }
    )
    assert cfg.target_output_bytes == 1024
    assert cfg.flush_deadline_s == 5
    assert cfg.slow_lane_deadline_s == 43200
    assert cfg.min_flush_bytes == 32
    assert cfg.max_files_per_commit == 3
    assert cfg.consume_batch_size == 11
    assert cfg.consume_batch_max_bytes == 4096
    assert cfg.poll_timeout_ms == 42


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


# -- remaining knobs ----------------------------------------------------------------------


def test_event_time_policy():
    assert load(MINIMAL_ENV).event_time_policy is EventTimePolicy.DROP
    assert (
        load(MINIMAL_ENV | {"MILLRACE_EVENT_TIME_POLICY": "clamp"}).event_time_policy
        is EventTimePolicy.CLAMP
    )
    (p,) = problems(MINIMAL_ENV | {"MILLRACE_EVENT_TIME_POLICY": "quarantine"})
    assert "MILLRACE_EVENT_TIME_POLICY" in p and "clamp" in p and "drop" in p


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
