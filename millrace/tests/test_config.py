"""Config validation: every knob parses, defaults, and refuses with a
message naming the knob and the problem — and ALL problems are reported
in one refusal, not one per restart."""

from __future__ import annotations

import pytest

from millrace.config import (
    AutoOffsetReset,
    Config,
    ConfigError,
    PartitionFieldSpec,
    SortFieldSpec,
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
    assert cfg.flush_bytes == 256 * 1024 * 1024
    assert cfg.flush_deadline_s == 900
    assert cfg.slow_lane_deadline_s == 21600
    assert cfg.metrics_port == 8000
    assert cfg.kafka_auto_offset_reset is AutoOffsetReset.LATEST
    assert cfg.kafka_consumer_overrides == ()
    # Constants (no environment variable).
    assert cfg.min_flush_bytes == 1024 * 1024
    assert cfg.max_files_per_commit == 512
    assert cfg.flush_concurrency == 4
    assert cfg.flush_sweep_s == 1.0
    assert cfg.consume_batch_size == 65536
    assert cfg.consume_batch_max_bytes == 256 * 1024 * 1024
    assert cfg.poll_timeout_ms == 500


def test_planner_knobs_projection():
    cfg = load(
        MINIMAL_ENV
        | {
            "MILLRACE_FLUSH_BYTES": "4096",
            "MILLRACE_FLUSH_DEADLINE_S": "60",
            "MILLRACE_SLOW_LANE_DEADLINE_S": "25200",
        }
    )
    knobs = cfg.planner_knobs()
    assert knobs.flush_bytes == 4096
    assert knobs.flush_deadline_s == 60
    assert knobs.slow_lane_deadline_s == 25200
    assert knobs.min_flush_bytes == 1024 * 1024


#: Knobs of earlier designs: no longer read, and not refused either.
RETIRED_KNOBS = {
    "MILLRACE_TARGET_OUTPUT_BYTES": "abc",
    "MILLRACE_TARGET_FILE_BYTES": "1",
    "MILLRACE_REAP_DEADLINE_S": "1",
    "MILLRACE_MIN_FLUSH_BYTES": "0",
    "MILLRACE_MAX_FILES_PER_COMMIT": "-1",
    "MILLRACE_EVENT_TIME_POLICY": "quarantine",
    "MILLRACE_EVENT_TIME_MAX_PAST_S": "x",
    "MILLRACE_EVENT_TIME_MAX_FUTURE_S": "x",
    "MILLRACE_FLUSH_SWEEP_S": "0",
    "MILLRACE_FLUSH_SCAN_MIN_BYTES": "0",
    "MILLRACE_FLUSH_SCAN_MAX_ROWS": "0",
    "MILLRACE_COMPRESSION_RATIO_HALFLIFE": "0",
    "MILLRACE_FLUSH_CONCURRENCY": "0",
    "MILLRACE_CONSUME_BATCH_SIZE": "0",
    "MILLRACE_CONSUME_BATCH_MAX_BYTES": "0",
    "MILLRACE_POLL_TIMEOUT_MS": "0",
    "MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES": "0",
    "MILLRACE_KAFKA_FETCH_MAX_BYTES": "0",
    "MILLRACE_KAFKA_QUEUED_MAX_MESSAGES_KBYTES": "0",
    "MILLRACE_SLATEDB_GC_ENABLED": "maybe",
    "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES": "0",
    "MILLRACE_SLATEDB_L0_MAX_SSTS": "0",
    "MILLRACE_SLATEDB_BLOCK_CACHE_BYTES": "1",
    "MILLRACE_SLATEDB_META_CACHE_BYTES": "1",
}


def test_retired_knobs_are_simply_not_read():
    assert load(MINIMAL_ENV | RETIRED_KNOBS) == load(MINIMAL_ENV)


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
        "MILLRACE_FLUSH_BYTES",
        "MILLRACE_FLUSH_DEADLINE_S",
        "MILLRACE_SLOW_LANE_DEADLINE_S",
        "MILLRACE_REPLICA_COUNT",
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
            "MILLRACE_FLUSH_BYTES": "1024",
            "MILLRACE_FLUSH_DEADLINE_S": "5",
            "MILLRACE_SLOW_LANE_DEADLINE_S": "43200",
        }
    )
    assert cfg.flush_bytes == 1024
    assert cfg.flush_deadline_s == 5
    assert cfg.slow_lane_deadline_s == 43200


# -- SlateDB tuning (constants, validated) ---------------------------------------


def test_slatedb_defaults_match_the_documented_deployment():
    cfg = load(MINIMAL_ENV)
    assert cfg.slatedb.max_unflushed_bytes == 256 * 1024 * 1024
    assert cfg.slatedb.l0_max_ssts == 8
    assert cfg.slatedb.block_cache_bytes == 128 * 1024 * 1024
    assert cfg.slatedb.meta_cache_bytes == 128 * 1024 * 1024


def test_slatedb_config_validates_its_fields():
    from millrace.config import SlateDbConfig, validate_slatedb_cache_bytes

    with pytest.raises(ValueError, match="l0_sst_size_bytes"):
        SlateDbConfig(max_unflushed_bytes=64 * 1024 * 1024)
    with pytest.raises(ValueError, match="l0_max_ssts"):
        SlateDbConfig(l0_max_ssts=0)
    with pytest.raises(ValueError, match="both be 0"):
        SlateDbConfig(block_cache_bytes=0, meta_cache_bytes=4096)
    with pytest.raises(ValueError, match="meta_cache_bytes"):
        SlateDbConfig(block_cache_bytes=4096, meta_cache_bytes=-5)
    with pytest.raises(ValueError, match="below 4096"):
        SlateDbConfig(block_cache_bytes=1, meta_cache_bytes=4096)
    with pytest.raises(ValueError, match="2\\*\\*64"):
        SlateDbConfig(block_cache_bytes=2**64, meta_cache_bytes=4096)
    SlateDbConfig(block_cache_bytes=0, meta_cache_bytes=0)
    validate_slatedb_cache_bytes(0, 0)
    validate_slatedb_cache_bytes(4096, 2**64 - 1)


# -- remaining knobs ----------------------------------------------------------------------


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


# -- destination creation knobs (config.py §Destination creation) -----------------


def test_destination_knobs_default_to_unset():
    cfg = load(MINIMAL_ENV)
    assert cfg.catalog_data_path is None
    assert cfg.table_columns is None
    assert cfg.partition_by is None
    assert cfg.sort_by is None


class TestCatalogDataPath:
    def test_s3_url_is_kept_verbatim(self):
        cfg = load(
            MINIMAL_ENV
            | {"MILLRACE_CATALOG_DATA_PATH": "s3://posthog-lake/millrace/millrace-dev/"}
        )
        assert cfg.catalog_data_path == "s3://posthog-lake/millrace/millrace-dev/"

    @pytest.mark.parametrize(
        ("value", "needle"),
        [
            ("s3://lake-bkt", "prefix"),
            ("s3://Bad_Bucket/lake/", "bucket"),
            ("file:///var/lake", "s3://bucket/prefix"),
            ("gs://lake-bkt/lake/", "s3://bucket/prefix"),
        ],
    )
    def test_invalid_values_refused_naming_the_knob(self, value, needle):
        (p,) = problems(MINIMAL_ENV | {"MILLRACE_CATALOG_DATA_PATH": value})
        assert "MILLRACE_CATALOG_DATA_PATH" in p and needle in p

    def test_stage_bucket_equal_to_lake_bucket_is_refused(self):
        # The lake role's prefix-scoped grants would let SlateDB half-write
        # a staging root into the lake bucket; the two must differ.
        (p,) = problems(
            MINIMAL_ENV
            | {
                "MILLRACE_STAGE_URL": "s3://data-bucket/millrace",
                "MILLRACE_CATALOG_DATA_PATH": "s3://data-bucket/lake/",
            }
        )
        assert "MILLRACE_STAGE_URL" in p and "MILLRACE_CATALOG_DATA_PATH" in p
        assert "'data-bucket'" in p and "different buckets" in p

    def test_different_buckets_are_fine(self):
        cfg = load(
            MINIMAL_ENV
            | {
                "MILLRACE_STAGE_URL": "s3://stage-bucket/millrace",
                "MILLRACE_CATALOG_DATA_PATH": "s3://data-bucket/lake/",
            }
        )
        assert cfg.stage_store_url == "s3://stage-bucket"
        assert cfg.catalog_data_path == "s3://data-bucket/lake/"

    @pytest.mark.parametrize("stage", ["file:///var/lib/millrace", "memory:///"])
    def test_non_s3_stage_with_an_s3_data_path_is_fine(self, stage):
        cfg = load(
            MINIMAL_ENV
            | {
                "MILLRACE_STAGE_URL": stage,
                "MILLRACE_CATALOG_DATA_PATH": "s3://data-bucket/lake/",
            }
        )
        assert cfg.catalog_data_path == "s3://data-bucket/lake/"


class TestTableColumns:
    """MILLRACE_TABLE_COLUMNS: ``name:type,…`` with hoglake type names."""

    def test_typed_columns_parse_in_order_all_nullable(self):
        cfg = load(
            MINIMAL_ENV
            | {
                "MILLRACE_TABLE_COLUMNS": (
                    " team_id:long, timestamp:timestamptz,uuid:UUID, "
                    "amount:decimal(10,2), props:json ,ok:boolean,"
                )
            }
        )
        assert cfg.table_columns is not None
        assert [(c.name, c.col_type, c.params()) for c in cfg.table_columns] == [
            ("team_id", "long", None),
            ("timestamp", "timestamptz", None),
            ("uuid", "uuid", None),
            ("amount", "decimal", {"precision": 10, "scale": 2}),
            ("props", "json", None),
            ("ok", "boolean", None),
        ]
        # Every type maps through pyhoglake's coltype_to_arrow.
        import pyarrow as pa

        assert cfg.table_columns[3].arrow_type() == pa.decimal128(10, 2)

    @pytest.mark.parametrize(
        ("value", "needle"),
        [
            ("team_id", "name:type"),
            ("team_id:", "name:type"),
            (":long", "name:type"),
            ("team_id:bigint", "unknown hoglake column type 'bigint'"),
            ("amount:decimal", "precision and scale"),
            ("props:struct", "nested type 'struct'"),
            ("tags:list", "nested type 'list'"),
            ("m:map", "nested type 'map'"),
            ("v:variant", "variant"),
            ("team_id:long,team_id:string", "duplicated"),
            ("bad.col:long", "not a valid hoglake identifier"),
            ("_hog_row_id:long", "reserved prefix"),
            (",", "contains no entries"),
        ],
    )
    def test_refusals_name_the_knob(self, value, needle):
        found = problems(MINIMAL_ENV | {"MILLRACE_TABLE_COLUMNS": value})
        assert any("MILLRACE_TABLE_COLUMNS" in p and needle in p for p in found), found

    def test_every_bad_entry_is_reported_at_once(self):
        found = problems(
            MINIMAL_ENV | {"MILLRACE_TABLE_COLUMNS": "a:nope,b:struct,a:long,c:long"}
        )
        assert len(found) == 3


class TestPartitionBy:
    """MILLRACE_PARTITION_BY is millpond's HOGLAKE_PARTITION_BY grammar
    (millpond/config.py ``_parse_hoglake_partition_by``; the cases are
    its tests/unit/test_config.py::TestHoglakePartitionBy)."""

    def test_bare_column_is_identity(self):
        cfg = load(MINIMAL_ENV | {"MILLRACE_PARTITION_BY": "team_id"})
        assert cfg.partition_by == (PartitionFieldSpec("team_id", "identity", None),)

    def test_temporal_transforms(self):
        cfg = load(
            MINIMAL_ENV
            | {
                "MILLRACE_PARTITION_BY": (
                    "year(_inserted_at), month(_inserted_at), day(_inserted_at), "
                    "hour(_inserted_at)"
                )
            }
        )
        assert cfg.partition_by == (
            PartitionFieldSpec("_inserted_at", "year", None),
            PartitionFieldSpec("_inserted_at", "month", None),
            PartitionFieldSpec("_inserted_at", "day", None),
            PartitionFieldSpec("_inserted_at", "hour", None),
        )

    def test_bucket_with_param(self):
        cfg = load(MINIMAL_ENV | {"MILLRACE_PARTITION_BY": "bucket(team_id, 16)"})
        assert cfg.partition_by == (PartitionFieldSpec("team_id", "bucket", 16),)
        assert cfg.partition_by[0].describe() == "bucket(team_id, 16)"

    def test_mixed_expression(self):
        cfg = load(
            MINIMAL_ENV | {"MILLRACE_PARTITION_BY": "team_id,month(_inserted_at)"}
        )
        assert cfg.partition_by == (
            PartitionFieldSpec("team_id", "identity", None),
            PartitionFieldSpec("_inserted_at", "month", None),
        )

    def test_the_recommended_dev_shape(self):
        cfg = load(
            MINIMAL_ENV | {"MILLRACE_PARTITION_BY": "identity(team_id),day(timestamp)"}
        )
        assert cfg.partition_by == (
            PartitionFieldSpec("team_id", "identity", None),
            PartitionFieldSpec("timestamp", "day", None),
        )

    @pytest.mark.parametrize(
        "expr",
        [
            "truncate(ts)",  # client-side-only transform; server refuses it
            "void(ts)",
            "date_trunc(day, ts)",
        ],
    )
    def test_unknown_transform_refused_listing_vocabulary(self, expr):
        (p,) = problems(MINIMAL_ENV | {"MILLRACE_PARTITION_BY": expr})
        assert "MILLRACE_PARTITION_BY" in p
        assert "identity, year, month, day, hour, bucket" in p

    @pytest.mark.parametrize(
        "expr",
        [
            "year(",  # unbalanced
            "year()",  # no column
            "year(a,b)",  # arity
            "bucket(team_id)",  # bucket needs a param
            "bucket(team_id, 0)",  # param must be positive
            "bucket(team_id, x)",  # param must be an int
            "hour(bad.col)",  # unsafe column name
            "team_id,team_id",  # duplicated
            ",",  # empty entries only
        ],
    )
    def test_malformed_expression_refused(self, expr):
        found = problems(MINIMAL_ENV | {"MILLRACE_PARTITION_BY": expr})
        assert found and all("MILLRACE_PARTITION_BY" in p for p in found)

    def test_without_columns_it_is_only_an_agreement_check(self):
        # No MILLRACE_TABLE_COLUMNS: nothing to resolve the sources
        # against at boot; the live table is what they are compared to.
        cfg = load(
            MINIMAL_ENV
            | {"MILLRACE_PARTITION_BY": "team_id", "MILLRACE_SORT_BY": "timestamp"}
        )
        assert cfg.table_columns is None
        assert cfg.partition_by is not None
        assert cfg.sort_by == (SortFieldSpec("timestamp", "asc", "nulls_last"),)

    def test_source_must_be_a_configured_column(self):
        (p,) = problems(
            MINIMAL_ENV
            | {
                "MILLRACE_TABLE_COLUMNS": "team_id:long",
                "MILLRACE_PARTITION_BY": "day(timestamp)",
            }
        )
        assert "MILLRACE_PARTITION_BY day(timestamp)" in p
        assert "'timestamp'" in p and "MILLRACE_TABLE_COLUMNS" in p

    @pytest.mark.parametrize(
        ("columns", "expr", "needle"),
        [
            ("ts:string", "day(ts)", "day transform cannot be applied"),
            ("d:date", "hour(d)", "hour transform cannot be applied"),
            ("x:double", "bucket(x, 8)", "bucket cannot be applied"),
            ("j:json", "bucket(j, 8)", "bucket cannot be applied"),
        ],
    )
    def test_transform_must_be_valid_for_the_source_type(self, columns, expr, needle):
        (p,) = problems(
            MINIMAL_ENV
            | {"MILLRACE_TABLE_COLUMNS": columns, "MILLRACE_PARTITION_BY": expr}
        )
        assert "MILLRACE_PARTITION_BY" in p and needle in p

    def test_valid_transforms_against_configured_columns(self):
        cfg = load(
            MINIMAL_ENV
            | {
                "MILLRACE_TABLE_COLUMNS": "team_id:long,timestamp:timestamptz,u:uuid",
                "MILLRACE_PARTITION_BY": "team_id,day(timestamp),bucket(u, 4)",
                "MILLRACE_SORT_BY": "timestamp",
            }
        )
        assert cfg.partition_by is not None and len(cfg.partition_by) == 3


class TestSortBy:
    """MILLRACE_SORT_BY is a superset of millpond's MILLPOND_SORT_BY
    grammar (millpond/config.py ``_parse_column_list``; the bare-name
    cases are its tests/unit/test_config.py::TestSortByConfig and must
    parse to the order millpond declares, asc nulls_last): each entry
    ``col [asc|desc] [nulls_first|nulls_last]``."""

    def test_single_field(self):
        cfg = load(MINIMAL_ENV | {"MILLRACE_SORT_BY": "team_id"})
        assert cfg.sort_by == (SortFieldSpec("team_id", "asc", "nulls_last"),)

    def test_multi_field_order_preserved(self):
        cfg = load(MINIMAL_ENV | {"MILLRACE_SORT_BY": "team_id,timestamp,distinct_id"})
        assert cfg.sort_by == (
            SortFieldSpec("team_id"),
            SortFieldSpec("timestamp"),
            SortFieldSpec("distinct_id"),
        )

    def test_whitespace_around_fields_trimmed(self):
        cfg = load(MINIMAL_ENV | {"MILLRACE_SORT_BY": "  team_id ,  timestamp  "})
        assert cfg.sort_by == (SortFieldSpec("team_id"), SortFieldSpec("timestamp"))

    def test_empty_tokens_dropped(self):
        cfg = load(MINIMAL_ENV | {"MILLRACE_SORT_BY": "team_id,,timestamp,"})
        assert cfg.sort_by == (SortFieldSpec("team_id"), SortFieldSpec("timestamp"))

    def test_duplicates_deduplicated_first_wins(self):
        cfg = load(
            MINIMAL_ENV | {"MILLRACE_SORT_BY": "timestamp desc,team_id,timestamp"}
        )
        assert cfg.sort_by == (
            SortFieldSpec("timestamp", "desc", "nulls_last"),
            SortFieldSpec("team_id"),
        )

    def test_whitespace_only_value_yields_none(self):
        cfg = load(MINIMAL_ENV | {"MILLRACE_SORT_BY": "   "})
        assert cfg.sort_by is None

    def test_unsafe_field_name_rejected(self):
        found = problems(
            MINIMAL_ENV | {"MILLRACE_SORT_BY": "team_id, foo; DROP TABLE x"}
        )
        assert any(
            "MILLRACE_SORT_BY" in p and "not a valid hoglake identifier" in p
            for p in found
        )

    @pytest.mark.parametrize(
        ("entry", "direction", "null_order"),
        [
            ("timestamp", "asc", "nulls_last"),
            ("timestamp asc", "asc", "nulls_last"),
            ("timestamp desc", "desc", "nulls_last"),
            ("timestamp nulls_first", "asc", "nulls_first"),
            ("timestamp nulls_last", "asc", "nulls_last"),
            ("timestamp asc nulls_first", "asc", "nulls_first"),
            ("timestamp asc nulls_last", "asc", "nulls_last"),
            ("timestamp desc nulls_first", "desc", "nulls_first"),
            ("timestamp desc nulls_last", "desc", "nulls_last"),
            ("timestamp DESC NULLS FIRST", "desc", "nulls_first"),
            ("timestamp  desc   nulls last", "desc", "nulls_last"),
        ],
    )
    def test_direction_and_null_order_combinations(self, entry, direction, null_order):
        cfg = load(MINIMAL_ENV | {"MILLRACE_SORT_BY": entry})
        assert cfg.sort_by == (SortFieldSpec("timestamp", direction, null_order),)
        assert cfg.sort_by[0].describe() == f"timestamp {direction} {null_order}"

    @pytest.mark.parametrize(
        "entry",
        [
            "timestamp sideways",
            "timestamp nulls_first desc",  # order of modifiers is fixed
            "timestamp asc asc",
            "timestamp desc nulls",
        ],
    )
    def test_garbage_modifiers_refused(self, entry):
        (p,) = problems(MINIMAL_ENV | {"MILLRACE_SORT_BY": entry})
        assert (
            "MILLRACE_SORT_BY" in p and "col [asc|desc] [nulls_first|nulls_last]" in p
        )

    def test_column_must_be_configured_when_columns_are(self):
        (p,) = problems(
            MINIMAL_ENV
            | {
                "MILLRACE_TABLE_COLUMNS": "team_id:long",
                "MILLRACE_SORT_BY": "timestamp desc",
            }
        )
        assert "MILLRACE_SORT_BY column 'timestamp'" in p
        assert "MILLRACE_TABLE_COLUMNS" in p


# -- review additions: grammar edges ------------------------------------------------

_ID_128 = "c" * 128
_ID_129 = "c" * 129


class TestGrammarEdges:
    @pytest.mark.parametrize(
        ("expr", "expected"),
        [
            ("bucket(team_id,16)", (PartitionFieldSpec("team_id", "bucket", 16),)),
            ("day( timestamp )", (PartitionFieldSpec("timestamp", "day"),)),
            ("DAY(ts)", (PartitionFieldSpec("ts", "day"),)),
            ("team_id,", (PartitionFieldSpec("team_id", "identity"),)),
            ("team-id", (PartitionFieldSpec("team-id", "identity"),)),
            (f"day({_ID_128})", (PartitionFieldSpec(_ID_128, "day"),)),
        ],
    )
    def test_partition_by_accepts(self, expr, expected):
        assert (
            load(MINIMAL_ENV | {"MILLRACE_PARTITION_BY": expr}).partition_by == expected
        )

    @pytest.mark.parametrize(
        ("expr", "needle"),
        [
            ("a,identity(a)", "duplicated"),
            ("identity(a),a", "duplicated"),
            (f"day({_ID_129})", "column name"),
        ],
    )
    def test_partition_by_refuses(self, expr, needle):
        found = problems(MINIMAL_ENV | {"MILLRACE_PARTITION_BY": expr})
        assert found and all("MILLRACE_PARTITION_BY" in p for p in found)
        assert any(needle in p for p in found), found

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (
                "amount:decimal(10, 2)",
                [("amount", "decimal", {"precision": 10, "scale": 2})],
            ),
            (
                "amount:DECIMAL(10,2)",
                [("amount", "decimal", {"precision": 10, "scale": 2})],
            ),
            (
                "amount:decimal(38,38)",
                [("amount", "decimal", {"precision": 38, "scale": 38})],
            ),
            (
                "amount:decimal(1,0)",
                [("amount", "decimal", {"precision": 1, "scale": 0})],
            ),
            ("t:LONG", [("t", "long", None)]),
            ("team-id:long", [("team-id", "long", None)]),
            (f"{_ID_128}:long", [(_ID_128, "long", None)]),
            ("t:long,", [("t", "long", None)]),
        ],
    )
    def test_table_columns_accepts(self, value, expected):
        cfg = load(MINIMAL_ENV | {"MILLRACE_TABLE_COLUMNS": value})
        assert cfg.table_columns is not None
        assert [(c.name, c.col_type, c.params()) for c in cfg.table_columns] == expected

    @pytest.mark.parametrize(
        ("value", "needle"),
        [
            (f"{_ID_129}:long", "not a valid hoglake identifier"),
            ("amount:decimal(0,0)", "1 <= p <= 38"),
            ("amount:decimal(39,2)", "1 <= p <= 38"),
            ("amount:decimal(10,11)", "0 <= s <= p"),
            ("u:uint32", "'uint32' is stored as 'long' by the writer; configure long"),
        ],
    )
    def test_table_columns_refuses(self, value, needle):
        found = problems(MINIMAL_ENV | {"MILLRACE_TABLE_COLUMNS": value})
        assert len(found) == 1 and "MILLRACE_TABLE_COLUMNS" in found[0]
        assert needle in found[0], found

    def test_ddl_style_map_is_one_nested_type_refusal(self):
        found = problems(
            MINIMAL_ENV | {"MILLRACE_TABLE_COLUMNS": "m:map<string,string>,t:long"}
        )
        (p,) = found
        assert "nested type 'map<string,string>'" in p

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (f"{_ID_128}", (SortFieldSpec(_ID_128),)),
            ("team-id desc", (SortFieldSpec("team-id", "desc", "nulls_last"),)),
        ],
    )
    def test_sort_by_accepts(self, value, expected):
        assert load(MINIMAL_ENV | {"MILLRACE_SORT_BY": value}).sort_by == expected

    def test_sort_by_refuses_a_129_char_identifier(self):
        (p,) = problems(MINIMAL_ENV | {"MILLRACE_SORT_BY": _ID_129})
        assert "MILLRACE_SORT_BY" in p and "not a valid hoglake identifier" in p
