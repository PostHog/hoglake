from __future__ import annotations

import base64
import copy
import hashlib
import json
import subprocess
import time
from argparse import Namespace
from types import SimpleNamespace

import pytest

from hoglake_bench.costbench import cluster as cluster_module
from hoglake_bench.costbench.cluster import (
    Cluster,
    cache_delta,
    cache_metrics,
    cluster_main,
    nvme_proof,
)
from hoglake_bench.costbench.deployment import (
    LABEL,
    PROFILE,
    SELECTOR,
    WORKERS,
    metadata,
    resources,
    validate_config,
)


def config():
    return {
        "context": "example-context",
        "namespace": "costbench-example",
        "zone": "us-east-1a",
        "data_path": "s3://example-bucket/costbench/example/",
        "service_account": "costbench",
        "bench_image": "example.com/hoglake-bench@sha256:" + "a" * 64,
        "hourly_usd": 1.0,
    }


def items():
    return {
        (item["kind"], item["metadata"]["name"]): item for item in resources(config())
    }


def test_nine_worker_profile_enforces_cpu_memory_heap_cache_and_placement():
    objects = items()
    assert objects["NetworkPolicy", "costbench"]["apiVersion"] == "networking.k8s.io/v1"
    worker = objects["Deployment", "costbench-worker"]
    pod = worker["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert WORKERS == 9
    assert PROFILE["worker_count"] == 9
    assert pod["nodeSelector"] == {
        "kubernetes.io/arch": "arm64",
        "topology.kubernetes.io/zone": "us-east-1a",
        "node.kubernetes.io/instance-type": "r7gd.8xlarge",
    }
    assert container["resources"] == {
        "requests": {"cpu": "30666m", "memory": "240Gi", "ephemeral-storage": "176Gi"},
        "limits": {"cpu": "30666m", "memory": "240Gi", "ephemeral-storage": "200Gi"},
    }
    assert (
        pod["affinity"]["podAntiAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ][0]["topologyKey"]
        == "kubernetes.io/hostname"
    )
    assert {v["name"]: v for v in pod["volumes"]}["cache"]["emptyDir"] == {
        "sizeLimit": "176Gi"
    }
    data = objects["ConfigMap", "costbench-worker"]["data"]
    assert "-Xmx192G\n" in data["jvm.config"]
    assert "query.max-memory-per-node=96GB\n" in data["config.properties"]
    assert "query.max-memory=1728GB\n" in data["config.properties"]
    assert "task.max-worker-threads" not in data["config.properties"]
    assert "fs.cache.enabled=true\n" in data["benchmark.properties"]
    assert (
        "fs.cache.max-sizes=160GB\nfs.cache.page-size=1MB\nfs.cache.ttl=7d\n"
        in data["cache-manager-alluxio.properties"]
    )
    assert "s3.max-connections=500\n" in data["benchmark.properties"]
    assert objects["Deployment", "costbench-prewarm"]["spec"]["replicas"] == 9
    assert worker["spec"]["replicas"] == 0


def test_coordinator_has_no_cache_and_support_services_share_a_separate_host():
    objects = items()
    coordinator = objects["Deployment", "costbench-coordinator"]["spec"]["template"][
        "spec"
    ]
    assert (
        coordinator["nodeSelector"]["node.kubernetes.io/instance-type"] == "r7g.4xlarge"
    )
    assert coordinator["containers"][0]["resources"]["requests"] == {
        "cpu": "12",
        "memory": "96Gi",
    }
    assert not any(v["name"] == "cache" for v in coordinator["volumes"])
    data = objects["ConfigMap", "costbench-coordinator"]["data"]
    assert "fs.cache.enabled=false\n" in data["benchmark.properties"]
    assert "cache-manager.config-files" not in data["config.properties"]
    assert "-Xmx72G\n" in data["jvm.config"]
    assert "node-scheduler.include-coordinator=false\n" in data["config.properties"]
    assert "http-server.authentication.type=password\n" in data["config.properties"]
    assert objects["Service", "costbench-trino"]["spec"]["selector"] == {
        "app": "costbench-coordinator"
    }
    for name in ("costbench-postgres", "costbench-hoglake", "costbench-driver"):
        pod = objects["Deployment", name]["spec"]["template"]["spec"]
        assert pod["nodeSelector"]["node.kubernetes.io/instance-type"] == "m7g.2xlarge"
        if name != "costbench-postgres":
            assert pod["affinity"]["podAffinity"][
                "requiredDuringSchedulingIgnoredDuringExecution"
            ][0]["labelSelector"]["matchLabels"] == {"app": "costbench-postgres"}
    driver = objects["Deployment", "costbench-driver"]["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert driver["resources"]["requests"]["ephemeral-storage"] == "36Gi"


def test_manifest_only_contains_namespaced_fixture_resources_and_keeps_hydration_enabled():
    objects = items()
    assert all(
        obj["metadata"]["namespace"] == "costbench-example"
        and obj["metadata"]["labels"] == LABEL
        for obj in objects.values()
    )
    assert {kind for kind, _ in objects} == {
        "Deployment",
        "Service",
        "ConfigMap",
        "NetworkPolicy",
    }
    assert objects["NetworkPolicy", "costbench"]["spec"]["ingress"] == [
        {"from": [{"podSelector": {"matchLabels": LABEL}}]}
    ]
    env = {
        entry["name"]: entry.get("value")
        for entry in objects["Deployment", "costbench-hoglake"]["spec"]["template"][
            "spec"
        ]["containers"][0]["env"]
    }
    assert env["HOGLAKE_HYDRATOR_INTERVAL_MS"] == "10000"
    assert env["HOGLAKE_COMPACTION_INTERVAL_MS"] == "0"
    assert env["HOGLAKE_CLEANUP_INTERVAL_MS"] == "0"
    details = metadata(config())
    assert details["worker_count"] == 9
    assert (
        "context" not in details
        and "data_path" not in details
        and "namespace" not in details
    )


@pytest.mark.parametrize(
    "change",
    [
        {"namespace": "kube-system"},
        {"context": ""},
        {"service_account": "invalid/name"},
        {"region": "invalid", "zone": "invalida"},
        {"worker_node_selector": {"example.com/pool": 1}},
        {"zone": "eu-west-1a"},
        {"worker_count": 6},
        {"worker_node_selector": {"node.kubernetes.io/instance-type": "r7gd.12xlarge"}},
        {"support_node_selector": {"topology.kubernetes.io/zone": "us-east-1b"}},
        {"bench_image": "example.com/bench:latest"},
        {"hourly_usd": float("nan")},
        {"hourly_usd": True},
        {"data_path": "s3://example-bucket/"},
        {"data_path": "s3://example-bucket/prefix/?token=secret"},
    ],
)
def test_invalid_or_different_hardware_configuration_is_refused(change):
    with pytest.raises(ValueError):
        validate_config({**config(), **change})


def test_operator_can_supply_pool_selectors_and_tolerations_without_changing_the_shape():
    value = config()
    value["worker_node_selector"] = {"example.com/pool": "nvme"}
    value["tolerations"] = [
        {
            "key": "example.com/pool",
            "operator": "Equal",
            "value": "benchmark",
            "effect": "NoSchedule",
        }
    ]
    worker = next(
        item
        for item in resources(value)
        if item["metadata"]["name"] == "costbench-worker"
        and item["kind"] == "Deployment"
    )
    pod = worker["spec"]["template"]["spec"]
    assert pod["nodeSelector"]["example.com/pool"] == "nvme"
    assert pod["tolerations"] == value["tolerations"]


@pytest.mark.parametrize(
    "proof",
    [
        "raid=raid0\nmodel=Amazon Elastic Block Store\nmodel=Amazon Elastic Block Store\n",
        "raid=raid1\nmodel=Amazon EC2 NVMe Instance Storage\nmodel=Amazon EC2 NVMe Instance Storage\n",
        "raid=raid0\nmodel=Amazon EC2 NVMe Instance Storage\n",
    ],
)
def test_nvme_probe_rejects_ebs_wrong_raid_and_missing_devices(proof):
    with pytest.raises(ValueError, match="NVMe instance-store"):
        nvme_proof(proof)


def test_nvme_proof_only_contains_storage_facts():
    proof = nvme_proof(
        "raid=raid0\nmodel=Amazon EC2 NVMe Instance Storage\nmodel=Amazon EC2 NVMe Instance Storage\n"
    )
    assert proof == {
        "raid_level": "raid0",
        "device_count": 2,
        "device_models": ["Amazon EC2 NVMe Instance Storage"] * 2,
    }


def test_cache_and_s3_counters_use_bytes_and_total_counts_without_metric_labels():
    text = (
        'JMX_AlluxioCacheStats_name_benchmark_CacheReads_AllTime_sum{host="example"} 4096\n'
        "JMX_AlluxioCacheStats_name_benchmark_ExternalReads_AllTime_Total 8192\n"
        "JMX_io_trino_filesystem_s3_NAME_S3FileSystemStats_ATTRIBUTE_GetObject_Calls_TotalCount 3\n"
        "JMX_io_trino_filesystem_s3_NAME_S3FileSystemStats_ATTRIBUTE_GetObject_Calls_OneMinute_Count 2.5"
    )
    assert cache_metrics(text) == {
        "initialized": True,
        "cache_read_bytes": 4096,
        "external_read_bytes": 8192,
        "s3_api_metrics": {"GetObject_Calls": 3},
    }
    assert "example" not in json.dumps(cache_metrics(text))
    with pytest.raises(ValueError, match="missing"):
        cache_metrics(text.splitlines()[0])
    with pytest.raises(ValueError, match="ambiguous"):
        cache_metrics(text + "\n" + text)


def test_lazy_cache_initialization_is_valid_and_counter_resets_or_worker_changes_fail():
    before = {f"worker-{i}": cache_metrics("") for i in range(9)}
    after = {
        key: {
            "initialized": True,
            "cache_read_bytes": 1024,
            "external_read_bytes": 2048,
            "s3_api_metrics": {"GetObject_Calls": 3},
        }
        for key in before
    }
    delta = cache_delta(before, after)
    assert delta["cache_read_bytes"] == 9216
    assert delta["nodes"][0]["s3_api_metrics"]["GetObject_Calls"] == 3
    assert "worker-" not in json.dumps(delta)
    with pytest.raises(ValueError, match="changed"):
        cache_delta(before, {**after, "replacement": after["worker-0"]})
    with pytest.raises(ValueError, match="reset"):
        cache_delta(after, before)
    no_hits = copy.deepcopy(after)
    for node in no_hits.values():
        node["cache_read_bytes"] = 0
    with pytest.raises(ValueError, match="no filesystem-cache reads"):
        cache_delta(before, no_hits)


def test_tls_and_password_secrets_are_created_without_secrets_in_command_arguments(
    monkeypatch,
):
    cluster = Cluster(config())
    applied = []
    monkeypatch.setattr(cluster, "apply", applied.extend)
    actual = subprocess.run
    commands = []
    passphrases = []

    def run(command, **kwargs):
        commands.append(command)
        if "pkcs12" in command:
            assert command[command.index("-passout") + 1] == "stdin"
            passphrases.append(kwargs["input"])
        return actual(command, **kwargs)

    monkeypatch.setattr(cluster_module.subprocess, "run", run)
    cluster.auth()
    assert [item["metadata"]["name"] for item in applied] == [
        "costbench-auth",
        "costbench-tls",
    ]
    auth = {
        key: base64.b64decode(value).decode()
        for key, value in applied[0]["data"].items()
    }
    user, iterations, salt, digest = auth["password.db"].strip().split(":")
    assert passphrases == [auth["tls-password"] + "\n"]
    assert user == "benchmark"
    assert (
        hashlib.pbkdf2_hmac(
            "sha1",
            auth["password"].encode(),
            bytes.fromhex(salt),
            int(iterations),
            dklen=32,
        ).hex()
        == digest
    )
    assert all(
        value not in json.dumps(commands)
        for key, value in auth.items()
        if key != "password.db"
    )
    assert all(item["metadata"]["labels"] == LABEL for item in applied)


def test_deploy_waits_for_prewarm_pod_deletion_before_allocating_full_workers(
    monkeypatch,
):
    cluster = Cluster(config())
    events = []

    def kube(*args, **kwargs):
        events.append(args)
        if args[:2] == ("get", "pods"):
            return subprocess.CompletedProcess(
                args, 0, '{"items":[{"metadata":{"name":"prewarm-pod"}}]}', ""
            )
        return subprocess.CompletedProcess(args, 0, '{"items":[]}', "")

    monkeypatch.setattr(cluster, "kube", kube)
    monkeypatch.setattr(cluster, "auth", lambda: events.append(("auth",)))
    monkeypatch.setattr(cluster, "apply", lambda _: events.append(("apply",)))
    monkeypatch.setattr(cluster, "verify", lambda: events.append(("verify",)))
    cluster.deploy()
    deleted = next(
        i for i, event in enumerate(events) if event[:2] == ("wait", "--for=delete")
    )
    scaled = next(
        i
        for i, event in enumerate(events)
        if event[:2] == ("scale", "deployment/costbench-worker")
    )
    assert deleted < scaled
    assert events[scaled][-1] == "--replicas=9"
    assert events[-1] == ("verify",)


def test_cleanup_is_explicitly_scoped_and_does_not_delete_infrastructure(
    tmp_path, monkeypatch
):
    path = tmp_path / "private.json"
    path.write_text(json.dumps(config()))
    calls = []
    monkeypatch.setattr(
        Cluster, "kube", lambda self, *args, **kwargs: calls.append(args)
    )
    assert cluster_main(Namespace(config=path, action="cleanup", run_id="run-1")) == 0
    assert calls == [
        (
            "delete",
            "deployment,service,configmap,secret,networkpolicy",
            "-l",
            SELECTOR,
            "--wait=true",
            "--timeout=5m",
        )
    ]


def test_worker_validation_rejects_partial_or_colocated_fleets(monkeypatch):
    cluster = Cluster(config())
    pods = [
        {
            "metadata": {"name": f"worker-{i}"},
            "spec": {"nodeName": f"node-{i}"},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
        for i in range(9)
    ]
    monkeypatch.setattr(
        cluster,
        "kube",
        lambda *a, **k: subprocess.CompletedProcess(
            [], 0, json.dumps({"items": pods}), ""
        ),
    )
    assert len(cluster.workers()) == 9
    pods[0]["spec"]["nodeName"] = "node-1"
    with pytest.raises(ValueError, match="distinct"):
        cluster.workers()
    pods[0]["spec"]["nodeName"] = "node-0"
    pods[0]["status"]["conditions"][0]["status"] = "False"
    with pytest.raises(ValueError, match="ready"):
        cluster.workers()


def test_run_refuses_to_mix_local_artifacts_before_touching_the_cluster(
    tmp_path, monkeypatch
):
    cluster = Cluster(config())
    (tmp_path / "results.jsonl").write_text("existing run\n")
    monkeypatch.setattr(
        cluster, "driver", lambda *a, **kw: pytest.fail("cluster was changed")
    )
    with pytest.raises(ValueError, match="new artifact directory"):
        cluster.run("run-2", tmp_path)
    assert (tmp_path / "results.jsonl").read_text() == "existing run\n"


@pytest.mark.parametrize("fail", [False, True])
def test_run_preserves_attempts_without_retries_and_resets_caches_before_timing(
    tmp_path, monkeypatch, fail
):
    cluster = Cluster(config())
    files = {
        "/work/load.json": json.dumps(
            {"rows": 1_000_000_000, "source": {"sha256": "a" * 64}}
        )
    }
    events = []

    def driver(*args, data=None, **kwargs):
        events.append(args)
        if args[0] == "cat":
            return files[args[1]]
        if args[0] == "cp":
            files[args[2]] = files[args[1]]
        if args[:2] == ("sh", "-c"):
            files[args[-1]] = data
        return ""

    def kube(*args, **kwargs):
        events.append(args)
        if args[0] == "exec" and "sh" in args:
            return subprocess.CompletedProcess(
                [], 0 if args[-1] in files else 4, files.get(args[-1], ""), ""
            )
        return subprocess.CompletedProcess([], 0, "", "")

    def bench(*args):
        events.append(("benchmark", *args))
        files["/work/run-1/results.jsonl"] = '{"record_type":"attempt"}\n'
        if fail:
            raise RuntimeError("unconfirmed query cancellation")

    snapshots = iter(
        [
            {f"worker-{i}": cache_metrics("") for i in range(9)},
            {
                f"worker-{i}": {
                    "initialized": True,
                    "cache_read_bytes": 1,
                    "external_read_bytes": 2,
                    "s3_api_metrics": {},
                }
                for i in range(9)
            },
        ]
    )
    monkeypatch.setattr(cluster, "driver", driver)
    monkeypatch.setattr(cluster, "kube", kube)
    monkeypatch.setattr(cluster, "verify", lambda: {"workers": 9})
    monkeypatch.setattr(cluster, "snapshot", lambda: next(snapshots))
    monkeypatch.setattr(cluster, "bench", bench)
    artifacts = tmp_path / "artifacts"
    if fail:
        with pytest.raises(RuntimeError, match="unconfirmed"):
            cluster.run("run-1", artifacts)
    else:
        cluster.run("run-1", artifacts)
        assert (
            json.loads((artifacts / "cache-counters.json").read_text())[
                "cache_read_bytes"
            ]
            == 9
        )
    assert (artifacts / "results.jsonl").read_text() == files[
        "/work/run-1/results.jsonl"
    ]
    measured = [event for event in events if event[:2] == ("benchmark", "run")]
    assert len(measured) == 1
    assert next(
        i
        for i, e in enumerate(events)
        if e[:3] == ("rollout", "restart", "deployment/costbench-worker")
    ) < events.index(measured[0])


def test_tolerations_must_be_a_list():
    with pytest.raises(TypeError, match="toleration list"):
        validate_config({**config(), "tolerations": {}})


def test_collection_does_not_mistake_cluster_errors_for_missing_artifacts(
    tmp_path, monkeypatch
):
    cluster = Cluster(config())
    monkeypatch.setattr(
        cluster,
        "kube",
        lambda *a, **kw: subprocess.CompletedProcess(
            [], 1, "", "synthetic connection failure"
        ),
    )
    with pytest.raises(subprocess.CalledProcessError):
        cluster.collect("run-1", tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("value", ["-1", "NaN", "Infinity"])
def test_cache_counters_refuse_invalid_byte_values(value):
    text = f"JMX_AlluxioCacheStats_name_benchmark_CacheReads_AllTime_sum {value}\nJMX_AlluxioCacheStats_name_benchmark_ExternalReads_AllTime_sum 12\n"
    with pytest.raises(ValueError, match="invalid cache or S3 counter"):
        cache_metrics(text)


@pytest.mark.parametrize("problem", ["missing", "reset"])
def test_s3_counter_coverage_and_monotonicity_are_required(problem):
    before = {
        f"worker-{i}": {
            "initialized": True,
            "cache_read_bytes": 1,
            "external_read_bytes": 2,
            "s3_api_metrics": {"GetObject_Calls": 3},
        }
        for i in range(9)
    }
    after = copy.deepcopy(before)
    for node in after.values():
        node["cache_read_bytes"] += 1
        node["s3_api_metrics"] = {} if problem == "missing" else {"GetObject_Calls": 2}
    with pytest.raises(ValueError, match="S3 counter"):
        cache_delta(before, after)


def test_deploy_refuses_an_existing_fixture_before_replacing_secrets(monkeypatch):
    cluster = Cluster(config())
    monkeypatch.setattr(
        cluster,
        "kube",
        lambda *a, **kw: subprocess.CompletedProcess([], 0, '{"items":[{}]}', ""),
    )
    monkeypatch.setattr(
        cluster, "auth", lambda: pytest.fail("replaced live credentials")
    )
    with pytest.raises(ValueError, match="fixture already exists"):
        cluster.deploy()


def test_smoke_dataset_is_refused_before_worker_restart(tmp_path, monkeypatch):
    cluster = Cluster(config())
    monkeypatch.setattr(
        cluster,
        "driver",
        lambda *a, **kw: json.dumps({"rows": 1000, "source": {"sha256": "a" * 64}}),
    )
    monkeypatch.setattr(
        cluster,
        "kube",
        lambda *a, **kw: pytest.fail("started measurement on smoke data"),
    )
    with pytest.raises(ValueError, match="1B-row"):
        cluster.run("run-1", tmp_path / "artifacts")


def test_invalid_run_id_is_refused_before_cleanup(tmp_path, monkeypatch):
    path = tmp_path / "private.json"
    path.write_text(json.dumps(config()))
    monkeypatch.setattr(
        Cluster,
        "kube",
        lambda *a, **kw: pytest.fail("touched cluster with invalid run ID"),
    )
    with pytest.raises(ValueError, match="run ID"):
        cluster_main(Namespace(config=path, action="cleanup", run_id="../other-run"))


@pytest.mark.parametrize("discovered", [False, True])
def test_discovery_uses_a_short_query_deadline_and_a_bounded_overall_wait(
    monkeypatch, discovered
):
    cluster = Cluster(config())
    now = [0]
    calls = []

    class Client:
        def __init__(self, *args, **kwargs):
            assert kwargs["timeout_s"] == 10

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, **kwargs):
            calls.append(sql)
            assert len(calls) <= 120, "discovery exceeded its deadline"
            assert "system.runtime.nodes" in sql
            return SimpleNamespace(rows=[[9 if discovered and len(calls) >= 3 else 8]])

    monkeypatch.setattr(cluster_module, "TrinoClient", Client)
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds)
    )
    monkeypatch.setenv("COSTBENCH_TRINO_URL", cluster.trino_url)
    monkeypatch.setenv("COSTBENCH_PASSWORD", "example-test-password")
    monkeypatch.setattr(
        cluster,
        "workers",
        lambda: [{"metadata": {"name": f"worker-{i}"}} for i in range(9)],
    )
    proof = "raid=raid0\nmodel=Amazon EC2 NVMe Instance Storage\nmodel=Amazon EC2 NVMe Instance Storage\n"
    monkeypatch.setattr(cluster, "kube", lambda *a, **kw: SimpleNamespace(stdout=proof))

    def driver(*args):
        assert (
            args[-1]
            == "from hoglake_bench.costbench.cluster import wait_for_workers; wait_for_workers()"
        )
        cluster_module.wait_for_workers()

    monkeypatch.setattr(cluster, "driver", driver)
    if discovered:
        assert cluster.verify()["workers"] == 9
        assert len(calls) == 3
    else:
        with pytest.raises(RuntimeError, match="discover exactly nine"):
            cluster.verify()
        assert now[0] == 600 and len(calls) == 120
