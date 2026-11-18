"""Host-side kubectl adapter for the public benchmark fixture.

Run from a machine with kubectl/OpenSSL and an explicitly selected context.
The benchmark image itself needs neither kubectl nor cloud provisioning tools.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import secrets
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from .client import TrinoClient
from .deployment import LABEL, SELECTOR, WORKERS, metadata, read_config, resources
from .runner import public_metadata

NVME_PROBE = r"""
set -eu
device=''
while read -r mount_id parent_id major_minor root mountpoint rest; do
    if [ "$mountpoint" = /cache/trino ]; then device="$major_minor"; break; fi
done < /proc/self/mountinfo
[ -n "$device" ]
printf 'raid=%s\n' "$(cat /sys/dev/block/"$device"/md/level)"
for file in /sys/dev/block/"$device"/slaves/*/device/model; do
    [ -r "$file" ]
    printf 'model=%s\n' "$(cat "$file")"
done
"""


def nvme_proof(text: str) -> dict[str, Any]:
    models = [
        line.removeprefix("model=").strip()
        for line in text.splitlines()
        if line.startswith("model=")
    ]
    if (
        "raid=raid0" not in text.splitlines()
        or len(models) != 2
        or any(model != "Amazon EC2 NVMe Instance Storage" for model in models)
    ):
        raise ValueError(
            "cache mount must use RAID0 over both EC2 NVMe instance-store devices"
        )
    return {"raid_level": "raid0", "device_count": len(models), "device_models": models}


def cache_metrics(text: str) -> dict[str, Any]:
    values: dict[str, float] = {}
    s3: dict[str, float] = {}
    seen = False
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        name = fields[0].split("{", 1)[0]
        cache = "AlluxioCacheStats" in name and "name_benchmark_" in name.lower()
        if cache:
            seen = True
        key = next(
            (
                key
                for attribute, key in (
                    ("CacheReads", "cache_read_bytes"),
                    ("ExternalReads", "external_read_bytes"),
                )
                if cache
                and name.endswith(
                    (f"_{attribute}_AllTime_sum", f"_{attribute}_AllTime_Total")
                )
            ),
            None,
        )
        sdk = re.search(
            r"S3FileSystemStats_ATTRIBUTE_([A-Za-z]+)_(Calls|Retries)_TotalCount$", name
        )
        if key is None and sdk is None:
            continue
        value = float(fields[1])
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid cache or S3 counter")
        target = values if key else s3
        target_key = key or f"{sdk[1]}_{sdk[2]}"
        if target_key in target:
            raise ValueError("ambiguous cache or S3 counter")
        target[target_key] = value
    if seen and set(values) != {"cache_read_bytes", "external_read_bytes"}:
        raise ValueError(
            "worker is missing the expected filesystem cache byte counters"
        )
    return {
        "initialized": seen,
        "cache_read_bytes": values.get("cache_read_bytes", 0),
        "external_read_bytes": values.get("external_read_bytes", 0),
        "s3_api_metrics": s3,
    }


def cache_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    if set(before) != set(after) or len(before) != WORKERS:
        raise ValueError("worker pods changed during measurement")
    nodes = []
    for pod in sorted(before):
        first, last = before[pod], after[pod]
        node = {
            key: last[key] - first[key]
            for key in ("cache_read_bytes", "external_read_bytes")
        }
        if not last["initialized"] or any(value < 0 for value in node.values()):
            raise ValueError("cache counters are missing or reset during measurement")
        if set(first["s3_api_metrics"]) - set(last["s3_api_metrics"]):
            raise ValueError("S3 counter coverage changed during measurement")
        node["s3_api_metrics"] = {
            key: value - first["s3_api_metrics"].get(key, 0)
            for key, value in last["s3_api_metrics"].items()
        }
        if any(value < 0 for value in node["s3_api_metrics"].values()):
            raise ValueError("S3 counters reset during measurement")
        nodes.append(node)
    hits = sum(node["cache_read_bytes"] for node in nodes)
    if hits <= 0:
        raise ValueError(
            "no filesystem-cache reads were observed during the timed queries"
        )
    return {
        "workers": WORKERS,
        "nodes": nodes,
        "cache_read_bytes": hits,
        "external_read_bytes": sum(node["external_read_bytes"] for node in nodes),
        "scope": "worker counters during all 129 query executions; collection occurs outside query timing",
    }


def wait_for_workers() -> None:
    """Driver-side metadata preflight; neither the table nor its cache is read."""
    deadline = time.monotonic() + 600
    with TrinoClient(
        os.environ["COSTBENCH_TRINO_URL"],
        password=os.environ["COSTBENCH_PASSWORD"],
        user="benchmark",
        catalog="benchmark",
        ca_file=Path("/tls/ca.crt"),
        timeout_s=10,
    ) as client:
        while time.monotonic() < deadline:
            result = client.execute(
                "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active' AND coordinator = false",
                collect_rows=True,
            )
            if result.rows == [[WORKERS]]:
                return
            time.sleep(5)
    raise RuntimeError("coordinator did not discover exactly nine workers")


class Cluster:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.kubectl = [
            "kubectl",
            "--context",
            config["context"],
            "--namespace",
            config["namespace"],
        ]

    def kube(
        self,
        *args: str,
        data: str | None = None,
        capture: bool = True,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        request_timeout = (
            "0" if args[0] in {"exec", "wait", "rollout", "delete"} else "60s"
        )
        return subprocess.run(
            [*self.kubectl, f"--request-timeout={request_timeout}", *args],
            input=data,
            text=True,
            capture_output=capture,
            check=check,
        )

    def apply(self, items: list[dict[str, Any]]) -> None:
        self.kube(
            "apply",
            "-f",
            "-",
            data=json.dumps({"apiVersion": "v1", "kind": "List", "items": items}),
        )

    def driver(self, *args: str, data: str | None = None, capture: bool = True) -> str:
        return (
            self.kube(
                "exec",
                "-i",
                "deployment/costbench-driver",
                "--",
                *args,
                data=data,
                capture=capture,
            ).stdout
            or ""
        )

    def bench(self, *args: str) -> None:
        self.driver("hoglake-bench", "costbench", *args, capture=False)

    def wait(self, name: str) -> None:
        self.kube(
            "rollout", "status", f"deployment/{name}", "--timeout=15m", capture=False
        )

    def auth(self) -> None:
        """Generate short-lived fixture secrets; no values appear in argv or logs."""
        with tempfile.TemporaryDirectory(prefix="costbench-auth-") as scratch:
            root = Path(scratch)
            password = secrets.token_hex(32)
            tls_password = secrets.token_hex(32)
            namespace = self.config["namespace"]
            # One temporary self-signed trust anchor, mounted only into this
            # fixture. SANs cover the in-cluster endpoint and local inspection.
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "7",
                    "-subj",
                    "/CN=costbench",
                    "-addext",
                    f"subjectAltName=DNS:costbench-trino.{namespace}.svc,DNS:localhost",
                    "-keyout",
                    str(root / "server.key"),
                    "-out",
                    str(root / "ca.crt"),
                ],
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "openssl",
                    "pkcs12",
                    "-export",
                    "-name",
                    "costbench",
                    "-inkey",
                    str(root / "server.key"),
                    "-in",
                    str(root / "ca.crt"),
                    "-passout",
                    "stdin",
                    "-out",
                    str(root / "keystore.p12"),
                ],
                input=tls_password + "\n",
                text=True,
                check=True,
                capture_output=True,
            )
            salt = secrets.token_bytes(16)
            digest = hashlib.pbkdf2_hmac(
                "sha1", password.encode(), salt, 100_000, dklen=32
            )
            auth = {
                "password": password,
                "password.db": f"benchmark:100000:{salt.hex()}:{digest.hex()}\n",
                "tls-password": tls_password,
                "postgres-password": secrets.token_hex(32),
                "internal-secret": secrets.token_hex(32),
            }
            items = []
            for name, data in (
                ("costbench-auth", {k: v.encode() for k, v in auth.items()}),
                (
                    "costbench-tls",
                    {
                        key: (root / key).read_bytes()
                        for key in ("ca.crt", "keystore.p12")
                    },
                ),
            ):
                items.append(
                    {
                        "apiVersion": "v1",
                        "kind": "Secret",
                        "type": "Opaque",
                        "metadata": {
                            "name": name,
                            "namespace": namespace,
                            "labels": LABEL.copy(),
                        },
                        "data": {
                            key: base64.b64encode(value).decode()
                            for key, value in data.items()
                        },
                    }
                )
            self.apply(items)

    def deploy(self) -> None:
        self.kube("get", "serviceaccount", self.config["service_account"])
        existing = json.loads(
            self.kube(
                "get",
                "deployment,secret,configmap,service,networkpolicy",
                "-l",
                SELECTOR,
                "-o",
                "json",
            ).stdout
        )
        if existing["items"]:
            raise ValueError(
                "fixture already exists; use verify/load/run or collect and cleanup before redeploying"
            )
        self.auth()
        self.apply(resources(self.config))
        for name in (
            "costbench-postgres",
            "costbench-hoglake",
            "costbench-driver",
            "costbench-prewarm",
        ):
            self.wait(name)
        # Delete the owner before listing remaining pods, so it cannot replace
        # them. Wait by name only when pods remain; an empty selector is done.
        self.kube(
            "delete",
            "deployment",
            "costbench-prewarm",
            "--cascade=foreground",
            "--wait=true",
            "--timeout=2m",
        )
        pending = json.loads(
            self.kube("get", "pods", "-l", "app=costbench-prewarm", "-o", "json").stdout
        )["items"]
        if pending:
            self.kube(
                "wait",
                "--for=delete",
                *[f"pod/{pod['metadata']['name']}" for pod in pending],
                "--timeout=2m",
            )
        for role, count in (("coordinator", 1), ("worker", WORKERS)):
            self.kube("scale", f"deployment/costbench-{role}", f"--replicas={count}")
            self.wait(f"costbench-{role}")
        self.verify()

    def workers(self) -> list[dict[str, Any]]:
        pods = json.loads(
            self.kube("get", "pods", "-l", "app=costbench-worker", "-o", "json").stdout
        )["items"]
        active = [pod for pod in pods if not pod["metadata"].get("deletionTimestamp")]
        if (
            len(active) != WORKERS
            or len({pod["spec"].get("nodeName") for pod in active}) != WORKERS
        ):
            raise ValueError("expected nine workers on nine distinct nodes")
        for pod in active:
            conditions = pod.get("status", {}).get("conditions", [])
            if not any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in conditions
            ):
                raise ValueError("all nine workers must be ready")
        return sorted(active, key=lambda pod: pod["metadata"]["name"])

    def verify(self) -> dict[str, Any]:
        nodes = []
        for pod in self.workers():
            proof = self.kube(
                "exec", pod["metadata"]["name"], "--", "sh", "-c", NVME_PROBE
            ).stdout
            nodes.append(nvme_proof(proof))
        # Pod readiness alone does not prove coordinator discovery. This query
        # touches only system metadata, never the benchmark table.
        script = "from hoglake_bench.costbench.cluster import wait_for_workers; wait_for_workers()"
        self.driver(
            "env", f"COSTBENCH_TRINO_URL={self.trino_url}", "python", "-c", script
        )
        return {"workers": WORKERS, "nodes": nodes}

    @property
    def trino_url(self) -> str:
        return f"https://costbench-trino.{self.config['namespace']}.svc:8443"

    def connection_args(self) -> list[str]:
        return [
            "--trino-url",
            self.trino_url,
            "--catalog",
            "benchmark",
            "--user",
            "benchmark",
            "--password-env",
            "COSTBENCH_PASSWORD",
            "--ca-file",
            "/tls/ca.crt",
        ]

    def snapshot(self) -> dict[str, Any]:
        return {
            pod["metadata"]["name"]
            + "/"
            + pod["metadata"]["uid"]
            + "/"
            + str(
                [
                    c.get("restartCount", 0)
                    for c in pod["status"].get("containerStatuses", [])
                ]
            ): cache_metrics(
                self.kube(
                    "exec",
                    pod["metadata"]["name"],
                    "--",
                    "curl",
                    "--fail",
                    "--silent",
                    "--show-error",
                    "--max-time",
                    "30",
                    "-H",
                    "X-Trino-User:benchmark",
                    "http://localhost:8080/metrics",
                ).stdout
            )
            for pod in self.workers()
        }

    def run(self, run_id: str, artifacts: Path) -> None:
        remote = f"/work/{run_id}"
        if (artifacts / "results.jsonl").exists():
            raise ValueError("use a new artifact directory for each run")
        # An existing run directory may contain an interrupted query. Never
        # restart services or overwrite it as an implicit retry.
        self.driver("mkdir", remote)
        artifacts.mkdir(parents=True, exist_ok=True)
        dataset_bytes = self.driver("cat", "/work/load.json")
        dataset = json.loads(dataset_bytes)
        if dataset.get("rows") != 1_000_000_000:
            raise ValueError(
                "the warehouse comparison requires the validated 1B-row load manifest"
            )
        details = public_metadata(
            {
                **metadata(self.config),
                "dataset_rows": dataset["rows"],
                "dataset_sha256": dataset["source"]["sha256"],
                "loader_manifest_sha256": hashlib.sha256(
                    dataset_bytes.encode()
                ).hexdigest(),
            }
        )
        self.driver(
            "sh",
            "-c",
            'cat > "$1"',
            "sh",
            f"{remote}/metadata.json",
            data=json.dumps(details),
        )
        self.driver("cp", "/work/load.json", f"{remote}/load.json")
        # Fresh pod emptyDirs remove cache effects from loading/validation.
        # Caches then remain enabled throughout all 43 × 3 executions.
        for name in ("costbench-worker", "costbench-coordinator"):
            self.kube("rollout", "restart", f"deployment/{name}")
            self.wait(name)
        proof = self.verify()
        (artifacts / "nvme-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
        before = self.snapshot()
        try:
            self.bench(
                "run",
                *self.connection_args(),
                "--hourly-usd",
                str(self.config["hourly_usd"]),
                "--metadata",
                f"{remote}/metadata.json",
                "--output",
                f"{remote}/results.jsonl",
            )
        finally:
            # Preserve raw attempts even on failure; never hide an aborted run
            # behind missing report/metric files or automatically rerun queries.
            self.collect(run_id, artifacts)
        after = self.snapshot()
        # Stable synthetic positions preserve counter provenance without
        # publishing Kubernetes names or UIDs. Validate identity before mapping.
        delta = cache_delta(before, after)
        counters = {
            stage: {
                f"worker-{i}": snapshot[pod] for i, pod in enumerate(sorted(before), 1)
            }
            for stage, snapshot in (("before", before), ("after", after))
        }
        (artifacts / "cache-metrics.json").write_text(
            json.dumps(counters, indent=2) + "\n"
        )
        (artifacts / "cache-counters.json").write_text(
            json.dumps(delta, indent=2) + "\n"
        )
        for timing in ("client", "server"):
            self.bench(
                "report",
                "--input",
                f"{remote}/results.jsonl",
                "--timing",
                timing,
                "--output",
                f"{remote}/report-{timing}.json",
            )
        self.collect(run_id, artifacts)

    def collect(self, run_id: str, artifacts: Path) -> None:
        artifacts.mkdir(parents=True, exist_ok=True)
        for name in (
            "load.json",
            "metadata.json",
            "results.jsonl",
            "report-client.json",
            "report-server.json",
        ):
            path = f"/work/{run_id}/{name}"
            result = self.kube(
                "exec",
                "deployment/costbench-driver",
                "--",
                "sh",
                "-c",
                'if [ -f "$1" ]; then cat "$1"; else exit 4; fi',
                "sh",
                path,
                check=False,
            )
            if result.returncode == 4:
                continue
            result.check_returncode()
            (artifacts / name).write_text(result.stdout)


def add_cluster_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "action",
        choices=("render", "deploy", "verify", "load", "run", "collect", "cleanup"),
    )
    parser.add_argument(
        "--config", type=Path, required=True, help="private runtime JSON configuration"
    )
    parser.add_argument("--artifacts", type=Path, default=Path("costbench-artifacts"))
    parser.add_argument(
        "--run-id",
        default="run-1",
        help="new identifier for each measurement; never retried implicitly",
    )
    parser.add_argument(
        "--resume", action="store_true", help="resume a known completed load checkpoint"
    )


def cluster_main(args: argparse.Namespace) -> int:
    config = read_config(args.config)
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", args.run_id):
        raise ValueError("invalid run ID")
    cluster = Cluster(config)
    if args.action == "render":
        print(
            json.dumps(
                {"apiVersion": "v1", "kind": "List", "items": resources(config)},
                indent=2,
            )
        )
    elif args.action == "deploy":
        cluster.deploy()
    elif args.action == "verify":
        args.artifacts.mkdir(parents=True, exist_ok=True)
        (args.artifacts / "nvme-proof.json").write_text(
            json.dumps(cluster.verify(), indent=2) + "\n"
        )
    elif args.action == "load":
        cluster.verify()
        cluster.bench(
            "load",
            *cluster.connection_args(),
            "--hoglake-url",
            f"http://costbench-hoglake.{config['namespace']}.svc:8080",
            "--data-path",
            config["data_path"],
            "--region",
            config.get("region", "us-east-1"),
            "--seed-file",
            "/work/hits.parquet",
            "--state",
            "/work/load-state.private.json",
            "--manifest",
            "/work/load.json",
            *(["--resume"] if args.resume else []),
        )
        args.artifacts.mkdir(parents=True, exist_ok=True)
        (args.artifacts / "load.json").write_text(
            cluster.driver("cat", "/work/load.json")
        )
    elif args.action == "run":
        cluster.run(args.run_id, args.artifacts)
    elif args.action == "collect":
        cluster.collect(args.run_id, args.artifacts)
    else:
        # Never delete namespaces, ServiceAccounts, buckets, nodes or PVCs.
        cluster.kube(
            "delete",
            "deployment,service,configmap,secret,networkpolicy",
            "-l",
            SELECTOR,
            "--wait=true",
            "--timeout=5m",
            capture=False,
        )
    return 0
