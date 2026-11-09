"""Portable Kubernetes resources for a fixed nine-worker NVMe shape.

Infrastructure, S3 permissions, the namespace and ServiceAccount belong to the
operator. This module creates no cloud resources and contains no private adapter.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

LABEL = {"app.kubernetes.io/part-of": "hoglake-costbench"}
SELECTOR = "app.kubernetes.io/part-of=hoglake-costbench"
WORKERS = 9
TRINO_IMAGE = "ghcr.io/posthog/trino@sha256:1362d5a95e092fb13f7ff86cdeee03c0a6f72a3689c9ac85a675f6662b40343f"
HOGLAKE_IMAGE = "ghcr.io/posthog/hoglake-server@sha256:afc4f60de67a0e239bf97aca2cc9af34c349240cce059e7683a7b49f73af0588"
POSTGRES_IMAGE = "postgres:18@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"

PROFILE = {
    "hardware_profile": "nvme-9-v1",
    "worker_count": WORKERS,
    "architecture": "arm64",
    "worker_instance_type": "r7gd.8xlarge",
    "worker_cpu_limit": "30666m",
    "worker_memory_limit": "240Gi",
    "worker_heap": "192G",
    "worker_query_memory_per_node": "96GB",
    "coordinator_instance_type": "r7g.4xlarge",
    "coordinator_cpu_limit": "12",
    "coordinator_memory_limit": "96Gi",
    "coordinator_heap": "72G",
    "coordinator_query_memory_per_node": "36GB",
    "cluster_query_memory": "1728GB",
    "control_instance_type": "m7g.2xlarge",
    "filesystem_cache": True,
    "filesystem_cache_capacity": "160GiB per worker",
    "filesystem_cache_page_size": "1MiB",
    "cache_storage": "EC2 NVMe instance-store RAID0",
    "initial_cache_state": "fresh worker pods before the full query run",
}


def read_config(path: Path) -> dict[str, Any]:
    config = json.loads(path.read_text())
    validate_config(config)
    return config


def validate_config(config: dict[str, Any]) -> None:
    allowed = {
        "context",
        "namespace",
        "region",
        "zone",
        "data_path",
        "service_account",
        "trino_image",
        "hoglake_image",
        "postgres_image",
        "bench_image",
        "hourly_usd",
        "worker_node_selector",
        "coordinator_node_selector",
        "support_node_selector",
        "tolerations",
    }
    if not isinstance(config, dict) or config.keys() - allowed:
        raise ValueError("configuration contains unknown fields")
    for key in (
        "context",
        "namespace",
        "zone",
        "data_path",
        "bench_image",
        "service_account",
    ):
        if (
            not isinstance(config.get(key), str)
            or not config[key]
            or "<" in config[key]
        ):
            raise ValueError(f"set {key} in the private deployment configuration")
    if not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", config["namespace"]
    ) or config["namespace"] in {
        "default",
        "kube-system",
        "kube-public",
        "kube-node-lease",
    }:
        raise ValueError("use a dedicated benchmark namespace with a valid DNS label")
    if not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", config["service_account"]
    ):
        raise ValueError("invalid ServiceAccount name")
    region = config.get("region", "us-east-1")
    if not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[0-9]+", region):
        raise ValueError("invalid AWS region")
    if not re.fullmatch(re.escape(region) + r"[a-z]", config["zone"]):
        raise ValueError("zone must belong to the selected region")
    for key, default in (
        ("trino_image", TRINO_IMAGE),
        ("hoglake_image", HOGLAKE_IMAGE),
        ("postgres_image", POSTGRES_IMAGE),
        ("bench_image", ""),
    ):
        if not re.fullmatch(
            r"[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}", config.get(key, default)
        ):
            raise ValueError(f"{key} must be pinned by digest")
    data = urlsplit(config["data_path"])
    if (
        data.scheme != "s3"
        or not data.netloc
        or not data.path.strip("/")
        or data.username
        or data.password
        or data.query
        or data.fragment
        or any(c.isspace() for c in config["data_path"])
    ):
        raise ValueError("data_path must be s3://bucket/dedicated-prefix/")
    rate = config.get("hourly_usd")
    if (
        isinstance(rate, bool)
        or not isinstance(rate, (int, float))
        or not math.isfinite(rate)
        or rate < 0
    ):
        raise ValueError(
            "hourly_usd must be a finite, nonnegative fixed-stack compute rate"
        )
    for role in ("worker", "coordinator", "support"):
        selector = config.get(f"{role}_node_selector", {})
        if not isinstance(selector, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in selector.items()
        ):
            raise ValueError("node selectors must map strings to strings")
        fixed = node_selector(config, role)
        if any(
            key in selector and selector[key] != value for key, value in fixed.items()
        ):
            raise ValueError(
                "node selectors cannot override the nine-worker hardware profile"
            )
    if not isinstance(config.get("tolerations", []), list):
        raise TypeError("tolerations must be a Kubernetes toleration list")


def node_selector(config: dict[str, Any], role: str) -> dict[str, str]:
    instance = {
        "worker": "r7gd.8xlarge",
        "coordinator": "r7g.4xlarge",
        "support": "m7g.2xlarge",
    }[role]
    return {
        **config.get(f"{role}_node_selector", {}),
        "kubernetes.io/arch": "arm64",
        "topology.kubernetes.io/zone": config["zone"],
        "node.kubernetes.io/instance-type": instance,
    }


def metadata(config: dict[str, Any]) -> dict[str, Any]:
    return {
        **PROFILE,
        "region": config.get("region", "us-east-1"),
        "availability_zone": config["zone"],
        "trino_image": config.get("trino_image", TRINO_IMAGE).rsplit("@", 1)[-1],
        "hoglake_image": config.get("hoglake_image", HOGLAKE_IMAGE).rsplit("@", 1)[-1],
        "runner_image": config["bench_image"].rsplit("@", 1)[-1],
        "layout": "unpartitioned Parquet; no explicit sorting",
    }


def resources(config: dict[str, Any]) -> list[dict[str, Any]]:
    validate_config(config)
    namespace = config["namespace"]
    region = config.get("region", "us-east-1")
    result: list[dict[str, Any]] = []

    def add(kind: str, name: str, **body: Any) -> dict[str, Any]:
        version = "apps/v1" if kind == "Deployment" else "v1"
        if kind == "NetworkPolicy":
            version = "networking.k8s.io/v1"
        item = {
            "apiVersion": version,
            "kind": kind,
            "metadata": {"name": name, "namespace": namespace, "labels": LABEL.copy()},
            **body,
        }
        result.append(item)
        return item

    def service(name: str, ports: list[int]) -> None:
        add(
            "Service",
            name,
            spec={
                "selector": {"app": name},
                "ports": [
                    {"name": f"port-{port}", "port": port, "targetPort": port}
                    for port in ports
                ],
            },
        )

    def deployment(
        name: str,
        container: dict[str, Any],
        *,
        role: str = "support",
        replicas: int = 1,
        volumes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        pod = {
            "serviceAccountName": config["service_account"],
            "nodeSelector": node_selector(config, role),
            "tolerations": config.get("tolerations", []),
            "securityContext": {"fsGroup": 10001},
            "containers": [container],
            "volumes": volumes or [],
        }
        if role == "worker":
            pod["affinity"] = {
                "podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "labelSelector": {"matchLabels": {"app": name}},
                            "topologyKey": "kubernetes.io/hostname",
                        }
                    ]
                }
            }
        elif role == "support" and name != "costbench-postgres":
            pod["affinity"] = {
                "podAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "labelSelector": {
                                "matchLabels": {"app": "costbench-postgres"}
                            },
                            "topologyKey": "kubernetes.io/hostname",
                        }
                    ]
                }
            }
        return add(
            "Deployment",
            name,
            spec={
                "replicas": replicas,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {
                        "labels": {**LABEL, "app": name},
                        "annotations": {"karpenter.sh/do-not-disrupt": "true"},
                    },
                    "spec": pod,
                },
            },
        )

    def secret_env(name: str, key: str) -> dict[str, Any]:
        return {
            "name": name,
            "valueFrom": {"secretKeyRef": {"name": "costbench-auth", "key": key}},
        }

    def shape(cpu: str, memory: str) -> dict[str, Any]:
        return {
            "requests": {"cpu": cpu, "memory": memory},
            "limits": {"cpu": cpu, "memory": memory},
        }

    deployment(
        "costbench-postgres",
        {
            "name": "postgres",
            "image": config.get("postgres_image", POSTGRES_IMAGE),
            "env": [
                {"name": "POSTGRES_USER", "value": "hoglake"},
                {"name": "POSTGRES_DB", "value": "hoglake"},
                secret_env("POSTGRES_PASSWORD", "postgres-password"),
            ],
            "resources": shape("250m", "1Gi"),
            "volumeMounts": [{"name": "database", "mountPath": "/var/lib/postgresql"}],
            "readinessProbe": {
                "exec": {"command": ["pg_isready", "-U", "hoglake"]},
                "periodSeconds": 3,
            },
        },
        volumes=[{"name": "database", "emptyDir": {}}],
    )
    service("costbench-postgres", [5432])

    env = {
        "HOGLAKE_JDBC_URL": f"jdbc:postgresql://costbench-postgres.{namespace}.svc:5432/hoglake",
        "HOGLAKE_DB_USER": "hoglake",
        "HOGLAKE_S3_REGION": region,
        "HOGLAKE_S3_PATH_STYLE": "false",
        "HOGLAKE_AUTH_MODE": "off",
        "HOGLAKE_HYDRATOR_INTERVAL_MS": "10000",
        **{
            f"HOGLAKE_{loop}_INTERVAL_MS": "0"
            for loop in (
                "COMPACTION",
                "EXPIRY",
                "CLEANUP",
                "RETIREMENT",
                "REINDEX",
                "METRICS",
                "MAINTENANCE_SUMMARY",
            )
        },
    }
    deployment(
        "costbench-hoglake",
        {
            "name": "hoglake",
            "image": config.get("hoglake_image", HOGLAKE_IMAGE),
            "env": [{"name": k, "value": v} for k, v in env.items()]
            + [secret_env("HOGLAKE_DB_PASSWORD", "postgres-password")],
            "resources": shape("500m", "2Gi"),
            "startupProbe": {
                "httpGet": {"path": "/healthz", "port": 8080},
                "periodSeconds": 5,
                "failureThreshold": 120,
            },
            "readinessProbe": {
                "httpGet": {"path": "/healthz", "port": 8080},
                "periodSeconds": 5,
            },
        },
    )
    service("costbench-hoglake", [8080])

    for role in ("coordinator", "worker"):
        coordinator = role == "coordinator"
        name = f"costbench-{role}"
        properties = [
            f"coordinator={str(coordinator).lower()}",
            "http-server.http.port=8080",
            f"discovery.uri=http://costbench-trino.{namespace}.svc:8080",
            "query.max-memory=1728GB",
            f"query.max-memory-per-node={'36GB' if coordinator else '96GB'}",
            "catalog.management=static",
            "shutdown.grace-period=30s",
            "internal-communication.shared-secret=${ENV:TRINO_INTERNAL_SECRET}",
        ]
        if coordinator:
            properties += [
                "node-scheduler.include-coordinator=false",
                "http-server.https.enabled=true",
                "http-server.https.port=8443",
                "http-server.https.keystore.path=/tls/keystore.p12",
                "http-server.https.keystore.key=${ENV:TRINO_TLS_PASSWORD}",
                "http-server.authentication.type=password",
                "password-authenticator.config-files=etc/password-authenticator.properties",
            ]
        else:
            properties += [
                "cache-manager.config-files=etc/cache-manager-alluxio.properties,etc/cache-manager-memory.properties",
                "metrics.jmx-object-names=io.trino.blob.cache.alluxio:type=AlluxioCacheStats,name=benchmark|io.trino.filesystem.s3:*",
            ]
        data = {
            "node.properties": "node.environment=costbench\nnode.data-dir=/data/trino\n",
            "jvm.config": f"-server\n-Xmx{'72G' if coordinator else '192G'}\n-XX:+UseG1GC\n-XX:+ExitOnOutOfMemoryError\n-Dfile.encoding=UTF-8\n",
            "config.properties": "\n".join(properties) + "\n",
            "benchmark.properties": "\n".join(
                [
                    "connector.name=hoglake",
                    f"hoglake.uri=http://costbench-hoglake.{namespace}.svc:8080",
                    "hoglake.catalog=costbench",
                    "fs.s3.enabled=true",
                    f"s3.region={region}",
                    "s3.max-connections=500",
                    f"fs.cache.enabled={str(not coordinator).lower()}",
                    "",
                ]
            ),
        }
        if coordinator:
            data["password-authenticator.properties"] = (
                "password-authenticator.name=file\nfile.password-file=/auth/password.db\n"
            )
        else:
            data["cache-manager-alluxio.properties"] = (
                "cache-manager.name=alluxio\nfs.cache.directories=/cache/trino\nfs.cache.max-sizes=160GB\nfs.cache.page-size=1MB\nfs.cache.ttl=7d\n"
            )
            data["cache-manager-memory.properties"] = "cache-manager.name=memory\n"
        add("ConfigMap", name, data=data)
        volumes = [
            {
                "name": "config",
                "configMap": {
                    "name": name,
                    "items": [
                        {
                            "key": k,
                            "path": f"catalog/{k}"
                            if k == "benchmark.properties"
                            else k,
                        }
                        for k in data
                    ],
                },
            },
            {"name": "data", "emptyDir": {}},
        ]
        mounts = [
            {"name": "config", "mountPath": "/etc/trino"},
            {"name": "data", "mountPath": "/data/trino"},
        ]
        resources_ = shape(
            "12" if coordinator else "30666m", "96Gi" if coordinator else "240Gi"
        )
        if coordinator:
            volumes += [
                {"name": "tls", "secret": {"secretName": "costbench-tls"}},
                {
                    "name": "auth",
                    "secret": {
                        "secretName": "costbench-auth",
                        "items": [{"key": "password.db", "path": "password.db"}],
                    },
                },
            ]
            mounts += [
                {"name": "tls", "mountPath": "/tls", "readOnly": True},
                {"name": "auth", "mountPath": "/auth", "readOnly": True},
            ]
        else:
            volumes.append({"name": "cache", "emptyDir": {"sizeLimit": "176Gi"}})
            mounts.append({"name": "cache", "mountPath": "/cache/trino"})
            resources_["requests"]["ephemeral-storage"] = "176Gi"
            resources_["limits"]["ephemeral-storage"] = "200Gi"
        deployment(
            name,
            {
                "name": "trino",
                "image": config.get("trino_image", TRINO_IMAGE),
                "env": [
                    secret_env("TRINO_INTERNAL_SECRET", "internal-secret"),
                    secret_env("TRINO_TLS_PASSWORD", "tls-password"),
                ],
                "resources": resources_,
                "volumeMounts": mounts,
                "startupProbe": {
                    "httpGet": {"path": "/v1/info", "port": 8080},
                    "periodSeconds": 5,
                    "failureThreshold": 120,
                },
                "readinessProbe": {
                    "httpGet": {"path": "/v1/info", "port": 8080},
                    "periodSeconds": 5,
                },
            },
            role=role,
            replicas=0,
            volumes=volumes,
        )
    service("costbench-trino", [8080, 8443])
    result[-1]["spec"]["selector"] = {"app": "costbench-coordinator"}

    deployment(
        "costbench-prewarm",
        {
            "name": "prewarm",
            "image": config.get("trino_image", TRINO_IMAGE),
            "command": ["sh", "-c", "sleep infinity"],
            "resources": {
                "requests": {
                    "cpu": "10m",
                    "memory": "64Mi",
                    "ephemeral-storage": "176Gi",
                },
                "limits": {
                    "cpu": "10m",
                    "memory": "64Mi",
                    "ephemeral-storage": "176Gi",
                },
            },
        },
        role="worker",
        replicas=WORKERS,
    )

    deployment(
        "costbench-driver",
        {
            "name": "driver",
            "image": config["bench_image"],
            "command": ["sleep", "infinity"],
            "env": [
                secret_env("COSTBENCH_PASSWORD", "password"),
                {"name": "PYTHONUNBUFFERED", "value": "1"},
                {"name": "AWS_REGION", "value": region},
            ],
            "securityContext": {
                "runAsUser": 10001,
                "runAsGroup": 10001,
                "runAsNonRoot": True,
            },
            "resources": shape("2", "8Gi"),
            "volumeMounts": [
                {"name": "work", "mountPath": "/work"},
                {"name": "tls", "mountPath": "/tls", "readOnly": True},
            ],
        },
        volumes=[
            {"name": "work", "emptyDir": {"sizeLimit": "36Gi"}},
            {
                "name": "tls",
                "secret": {
                    "secretName": "costbench-tls",
                    "items": [{"key": "ca.crt", "path": "ca.crt"}],
                },
            },
        ],
    )
    driver_resources = result[-1]["spec"]["template"]["spec"]["containers"][0][
        "resources"
    ]
    driver_resources["requests"]["ephemeral-storage"] = "36Gi"
    driver_resources["limits"]["ephemeral-storage"] = "40Gi"

    # Catalog HTTP and worker metrics stay inside the isolated fixture. DNS/S3
    # egress is unchanged; the cluster must enforce Kubernetes NetworkPolicy.
    add(
        "NetworkPolicy",
        "costbench",
        spec={
            "podSelector": {"matchLabels": LABEL},
            "policyTypes": ["Ingress"],
            "ingress": [{"from": [{"podSelector": {"matchLabels": LABEL}}]}],
        },
    )
    return result
