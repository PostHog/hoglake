# CostBench 2025 on Trino + Hoglake

This is the read-only, 1B-row, 43-query workload from
[CostBench 9455db6](https://github.com/ClickHouse/CostBench/tree/9455db619f29f5141b35f9efd93025867af1f56c).
Its profile name is `costbench-query-2025`; current upstream CostBench measures
a different workload. This suite is independent of the catalog regression
scenarios and is excluded from `hoglake-bench all`.

The public loader, runner and deployment have no dependency on PostHog's private
perftest harness. The supported deployment profile is **`nvme-9-v1` only**.
Commands save their artifacts locally. No benchmark results are included here,
and the runner does not publish them.

## Hardware and infrastructure

Supply Kubernetes infrastructure and S3 access through your normal infrastructure
management process. The script does not create EC2 instances, IAM roles, buckets,
namespaces, ServiceAccounts or node pools.

| Role | Hosts | Instance | Pod CPU / memory | JVM heap |
|---|---:|---|---|---|
| Trino workers | 9 | `r7gd.8xlarge` | `30666m` / `240Gi` each | `192G` each |
| Trino coordinator | 1 | `r7g.4xlarge` | `12` / `96Gi` | `72G` |
| Driver, Hoglake, PostgreSQL | 1 shared host | `m7g.2xlarge` | 2 CPU / 8 GiB; 0.5 CPU / 2 GiB; 0.25 CPU / 1 GiB | image defaults |

All hosts use ARM64 and one availability zone. Steady compute totals 312 physical
vCPUs including support. Allow additional quota and pool capacity for replacement
nodes; Kubernetes requests and provider quotas both matter. The support host needs
at least 36 GiB of allocatable ephemeral storage for the 14.8 GB seed and artifacts
(an 80–100 GiB root volume leaves space for images and the OS).

Each worker has a **160 GiB filesystem cache**, 1 MiB pages and seven-day TTL,
inside a 176 GiB `emptyDir` (176 GiB ephemeral request, 200 GiB limit). Configure
the worker hosts to stripe their **two EC2 instance-store NVMe devices in RAID0**
and put kubelet/container ephemeral storage on that array. Merely choosing an
instance with NVMe is insufficient. `cluster verify` checks the actual mount's
block devices, RAID level and instance-store model on every worker; EBS fails.
The coordinator has no filesystem cache. Worker query memory is 96 GB per node,
coordinator query memory 36 GB, and the cluster cap is 1728 GB. The configured
cluster cap is a ceiling; the per-node limits also apply.

Provision a dedicated namespace (for example, `costbench-<suffix>`) and an existing
ServiceAccount usable by Hoglake, Trino and the driver. Its S3 identity must work
inside all three images and permit reading, listing, writing and deleting objects
under **your benchmark prefix**. IRSA or EKS Pod Identity can supply credentials;
keep associations and IAM in your infrastructure repository. Trino's image is
ARM64-only. The default Trino and Hoglake images are public and digest-pinned;
PostgreSQL 18 is also pinned. You can override these
three image digests, but disclose that version change in any comparison.

The namespace must support Kubernetes NetworkPolicy enforcement. Only fixture pods
can connect to its services; there is no external ingress. Trino uses HTTPS and a
generated password. Hoglake HTTP/auth-off and disposable PostgreSQL are intended
for this isolated benchmark fixture. Supply `kubectl` and OpenSSL on the machine
running `cluster`; the benchmark image does not need either.

## Build and configure

From a checkout containing this benchmark, install from the root lock:

```sh
cd bench
flox activate -- uv sync --locked
flox activate -- uv run hoglake-bench costbench --help
```

Alternatively, use Python 3.12+ and `uv sync --locked` without Flox. The published
`ghcr.io/posthog/hoglake-bench` image is built by the existing benchmark workflow.
Use an image built from a commit containing these commands and pin its **digest**.
Before that commit is published, build the same image from the repository root
and push it to your own registry:

```sh
docker buildx build --platform linux/arm64 -f bench/Dockerfile \
  --tag '<your-registry>/hoglake-bench:<source-commit>' --push .
docker buildx imagetools inspect '<your-registry>/hoglake-bench:<source-commit>'
```

From `bench/`, copy `deploy/costbench.example.json` to `costbench.private.json` and
set your context, namespace, ServiceAccount, S3 prefix, zone and benchmark image
digest. Add node selectors and tolerations for your pools. The fixed ARM64,
instance-type and zone selectors cannot be overridden. Keep this file and rendered
manifests private: they contain deployment-specific identifiers. Replace the
`hourly_usd` placeholder with your numeric compute rate for all eleven hosts.
Include whole hosts rather than only the CPU requested by each pod. The runner
does not look up prices automatically.

Review the resources locally before deploying:

```sh
uv run hoglake-bench costbench cluster render --config costbench.private.json \
  > /tmp/costbench-manifest.private.json
kubectl --context '<kubernetes-context>' apply --dry-run=server \
  -f /tmp/costbench-manifest.private.json
```

## Deploy, load, measure and collect

Run these commands from `bench/` (inside Flox if you use it):

```sh
uv run hoglake-bench costbench cluster deploy --config costbench.private.json
uv run hoglake-bench costbench cluster verify --config costbench.private.json
uv run hoglake-bench costbench cluster load --config costbench.private.json
uv run hoglake-bench costbench cluster run --config costbench.private.json \
  --run-id run-1 --artifacts costbench-artifacts/run-1
uv run hoglake-bench costbench cluster collect --config costbench.private.json \
  --run-id run-1 --artifacts costbench-artifacts/run-1
uv run hoglake-bench costbench compare \
  --input costbench-artifacts/run-1/report-client.json \
  --output costbench-artifacts/run-1/comparison.json \
  --markdown costbench-artifacts/run-1/comparison.md
```

Deployment prewarms nine distinct worker hosts with small pods, waits until
Kubernetes reports their real allocatable capacity, then removes **all** prewarm
pods before scheduling the full worker shape. This handles conservative initial
memory estimates without changing worker requests. It then checks device backing
and coordinator discovery of exactly nine workers.

Loading downloads and verifies the complete 14,779,976,446-byte public
[ClickBench Parquet seed](https://datasets.clickhouse.com/hits_compatible/hits.parquet)
(SHA-256 `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`).
The loader converts the pinned 105-column schema to upstream Trino signed integer
widths and UTC timestamps. It appends 99,997,497 rows, then doubles using Trino
`INSERT SELECT` and a final unordered `LIMIT` to reach exactly 1B rows. This matches
the historical expansion script; that final subset and file layout can vary.
There are no explicit partition or sort keys. Loading waits for file statistics
and validates counts and aggregate ranges before queries start. It is outside
the measured query time. Allow for loading time and cost separately.

For an interrupted load, `cluster load --resume --config costbench.private.json`
uses the checkpoint inside the **same driver pod**. It refuses unknown write
outcomes rather than risking duplicate rows. Checkpoints contain S3 paths and
table identities and are never collected as public results. If that pod is lost,
inspect the dataset before reconciling or starting a fresh load with a new prefix.

To reuse a completed dataset, leave the fixture running and execute another
`cluster run` with a **new** `--run-id` and artifact directory. The runner refuses
an existing remote run directory. Before each full run it replaces worker and
coordinator pods, removing local cache state from loading or earlier measurements.
It then runs all 43 queries serially, three consecutive attempts per query. There
are no extra reads of `hits`, hidden retries or cache resets between attempts.
The system-metadata readiness query does not read `hits`. Filesystem caching stays
enabled and query-result reuse stays disabled. Worker cache and S3 counters are
collected outside timing; changing/restarting workers or missing cache reads
invalidates that verification.

Raw attempts are collected even if query execution aborts. Unconfirmed cancellation
stops before the next attempt, avoiding overlapping queries. A run with failed or
missing attempts does not receive a complete score. Collection retrieves only the
dataset manifest, public metadata, raw attempts and client/server reports; it does
not copy the downloaded seed, credentials or private load checkpoint.

Collect results **before** cleanup:

```sh
uv run hoglake-bench costbench cluster cleanup --config costbench.private.json
```

Cleanup deletes only fixture-labelled Deployments, Services, ConfigMaps, Secrets
and NetworkPolicies in the configured namespace. It leaves infrastructure and S3
data intact. PostgreSQL metadata and driver scratch are disposable `emptyDir`s;
cleanup removes them, so this fixture cannot reuse its catalog after teardown.
Set an S3 lifecycle or remove your dedicated prefix through your normal tooling
when you no longer need its objects. Compute charges end when your infrastructure
scales down the empty nodes, not when the last query finishes. This runner does
not enforce a dollar budget.

## Use an existing deployment

The independent `load`, `run`, `report` and `compare` commands also work against
your own Trino + Hoglake deployment, without the Kubernetes adapter:

```sh
uv run hoglake-bench costbench load \
  --hoglake-url http://hoglake.example.test:8080 --hoglake-catalog costbench \
  --data-path 's3://<benchmark-bucket>/costbench/<run-id>/data/' \
  --trino-url https://trino.example.test:8443 --catalog benchmark \
  --password-env BENCHMARK_PASSWORD --ca-file /path/to/ca.crt \
  --seed-file /scratch/hits.parquet --state /scratch/load-state.private.json \
  --manifest /scratch/load.json
uv run hoglake-bench costbench run \
  --trino-url https://trino.example.test:8443 --catalog benchmark \
  --password-env BENCHMARK_PASSWORD --ca-file /path/to/ca.crt \
  --metadata /scratch/public-metadata.json --hourly-usd '<hourly-compute-usd>' \
  --output /scratch/attempts.jsonl
uv run hoglake-bench costbench report --input /scratch/attempts.jsonl \
  --output /scratch/report-client.json
uv run hoglake-bench costbench report --input /scratch/attempts.jsonl \
  --timing server --output /scratch/report-server.json
```

Supply version, worker/cache/layout and price information describing your own
deployment. The allowed metadata fields are defined in
[`public_metadata`](src/hoglake_bench/costbench/runner.py). Set `dataset_rows` from
the validated load manifest and record that manifest's SHA-256 as
`loader_manifest_sha256`. A run must declare one billion rows to enter the
warehouse ranking. `--queries 1,2` runs a partial smoke; it cannot earn a full score.
For a data smoke use `load --target-rows 1000 --table hits_smoke` with separate state
and manifest paths. **Even that smoke downloads/hashes the full seed** if it is not
already cached. Keep smoke activity separate from the main table's timed run.

## Metrics, comparison and licensing

The default client clock starts at submission and ends after draining all result
pages through one persistent HTTP client. The server clock is terminal
`elapsedTimeMillis`, not the sum of scheduled task time. Retain both. Historical
ClickHouse records suppress server output, so the client boundaries differ.

The legacy headline is `T = sum(min(three runtimes))` over 43 queries. Fixed-stack
compute cost is `T / 3600 × hourly_usd`; CostBench's score is runtime × compute cost.
Published records select runtime and compute-cost minima independently. The
separate `all_attempts_runtime_s` and `all_attempts_compute_usd` fields total all
**129 actual executions**. `mean_43_query_compute_usd` divides that total cost by
three; it is not the best-of-three headline. Both bases appear in comparison
tables. The totals exclude orchestration gaps, setup, loading, idle time, request
charges, storage, billing minimums and taxes. Our host-rate model is infrastructure
compute; the references are historical **2025 customer compute prices**. Neither
table is a full production cost estimate.

Storage is separate: use the loaded dataset's byte count and your storage rate.
NVMe is bundled with worker EC2; root EBS and any retained storage are additional.
Worker S3 counters cover the entire timed 129-execution workload, while
coordinator/catalog/setup requests are additional. Do not add the full workload's
request charges to a 43-query best-of-three score.

Comparison fetches eight explicitly named billing variants from seven immutable
CostBench records, verifies checksums and retains dates, tiers and sources.
Incomplete or unavailable entries remain visible without a rank. These are dated
configurations, not identical hardware, a controlled contemporary experiment, or
all-S3 systems. Engine-native sorting/indexing and managed-service storage differ.
This flat workload does not test shredded VARIANT performance.

The Trino SQL comes from pinned
[ClickBench b348345e](https://github.com/ClickHouse/ClickBench/tree/b348345eb251fb7ebe611d403d6089bcdcc700d4/trino-datalake-partitioned),
with checksum and attribution in every run. SQL and reference inputs are downloaded
separately; they are not vendored into this MIT-licensed code. The upstream input's
[CC BY-NC-SA 4.0 license](https://github.com/ClickHouse/ClickBench/blob/b348345eb251fb7ebe611d403d6089bcdcc700d4/LICENSE)
still applies. Review artifacts before publishing: allowlisted field names alone
cannot make arbitrary user-supplied metadata safe. Use public commits/image digests
and omit internal endpoints, buckets, account IDs and credentials.
