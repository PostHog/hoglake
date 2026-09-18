# hoglake-bench

Stress-test / benchmark harness for the [hoglake](../README.md) control
plane, driven through [pyhoglake](../pyhoglake/README.md). Its job is to
**prove the predecessor's pathological cost classes are gone and catch
them if they come back**.

## The history this guards against

Numbers observed operating DuckLake in production (see
[../README.md](../README.md) and the defect ledger) — the baselines
hoglake must embarrass, forever:

| Pathology | Predecessor | Guarded by |
|---|---|---|
| Commit cost scales with the catalog, not the write set: full-catalog stats loads of 5-7s per attempt, re-paid on every OCC retry | **190-264s** single-table commits | `commit-throughput` (preseed ratio + wide-catalog ratio) |
| Commit-storm convoys under writer contention; superlinear collapse on a busy catalog | 60-180s commits convoying every writer on the shard | `commit-contention` |
| Dropped tables' stats rows taxing every commit forever (99.4% of stats rows on one catalog were for dropped tables; purging bought 30-50x) | DDL churn = permanent commit tax | `ddl-churn` (before/after ratio) |
| Snapshot expiry at **~14ms/snapshot** (~70/s) regardless of chunk size | ~50h to drain a 15M-snapshot backlog | `expiry-throughput` |
| CDC as a read-barrier-bound crawl over the whole catalog | changefeed cost ∝ catalog size | `changefeed-scan` (correlations) |

Every scenario also validates its own correctness invariants after the
load (dense snapshot ids, row-range tiling, no lost DV updates, zero
still-referenced deletions) — a benchmark that corrupts silently is
worse than none, so an invariant failure aborts with exit code 4.

## Running

Toolchain: [flox](https://flox.dev) (python312 + uv). Dependencies:
pyhoglake (editable, from `../pyhoglake`), pyarrow, httpx — no locust,
no k6; latency capture is hand-rolled (`time.perf_counter_ns`, sorted
lists for percentiles). numpy + Faker come along for the `seed` task
only (vocabularies and vectorized row fabrication); no scenario's
measurement path touches them.

```sh
flox activate -- uv sync
just                # list recipes
just quick          # all scenarios, smoke profile (~2-3 min)
just full           # all scenarios, real sizes (tens of minutes)
just commit-throughput --preseed-snapshots 0,100000
flox activate -- uv run hoglake-bench commit-contention --writers 1,2,4,8,16
```

### Target server

Two options:

1. **Already running** (the usual dev setup): server at
   `http://localhost:8080`, MinIO at `:9000`
   (`hoglake`/`hoglake123`, path-style). Overridable via `--url`,
   `--s3-endpoint`, … or `HOGLAKE_URL` / `HOGLAKE_S3_*` env vars.
2. **Bring your own**: `just up` at the repo root (Postgres 18 + MinIO
   + the server + the webui, all containerized), or in `../server`,
   `docker compose up -d && flox activate -- gradle run` (ports
   overridable via `HOGLAKE_*_PORT`).

The harness never hammers a dead server: 10 consecutive
transport/5xx failures abort the run (exit 3) with a clear message.

Catalogs are `bench-*` prefixed and unique per run (run id in catalog
name and data path), so runs never collide and need no cleanup — there
is no catalog-delete API; bench catalogs are cheap metadata rows.
Real objects are only written by `expiry-throughput` (real-parquet
cleanup probes), `end-to-end-writer` and `analytics-lifecycle` (actual
parquet), under `s3://hoglake-bench/<run-id>/`.

## Scenarios

All scenarios take `--ops`-style sizing caps plus the shared
`--duration <seconds>` cap and `--warmup <n>` (warmup iterations are
excluded from stats).

### IO honesty labels

Every scenario declares an **IO mode**, printed in its banner and
journaled per run (`io_mode` in the JSONL), so a number can never be
mistaken for something it is not:

- **metadata-only** — file registrations are *fabricated* (unique paths
  + well-formed stats, no parquet bytes anywhere). This is deliberate:
  it isolates control-plane cost from object-store IO, which is exactly
  what the O(catalog) regression guards need. But these numbers exclude
  footer parse, upload and read IO **by construction** and must never
  be quoted as end-to-end performance.
- **end-to-end** — every byte is real: parquet encoded by the client,
  uploaded to the object store, stats extracted from the writer's own
  footer.
- **mixed** — both halves exist; the per-metric fields say which is
  which (expiry-throughput: `removed`/`removed_s` are real object
  deletes, `missing` is the metadata-only drain of fabricated paths).

| Scenario | IO | What it measures | Headline |
|---|---|---|---|
| `commit-throughput` | metadata-only | Single-writer sequential **registration-only** commits (fabricated paths + footer-shaped stats — the control plane only, no parquet) at `--files-per-commit 1,10,100,1000`, against catalogs preseeded with `--preseed-snapshots 0,10000` existing snapshots, plus a **wide-catalog** stage with `--tables 500` live tables (the predecessor's 59K-table stats pathology, scaled down; a full-scale run is a manual `--tables 59000`; `--tables 0` disables) | commits/s, files/s, and **the preseed + wide-catalog latency ratios** — p50 must not grow with catalog size in snapshots OR tables (>1.5x flags loudly; this is THE O(catalog) regression) |
| `commit-contention` | metadata-only | K threads (`--writers 1,2,4,8,16`), one catalog, mixed shared-table/private-table appends, `read_snapshot` supplied so OCC is exercised | aggregate commits/s vs K, 409 count (**must be ~0**: appends never conflict with appends), per-writer p99 max. Conflicted attempts are excluded from the success percentiles and reported separately (`conflict_p50_ms`). Caveat: all K writers share one client process, so at high K the reported latencies include a client-side share (GIL, httpx connection pool) — treat cross-K comparisons as client-inclusive, not pure server numbers |
| `delete-contention` | metadata-only | DV registration: K writers on disjoint files (expect 0 conflicts), then all writers racing on ONE file | 409 rate + retry-to-success latency; post-run proof that no DV update was lost. The hotfile retry loop is bounded (50 retries/op + the `--duration` deadline, checked inside the loop); abandoned ops are counted separately (`abandoned`). Same client-side-share caveat as `commit-contention` |
| `changefeed-scan` | metadata-only | `changes()` over windows of 10/100/1000/10000 snapshots, measured while the catalog grows through `--stages`; consumer offset commit rate | latency must correlate with window **rows returned**, not catalog snapshot count (both correlations + fixed-window ratio reported; >1.5x flags) |
| `expiry-throughput` | mixed | Seed `--snapshots` (10k default) fabricated registrations, drop, 1s retention, drain via repeated `/maintenance/expire --batch`; then physical cleanup. The `--objects` cleanup probes are **real parquet** (field-id'd schema, stats extracted from each written footer) so `removed` counts real object-store deletes; the fabricated seed drains as `missing` | **snapshots expired/s** (vs the predecessor's ~70/s), files queued/s, cleanup `removed_s` (real IO) vs `missing` (metadata-only), `still_referenced == 0` |
| `ddl-churn` | metadata-only | create/append/drop cycles (the report-table pattern), with an identical small-commit probe before and after | tables/s and the **before/after commit-p50 ratio** (>1.5x flags — the dropped-table stats tax) |
| `end-to-end-writer` | end-to-end | The realistic path: pyarrow tables through `Table.append` — real parquet encode, MinIO upload, footer stats, commit (`--rows 100000 --batch-rows 10000`). `--table-schema typed` swaps the simple 4-column table for the **full 1.1.0 type matrix** (every writable scalar + nested list/struct/map from `datagen`), deterministic per batch, reported as `append.real_parquet_typed` | rows/s end to end, per-append latency |
| `analytics-lifecycle` | end-to-end | A seeded, replayable **modeling workflow**, not uniform ops: staging tables load real parquet in cycles; schemas evolve (`--evolution-rate`: column adds, type promotions per the server's promotion matrix, nested-type introduction); CTAS-style intermediates + a mart derive from what was actually loaded; one staging table gets variant-archive DDL then drop/recreate; a changefeed consumer follows every staging table and commits offsets. `--seed` replays the identical plan and data; `--scale` sizes tables x cycles | per-step-kind latency (`lifecycle.load`, `.evolve`, `.ctas`, `.changefeed`, ...), rows/s; invariants: incarnation change observed, changefeed books balance per incarnation, row-id tiling |
| `all` | — | every scenario; `--quick` = smoke profile (~2-3 min, the default), `--full` = real sizes | CI-ish gate |

## Seeding a dev warehouse

Not every use of this harness is a measurement: `seed` fills a catalog
with a **realistic fake data warehouse**, sized by total bytes, so the
console, compaction planning and the changefeed have something with real
shape to look at.

```sh
just bench seed                               # 0.4 GB (default) — dense enough cells to exercise compaction
just bench seed --gb 2.5                      # or: --catalog my-catalog
flox activate -- uv run hoglake-bench seed --gb 0.3 --server http://localhost:8080
```

What you get (`--gb` is fractional; the target is rounded to the nearest
100 MB and echoed before a byte is written — `target 2.5 GB -> plan:
2.5 GB (25 x 100MB units)`; anything under 0.1 GB is refused):

| Table | Share | Shape |
|---|---|---|
| `events.pageviews` | 60% | the event stream: uuid, team_id, distinct_id, session, path, referrer, ts over ~6 months, device/utm/properties-ish columns. **Partitioned `identity(team_id) x month(ts)`** and written through pyhoglake's partitioned-append fanout — one parquet file per (team, month) cell, all cells of a commit registered atomically |
| `warehouse.fact_orders` | 15% | FK-shaped ids into the dims, quantities, amounts, order/ship timestamps |
| `warehouse.fact_sessions` | 10% | sessions per user/team, durations, pageview counts, referrers |
| `telemetry.device_metrics` | 5% | **every writable scalar of the 1.1.0 vocabulary** — unsigned ints (uint64 values beyond int64), all four timestamp precisions, timestamptz, date, time, decimal, json, uuid, binary — with varied null rates and a dense `reading_id` |
| `telemetry.app_events` | 5% | **the nested kinds**: string- and int32-keyed maps, list-of-struct spans, struct-of-struct/-list context, list-of-list samples, on warehouse-real event ids/teams/timestamps |
| `warehouse.dim_users` / `dim_products` / `dim_teams` | ~5% | names, emails, countries, cities, SKUs, prices, plans, regions — one small file each |

The telemetry pair (and `end-to-end-writer --table-schema typed`) are
built on `hoglake_bench.datagen`: deterministic fabrication for the
full 1.1.0 type system, respecting the server's nesting-depth cap (8)
and per-table node cap (10,000), with bounds-stressing values baked in
(planted integer domain extremes, NaN floats for the nan_count path,
multi-KB strings and long-shared-prefix pairs). **`variant` is excluded,
not faked**: pyarrow cannot write native Parquet VARIANT(1) and bench's
write path is pyhoglake/pyarrow, so variant data cannot be honestly
produced (`datagen.VARIANT_EXCLUSION_REASON`); `analytics-lifecycle`
still exercises variant *DDL* on a table that then drops.

- **Many files, not a few giants**: facts/events aim for `--file-mb`
  (default 32, clamped to 20-80), so compaction planning has real
  candidates to rank. Small budgets over the 6-team x 6-month grid
  produce smaller partition files by construction.
- **Realism without the cost**: [Faker](https://faker.readthedocs.io)
  mints a few thousand distinct values ONCE per run (names, emails,
  domains, cities, countries, products, campaigns); rows are then
  fabricated with numpy/pyarrow by sampling those vocabularies. Nothing
  is per-row Python, so millions of rows cost tens of milliseconds and
  ~100 MB lands in a few seconds.
- **Size control is server-sourced**: after every commit the writer reads
  the table's own `/tables` info and re-divides the remaining budget over
  the remaining work, so a bad bytes-per-row estimate corrects itself.
  Compression makes an exact target impossible — runs land within ~10%
  and the summary says by how much.
- **`--seed` is fixed by default**, so two runs produce the same shape.
- **`--catalog` reuse appends**, it never fails: an existing catalog
  grows (dims get new id ranges; the fixed team dimension is left alone).

The run ends with the catalog's own numbers, read back from the server:

```
seeded warehouse (per-table figures from the server's /tables info):
  table                        files           rows       bytes    this run
  -------------------------------------------------------------------------
  events.pageviews                36        715,123     66.0 MB     66.0 MB
  ...
wrote 101.0 MB in 43 files across 6 tables in 4.6s (target 100.0 MB, +1.0% — on plan)
```

`seed` is deliberately **not** a scenario: `all` never runs it, it
journals no metrics, and it flags no regressions.

## Streaming events continuously

`seed` fills a catalog and stops. `stream` never stops: it writes
synthetic events into one hour-partitioned table, in the shape millpond
produces, until you interrupt it. It is for **watching the maintenance
loops under live pressure** — compaction, expiry, the changefeed, the
console — rather than for producing a number.

```sh
just bench stream                                  # 20k events/s until Ctrl-C
just bench stream --rate 100000 --flush-mb 256     # harder
just bench stream --hours-per-minute 60            # roll hours every second
flox activate -- uv run hoglake-bench stream --rate-mb-s 25
```

The table is `events.pageviews` — the **seeded warehouse's own pageviews
column set**, plus a `properties` text blob (millpond's property payload;
properties stay VARCHAR, as they do in millpond's string-only mode, and
`variant` is out of scope here for the same reason it is in `seed`).

- **Partitioned `hour(ts)` — the timestamp alone.** No `team_id` in the
  spec: the production events table is not partitioned per tenant, so
  every team's events land in the same hourly cell. Hour granularity is
  the whole point — partitions close within the hour (within seconds
  under `--hours-per-minute`), so compaction and expiry always have
  settled cells to chew on while the stream keeps running.
- **Sort order `team_id, ts`,** declared on the table and honoured by
  every batch before it is handed to the writer — millpond does exactly
  this (`MILLPOND_SORT_BY=team_id,timestamp`). With no team
  partitioning, sortedness is the *only* thing that gives a reader team
  locality, which makes this table a real test of whether compaction
  preserves clustering: a sorted table should get **more** prunable each
  compaction round, and a concat-style merge that ignored sort order
  would destroy that.
- **The skew therefore lands inside the files, not across them.** Most
  rows of every file belong to the whale, the tail scattered through, so
  a file's `team_id` bounds span the active range — wide unless the sort
  actually held. That is the production shape, and it is what makes the
  per-file bounds worth reading.

### What the default distribution is, and is not

Teams are drawn from a **whale plus a Zipf tail**: one team takes
`--whale-share` of all events outright (default `0.45`), and the
remaining teams split the rest by a power law with exponent `--zipf`
(default `1.1`) over `--teams` teams (default 250). At the defaults the
whale (`team_id` 10000) carries 45%, the runner-up 11.4%, the tenth team
1.0%, and the smallest 0.03% — synthetic ids from 10000, identifying
nothing.

**That default is a plausible shape, not a measured production
distribution, and nothing in this repo or in millpond can make it one.**
What the trees actually contain: `seed`'s own `TEAM_WEIGHTS` is
self-labelled as a deliberate synthetic skew and its tests only assert
that the weights sum to 1; millpond's include/exclude config is an
allowlist — membership with no volume attached — whose live contents
come from a control-plane endpoint the repo deliberately knows nothing
about; and millpond's own load generator picks `team_id` *uniformly*,
applying its Zipf law to `distinct_id` instead. The nearest real
measurement that could exist is millpond's
`millpond_filter_matched_total{value=...}` series at run time. If
somebody reads it, change the default and say so in
`stream/distribution.py`.

Weights are fixed for the life of a run, so a whale stays a whale across
hours; `--seed` makes the whole stream reproducible.

### Rate, flush cadence and fanout

| Knob | Default | What it does |
|---|---|---|
| `--rate` | `20000` | target events/second; `0` = as fast as the stack takes it |
| `--rate-mb-s` | — | target Arrow MB/s instead, converted via a measured bytes-per-event probe (mutually exclusive with `--rate`) |
| `--flush-mb` | `100` | flush after this many MB of buffered **Arrow** bytes (millpond's `FLUSH_SIZE`) |
| `--flush-seconds` | `60` | ...or after this long, whichever comes first (millpond's `FLUSH_INTERVAL_MS`) |
| `--hours-per-minute` | real time | compress event time so hour partitions roll over without waiting |
| `--teams` / `--whale-share` / `--zipf` | `250` / `0.45` / `1.1` | the distribution above |
| `--properties-bytes` | `192` | size of the per-row properties blob |
| `--max-events` / `--duration` | unbounded | bounded runs, for tests |
| `--max-flush-retries` | `8` | attempts per flush before the run gives up |

Rate control is **backpressure, not a busy loop**: the producer sleeps
exactly the time it owes, in chunks so a signal is noticed promptly. A
long stall (a slow flush, a retry storm) re-anchors the limiter rather
than banking credit, so catching up can never become an unthrottled
burst. The achieved rate is reported on every progress line and again in
the summary.

**Fanout at the defaults.** Events cost ~492 Arrow bytes each
(`--properties-bytes 192`), so 20,000/s fills the 100 MB buffer in about
**10 seconds** — the byte trigger fires long before the 60 s one. Each
flush therefore covers ~10 seconds of event time, which lies inside one
hour cell, so **one flush writes one file**; only the flush that
straddles the boundary writes two. That is roughly **350 flushes and
~353 files per hour**, at ~58 MB of parquet each (the stream measures its
own Arrow-to-parquet ratio and prints it — ~1.7x on this payload, not
millpond's 3-4x, because a random property blob does not compress),
i.e. ~20 GB and 72M events per hour into a single hourly partition.
That is exactly the small-file population compaction exists for: 350
same-spec, same-partition files per hour, tier after tier.

Under `--hours-per-minute` the arithmetic changes, deliberately: a flush
that spans N hour cells fans out to N files. At `--hours-per-minute 60`
(one hour per real second) a 10-second flush writes ten files in ten
partitions — which is how you get a week of settled partitions to
compact in ten minutes.

### Progress, and stopping it

A line every `--progress-seconds` (10 by default), human-first but all
`key=value`:

```
[     9.6s] events=380,000 events_s=39,443 arrow_mb_s=19.41 flushes=19 files=19 hours=1 hour=2026-09-18T23Z retries=0 failures=0 buffered_rows=0 buffered_mb=0.0
```

A transient server error — 503 admission backpressure, a commit
conflict, a timeout, a server that goes away entirely — is retried with
capped exponential backoff and counted in `retries`; it never kills a
long run. A **permanent** error (422 validation, a changed table
incarnation) stops immediately with a message saying so, because
retrying a contract violation cannot help.

**Ctrl-C is the designed exit, not an error path.** The first
SIGINT/SIGTERM lets the in-flight flush finish — an append is one atomic
commit, so there is no such thing as a half-published flush, and
abandoning it mid-upload would only orphan parquet for cleanup to
reclaim. The **unflushed remainder of the buffer is then discarded** and
reported, so the table only ever contains policy-triggered flushes and a
Ctrl-C cannot leave a runt file behind for the next run to trip over. A
run that ends by hitting `--max-events`/`--duration` does flush its
remainder: that data was asked for. Then the summary prints and the
process exits **0**:

```
=== stream summary (stopped by SIGINT) ===
  elapsed              13.1s
  events               520,000 (39,552/s achieved, target 40,000/s)
  arrow bytes          255.8 MB (19.46 MB/s)
  flushes              26 (bytes 26)
  files written        26
  hour partitions      1
  retries              0 (flush failures 0)
  parquet bytes        149.3 MB (1.71x smaller than Arrow)
  top 10 teams by events
    team 10000             233,807   45.0%
    team 10001              59,364   11.4%
    ...
```

A **second** Ctrl-C exits immediately with 130, whatever is in flight.

Like `seed`, `stream` is a task and not a scenario — `all` never runs it
(a forever-loop would never finish) and it flags no regressions — but it
journals a normal `bench-results.jsonl` line with `io_mode:
"end-to-end"`, because every byte it writes is real parquet.

`--catalog` reuse appends: a restarted stream keeps filling the same
table, provided the existing table's partition spec and sort order match
(it refuses rather than mixing specs in one table).

### From the bench pod

Same image, same pod as everything else below — `stream` is just another
subcommand:

```sh
kubectl apply -f bench/deploy/bench-pod.yaml
kubectl -n gigahog exec -it bench-shell -- \
  hoglake-bench stream --rate 50000 --bucket posthog-gigahog-mw-dev
```

Ctrl-C reaches it through `exec -it` and it stops cleanly. If you
detached instead, `kubectl -n gigahog exec bench-shell -- pkill -INT -f
hoglake-bench` does the same thing (the task treats SIGTERM identically,
so deleting the pod is also a clean stop — with the unflushed buffer
dropped, as above). Run it under `nohup`/`setsid` if you want it to
outlive your exec session; deleting the pod is still the whole cleanup,
and the catalog it wrote stays until you drop it.

## Running in-cluster

Seeding a deployed environment means running bench *next to* the server
rather than from a laptop: the harness is a writer — it uploads the parquet
and the server only registers the paths — so it needs the bucket, and the
bucket is reachable from inside the cluster with no key material at all.

`bench/deploy/bench-pod.yaml` is a shell for that. It runs as the
`gigahog-server` ServiceAccount, one of the two identities the IRSA role
trusts, so AWS credentials arrive through the projected web-identity token.
The three `HOGLAKE_S3_*` settings are deliberately **empty**: an empty
endpoint means real AWS instead of the local MinIO default, and empty keys
mean the SDK's credential chain rather than the local stack's `hoglake` /
`hoglake123`.

The pod runs `ghcr.io/posthog/hoglake-bench`, which already contains the
harness and its dependencies (`bench/Dockerfile`, published per commit by
`.github/workflows/bench-image.yml`). Nothing to copy in, nothing to
install: apply, exec, run.

```sh
kubectl apply -f bench/deploy/bench-pod.yaml
kubectl -n gigahog exec -it bench-shell -- \
  hoglake-bench seed --gb 0.5 --bucket posthog-gigahog-mw-dev
```

`kubectl -n gigahog exec -it bench-shell -- bash` for a shell instead, if you
want to run several scenarios without waiting on a pull each time.

The image is tagged `latest` and by commit sha, and the publish job's summary
prints the multi-arch manifest digest; pin the pod to a sha tag or a digest
when a run has to be reproducible. It carries the `pyhoglake` from the same
commit as the harness — the pair the repo tests — rather than the newest
release on PyPI, which is a different client (main carries a `.dev0` version
PyPI has never seen). `uv sync --no-sources` is the escape hatch if you ever
need the published client instead; nothing in the image uses it.

`--bucket` is the environment's own bucket; bench writes under a per-run
prefix and never asks AWS to create it (the role carries no
`s3:CreateBucket`, and the bucket is terraform's). Catalogs are named
`bench-<scenario>-<run-id>-<n>`, so a run cannot collide with anything else
in the catalog and is obvious to find later.

Nothing in the pod starts on its own — it sleeps until you drive it. It is a
bare Pod rather than a Deployment, so deleting it is the whole cleanup:

```sh
kubectl -n gigahog delete pod bench-shell
```

The data it wrote is not cleaned up by that, which is the point; drop the
`bench-*` catalogs when you no longer want them.

### Iterating on an unmerged branch

The image only ever holds merged code. To run a branch that has not been
published yet, ship the working tree into `/work` and put it ahead of the
image's own install on `PYTHONPATH`:

```sh
git archive HEAD bench pyhoglake |
  kubectl -n gigahog exec -i bench-shell -- tar x -C /work
kubectl -n gigahog exec -it bench-shell -- env \
  PYTHONPATH=/work/bench/src:/work/pyhoglake/src \
  hoglake-bench seed --gb 0.5 --bucket posthog-gigahog-mw-dev
```

Both trees, not just `bench` — bench installs the client from
`../pyhoglake`, so a `bench`-only archive leaves the two halves from
different commits. The image installs both packages **non-editable**
precisely so `PYTHONPATH` wins; an editable install's import hook would
outrank it and silently keep running the baked-in harness.

`git archive` and not `kubectl cp`. `cp` tars the directory as it is on
disk, which means your `.venv` — macOS-built binaries, headed into a linux
container — plus a wall of tar ownership errors on the way. If a `.venv`
does land in there, uv warns about a non-existent interpreter and rebuilds
the environment: alarming, harmless, and entirely avoided by archiving from
the index instead of the working directory.

## Reading the output

One line per metric:

```
commit.preseed10000.fpc10   ops=200  wall_s=1.92  rate_s=104  p50=8.91ms p95=11.2ms p99=13.9ms max=17.3ms  files_s=1041
commit.scaling.fpc10        ops=0    wall_s=0     rate_s=0    p50_preseed0_ms=8.71  p50_preseed10000_ms=8.91  ratio=1.02
```

- `rate_s` — ops per second of wall time (warmup excluded).
- `p50/p95/p99/max` — per-op latency percentiles in milliseconds.
- `ratio` lines are the regression guards; anything above **1.5x**
  prints a `!!! REGRESSION` line on stderr and exits 2.

### Guard hygiene

Every ratio-guarded stage (commit-throughput preseed + wide stages,
ddl-churn probes, changefeed fixed-window stages):

- runs an identical fixed warm phase first (40 discarded ops), so both
  sides of a ratio are compared at the same thermal state — a cold
  fresh-catalog baseline vs a seed-warmed preseeded stage would hide
  real O(catalog) regressions behind cold-cache inflation;
- must measure at least **20 ops**, or the run aborts (exit 3) with an
  "insufficient samples for a trustworthy ratio" error instead of
  emitting a vacuous ratio (a zero-sample stage never passes silently).

When `--duration` cuts a phase short, any invariant check that depends
on the phase completing is skipped with a loud `NOTICE:` on stderr.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | everything ran, no flags |
| 2 | at least one `!!! REGRESSION` flag |
| 3 | abort: server unresponsive (transport bail-out) or insufficient samples in a guarded stage |
| 4 | invariant violation — the numbers are not trustworthy |
| 5 | unexpected exception (harness bug) |

Across `all`, per-scenario outcomes are severity-resolved with
precedence **4 > 2 > 5 > 3 > 0** — a later scenario can never demote an
earlier scenario's regression (e.g. scenario 3 flags a regression,
scenario 5 aborts → exit 2, and both are journaled). After an abort,
the runner re-checks `/healthz` and only continues to the remaining
scenarios if the server still answers.

### Results journal

Every run appends one JSON line per **completed-or-failed** scenario to
`bench-results.jsonl` (`--results` to redirect): timestamp, run id,
scenario, `status` (`ok` / `regression` / `aborted` /
`invariant_violation` / `error`), the full effective params (including
`duration`, `warmup`, `url`), the run config (profile), flags, error,
metrics — so runs are diffable over time and a duration-capped run is
distinguishable from a full one
(`jq 'select(.scenario=="commit-throughput")' bench-results.jsonl`).
Correlation fields (`rows_latency_corr`, `catalog_latency_corr`) are
always float-or-null, never a string.

## Tests

```sh
flox activate -- uv run pytest                       # unit + integration
flox activate -- uv run pytest -m "not integration"  # no server needed
```

Unit tests cover the percentile/correlation machinery, the loop
drivers (warmup exclusion, caps, failure bail-out), argument parsing +
profiles, and one full scenario loop against an in-memory fake of the
client surface (including a seeded snapshot-id-gap to prove the
invariant checks actually fire). The rework layers add: IO-mode
labeling (every scenario declares one; the journal carries it), the
real-parquet registration helper (bytes are valid parquet, stats are
footer-derived — bounds asserted against the written data), the
`datagen` matrix (coverage against the server scalar enum, byte-level
seed determinism, server-cap respect, null-rate and bound-stress
assertions), the typed seed tables, and the `analytics-lifecycle`
planner (plan determinism/replay, promotion legality against an
independently restated matrix, structural validity — tables exist
before use, no load while a variant column is live). `tests/test_adversarial.py` seeds
faults against the guards themselves: a modeled O(catalog)
per-snapshot commit tax must trip the preseed and wide-catalog ratio
flags (and a cold-cache-only model must not), zero-sample stages must
abort, the hotfile retry loop must respect its bounds, and exit-code
severity resolution must never demote a regression. The
`-m integration` smoke runs `all --quick` against `HOGLAKE_URL` and
skips cleanly when it's down.

The `seed` task has its own unit tests (budget rounding and the
proportional split, the sub-0.1 GB refusal, vocabulary determinism under
`--seed`, every fabricator against its declared schema, and the
month-window grid checked against pyhoglake's Iceberg `month` transform)
plus a live `-m integration` smoke that seeds 0.1 GB and verifies the
result from the server's `/tables` info — tables exist with plausible
row/byte counts, events carry two partition values per file, dims are
single files, and row-id ranges tile every table.

The `stream` task has its own unit tests (whale share within tolerance
over 200k samples and determinism under `--seed`; batch sorting against
the declared sort order; `hour` partition-tuple derivation checked
against pyhoglake's own `transforms.hour` rather than a restated
constant; flush triggers, bytes vs time, including the empty-buffer
refusal a partitioned append would 422 on; the rate limiter's pacing,
its no-credit-banking rule and the assertion that every wait is a real
sleep rather than a spin; signal handling, where a simulated SIGINT must
produce a clean summary and a second one must take the immediate-exit
path; and retry classification — 503/409/timeout retryable, 422 and a
changed incarnation fatal) plus live `-m integration` runs bounded with
`--max-events`: the hour spec and sort order read back off the API, the
one-file-per-flush fanout, hour rollover under time compression (with
every row checked against the hour cell its file is registered in, so a
`day` spec cannot pass), per-file sortedness read back out of the
written parquet, and the within-file skew.

Caveat on correlations: a 2-stage `--stages` list (the quick profile)
makes `catalog_latency_corr` a two-point Pearson, which is ±1 by
construction — read `fixed_window_ratio` instead there; the
correlation is only informative at 3+ stages.
