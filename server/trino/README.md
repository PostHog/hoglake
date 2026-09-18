# Hoglake / Trino integration tests

The native connector is maintained in
[PostHog/trino](https://github.com/PostHog/trino/tree/master/plugin/trino-hoglake).
It builds and ships with the engine, using the same SPI, Parquet reader, and S3
filesystem versions. Connector unit tests and configuration documentation live
there too.

This directory retains the server integration harness: it starts Postgres, MinIO,
and the Hoglake server, then runs JDBC queries against a Trino image containing
the connector. The REST contract and server behavior remain owned by Hoglake.

## Run

The fork publishes its server image (with `plugin/hoglake`) to the **public**
`ghcr.io/posthog/trino` on every master push — no registry auth needed, but the
image is arm64-only. Pick the newest ordered release tag
(`r<position>-<sha6>`; they sort lexicographically):

```bash
cd server
flox activate -- ./gradlew :trino:test \
  -PhoglakeTrinoImage=ghcr.io/posthog/trino:$( \
    curl -fsS -H "Authorization: Bearer $(curl -fsS 'https://ghcr.io/token?scope=repository:posthog/trino:pull' | jq -r .token)" \
      'https://ghcr.io/v2/posthog/trino/tags/list?n=1000' \
    | jq -r '.tags[]' | grep -E '^r[0-9]{12}-[0-9a-f]{6}$' | sort | tail -1)
```

Alternatively set `HOGLAKE_TRINO_IMAGE`. Pin an immutable tag or digest for
reproducible results. Docker must be running. The same setting is required when
running the full Gradle test suite, since it includes these integration tests.
`-PunitOnly` excludes the Docker integration harness.

CI runs this harness as `server.yml`'s `trino-test` job (ARM runner, same
newest-ordered-tag resolution, `HOGLAKE_TRINO_IMAGE` repo variable as the pin
override). The job is deliberately not required and not in `deploy`'s `needs`.

There is deliberately no default Trino image and no local plugin mount: the tests
exercise the connector shipped in the selected engine image. The old Gradle
`:trino:trinoPlugin` assembly task and Trino 446 SPI pin have been removed. The JDBC
client is pinned in `build.gradle.kts` (currently 483) and moves independently of
the server version, so this harness can continue using the server project's Java 21
toolchain. Its failure-message format is load-bearing: the assertions strip the
driver's `Query failed (#...)` preamble before matching, and assert that the
strip matched rather than trusting it.

The tests cover schema/table discovery, Parquet reads, aggregates, column
binding, type promotion, and deletion-vector application. The planned Iceberg
REST facade remains a separate server feature.

## Deletion vectors

The connector applies deletion vectors; it does not refuse them. A read of a
DV-backed table returns the surviving rows, and the harness asserts that as
correctness, not as a refusal. The fixtures write real puffin
`deletion-vector-v1` bytes (`TestPuffin`, the byte-level twin of the server
suite's `PuffinTestFiles`), because the connector opens the object, decodes the
roaring bitmap, and checks its `referenced-data-file` and cardinality against
the scan's pairing — a nominal DV path only ever produces a read failure.

The connector counts a DV-backed table on two paths, and the harness asserts
the same answer on both. An unfiltered `count(*)` needs no columns, so it
answers from catalog metadata (`record_count` minus the vector's
`delete_count`) after reading and validating the vector. Any read that needs a
column applies the vector per page inside the connector's page source, over
row positions relative to each data file, after row-group pruning. (That the
metadata path skips the Parquet file entirely is the connector's documented
behavior and is visible in `EXPLAIN ANALYZE` as a TableScan with no physical
input, but no assertion here observes it.)

Deleted positions are ordinals inside their own data file. A single-file table
cannot tell that apart from table-global numbering, so `multi_file_events`
holds two byte-identical files with the vector on the second one — the only
fixture where the two numberings give different answers.

Two refusals are asserted, both of them cases only the reader can catch
because hoglake never opens DV files: a vector whose cardinality disagrees
with the scan's `delete_count`, and a vector whose blob names a different data
file than the one it was paired with. The connector refuses more than these
(missing, truncated, bad checksum, wrong format, positions past the row count);
those are covered by the connector's own suite in PostHog/trino, not here.
The mismatch case is also the only assertion proving the metadata-count path
decodes the bitmap rather than trusting `delete_count`.

This puts a **floor under the fork image**: these assertions fail against any
image built before PostHog/trino's "Apply deletion vectors to Hoglake reads"
(`d4d5fa2`, first published as `r000000046914-6e5fcd`), which refuses
DV-bearing scans at split planning. There is no harness-side way to satisfy
both behaviors.

Note the floor currently has **zero margin**: `r000000046914-6e5fcd` is both
the floor and the newest published tag, so exactly one image passes. Newest-tag
resolution keeps normal runs above the floor only while the fork moves forward
— a revert or a re-tag below the DV commit reds this job, and the fix then is
to set `HOGLAKE_TRINO_IMAGE` to an image at or above `d4d5fa2` rather than to
weaken these assertions. This is an assumption, not a guarantee.

Message assertions here match no fork prose at all — no required phrase, no
excluded one. An exclusion evaporates silently the moment upstream rewords the
string it names; a required phrase reds a correct image the moment upstream
rewords it the other way, and even `deletion vector` is a live example, since
hoglake's own blob type is spelled `deletion-vector-v1`.
`deletionVectorFailureBody` therefore matches only object paths hoglake itself
registered, and discriminates nothing on its own.

Each test asserts its own discriminator on the returned body, with the reason
written beside it. The two in use are the `referenced-data-file` path, which
exists only inside the blob's bytes, and the pair of delete counts — declared
and decoded — which must differ, since the pre-DV refusal prints exactly one
number. That refusal also printed the topic, the vector's path, the paired data
file, and the declared count, so nothing weaker than these separates a
connector that validates vectors from one that refuses them unread.

Two known limits, both accepted rather than fixed. A path discriminator is
stronger than a numeric one: `assertStandaloneNumber` cannot tell the number it
wants from a coincidental one elsewhere in a sentence, and the cardinality
check is role-blind — a connector reporting the two counts the wrong way round
would pass. And nothing structural stops the next refusal test from calling
`deletionVectorFailureBody` and asserting nothing further, which would pass on
a pre-DV image; the javadoc says so in those words, and that is the only guard.

The connector also supports `CREATE TABLE`, `INSERT`, and CTAS as of
2026-09-16. The harness does not exercise the write path — the connector's own
suite does — so nothing here asserts read-only behavior.
