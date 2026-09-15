# Hoglake / Trino integration tests

The native read-only connector is maintained in
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
client remains at 446 independently of the server version so this harness can
continue using the server project's Java 21 toolchain.

The tests cover schema/table discovery, Parquet reads, aggregates, column
binding, type promotion, deletion-vector refusal, and read-only enforcement.
The planned Iceberg REST facade remains a separate server feature.
