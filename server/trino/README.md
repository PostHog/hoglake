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

After the connector migration lands in the Trino fork, build or select an image
from that fork containing `plugin/hoglake`, then run:

```bash
cd server
flox activate -- ./gradlew :trino:test -PhoglakeTrinoImage=<image-tag-or-digest>
```

Alternatively set `HOGLAKE_TRINO_IMAGE`. Pin an immutable tag or digest for
reproducible results. Docker must be running. The same setting is required when
running the full Gradle test suite, since it includes these integration tests.
`-PunitOnly` excludes the Docker integration harness.

There is deliberately no default Trino image and no local plugin mount: the tests
exercise the connector shipped in the selected engine image. The old Gradle
`:trino:trinoPlugin` assembly task and Trino 446 SPI pin have been removed. The JDBC
client remains at 446 independently of the server version so this harness can
continue using the server project's Java 21 toolchain.

The tests cover schema/table discovery, Parquet reads, aggregates, column
binding, type promotion, deletion-vector refusal, and read-only enforcement.
The planned Iceberg REST facade remains a separate server feature.
