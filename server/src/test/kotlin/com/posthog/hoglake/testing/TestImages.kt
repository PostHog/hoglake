package com.posthog.hoglake.testing

import org.testcontainers.containers.MinIOContainer
import org.testcontainers.utility.DockerImageName

/**
 * Central test-image pins. One place to edit on the next registry move.
 *
 * Object storage is PGSTY Silo, a maintained MinIO build, pinned by
 * release AND multi-arch digest. MinIO community images are gone: Docker
 * Hub deleted minio/minio on 2026-09-11, and quay.io/minio/minio had
 * stopped serving anonymous pulls by 2026-09-24 (CI:
 * ContainerFetchException on every MinIO start). Silo keeps MinIO's S3
 * API, `MINIO_*` settings and `/minio/health` probes, and its entrypoint
 * maps `server ...` onto its `silo` binary, so Testcontainers'
 * [MinIOContainer] drives it unchanged. Same image family as duckgres
 * (its #1175). The Trino harness (server/trino) repeats this pin.
 */
object TestImages {
    const val SILO =
        "docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z" +
            "@sha256:635197cb9f36d01bee221d34d1c7d7960f6a95c48b0b6c01d99cd13bdae51a46"

    fun minio(): MinIOContainer = MinIOContainer(DockerImageName.parse(SILO).asCompatibleSubstituteFor("minio/minio"))
}
