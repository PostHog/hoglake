package com.posthog.hoglake.testing

import org.testcontainers.containers.MinIOContainer
import org.testcontainers.utility.DockerImageName

/**
 * Central test-image pins. Docker Hub stopped serving minio/minio
 * anonymously (2026-09-11 CI breakage: "pull access denied ...
 * repository does not exist"); the same release tags remain public on
 * quay.io. One place to edit on the next registry move.
 */
object TestImages {
    private const val MINIO = "quay.io/minio/minio:RELEASE.2023-09-04T19-57-37Z"

    fun minio(): MinIOContainer = MinIOContainer(DockerImageName.parse(MINIO).asCompatibleSubstituteFor("minio/minio"))
}
