package com.posthog.hoglake.service

import java.util.UUID

/**
 * The upload-claim seam. The Postgres implementation is [UploadService];
 * the hoglite backend's is
 * [com.posthog.hoglake.hoglite.HogliteUploads]. The contract —
 * server-generated random paths under the claimed prefix, identity
 * pinned to the upload id, renewal beats expiry, abandonment is a
 * fenced tombstone that the removal queue drains — is documented on
 * [UploadService] and holds identically on both.
 */
interface UploadBackend {
    fun claim(
        catalog: String,
        id: UUID,
        owner: UUID,
        prefix: String,
        kind: String,
    ): UploadClaim

    fun renew(
        catalog: String,
        owner: UUID,
    ): Int

    fun abandon(
        catalog: String,
        owner: UUID,
        paths: List<String>,
    ): Int

    fun scheduleExpired(
        catalog: String,
        limit: Int = 1000,
    ): Int
}
