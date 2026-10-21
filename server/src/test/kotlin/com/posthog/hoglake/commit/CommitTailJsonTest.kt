package com.posthog.hoglake.commit

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.service.UploadClaim
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

/**
 * The read's JSON sections are decoded by `CommitTail.readSetMapper`,
 * and a `timestamptz` in them is rendered by `json_agg` in the SESSION's
 * time zone with that zone's offset. Unit: the mapper, not the
 * statement — `CommitLockRoundTripsIntegrationTest` runs the real read
 * on a UTC session, which only ever shows `+00:00`.
 */
class CommitTailJsonTest {
    @Test
    fun `a claim's expires_at decodes to the same instant whatever offset the session rendered it with`() {
        val id = UUID.randomUUID()
        val owner = UUID.randomUUID()

        fun claim(expiresAt: String) =
            """
            [{"upload_id":"$id","owner":"$owner","prefix":"s3://b/p/","path":"s3://b/p/a.parquet",
              "file_kind":"data","state":"active","expires_at":"$expiresAt"}]
            """.trimIndent()
        val utc = CommitTail.readSetMapper.readValue<List<UploadClaim>>(claim("2026-10-21T01:02:03.123456+00:00"))
        val cest = CommitTail.readSetMapper.readValue<List<UploadClaim>>(claim("2026-10-21T03:02:03.123456+02:00"))
        // MUTATION: drop JavaTimeModule from the mapper and both fail to
        // decode; render the offset without normalizing it and the second
        // reads two hours late.
        assertThat(utc.single().expiresAt).isEqualTo(Instant.parse("2026-10-21T01:02:03.123456Z"))
        assertThat(cest.single().expiresAt).isEqualTo(utc.single().expiresAt)
        assertThat(utc.single().uploadId).isEqualTo(id)
    }
}
