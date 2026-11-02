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

    @Test
    fun `the object-prefix columns decode, and are null for a table without the option`() {
        val uuid = UUID.randomUUID()
        val tables =
            CommitTail.readSetMapper.readValue<List<CommitTail.ResolvedTable>>(
                """
                [{"ord":1,"table_id":7,"table_uuid":"$uuid","spec_id":2,"field_count":2,"next_row_id":0,
                  "prefix_field_id":1,"prefix_field_name":"team_id","prefix_col_type":"long",
                  "prefix_key_indexes":[1]},
                 {"ord":2,"table_id":8,"table_uuid":"$uuid","spec_id":null,"field_count":null,"next_row_id":null,
                  "prefix_field_id":null,"prefix_field_name":null,"prefix_col_type":null,
                  "prefix_key_indexes":null}]
                """.trimIndent(),
            )
        assertThat(tables[0].prefixFieldId).isEqualTo(1)
        assertThat(tables[0].prefixFieldName).isEqualTo("team_id")
        assertThat(tables[0].prefixColType).isEqualTo("long")
        assertThat(tables[0].prefixKeyIndexes).containsExactly(1)
        assertThat(listOf(tables[1].prefixFieldId, tables[1].prefixFieldName, tables[1].prefixKeyIndexes))
            .containsOnlyNulls()

        // A target's value list keeps a NULL partition value as a null
        // element, which CommitService must refuse rather than skip.
        val targets =
            CommitTail.readSetMapper.readValue<List<CommitTail.Target>>(
                """
                [{"data_file_id":1,"table_id":7,"record_count":5,"begin_snapshot":1,"end_snapshot":null,
                  "prefix_values":["4"]},
                 {"data_file_id":2,"table_id":7,"record_count":5,"begin_snapshot":1,"end_snapshot":null,
                  "prefix_values":[null]},
                 {"data_file_id":3,"table_id":8,"record_count":5,"begin_snapshot":1,"end_snapshot":null,
                  "prefix_values":null}]
                """.trimIndent(),
            )
        assertThat(targets.map { it.prefixValues }).containsExactly(listOf("4"), listOf(null), null)
    }
}
