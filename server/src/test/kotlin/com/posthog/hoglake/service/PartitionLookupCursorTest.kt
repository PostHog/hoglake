package com.posthog.hoglake.service

import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.Base64
import java.util.UUID

class PartitionLookupCursorTest {
    private val uuid = UUID.randomUUID()

    @Test
    fun `round trips without precision loss`() {
        val cursor = PartitionLookupCursor(Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE, 500, "0".repeat(64))
        assertThat(PartitionLookupCursor.parse(cursor.encode())).isEqualTo(cursor)
    }

    @Test
    fun `scope distinguishes every identity and nullable tuple boundary`() {
        fun scope(
            c: Long = 1,
            t: Long = 2,
            u: UUID = uuid,
            s: Long = 3,
            v: List<String?> = listOf("a", null),
            upper: Long = 100,
            snapshot: Long = 5,
            limit: Int = 100,
        ) = PartitionLookupCursor.scope(c, t, u, s, v, upper, snapshot, limit)
        val base = scope()
        listOf(
            scope(
                c = 2,
            ),
            scope(
                t = 3,
            ),
            scope(
                u = UUID.randomUUID(),
            ),
            scope(
                s = 4,
            ),
            scope(
                v = listOf("a", ""),
            ),
            scope(
                v = listOf("a", "null"),
            ),
            scope(
                v = listOf(null, "a"),
            ),
            scope(v = listOf("a|null")), scope(v = listOf("a", null, null)), scope(upper = 101),
            scope(snapshot = 6), scope(limit = 101),
        ).forEach {
            assertThat(it).isNotEqualTo(base)
        }
    }

    @Test
    fun `refuses malformed noncanonical cursors`() {
        val valid = PartitionLookupCursor(2, 3, 10, 100, "0".repeat(64)).encode()
        val badPayloads =
            listOf(
                "1|2|3|10|100|${"0".repeat(64)}",
                "2|-1|3|10|100|${"0".repeat(64)}",
                "2|2|-1|10|100|${"0".repeat(64)}",
                "2|02|3|10|100|${"0".repeat(64)}",
                "2|2|3|10|0|${"0".repeat(64)}",
                "2|2|3|10|100|not-a-hash",
                "2|2|3|3|100|${"0".repeat(64)}",
                "2|2|3|-1|100|${"0".repeat(64)}",
            )
        (
            listOf("", "!", "x".repeat(1025), "$valid=") +
                badPayloads.map {
                    Base64.getUrlEncoder().withoutPadding().encodeToString(it.toByteArray())
                }
        ).forEach {
            assertThatThrownBy {
                PartitionLookupCursor.parse(
                    it,
                )
            }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        }
    }
}
