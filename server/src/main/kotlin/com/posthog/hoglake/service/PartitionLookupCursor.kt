package com.posthog.hoglake.service

import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.security.MessageDigest
import java.util.Base64
import java.util.UUID

/** A continuation is scoped to the resolved table incarnation and full lookup request. */
internal data class PartitionLookupCursor(
    val snapshot: Long,
    val after: Long,
    val upper: Long,
    val limit: Int,
    val scope: String,
) {
    fun encode(): String =
        Base64.getUrlEncoder().withoutPadding()
            .encodeToString("2|$snapshot|$after|$upper|$limit|$scope".toByteArray(Charsets.US_ASCII))

    companion object {
        const val MAX_LENGTH = 1024
        private val encodingPattern = Regex("[A-Za-z0-9_-]+")
        private val scopePattern = Regex("[0-9a-f]{64}")

        fun parse(raw: String): PartitionLookupCursor {
            fun invalid(): Nothing = throw PartitionLookupInvalidRequest("invalid partition lookup cursor")
            if (raw.length !in 1..MAX_LENGTH || !encodingPattern.matches(raw)) invalid()
            val decoded =
                try {
                    Base64.getUrlDecoder().decode(raw).toString(Charsets.US_ASCII)
                } catch (_: IllegalArgumentException) {
                    invalid()
                }
            val parts = decoded.split('|')
            if (parts.size != 6 || parts[0] != "2") invalid()
            val snapshot = parts[1].toLongOrNull() ?: invalid()
            val after = parts[2].toLongOrNull() ?: invalid()
            val upper = parts[3].toLongOrNull() ?: invalid()
            val limit = parts[4].toIntOrNull() ?: invalid()
            if (snapshot < 0 || after < 0 || upper <= after || limit !in 1..500 ||
                !scopePattern.matches(parts[5])
            ) {
                invalid()
            }
            val cursor = PartitionLookupCursor(snapshot, after, upper, limit, parts[5])
            if (cursor.encode() != raw) invalid()
            return cursor
        }

        fun scope(
            catalogId: Long,
            tableId: Long,
            tableUuid: UUID,
            specId: Long,
            values: List<String?>,
            upper: Long,
            snapshot: Long,
            limit: Int,
        ): String {
            val bytes = ByteArrayOutputStream()
            DataOutputStream(bytes).use { out ->
                out.writeLong(catalogId)
                out.writeLong(tableId)
                out.writeLong(tableUuid.mostSignificantBits)
                out.writeLong(tableUuid.leastSignificantBits)
                out.writeLong(specId)
                out.writeLong(upper)
                out.writeLong(snapshot)
                out.writeInt(limit)
                out.writeInt(values.size)
                values.forEach { value ->
                    if (value == null) {
                        out.writeInt(-1)
                    } else {
                        val utf8 = value.toByteArray(Charsets.UTF_8)
                        out.writeInt(utf8.size)
                        out.write(utf8)
                    }
                }
            }
            return MessageDigest.getInstance(
                "SHA-256",
            ).digest(bytes.toByteArray()).joinToString("") { "%02x".format(it) }
        }
    }
}
