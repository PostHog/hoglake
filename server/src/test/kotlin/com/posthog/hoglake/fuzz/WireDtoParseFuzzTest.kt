package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.junit.FuzzTest
import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.api.AbandonUploadsDto
import com.posthog.hoglake.api.AlterTableRequestDto
import com.posthog.hoglake.api.ClaimUploadDto
import com.posthog.hoglake.api.CommitRequestDto
import com.posthog.hoglake.api.CreateCatalogRequestDto
import com.posthog.hoglake.api.PrepareTableCreationDto
import com.posthog.hoglake.api.PublishTableCreationDto
import com.posthog.hoglake.api.UploadOwnerDto
import com.posthog.hoglake.api.parseExpectedTableUuid
import com.posthog.hoglake.api.parseLongQuery
import com.posthog.hoglake.api.parseScanStatsRequest
import com.posthog.hoglake.commit.commitFingerprint
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.plugins.BadRequestException

/**
 * Fuzz target (docs/fuzzing.md layer 4, target f): the wire parse path for
 * every structured request body the server accepts — CommitRequestDto,
 * the polymorphic AlterTableRequestDto (`op`-discriminated), the
 * two-phase table-creation pair and the upload-claim bodies — over
 * arbitrary bytes, through the production wire mapper itself
 * (api/WireJson.kt), shared with App.module rather than reconstructed
 * here.
 *
 * The two phases have different contracts, and conflating them produces
 * false findings:
 *
 *  - **readValue** runs inside ktor's ContentNegotiation, which wraps
 *    *any* converter throw into JsonConvertException/BadRequestException
 *    -> 400. Jackson's encoding detector can raise plain IOExceptions
 *    (e.g. CharConversionException on a byte run it reads as UTF-32);
 *    those are still 400s on the wire, so a parse throw is never a
 *    finding here.
 *  - **toModel** runs in the route handler, *outside* that wrapping. An
 *    exception escaping it surfaces as a 500 on hostile input unless it
 *    is one ErrorMapping turns into 4xx: [JacksonException],
 *    [BadRequestException] (AlterOpDto's unknown-op / missing-field), or
 *    [HoglakeException] (Validation -> 422, e.g. unknown enum literals).
 *
 * The end-to-end complement — hostile bytes really do get a 400 through
 * the whole ktor stack — is pinned in `WireParseErrorMappingTest`.
 */
class WireDtoParseFuzzTest {
    @FuzzTest(maxDuration = "120s")
    fun wireParseFailsOnlyWithMappedExceptions(data: ByteArray) {
        if (data.size > MAX_INPUT_BYTES) return

        try {
            val uuid = parseExpectedTableUuid(data.toString(Charsets.UTF_8))
            check(parseExpectedTableUuid(uuid.toString()) == uuid)
        } catch (e: Exception) {
            checkAllowed("expected_table_uuid", e)
        }

        for (name in listOf("read_snapshot", "expected_namespace_id")) {
            try {
                val raw = data.toString(Charsets.UTF_8)
                val value = parseLongQuery(name, raw)
                check(raw.toLongOrNull() == value)
            } catch (e: Exception) {
                checkAllowed(name, e)
            }
        }

        // GET .../scan's stats request: the fuzz input split at its first
        // NUL into `include` and `stats_fields`, so both halves (and the
        // cross-parameter rule) see arbitrary text. Oracle: any request it
        // accepts names column_stats, and its field ids re-parse to
        // themselves.
        try {
            val raw = data.toString(Charsets.UTF_8)
            val include = raw.substringBefore('\u0000')
            val fields = raw.substringAfter('\u0000', missingDelimiterValue = "").ifEmpty { null }
            parseScanStatsRequest(include, fields)?.let { request ->
                check(include.split(',').any { it.trim() == "column_stats" })
                request.fieldIds?.let { ids ->
                    check(parseScanStatsRequest("column_stats", ids.joinToString(","))?.fieldIds == ids)
                }
            }
        } catch (e: Exception) {
            checkAllowed("scan include/stats_fields", e)
        }

        parseOrNull { mapper.readValue<CreateCatalogRequestDto>(data) }?.let { dto ->
            check(mapper.readValue<CreateCatalogRequestDto>(mapper.writeValueAsBytes(dto)) == dto)
        }

        // Every commit-shaped endpoint added for guarded DML receives
        // this same DTO — /commit/prepared, /commit/deletes/prepared,
        // /commit/mutations/prepared, /commit/uploads and
        // /commit/transaction all `call.receive<CommitRequestDto>()`
        // (api/Routes.kt) and differ only in the required-field checks
        // they run afterwards. /truncate has no body at all: its guard
        // is the mandatory `expected_table_uuid` query parameter, which
        // parseExpectedTableUuid above already fuzzes.
        parseOrNull { mapper.readValue<CommitRequestDto>(data) }?.let { dto ->
            try {
                val request = dto.toModel()
                val fingerprint = commitFingerprint(request)
                check(commitFingerprint(request.copy(allowPendingDeletes = true)) != fingerprint)
                val pending = request.deletes.flatMap { it.files }.filter { it.dataFilePath != null }
                pending.forEach { file ->
                    check(
                        mapper.readValue<com.posthog.hoglake.model.DeleteFileRegistration>(
                            mapper.writeValueAsBytes(file),
                        ) == file,
                    )
                }
                val reordered =
                    request.copy(
                        appends =
                            request.appends.reversed().map {
                                it.copy(
                                    files =
                                        it.files.reversed().map {
                                                file ->
                                            file.copy(columnStats = file.columnStats?.reversed())
                                        },
                                )
                            },
                    )
                check(commitFingerprint(reordered) == fingerprint)
                check(commitFingerprint(request.copy(readSnapshot = (request.readSnapshot ?: 0) xor 1)) != fingerprint)
            } catch (e: Exception) {
                checkAllowed("CommitRequestDto", e)
            }
        }

        // The upload-claim bodies (api/UploadRoutes.kt). Each one is
        // parsed AND reserialized: a DTO that binds but cannot be
        // written back is a response the server cannot produce either,
        // and the round trip is what catches a field whose wire name
        // and property name have drifted apart.
        parseOrNull { mapper.readValue<ClaimUploadDto>(data) }?.let { dto ->
            check(mapper.readValue<ClaimUploadDto>(mapper.writeValueAsBytes(dto)) == dto)
        }
        parseOrNull { mapper.readValue<AbandonUploadsDto>(data) }?.let { dto ->
            check(mapper.readValue<AbandonUploadsDto>(mapper.writeValueAsBytes(dto)) == dto)
        }
        parseOrNull { mapper.readValue<UploadOwnerDto>(data) }?.let { dto ->
            check(mapper.readValue<UploadOwnerDto>(mapper.writeValueAsBytes(dto)) == dto)
        }

        // The publish half of the two-phase table creation
        // (api/TableCreationRoutes.kt): a list of file registrations,
        // each of which also has to survive toModel().
        parseOrNull { mapper.readValue<PublishTableCreationDto>(data) }?.let { dto ->
            // Reserialized and re-parsed, but NOT compared: a file
            // registration carries ColumnStatsDto, whose ByteArray
            // bounds keep identity equals on purpose (api/Dto.kt), so
            // `==` here would fail on every body with bounds in it and
            // assert nothing about the wire.
            check(mapper.readValue<PublishTableCreationDto>(mapper.writeValueAsBytes(dto)).files.size == dto.files.size)
            try {
                dto.files.forEach { it.toModel() }
            } catch (e: Exception) {
                checkAllowed("PublishTableCreationDto", e)
            }
        }

        parseOrNull { mapper.readValue<PrepareTableCreationDto>(data) }?.let { dto ->
            try {
                val definition =
                    TableCreationDefinition(
                        dto.namespace,
                        dto.name,
                        dto.columns.map { it.toModel() },
                        dto.replacement,
                        dto.partitionFields.map { it.toModel() },
                        dto.sortFields.map { it.toModel() },
                        dto.comment,
                        dto.properties,
                    )
                if ((dto.replacement?.readSnapshot ?: 0) >= 0 &&
                    dto.partitionFields.all {
                        it.sourceFieldId > 0
                    } && dto.sortFields.all { it.sourceFieldId > 0 }
                ) {
                    val encoded = TableCreationDefinitionCodec.encode(definition)
                    check(TableCreationDefinitionCodec.decode(encoded) == definition)
                }
            } catch (e: Exception) {
                checkAllowed("PrepareTableCreationDto", e)
            }
        }

        parseOrNull { mapper.readValue<AlterTableRequestDto>(data) }?.let { dto ->
            try {
                dto.ops.forEach { it.toModel() }
            } catch (e: Exception) {
                checkAllowed("AlterTableRequestDto", e)
            }
        }
    }

    /** ContentNegotiation turns every parse failure into a 400; not a finding. */
    private fun <T> parseOrNull(parse: () -> T): T? =
        try {
            parse()
        } catch (_: Exception) {
            null
        }

    private fun checkAllowed(
        dto: String,
        e: Exception,
    ) {
        // KNOWN GAP, same one CommitReceiptFuzzTest documents and does
        // not share: `JacksonException` is NOT a family ErrorMapping
        // handles. StatusPages installs handlers for HoglakeException,
        // CorruptDefinitionException, BadRequestException,
        // JsonConvertException and ContentTransformationException, and
        // everything else reaches the Throwable arm as a 500 — so a
        // JacksonException escaping a handler-phase toModel() is waved
        // through here as "mapped" when the wire answer would be 500.
        // Tightening it belongs with this target, not with the new one.
        check(e is JacksonException || e is BadRequestException || e is HoglakeException) {
            "$dto parse escaped with unmapped ${e.javaClass.name}: ${e.message?.take(200)}"
        }
    }

    private companion object {
        const val MAX_INPUT_BYTES = 1 shl 20

        /**
         * The PRODUCTION wire mapper, not a copy of it (api/WireJson.kt):
         * a hand-rolled twin here would let this target pass while the
         * real mapper behaves differently.
         */
        val mapper: ObjectMapper = wireObjectMapper()
    }
}
