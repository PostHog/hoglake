package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.StreamReadConstraints
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.api.ColumnDto
import com.posthog.hoglake.api.DataFileDto
import com.posthog.hoglake.api.DeleteFileDto
import com.posthog.hoglake.api.NamespaceContainmentDto
import com.posthog.hoglake.api.NamespaceDto
import com.posthog.hoglake.api.ScanFileDto
import com.posthog.hoglake.api.TableDto
import com.posthog.hoglake.model.HoglakeException
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Flow
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/** Result of one typed metadata call to a Hoglite owner. */
internal sealed interface ScopedReadOutcome<out T> {
    data class Found<T>(val value: T) : ScopedReadOutcome<T>

    data object NotFound : ScopedReadOutcome<Nothing>

    data object Forbidden : ScopedReadOutcome<Nothing>
}

/** Validated table payload returned by Hoglite's typed table lookup. */
internal data class ScopedTableResult(
    val namespaceId: Long,
    val tableId: Long,
    val containment: NamespaceContainmentDto,
    val table: TableDto,
)

/** Bounded codec and transport for typed Hoglite metadata operations. */
internal object HogliteScopedNamespaceRead {
    private const val MAX_TARGET_BYTES = 4 * 1024
    private const val MAX_TOKEN_BYTES = 64 * 1024
    private const val MAX_NAMESPACE_RESPONSE_BYTES = 4 * 1024
    private const val MAX_TABLE_RESPONSE_BYTES = 256 * 1024
    private const val MAX_SCAN_RESPONSE_BYTES = 1024 * 1024
    private const val MAX_SCAN_FILES = 1000
    private const val BUDGET_MS = 5_000L
    private val compactToken = Regex("[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+")
    private val namespaceJson = mapper(maxNestingDepth = 2)
    private val tableJson = mapper(maxNestingDepth = 64)
    private val scanJson = mapper(maxNestingDepth = 16)

    private val columnRequired = setOf("field_id", "ordinal", "name", "type", "nullable")
    private val columnOptional = setOf("type_params", "children", "comment")
    private val dataFileRequired =
        setOf(
            "data_file_id",
            "path",
            "file_format",
            "record_count",
            "file_size_bytes",
            "row_id_start",
            "stats_state",
            "begin_snapshot",
            "explicit_row_ids",
        )
    private val dataFileOptional = setOf("footer_size")
    private val deleteFileRequired =
        setOf("delete_file_id", "path", "delete_count", "file_size_bytes", "begin_snapshot")
    private val deleteFileOptional = setOf("data_file_id", "file_format")

    private fun mapper(maxNestingDepth: Int): ObjectMapper =
        ObjectMapper(
            JsonFactory.builder()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(maxNestingDepth).build())
                .build(),
        ).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

    fun unavailable() = HoglakeException.BackendUnavailable("hoglite scoped namespace read unavailable")

    fun tableUnavailable() = HoglakeException.BackendUnavailable("hoglite scoped table read unavailable")

    fun scanUnavailable() = HoglakeException.BackendUnavailable("hoglite scoped scan read unavailable")

    fun read(
        http: HttpClient,
        standardUrl: String,
        token: String,
        tenant: String,
        catalog: String,
        namespace: String,
        timeoutMs: Long,
        onUnauthorized: (backendClockSeconds: Long?) -> HoglakeException,
    ): NamespaceDto? =
        execute(
            http = http,
            standardUrl = standardUrl,
            token = token,
            path =
                "/v1/t/${segment(tenant, ::unavailable)}/metadata/v1/catalogs/" +
                    "${segment(catalog, ::unavailable)}/namespaces/${segment(namespace, ::unavailable)}",
            query = null,
            timeoutMs = timeoutMs,
            maxResponseBytes = MAX_NAMESPACE_RESPONSE_BYTES,
            acceptedStatuses = setOf(200, 401, 404),
            onUnavailable = ::unavailable,
            onUnauthorized = onUnauthorized,
        ) { status, body ->
            decodeNamespace(status, body, namespace)
        }

    fun readTable(
        http: HttpClient,
        standardUrl: String,
        token: String,
        tenant: String,
        catalog: String,
        namespace: String,
        table: String,
        timeoutMs: Long,
        onUnauthorized: (backendClockSeconds: Long?) -> HoglakeException,
    ): ScopedReadOutcome<ScopedTableResult> =
        execute(
            http = http,
            standardUrl = standardUrl,
            token = token,
            path =
                "/v1/t/${segment(tenant, ::tableUnavailable)}/metadata/v1/catalogs/" +
                    "${segment(catalog, ::tableUnavailable)}/namespaces/" +
                    "${segment(namespace, ::tableUnavailable)}/tables/${segment(table, ::tableUnavailable)}",
            query = null,
            timeoutMs = timeoutMs,
            maxResponseBytes = MAX_TABLE_RESPONSE_BYTES,
            acceptedStatuses = setOf(200, 401, 403, 404),
            onUnavailable = ::tableUnavailable,
            onUnauthorized = onUnauthorized,
        ) { status, body ->
            decodeTable(status, body, namespace, table)
        }

    fun readScan(
        http: HttpClient,
        standardUrl: String,
        token: String,
        tenant: String,
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long,
        tableUuid: UUID,
        timeoutMs: Long,
        onUnauthorized: (backendClockSeconds: Long?) -> HoglakeException,
    ): ScopedReadOutcome<List<ScanFileDto>> {
        if (snapshot < 0) throw scanUnavailable()
        val query = "snapshot=$snapshot&table_uuid=$tableUuid"
        return execute(
            http = http,
            standardUrl = standardUrl,
            token = token,
            path =
                "/v1/t/${segment(tenant, ::scanUnavailable)}/metadata/v1/catalogs/" +
                    "${segment(catalog, ::scanUnavailable)}/namespaces/" +
                    "${segment(namespace, ::scanUnavailable)}/tables/${segment(table, ::scanUnavailable)}/scan",
            query = query,
            timeoutMs = timeoutMs,
            maxResponseBytes = MAX_SCAN_RESPONSE_BYTES,
            acceptedStatuses = setOf(200, 401, 403, 404),
            onUnavailable = ::scanUnavailable,
            onUnauthorized = onUnauthorized,
        ) { status, body ->
            decodeScan(status, body, namespace, tableUuid, snapshot)
        }
    }

    private fun <T> execute(
        http: HttpClient,
        standardUrl: String,
        token: String,
        path: String,
        query: String?,
        timeoutMs: Long,
        maxResponseBytes: Int,
        acceptedStatuses: Set<Int>,
        onUnavailable: () -> HoglakeException.BackendUnavailable,
        onUnauthorized: (backendClockSeconds: Long?) -> HoglakeException,
        decode: (Int, Body) -> T,
    ): T {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(minOf(BUDGET_MS, timeoutMs))
        val subscriber = AtomicReference<BoundedBodySubscriber?>()
        var pending: CompletableFuture<HttpResponse<Body>>? = null
        try {
            if (token.length > MAX_TOKEN_BYTES || !compactToken.matches(token)) throw onUnavailable()
            val target = if (query == null) path else "$path?$query"
            val uri = URI.create(URI.create(standardUrl.trimEnd('/') + target).toASCIIString())
            val rawTargetLength = uri.rawPath.length + (uri.rawQuery?.let { it.length + 1 } ?: 0)
            if (
                uri.scheme !in setOf("http", "https") || uri.host == null ||
                uri.rawUserInfo != null || uri.rawQuery != query || uri.rawFragment != null ||
                rawTargetLength > MAX_TARGET_BYTES ||
                uri.rawPath.split('/').any { it == "." || it == ".." }
            ) {
                throw onUnavailable()
            }
            val remaining = remaining(deadline, onUnavailable)
            val request =
                HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofNanos(remaining))
                    .header("Authorization", "Bearer $token")
                    .header("Accept", "application/json")
                    .GET()
                    .build()
            val responseFuture =
                http.sendAsync(request) { info ->
                    val lengths = info.headers().allValues("Content-Length")
                    val length = lengths.singleOrNull()?.toLongOrNull()
                    val unsupported =
                        info.statusCode() !in acceptedStatuses ||
                            (
                                lengths.isNotEmpty() &&
                                    (lengths.size != 1 || length == null || length !in 0..maxResponseBytes.toLong())
                            )
                    BoundedBodySubscriber(
                        rejected = unsupported,
                        maxBytes = maxResponseBytes,
                        initialBytes = length?.toInt() ?: 0,
                        onUnavailable = onUnavailable,
                    ).also { subscriber.set(it) }
                }
            pending = responseFuture
            val response = responseFuture.get(remaining(deadline, onUnavailable), TimeUnit.NANOSECONDS)
            remaining(deadline, onUnavailable)
            if (response.statusCode() == 401) {
                // Only hoglite's exact refusal object; anything else under a 401 stays unavailable.
                if (!decodeUtf8(response.body(), namespaceJson).hasCode("unauthorized")) throw onUnavailable()
                throw Unauthorized(HogliteClient.backendClock(response.headers()))
            }
            val result = decode(response.statusCode(), response.body())
            remaining(deadline, onUnavailable)
            return result
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw onUnavailable()
        } catch (e: Unauthorized) {
            // Not an outage: onUnauthorized answers it from the token and hoglite's clock (#681).
            // An outage's 503 would retry a refusal that no delay ends.
            throw onUnauthorized(e.backendClockSeconds)
        } catch (_: Exception) {
            // Never retain a peer body, a target, or a credential in the exception chain.
            throw onUnavailable()
        } finally {
            subscriber.get()?.cancel()
            pending?.cancel(true)
        }
    }

    /** hoglite's own verifier refused the forwarded token; carries only its `Date`, as epoch seconds. */
    private class Unauthorized(
        val backendClockSeconds: Long?,
    ) : RuntimeException(null, null, false, false)

    private fun remaining(
        deadline: Long,
        onUnavailable: () -> HoglakeException.BackendUnavailable,
    ): Long {
        val nanos = deadline - System.nanoTime()
        if (nanos <= 0) throw onUnavailable()
        return nanos
    }

    /** Validate Unicode before UTF-8 encoding can replace an unpaired surrogate. */
    private fun segment(
        value: String,
        onUnavailable: () -> HoglakeException.BackendUnavailable,
    ): String {
        if (value.isEmpty() || value.length > MAX_TARGET_BYTES || value == "." || value == "..") throw onUnavailable()
        var index = 0
        while (index < value.length) {
            val char = value[index]
            if (char == '/' || char == '\\' || char == '%' || char.isISOControl()) throw onUnavailable()
            if (char.isHighSurrogate()) {
                if (index + 1 >= value.length || !value[index + 1].isLowSurrogate()) throw onUnavailable()
                index++
            } else if (char.isLowSurrogate()) {
                throw onUnavailable()
            }
            index++
        }
        return buildString {
            val hex = "0123456789ABCDEF"
            for (byte in value.toByteArray(Charsets.UTF_8)) {
                val unsigned = byte.toInt() and 255
                if (unsigned in 65..90 || unsigned in 97..122 || unsigned in 48..57 || unsigned.toChar() in "-._~") {
                    append(unsigned.toChar())
                } else {
                    append('%')
                    append(hex[unsigned ushr 4])
                    append(hex[unsigned and 15])
                }
                if (length > MAX_TARGET_BYTES) throw onUnavailable()
            }
        }
    }

    private fun decodeUtf8(
        body: Body,
        mapper: ObjectMapper,
    ): JsonNode {
        val text =
            Charsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body.bytes, 0, body.length))
                .toString()
        return mapper.readTree(text)
    }

    private fun decodeNamespace(
        status: Int,
        body: Body,
        namespace: String,
    ): NamespaceDto? {
        val root = decodeUtf8(body, namespaceJson)
        if (status == 404) {
            if (!root.hasCode("not_found")) throw unavailable()
            return null
        }
        if (status != 200 || !root.hasExactly("namespace_id", "name", "containment")) throw unavailable()
        val name = root["name"]
        if (!name.isTextual || name.textValue() != namespace) throw unavailable()
        val mark = root["containment"]
        if (!mark.hasExactly("field", "value") || !mark["field"].isTextual || mark["field"].textValue().isEmpty()) {
            throw unavailable()
        }
        return NamespaceDto(
            name = name.textValue(),
            namespaceId = root["namespace_id"].positiveLong(::unavailable),
            containment = NamespaceContainmentDto(mark["field"].textValue(), mark["value"].positiveLong(::unavailable)),
        )
    }

    private fun decodeTable(
        status: Int,
        body: Body,
        namespace: String,
        table: String,
    ): ScopedReadOutcome<ScopedTableResult> {
        val root = decodeUtf8(body, tableJson)
        if (status == 404) {
            if (!root.hasCode("not_found")) throw tableUnavailable()
            return ScopedReadOutcome.NotFound
        }
        if (status == 403) {
            if (!root.hasCode("forbidden")) throw tableUnavailable()
            return ScopedReadOutcome.Forbidden
        }
        if (status != 200 ||
            !root.hasExactly(
                "namespace_id",
                "namespace",
                "containment",
                "table_id",
                "table_uuid",
                "name",
                "read_snapshot_id",
                "columns",
            )
        ) {
            throw tableUnavailable()
        }
        val namespaceId = root["namespace_id"].positiveLong(::tableUnavailable)
        val returnedNamespace = root["namespace"]
        if (!returnedNamespace.isTextual || returnedNamespace.textValue() != namespace) throw tableUnavailable()
        val mark = root["containment"]
        if (!mark.hasExactly("field", "value")) throw tableUnavailable()
        val containment =
            NamespaceContainmentDto(
                field = mark["field"].nonEmptyText(::tableUnavailable),
                value = mark["value"].positiveLong(::tableUnavailable),
            )
        val tableId = root["table_id"].positiveLong(::tableUnavailable)
        val tableUuid = root["table_uuid"].canonicalUuid(::tableUnavailable)
        val returnedName = root["name"]
        if (!returnedName.isTextual || returnedName.textValue() != table) throw tableUnavailable()
        val readSnapshotId = root["read_snapshot_id"].nonNegativeLong(::tableUnavailable)
        val columnsNode = root["columns"]
        if (!columnsNode.isArray || columnsNode.size() == 0) throw tableUnavailable()
        val columns = columnsNode.map { decodeColumn(it) }
        return ScopedReadOutcome.Found(
            ScopedTableResult(
                namespaceId = namespaceId,
                tableId = tableId,
                containment = containment,
                table =
                    TableDto(
                        name = table,
                        namespace = namespace,
                        tableUuid = tableUuid,
                        columns = columns,
                        readSnapshotId = readSnapshotId,
                        recordCount = null,
                        fileCount = null,
                        fileSizeBytes = null,
                        remote = false,
                    ),
            ),
        )
    }

    private fun decodeColumn(node: JsonNode): ColumnDto {
        if (!node.hasKeys(columnRequired, columnOptional)) throw tableUnavailable()
        val fieldId = node["field_id"].positiveLong(::tableUnavailable)
        val ordinal = node["ordinal"].nonNegativeInt(::tableUnavailable)
        val name = node["name"].nonEmptyText(::tableUnavailable)
        val type = node["type"].nonEmptyText(::tableUnavailable)
        val nullableNode = node["nullable"]
        if (!nullableNode.isBoolean) throw tableUnavailable()
        val typeParams =
            node.get("type_params")?.takeIf { !it.isNull }?.let { params ->
                if (!params.isObject) throw tableUnavailable()
                params.properties().associate { it.key to decodeJsonValue(it.value) }
            }
        val children =
            node.get("children")?.takeIf { !it.isNull }?.let { array ->
                if (!array.isArray) throw tableUnavailable()
                array.map(::decodeColumn).takeIf { it.isNotEmpty() }
            }
        val comment =
            node.get("comment")?.takeIf { !it.isNull }?.let { commentNode ->
                if (!commentNode.isTextual) throw tableUnavailable()
                commentNode.textValue()
            }
        return ColumnDto(
            name = name,
            type = type,
            typeParams = typeParams,
            nullable = nullableNode.booleanValue(),
            fieldId = fieldId,
            ordinal = ordinal,
            children = children,
            comment = comment,
        )
    }

    private fun decodeScan(
        status: Int,
        body: Body,
        namespace: String,
        tableUuid: UUID,
        snapshot: Long,
    ): ScopedReadOutcome<List<ScanFileDto>> {
        val root = decodeUtf8(body, scanJson)
        if (status == 404) {
            if (!root.hasCode("not_found")) throw scanUnavailable()
            return ScopedReadOutcome.NotFound
        }
        if (status == 403) {
            if (!root.hasCode("forbidden")) throw scanUnavailable()
            return ScopedReadOutcome.Forbidden
        }
        if (status != 200 ||
            !root.hasExactly("namespace_id", "namespace", "table_uuid", "read_snapshot_id", "files")
        ) {
            throw scanUnavailable()
        }
        root["namespace_id"].positiveLong(::scanUnavailable)
        val returnedNamespace = root["namespace"]
        if (!returnedNamespace.isTextual || returnedNamespace.textValue() != namespace) throw scanUnavailable()
        if (root["table_uuid"].canonicalUuid(::scanUnavailable) != tableUuid) throw scanUnavailable()
        if (root["read_snapshot_id"].nonNegativeLong(::scanUnavailable) != snapshot) throw scanUnavailable()
        val filesNode = root["files"]
        if (!filesNode.isArray || filesNode.size() > MAX_SCAN_FILES) throw scanUnavailable()
        val files =
            filesNode.map { entry ->
                if (!entry.hasKeys(setOf("data_file"), setOf("delete_file"))) throw scanUnavailable()
                val dataFile = decodeDataFile(entry["data_file"])
                val deleteFile =
                    entry.get("delete_file")?.takeIf { !it.isNull }?.let { decodeDeleteFile(it, dataFile.dataFileId) }
                ScanFileDto(dataFile = dataFile, deleteFile = deleteFile)
            }
        return ScopedReadOutcome.Found(files)
    }

    private fun decodeDataFile(node: JsonNode): DataFileDto {
        if (!node.hasKeys(dataFileRequired, dataFileOptional)) throw scanUnavailable()
        val footerSize =
            node.get("footer_size")?.takeIf { !it.isNull }?.nonNegativeLong(::scanUnavailable)
        val explicitRowIdsNode = node["explicit_row_ids"]
        if (!explicitRowIdsNode.isBoolean) throw scanUnavailable()
        return DataFileDto(
            dataFileId = node["data_file_id"].positiveLong(::scanUnavailable),
            path = node["path"].textOrFail(::scanUnavailable),
            fileFormat = node["file_format"].nonEmptyText(::scanUnavailable),
            recordCount = node["record_count"].nonNegativeLong(::scanUnavailable),
            fileSizeBytes = node["file_size_bytes"].nonNegativeLong(::scanUnavailable),
            footerSize = footerSize,
            rowIdStart = node["row_id_start"].nonNegativeLong(::scanUnavailable),
            statsState = node["stats_state"].nonEmptyText(::scanUnavailable),
            beginSnapshot = node["begin_snapshot"].nonNegativeLong(::scanUnavailable),
            explicitRowIds = explicitRowIdsNode.booleanValue(),
        )
    }

    private fun decodeDeleteFile(
        node: JsonNode,
        expectedDataFileId: Long,
    ): DeleteFileDto {
        if (!node.hasKeys(deleteFileRequired, deleteFileOptional)) throw scanUnavailable()
        val dataFileId =
            node.get("data_file_id")?.takeIf { !it.isNull }?.positiveLong(::scanUnavailable)?.also {
                if (it != expectedDataFileId) throw scanUnavailable()
            } ?: expectedDataFileId
        val fileFormat =
            node.get("file_format")?.takeIf { !it.isNull }?.nonEmptyText(::scanUnavailable) ?: "puffin"
        return DeleteFileDto(
            deleteFileId = node["delete_file_id"].positiveLong(::scanUnavailable),
            dataFileId = dataFileId,
            path = node["path"].textOrFail(::scanUnavailable),
            fileFormat = fileFormat,
            deleteCount = node["delete_count"].nonNegativeLong(::scanUnavailable),
            fileSizeBytes = node["file_size_bytes"].nonNegativeLong(::scanUnavailable),
            beginSnapshot = node["begin_snapshot"].nonNegativeLong(::scanUnavailable),
        )
    }

    private fun decodeJsonValue(node: JsonNode): Any? =
        when {
            node.isNull -> null
            node.isBoolean -> node.booleanValue()
            node.isIntegralNumber -> if (node.canConvertToInt()) node.intValue() else node.longValue()
            node.isFloatingPointNumber -> node.doubleValue()
            node.isTextual -> node.textValue()
            node.isArray -> node.map(::decodeJsonValue)
            node.isObject -> node.properties().associate { it.key to decodeJsonValue(it.value) }
            else -> throw tableUnavailable()
        }

    private fun JsonNode.hasCode(expected: String): Boolean =
        hasExactly("code") && this["code"].isTextual && this["code"].textValue() == expected

    private fun JsonNode.hasExactly(vararg fields: String): Boolean =
        isObject && size() == fields.size && fields.all { has(it) }

    private fun JsonNode.hasKeys(
        required: Set<String>,
        optional: Set<String>,
    ): Boolean {
        if (!isObject) return false
        val actual = properties().map { it.key }.toSet()
        return actual.minus(optional) == required
    }

    private fun JsonNode.textOrFail(onUnavailable: () -> HoglakeException.BackendUnavailable): String {
        if (!isTextual) throw onUnavailable()
        return textValue()
    }

    private fun JsonNode.nonEmptyText(onUnavailable: () -> HoglakeException.BackendUnavailable): String {
        if (!isTextual || textValue().isEmpty()) throw onUnavailable()
        return textValue()
    }

    private fun JsonNode.canonicalUuid(onUnavailable: () -> HoglakeException.BackendUnavailable): UUID {
        val raw = textOrFail(onUnavailable)
        val parsed = runCatching { UUID.fromString(raw) }.getOrNull() ?: throw onUnavailable()
        if (parsed.toString() != raw) throw onUnavailable()
        return parsed
    }

    private fun JsonNode.positiveLong(onUnavailable: () -> HoglakeException.BackendUnavailable): Long {
        if (!isIntegralNumber || !canConvertToLong() || longValue() <= 0) throw onUnavailable()
        return longValue()
    }

    private fun JsonNode.nonNegativeLong(onUnavailable: () -> HoglakeException.BackendUnavailable): Long {
        if (!isIntegralNumber || !canConvertToLong() || longValue() < 0) throw onUnavailable()
        return longValue()
    }

    private fun JsonNode.nonNegativeInt(onUnavailable: () -> HoglakeException.BackendUnavailable): Int {
        if (!isIntegralNumber || !canConvertToInt() || intValue() < 0) throw onUnavailable()
        return intValue()
    }

    private class Body(val bytes: ByteArray, val length: Int)

    /** Check every incoming buffer before copying any response bytes. */
    private class BoundedBodySubscriber(
        rejected: Boolean,
        private val maxBytes: Int,
        initialBytes: Int,
        private val onUnavailable: () -> HoglakeException.BackendUnavailable,
    ) : HttpResponse.BodySubscriber<Body> {
        private val result = CompletableFuture<Body>()
        private val buffer =
            if (rejected) {
                null
            } else {
                ByteArrayOutputStream(initialBytes.coerceIn(0, minOf(maxBytes, 16 * 1024)))
            }
        private var size = 0
        private var subscription: Flow.Subscription? = null

        init {
            if (rejected) result.completeExceptionally(onUnavailable())
        }

        override fun getBody(): CompletionStage<Body> = result

        @Synchronized
        override fun onSubscribe(subscription: Flow.Subscription) {
            this.subscription = subscription
            if (result.isDone) subscription.cancel() else subscription.request(1)
        }

        @Synchronized
        override fun onNext(items: List<ByteBuffer>) {
            if (result.isDone) return
            for (item in items) {
                val count = item.remaining()
                if (count > maxBytes - size) {
                    cancel()
                    return
                }
                val chunk = ByteArray(count)
                item.get(chunk)
                buffer!!.write(chunk)
                size += count
            }
            subscription?.request(1)
        }

        @Synchronized
        override fun onError(throwable: Throwable) {
            result.completeExceptionally(onUnavailable())
        }

        @Synchronized
        override fun onComplete() {
            val out = buffer ?: return
            val bytes = out.toByteArray()
            result.complete(Body(bytes, bytes.size))
        }

        @Synchronized
        fun cancel() {
            result.completeExceptionally(onUnavailable())
            subscription?.cancel()
        }
    }
}
