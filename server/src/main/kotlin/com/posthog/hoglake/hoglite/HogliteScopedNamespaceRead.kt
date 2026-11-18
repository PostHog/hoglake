package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.StreamReadConstraints
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.api.NamespaceContainmentDto
import com.posthog.hoglake.api.NamespaceDto
import com.posthog.hoglake.model.HoglakeException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Flow
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/** The bounded codec and transport for one typed metadata operation. */
internal object HogliteScopedNamespaceRead {
    private const val MAX_TARGET_BYTES = 4 * 1024
    private const val MAX_TOKEN_BYTES = 64 * 1024
    private const val MAX_RESPONSE_BYTES = 4 * 1024
    private const val BUDGET_MS = 5_000L
    private val compactToken = Regex("[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+")
    private val json =
        ObjectMapper(
            JsonFactory.builder()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(2).build())
                .build(),
        ).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

    fun unavailable() = HoglakeException.BackendUnavailable("hoglite scoped namespace read unavailable")

    fun read(
        http: HttpClient,
        standardUrl: String,
        token: String,
        tenant: String,
        catalog: String,
        namespace: String,
        timeoutMs: Long,
    ): NamespaceDto? {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(minOf(BUDGET_MS, timeoutMs))
        val subscriber = AtomicReference<BoundedBodySubscriber?>()
        var pending: CompletableFuture<HttpResponse<Body>>? = null
        try {
            if (token.length > MAX_TOKEN_BYTES || !compactToken.matches(token)) throw unavailable()
            val path =
                "/v1/t/${segment(tenant)}/metadata/v1/catalogs/${segment(catalog)}/namespaces/${segment(namespace)}"
            val uri = URI.create(URI.create(standardUrl.trimEnd('/') + path).toASCIIString())
            if (
                uri.scheme !in setOf("http", "https") || uri.host == null ||
                uri.rawUserInfo != null || uri.rawQuery != null || uri.rawFragment != null ||
                uri.rawPath.length > MAX_TARGET_BYTES ||
                uri.rawPath.split('/').any { it == "." || it == ".." }
            ) {
                throw unavailable()
            }
            val remaining = remaining(deadline)
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
                        info.statusCode() !in setOf(200, 404) ||
                            (
                                lengths.isNotEmpty() &&
                                    (lengths.size != 1 || length == null || length !in 0..MAX_RESPONSE_BYTES.toLong())
                            )
                    BoundedBodySubscriber(unsupported).also { subscriber.set(it) }
                }
            pending = responseFuture
            val response = responseFuture.get(remaining(deadline), TimeUnit.NANOSECONDS)
            remaining(deadline)
            val result = decode(response.statusCode(), response.body(), namespace)
            remaining(deadline)
            return result
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw unavailable()
        } catch (_: Exception) {
            // Never retain a peer body, a target, or a credential in the exception chain.
            throw unavailable()
        } finally {
            subscriber.get()?.cancel()
            pending?.cancel(true)
        }
    }

    private fun remaining(deadline: Long): Long {
        val nanos = deadline - System.nanoTime()
        if (nanos <= 0) throw unavailable()
        return nanos
    }

    /** Validate Unicode before UTF-8 encoding can replace an unpaired surrogate. */
    private fun segment(value: String): String {
        if (value.isEmpty() || value.length > MAX_TARGET_BYTES || value == "." || value == "..") throw unavailable()
        var index = 0
        while (index < value.length) {
            val char = value[index]
            if (char == '/' || char == '\\' || char == '%' || char.isISOControl()) throw unavailable()
            if (char.isHighSurrogate()) {
                if (index + 1 >= value.length || !value[index + 1].isLowSurrogate()) throw unavailable()
                index++
            } else if (char.isLowSurrogate()) {
                throw unavailable()
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
                if (length > MAX_TARGET_BYTES) throw unavailable()
            }
        }
    }

    private fun decode(
        status: Int,
        body: Body,
        namespace: String,
    ): NamespaceDto? {
        val text =
            Charsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body.bytes, 0, body.length))
                .toString()
        val root = json.readTree(text)
        if (status == 404) {
            if (!root.hasExactly("code") || !root["code"].isTextual || root["code"].textValue() != "not_found") {
                throw unavailable()
            }
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
            namespaceId = root["namespace_id"].positiveLong(),
            containment = NamespaceContainmentDto(mark["field"].textValue(), mark["value"].positiveLong()),
        )
    }

    private fun JsonNode.hasExactly(vararg fields: String): Boolean =
        isObject && size() == fields.size && fields.all { has(it) }

    private fun JsonNode.positiveLong(): Long {
        if (!isIntegralNumber || !canConvertToLong() || longValue() <= 0) throw unavailable()
        return longValue()
    }

    private class Body(val bytes: ByteArray, val length: Int)

    /** Check every incoming buffer before copying any response bytes. */
    private class BoundedBodySubscriber(rejected: Boolean) : HttpResponse.BodySubscriber<Body> {
        private val result = CompletableFuture<Body>()
        private val bytes = if (rejected) null else ByteArray(MAX_RESPONSE_BYTES)
        private var size = 0
        private var subscription: Flow.Subscription? = null

        init {
            if (rejected) result.completeExceptionally(unavailable())
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
                if (count > MAX_RESPONSE_BYTES - size) {
                    cancel()
                    return
                }
                item.get(bytes!!, size, count)
                size += count
            }
            subscription?.request(1)
        }

        @Synchronized
        override fun onError(throwable: Throwable) {
            result.completeExceptionally(unavailable())
        }

        @Synchronized
        override fun onComplete() {
            if (bytes != null) result.complete(Body(bytes, size))
        }

        @Synchronized
        fun cancel() {
            result.completeExceptionally(unavailable())
            subscription?.cancel()
        }
    }
}
