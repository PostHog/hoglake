package com.posthog.hoglake.hoglite

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/**
 * An HTTP proxy in front of a live hoglited. A [Hook] runs an action
 * before the proxy forwards a matching request. A race test uses it to
 * put a concurrent write between a store call's pre-flight reads and its
 * transaction. That is the interleaving each in-transaction guard exists
 * for, and the proxy makes it deterministic.
 *
 * [store] goes through the proxy. The engine harness's own store goes
 * straight to the engine, so a hook can use it without matching itself.
 */
internal class HogliteInterposer(
    private val targetUrl: String,
) : AutoCloseable {
    /** Runs [before] once for the first request that [match]es (method, path, body). */
    class Hook(
        val match: (String, String, String) -> Boolean,
        val before: () -> Unit,
    ) {
        @Volatile var fired = false
    }

    private val hooks = CopyOnWriteArrayList<Hook>()

    /** Every request body the proxy saw, in arrival order. */
    val requests = CopyOnWriteArrayList<String>()
    private val forward = HttpClient.newHttpClient()
    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            executor = Executors.newCachedThreadPool()
            createContext("/") { ex ->
                try {
                    val body = ex.requestBody.readBytes()
                    requests += body.decodeToString()
                    val hook =
                        synchronized(hooks) {
                            hooks.firstOrNull {
                                !it.fired && it.match(ex.requestMethod, ex.requestURI.toString(), body.decodeToString())
                            }?.also { it.fired = true }
                        }
                    hook?.before?.invoke()
                    val publisher =
                        if (body.isEmpty()) {
                            HttpRequest.BodyPublishers.noBody()
                        } else {
                            HttpRequest.BodyPublishers.ofByteArray(body)
                        }
                    val answer =
                        forward.send(
                            HttpRequest.newBuilder(URI.create(targetUrl + ex.requestURI))
                                .header("Content-Type", "application/json")
                                .method(ex.requestMethod, publisher)
                                .build(),
                            HttpResponse.BodyHandlers.ofByteArray(),
                        )
                    val bytes = answer.body()
                    ex.responseHeaders.add("Content-Type", "application/json")
                    ex.sendResponseHeaders(answer.statusCode(), if (bytes.isEmpty()) -1 else bytes.size.toLong())
                    if (bytes.isNotEmpty()) ex.responseBody.use { it.write(bytes) } else ex.close()
                } catch (e: Throwable) {
                    // A failed hook or forward is a test failure: answer an
                    // unretried status, never a silent pass-through.
                    val msg = "{\"error\":\"interposer: ${e.javaClass.simpleName}\"}".toByteArray()
                    ex.sendResponseHeaders(400, msg.size.toLong())
                    ex.responseBody.use { it.write(msg) }
                }
            }
            start()
        }

    val client = HogliteClient(standardUrl = "http://127.0.0.1:${server.address.port}", sleeper = {})
    val store = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)

    fun hook(
        match: (String, String, String) -> Boolean,
        before: () -> Unit,
    ): Hook = Hook(match, before).also { hooks += it }

    override fun close() {
        client.close()
        server.stop(0)
        (server.executor as java.util.concurrent.ExecutorService).shutdownNow()
    }

    companion object {
        /** A write `/tx`. A commit's pre-flight reads are a read-only `/tx` too, and do not match. */
        fun isWriteTx(
            method: String,
            path: String,
            body: String,
        ): Boolean = method == "POST" && path.endsWith("/tx") && "\"txid\":\"${HogliteClient.READ_TXID_PREFIX}" !in body
    }
}
