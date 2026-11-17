package com.posthog.hoglake.store

import com.posthog.hoglake.auth.AuthPolicy
import io.ktor.http.HttpMethod
import java.net.ServerSocket
import java.net.URI

/** What the remote schema test classes share: the routes they walk and a Host that is down. */
internal object RemoteSchemaFixtures {
    /** The four routes a remote schema's reader is relayed on, all `GET`. */
    val relayedPatterns =
        setOf(
            "/v1/catalogs/{catalog}/namespaces/{namespace}",
            "/v1/catalogs/{catalog}/namespaces/{namespace}/tables",
            "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}",
            "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan",
        )

    fun isRelayed(rule: AuthPolicy.Entry) = rule.method == HttpMethod.Get && rule.pattern in relayedPatterns

    /**
     * Every route of the route table whose path names a namespace, with
     * [schema] as that namespace of [catalog]: a route added later is walked too.
     */
    fun namespaceRoutes(
        catalog: String,
        schema: String = "posthog",
    ): List<Pair<AuthPolicy.Entry, String>> =
        AuthPolicy.rules.filter { "{namespace}" in it.pattern }.map { rule ->
            rule to
                rule.pattern.replace("{catalog}", catalog).replace("{namespace}", schema)
                    .replace("{table}", "events").replace("{fileId}", "1").replace("{view}", "v")
        }

    /**
     * A Host that is down: it holds its port for as long as the test runs,
     * and closes every connection it accepts before it reads a byte. A port
     * taken from a socket that was closed first could be another process's
     * by the time the request is sent.
     */
    class DeadHost : AutoCloseable {
        private val socket = ServerSocket(0)
        private val acceptor =
            Thread {
                while (!socket.isClosed) {
                    runCatching { socket.accept().close() }
                }
            }.apply {
                isDaemon = true
                start()
            }
        val port: Int get() = socket.localPort
        val origin: URI get() = URI("http://127.0.0.1:$port")

        override fun close() {
            socket.close()
            acceptor.join(1_000)
        }
    }
}
