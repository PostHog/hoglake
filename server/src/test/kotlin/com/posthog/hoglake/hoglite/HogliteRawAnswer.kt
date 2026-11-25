package com.posthog.hoglake.hoglite

import java.io.InputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicInteger

/**
 * A loopback HTTP/1.1 server that answers every request with one fixed
 * response, its `Date` included, or with the (status, body) [byPath] holds
 * for the request's path. The JDK's HttpServer stamps its own `Date` on
 * every answer, so a test of hoglite's clock needs this one.
 */
internal class HogliteRawAnswer(
    private val status: Int,
    private val date: String,
    private val body: String,
    private val byPath: Map<String, Pair<Int, String>> = emptyMap(),
) : AutoCloseable {
    private val socket = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    val calls = AtomicInteger()
    val url = "http://127.0.0.1:${socket.localPort}"

    init {
        Thread.ofVirtual().start {
            while (!socket.isClosed) {
                val connection = runCatching { socket.accept() }.getOrNull() ?: break
                runCatching { connection.use(::answer) }
            }
        }
    }

    private fun answer(connection: Socket) {
        val input = connection.getInputStream()
        val head = readHead(input)
        val length =
            head.lineSequence()
                .map { it.split(":", limit = 2) }
                .firstOrNull { it.size == 2 && it[0].trim().equals("Content-Length", ignoreCase = true) }
                ?.get(1)?.trim()?.toInt() ?: 0
        input.readNBytes(length)
        calls.incrementAndGet()
        val path = head.substringBefore("\r\n").split(" ").getOrNull(1)?.substringBefore("?")
        val (answerStatus, answerBody) = byPath[path] ?: (status to body)
        val bytes = answerBody.toByteArray()
        val response =
            "HTTP/1.1 $answerStatus Status\r\nDate: $date\r\nContent-Type: application/json\r\n" +
                "Content-Length: ${bytes.size}\r\nConnection: close\r\n\r\n"
        connection.getOutputStream().apply {
            write(response.toByteArray())
            write(bytes)
            flush()
        }
    }

    private fun readHead(input: InputStream): String {
        val head = StringBuilder()
        while (!head.endsWith("\r\n\r\n")) {
            val b = input.read()
            if (b < 0) break
            head.append(b.toChar())
        }
        return head.toString()
    }

    override fun close() = socket.close()

    companion object {
        /** [epochSecond] as an HTTP `Date`, the form hoglited's Go server writes. */
        fun httpDate(epochSecond: Long): String =
            DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.ofEpochSecond(epochSecond).atOffset(ZoneOffset.UTC))
    }
}
