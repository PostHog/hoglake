package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress

/** The column forest that columnsAt builds from flat column rows, in any row order. */
class HogliteColumnTreeTest {
    private val json = jacksonObjectMapper()

    /** field_id, parent_field_id, ordinal, name, type, type_params_json, nullable, comment. */
    @Volatile private var rows: List<List<Any?>> = emptyList()

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                ex.requestBody.readBytes()
                val bytes = json.writeValueAsBytes(mapOf("cols" to List(8) { "c$it" }, "rows" to rows, "affected" to 0))
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            start()
        }

    private val client = HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {})
    private val store = HogliteCatalogStore(client, { "t-$it" })

    @AfterEach
    fun stop() {
        client.close()
        server.stop(0)
    }

    private fun columns() = store.catalogOperation("wh") { store.columnsAt("wh", 7, 3) }

    @Test
    fun `nested columns nest under their parents and each level follows ordinal order`() {
        // Children come before their parents and out of ordinal order.
        rows =
            listOf(
                listOf(4, 2, 1, "y", "long", null, 1, null),
                listOf(3, 2, 0, "x", "int", null, 0, "first"),
                listOf(6, 5, 0, "element", "string", null, 1, null),
                listOf(5, null, 2, "l", "list", null, 1, null),
                listOf(2, null, 1, "s", "struct", null, 1, null),
                listOf(1, null, 0, "a", "long", "{\"k\":1}", 0, null),
            )

        val forest = columns()

        assertThat(forest.map { it.def.name }).containsExactly("a", "s", "l")
        assertThat(forest[0].def.typeParams).isEqualTo(mapOf("k" to 1))
        assertThat(forest[0].children).isEmpty()
        assertThat(forest[1].children.map { it.def.name }).containsExactly("x", "y")
        assertThat(forest[1].children[0].def.nullable).isFalse()
        assertThat(forest[1].children[0].def.comment).isEqualTo("first")
        assertThat(forest[2].children.single().def.name).isEqualTo("element")
        assertThat(forest.flatMap { it.selfAndDescendants() }.map { it.fieldId })
            .containsExactly(1L, 2L, 3L, 4L, 5L, 6L)
    }

    @Test
    fun `a flat schema at the column limit builds every top-level column`() {
        val n = 10_000
        rows = (n downTo 1).map { listOf(it, null, it - 1, "c$it", "long", null, 1, null) }

        val forest = columns()

        assertThat(forest).hasSize(n)
        assertThat(forest.first().fieldId).isEqualTo(1L)
        assertThat(forest.last().fieldId).isEqualTo(n.toLong())
        assertThat(forest).allMatch { it.children.isEmpty() }
    }
}
