package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.FileRepo
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/** More than two pages exercise continuation keys, ties, and fixed snapshot bounds. */
class HoglitePagingTest {
    private val json = jacksonObjectMapper()
    private val uuid = UUID.randomUUID().toString()
    private val files =
        (1L..600L).map { id ->
            listOf<Any?>(
                id, 7, "s3://b/$id", "parquet", id % 3, id % 4, 1, id % 5, 0,
                null, null, "pending", null, null, 2 + id % 2,
            )
        }
    private var expireAfterPage = false
    private val reads = CopyOnWriteArrayList<Pair<String, List<Any?>>>()
    private val server =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val body = json.readTree(ex.requestBody.readBytes())
                val sql = body["sql"].asText()
                val args = body["args"].map { json.treeToValue(it, Any::class.java) }
                reads.add(sql to args)
                val readPage = reads.any { it.first.contains("FROM files f WHERE f.table_id") }
                val floor = if (expireAfterPage && readPage) 5 else 0
                val rows: List<List<Any?>> =
                    when {
                        sql.startsWith(
                            "SELECT name, data_path,",
                        ) -> listOf(listOf("cat", "s3://b", 4, 0, floor, null, null, 0))
                        sql == "SELECT earliest_snapshot_id FROM meta WHERE id = 1" -> listOf(listOf(floor))
                        sql.startsWith("SELECT id, name, contained_field") -> listOf(listOf(1, "ns", null, null))
                        sql.startsWith(
                            "SELECT t.id, t.table_uuid, v.namespace_id",
                        ) -> listOf(listOf(7, uuid, 1, "ns", "t", null, "{}", 0, 0))
                        sql.startsWith("SELECT delete_file_id") -> {
                            val cursor = if (args.size > 4) (args[2] as Number).toLong() else 0L
                            files.filter { (it[0] as Long) > cursor }.take((args.last() as Number).toInt()).map {
                                listOf(it[0], it[0], "s3://b/dv/${it[0]}", 1, 1, 3)
                            }
                        }
                        sql.contains("FROM files f WHERE f.table_id") -> filePage(sql, args)
                        else -> emptyList()
                    }
                val bytes =
                    json.writeValueAsBytes(
                        mapOf(
                            "cols" to
                                List(rows.firstOrNull()?.size ?: 0) {
                                    "c$it"
                                },
                            "rows" to rows,
                            "affected" to 0,
                        ),
                    )
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            start()
        }
    private val client = HogliteClient("http://127.0.0.1:${server.address.port}")
    private val store = HogliteCatalogStore(client, { "opaque_1" })

    private fun filePage(
        sql: String,
        args: List<Any?>,
    ): List<List<Any?>> {
        val changes = sql.contains("NULL AS stats_json")
        val order = sql.substringAfter(" ORDER BY ").substringBefore(" LIMIT")
        val primary = order.substringBefore(',').substringBefore(' ')
        val index =
            when (primary) {
                "id", "f.id" -> 0
                "path" -> 2
                "record_count" -> 4
                "file_size_bytes" -> 5
                "stats_state" -> 11
                else -> 14
            }
        val defaultOrder = !changes && order == "begin_snapshot, row_id_start, id"

        fun compare(
            a: List<Any?>,
            b: List<Any?>,
        ): Int {
            fun value(i: Int): Int =
                if (a[i] is Number) {
                    (a[i] as Number).toLong().compareTo(
                        (b[i] as Number).toLong(),
                    )
                } else {
                    a[i].toString().compareTo(b[i].toString())
                }
            val first = value(index) * if (order.contains("DESC")) -1 else 1
            return if (first != 0) {
                first
            } else if (defaultOrder && value(7) != 0) {
                value(7)
            } else {
                value(0)
            }
        }
        val count = (args[args.size - if (changes) 1 else 2] as Number).toInt()
        val offset = if (changes) 0 else (args.last() as Number).toInt()
        val cursorArgs =
            if (changes) {
                if (args.size > 4) args.subList(1, 3) else emptyList()
            } else {
                args.drop(3).dropLast(2)
            }
        val cursor =
            if (cursorArgs.isEmpty()) {
                null
            } else {
                MutableList<Any?>(15) { null }.also {
                    it[index] = cursorArgs[0]
                    if (defaultOrder) {
                        it[7] = cursorArgs[1]
                        it[0] = cursorArgs[2]
                    } else {
                        it[0] = cursorArgs.last()
                    }
                }
            }
        return files.sortedWith(::compare).filter { cursor == null || compare(it, cursor) > 0 }.drop(offset).take(count)
    }

    @AfterEach
    fun stop() {
        client.close()
        server.stop(0)
    }

    @Test
    fun `unbounded file listing preserves every sort direction and tie across engine pages`() {
        for (sort in listOf(null) + FileRepo.FileSortColumn.entries) {
            for (desc in listOf(false, true)) {
                reads.clear()
                val page = store.listFiles("cat", "ns", "t", null, null, sort, desc, null, 0, emptyMap())
                val index =
                    when (sort) {
                        null, FileRepo.FileSortColumn.BEGIN_SNAPSHOT -> 14
                        FileRepo.FileSortColumn.ID -> 0
                        FileRepo.FileSortColumn.PATH -> 2
                        FileRepo.FileSortColumn.RECORD_COUNT -> 4
                        FileRepo.FileSortColumn.SIZE -> 5
                        FileRepo.FileSortColumn.STATS -> 11
                    }
                val expected =
                    files.sortedWith { a, b ->
                        val primary =
                            if (a[index] is Number) {
                                (a[index] as Number).toLong().compareTo((b[index] as Number).toLong())
                            } else {
                                a[index].toString().compareTo(b[index].toString())
                            }
                        val directed = primary * if (sort != null && desc) -1 else 1
                        val row = if (sort == null) (a[7] as Long).compareTo(b[7] as Long) else 0
                        if (directed != 0) {
                            directed
                        } else if (row != 0) {
                            row
                        } else {
                            (a[0] as Long).compareTo(b[0] as Long)
                        }
                    }.map { it[0] as Long }
                assertThat(page.map { it.dataFileId }).containsExactlyElementsOf(expected)
                assertThat(page).hasSize(600)
                assertThat(page.map { it.dataFileId }.toSet()).hasSize(600)
                val chunks = reads.filter { it.first.contains("FROM files f WHERE f.table_id") }
                assertThat(chunks).hasSize(3)
                assertThat(chunks).allMatch { it.first.contains("LIMIT ?") }
                val after = chunks.drop(1).map { it.first }
                if (sort == null) {
                    assertThat(after).allMatch {
                        it.contains("(f.begin_snapshot, f.row_id_start, f.id) > (?, ?, ?)")
                    }
                } else {
                    val primary =
                        when (sort) {
                            FileRepo.FileSortColumn.ID -> "id"
                            FileRepo.FileSortColumn.PATH -> "path"
                            FileRepo.FileSortColumn.RECORD_COUNT -> "record_count"
                            FileRepo.FileSortColumn.SIZE -> "file_size_bytes"
                            FileRepo.FileSortColumn.STATS -> "stats_state"
                            FileRepo.FileSortColumn.BEGIN_SNAPSHOT -> "begin_snapshot"
                        }
                    val comparison = if (desc) "<" else ">"
                    assertThat(after).allMatch {
                        it.contains("($primary $comparison ? OR ($primary = ? AND f.id > ?))")
                    }
                }
            }
        }
    }

    @Test
    fun `bounded file listing applies offset only to the first engine page`() {
        val page = store.listFiles("cat", "ns", "t", null, null, FileRepo.FileSortColumn.ID, false, 300, 50, emptyMap())
        assertThat(page.map { it.dataFileId }).containsExactlyElementsOf(51L..350L)
    }

    @Test
    fun `expiry past the listing snapshot refuses instead of returning a partial file set`() {
        expireAfterPage = true
        assertThatThrownBy {
            store.listFiles("cat", "ns", "t", null, null, null, false, null, 0, emptyMap())
        }.isInstanceOf(HoglakeException.Expired::class.java)
    }

    @Test
    fun `changes pages both file and deletion collections inside the pinned window`() {
        val plan = store.changes("cat", "ns", "t", 1, 4)
        assertThat(plan.files).hasSize(600)
        assertThat(plan.deleteFiles).hasSize(600)
        assertThat(plan.files.map { it.dataFileId }.toSet()).hasSize(600)
        assertThat(plan.deleteFiles.map { it.deleteFileId }.toSet()).hasSize(600)
        val chunks =
            reads.filter {
                it.first.contains("NULL AS stats_json") || it.first.startsWith("SELECT delete_file_id")
            }
        assertThat(chunks).hasSize(6)
        assertThat(chunks).allMatch {
            val upper = if (it.second.size > 4) 3 else 2
            (it.second[upper] as Number).toLong() == 4L && (it.second[1] as Number).toLong() in 1L..4L
        }
        assertThat(chunks).allMatch { it.first.contains("LIMIT ?") }
    }
}
