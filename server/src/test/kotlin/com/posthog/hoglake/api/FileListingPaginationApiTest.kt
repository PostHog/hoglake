package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Paging and server-side sort on GET .../tables/{table}/files, the change
 * that lets the console show a table with tens of thousands of files.
 *
 * The response stays a bare array — no shape change, so an unpaged caller
 * and every prior test are untouched. What is pinned here:
 *
 *  - no `limit` returns EVERY file, the historical unbounded behavior;
 *  - `limit`/`offset` walk the result in a stable order, so a file never
 *    appears on two pages nor is skipped between them — the property that
 *    makes offset paging safe, which needs the deterministic tiebreak;
 *  - `sort`/`order` order by a raw column across the WHOLE table, not
 *    within a page (the point of moving the sort to the server);
 *  - an unknown sort column is refused rather than silently ignored.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileListingPaginationApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalog = "paging"
    private val filesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables/t/files"

    // 25 files, sizes chosen so size order differs from insertion (id)
    // order — otherwise a "sort by size" test would pass on the default
    // order and prove nothing.
    private val fileCount = 25

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
            client.postJson(
                "/v1/catalogs/$catalog/namespaces/ns/tables",
                """{"name": "t", "columns": [{"name": "id", "type": "long"}]}""",
            )
            for (i in 0 until fileCount) {
                // size = (i * 37 % 25) * 1000 + 1: a shuffled permutation,
                // so the largest file is not the last inserted.
                val size = ((i * 37) % fileCount) * 1000L + 1
                val res =
                    client.postJson(
                        "/v1/catalogs/$catalog/commit",
                        """{"appends": [{"namespace": "ns", "table": "t", "files": [
                           {"path": "s3://b/$catalog/f$i.parquet",
                            "record_count": ${i + 1}, "file_size_bytes": $size}]}]}""",
                    )
                assertThat(res.status).describedAs(res.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            }
        }

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun ids(response: HttpResponse): List<Long> =
        json.readTree(response.bodyAsText()).map { it["data_file_id"].asLong() }

    private suspend fun sizes(response: HttpResponse): List<Long> =
        json.readTree(response.bodyAsText()).map { it["file_size_bytes"].asLong() }

    @Test
    fun `no limit returns every file, unchanged`() =
        api { client ->
            val all = ids(client.get(filesUrl))
            assertThat(all).hasSize(fileCount)
        }

    @Test
    fun `limit and offset page the result without gaps or overlaps`() =
        api { client ->
            val pageSize = 10
            val seen = mutableListOf<Long>()
            var offset = 0
            while (true) {
                val page = ids(client.get("$filesUrl?limit=$pageSize&offset=$offset"))
                if (page.isEmpty()) break
                assertThat(page.size).isLessThanOrEqualTo(pageSize)
                seen += page
                offset += pageSize
                if (page.size < pageSize) break
            }
            // Every file exactly once: paging is a clean partition of the
            // whole set, which is only true because the order is total.
            assertThat(seen).hasSize(fileCount)
            assertThat(seen.toSet()).hasSize(fileCount)
        }

    @Test
    fun `sort orders across the whole table, not within a page`() =
        api { client ->
            // Ask for the single largest file. If the sort ran per page
            // this would return the largest of the FIRST page, not of the
            // table.
            val top = sizes(client.get("$filesUrl?sort=size&order=desc&limit=1"))
            val everySize = sizes(client.get(filesUrl))
            assertThat(top).containsExactly(everySize.max())

            // And the full descending order is the sorted set.
            val desc = sizes(client.get("$filesUrl?sort=size&order=desc"))
            assertThat(desc).isEqualTo(everySize.sortedDescending())
        }

    @Test
    fun `sort with paging stays consistent across pages`() =
        api { client ->
            // The concatenation of size-sorted pages equals one sorted
            // list: the tiebreak keeps the boundary stable.
            val page1 = sizes(client.get("$filesUrl?sort=size&order=asc&limit=10&offset=0"))
            val page2 = sizes(client.get("$filesUrl?sort=size&order=asc&limit=10&offset=10"))
            val page3 = sizes(client.get("$filesUrl?sort=size&order=asc&limit=10&offset=20"))
            assertThat(page1 + page2 + page3).isEqualTo(sizes(client.get(filesUrl)).sorted())
        }

    // NOTE: the data_file_id tiebreak (FileRepo.listAt) keeps offset
    // paging stable when the sort column ties. It is not exercised here
    // on purpose: against a small, freshly-loaded table Postgres returns
    // tied rows in an incidentally stable order, so removing the tiebreak
    // does not reproduce a duplicate/skip and any test claiming to prove
    // it would pass with the tiebreak gone. The guard is defensive
    // against plan changes and future indexes, where the stability is not
    // free — the distinct-value paging test above pins the observable
    // partition-cleanliness property.

    @Test
    fun `an unknown sort column is refused, not silently ignored`() =
        api { client ->
            val res = client.get("$filesUrl?sort=bogus")
            assertThat(res.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(res.bodyAsText()).contains("sort")
        }

    @Test
    fun `a bad order or negative limit is refused`() =
        api { client ->
            assertThat(client.get("$filesUrl?sort=size&order=sideways").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$filesUrl?limit=0").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$filesUrl?limit=5&offset=-1").status)
                .isEqualTo(HttpStatusCode.BadRequest)
        }

    // ---- keyset paging (`after`) -----------------------------------------

    @Test
    fun `after pages the table by id without gaps or overlaps`() =
        api { client ->
            val pageSize = 10
            val seen = mutableListOf<Long>()
            // after=0 is the first page: data_file_id allocation starts
            // at 1, so nothing is skipped.
            var after = 0L
            while (true) {
                val page = ids(client.get("$filesUrl?limit=$pageSize&after=$after"))
                if (page.isEmpty()) break
                assertThat(page.size).isLessThanOrEqualTo(pageSize)
                seen += page
                after = page.last()
                if (page.size < pageSize) break
            }
            // The keyset walk IS the ascending id order, and it partitions
            // the table exactly: the same cleanliness property the offset
            // walk pins, at a per-page cost that does not grow with depth.
            assertThat(seen).isEqualTo(ids(client.get(filesUrl)).sorted())
        }

    @Test
    fun `after without a limit returns everything past the key`() =
        api { client ->
            val all = ids(client.get(filesUrl)).sorted()
            val key = all[9]
            assertThat(ids(client.get("$filesUrl?after=$key"))).isEqualTo(all.drop(10))
        }

    @Test
    fun `after past the last id is an empty page, not an error`() =
        api { client ->
            val last = ids(client.get(filesUrl)).max()
            assertThat(ids(client.get("$filesUrl?after=$last&limit=10"))).isEmpty()
        }

    @Test
    fun `after composes with the snapshot filter`() =
        api { client ->
            // The seed commits one file per snapshot, so the tenth file's
            // begin_snapshot pins a read that sees exactly ten files.
            val pin =
                json.readTree(client.get(filesUrl).bodyAsText())[9]["begin_snapshot"].asLong()
            val visible = ids(client.get("$filesUrl?snapshot=$pin")).sorted()
            assertThat(visible).hasSize(10)

            // A keyset page at the pin starts after the key and stops at
            // the snapshot's last file, not the table's.
            val page = ids(client.get("$filesUrl?snapshot=$pin&after=${visible[3]}&limit=4"))
            assertThat(page).isEqualTo(visible.drop(4).take(4))
            assertThat(ids(client.get("$filesUrl?snapshot=$pin&after=${visible.last()}"))).isEmpty()
        }

    @Test
    fun `after refuses the order-bearing parameters it replaces`() =
        api { client ->
            // sort/order/offset all describe an order or a skip, which
            // keyset paging already defines — a 400, never a silently
            // ignored parameter.
            assertThat(client.get("$filesUrl?after=5&sort=id").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$filesUrl?after=5&order=asc").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$filesUrl?after=5&limit=10&offset=10").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$filesUrl?after=-1").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$filesUrl?after=soon").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            // offset=0 is the default, not a contradiction.
            assertThat(client.get("$filesUrl?after=5&offset=0&limit=1").status)
                .isEqualTo(HttpStatusCode.OK)
        }
}
