package com.posthog.hoglake.api

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.RetirementService
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.UploadService
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import io.ktor.client.HttpClient
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.parameter
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MinIOContainer
import java.util.UUID

/**
 * `DELETE /v1/catalogs/{catalog}` (#412) over HTTP, against the real
 * service, the real maintenance loops and a real object store.
 *
 * One case per refusal reason, each built the way production gets there
 * (the service calls, not seeded rows, wherever a service exists), each
 * asserting the 409's `reason` AND that the catalog and the blocking state
 * survived — a refusal that answered correctly after deleting something
 * would pass a status-only assertion. Then the metadata rules, the 404,
 * and the whole lifecycle to a successful drop, which asserts the cascade
 * by TABLE NAME against the live catalog of foreign keys.
 *
 * The concurrency cases are in `service.CatalogDropConcurrencyIntegrationTest`.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogDropApiIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)
    private val json = ObjectMapper()

    /**
     * Every loop off: the lifecycle case drives expiry, retirement and
     * cleanup by hand, and a background cleanup with no S3 credentials
     * would CLAIM its removal rows (a 900 s lease) and fail them.
     */
    private val app =
        App.build(
            Config(
                hydratorIntervalMs = 0,
                metricsIntervalMs = 0,
                expiryIntervalMs = 0,
                cleanupIntervalMs = 0,
                maintenanceSummaryIntervalMs = 0,
            ),
            jdbi,
        )

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        const val BUCKET = "hoglake-catalog-drop"
        const val AUTHOR = "operator/λ"
        const val MESSAGE = "retire the re-partition trial & reclaim"

        /**
         * The thirteen foreign keys to `hog_catalog`, BY NAME. The lifecycle
         * case compares this against `pg_constraint`, so a new table that
         * references the catalog fails it until it is added here — and it
         * must be added with `ON DELETE CASCADE`, which the same case
         * asserts, or the drop's DELETE would fail on it in production.
         */
        val CATALOG_FK_TABLES =
            setOf(
                "hog_snapshot",
                "hog_namespace",
                "hog_table",
                "hog_consumer_offset",
                "hog_view",
                "hog_file_removal",
                "hog_maintenance_run",
                "hog_maintenance_summary",
                "hog_maintenance_summary_tier",
                "hog_table_creation",
                "hog_commit_receipt",
                "hog_upload",
                "hog_compaction_claim",
            )

        val minio: MinIOContainer by lazy { TestImages.minio().also { it.start() } }

        val objects: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }

        val removals: RemovalStore by lazy {
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }
    }

    // ---- helpers -------------------------------------------------------------

    private fun api(block: suspend HttpClient.() -> Unit) =
        testApplication {
            application { app.module(this) }
            client.block()
        }

    private suspend fun HttpClient.drop(
        catalog: String,
        author: String? = AUTHOR,
        message: String? = MESSAGE,
    ): HttpResponse =
        delete("/v1/catalogs/$catalog") {
            author?.let { parameter("author", it) }
            message?.let { parameter("message", it) }
        }

    private suspend fun HttpResponse.body(): JsonNode = json.readTree(bodyAsText())

    private fun catalogRows(name: String): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT count(*) FROM hog_catalog WHERE name = :n")
                .bind("n", name).mapTo(Long::class.java).one()
        }

    private fun rows(
        table: String,
        catalogId: Long,
        where: String = "TRUE",
    ): Long =
        jdbi.withHandleUnchecked { h ->
            // `table` and `where` are test literals, never input.
            h.createQuery("SELECT count(*) FROM $table WHERE catalog_id = :c AND $where")
                .bind("c", catalogId).mapTo(Long::class.java).one()
        }

    /** A 409 catalog_not_empty with [reason], and the catalog still there. */
    private suspend fun HttpClient.assertRefused(
        catalog: String,
        reason: String,
    ): JsonNode {
        val response = drop(catalog)
        assertThat(response.status).isEqualTo(HttpStatusCode.Conflict)
        val body = response.body()
        assertThat(body["error"].asText()).isEqualTo("catalog_not_empty")
        assertThat(body["reason"].asText()).isEqualTo(reason)
        assertThat(catalogRows(catalog)).describedAs("a refusal deletes nothing").isEqualTo(1)
        assertThat(get("/v1/catalogs/$catalog").status).isEqualTo(HttpStatusCode.OK)
        return body
    }

    private fun create(name: String) = catalogs.createCatalog(name, "s3://$BUCKET/$name")

    private fun tableWithFile(
        catalog: String,
        file: String = "f.parquet",
    ) {
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        commits.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(TableAppend("ns", "t", listOf(FileRegistration("s3://$BUCKET/$catalog/$file", 10, 100)))),
            ),
        )
    }

    private fun dropTableAndNamespace(catalog: String) {
        catalogs.dropTable(catalog, "ns", "t", author = AUTHOR, message = "fixture: drop table")
        catalogs.dropNamespace(catalog, "ns", author = AUTHOR, message = "fixture: drop namespace")
    }

    // ---- one refusal per precondition ---------------------------------------

    @Test
    fun `a live namespace refuses the drop`() =
        api {
            create("cd-live-ns")
            catalogs.createNamespace("cd-live-ns", "main")
            ViewService(jdbi).create("cd-live-ns", "main", "v", "SELECT 1")
            val body = assertRefused("cd-live-ns", "live_namespaces")
            assertThat(body["detail"].asText()).contains("main")
            assertThat(catalogs.getNamespace("cd-live-ns", "main").name).isEqualTo("main")
        }

    @Test
    fun `a dropped table whose rows retirement has not reached refuses the drop`() =
        api {
            // The table is dropped ABOVE the floor (no retention, floor 0),
            // so RetirementService.ELIGIBLE_PREDICATE does not match it — and
            // every one of its file rows names an object nothing has queued.
            // MUTATION: restricting the live-files arm to ELIGIBLE_PREDICATE
            // (its floor clause) lets this drop through and orphans f.parquet.
            create("cd-retire")
            tableWithFile("cd-retire")
            dropTableAndNamespace("cd-retire")
            val id = catalogs.getCatalog("cd-retire").catalogId
            assertRefused("cd-retire", "tables_pending_retirement")
            assertThat(rows("hog_data_file", id, "end_snapshot IS NULL")).isEqualTo(1)
        }

    @Test
    fun `ended file rows expiry has not purged refuse the drop`() =
        api {
            // Truncate ENDS the rows; the drop then leaves no live row, so
            // retirement never touches this table — expiry's purge does,
            // once the floor passes the truncate. MUTATION: removing the
            // ended-files arm lets this drop through and orphans f.parquet.
            create("cd-expiry")
            tableWithFile("cd-expiry")
            val uuid = catalogs.getTable("cd-expiry", "ns", "t").tableUuid
            catalogs.truncateTable("cd-expiry", "ns", "t", uuid, AUTHOR, "fixture: truncate")
            dropTableAndNamespace("cd-expiry")
            val id = catalogs.getCatalog("cd-expiry").catalogId
            assertThat(rows("hog_data_file", id, "end_snapshot IS NULL")).isZero()
            assertRefused("cd-expiry", "files_pending_expiry")
            assertThat(rows("hog_data_file", id)).isEqualTo(1)
        }

    @Test
    fun `deletion vectors on ended data files refuse the drop as files_pending_expiry`() =
        api {
            // The Puffin half of the orphan hole: a vector riding a data file
            // that a truncate ENDED. Both rows survive the refusal.
            create("cd-dv")
            tableWithFile("cd-dv")
            val id = catalogs.getCatalog("cd-dv").catalogId
            val dataFileId =
                jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT data_file_id FROM hog_data_file WHERE catalog_id = :c")
                        .bind("c", id).mapTo(Long::class.java).one()
                }
            commits.commit(
                "cd-dv",
                CommitRequest(
                    readSnapshot = db.head("cd-dv"),
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(DeleteFileRegistration(dataFileId, "s3://$BUCKET/cd-dv/f.dv", 2, 16)),
                            ),
                        ),
                ),
            )
            val uuid = catalogs.getTable("cd-dv", "ns", "t").tableUuid
            catalogs.truncateTable("cd-dv", "ns", "t", uuid, AUTHOR, "fixture: truncate")
            dropTableAndNamespace("cd-dv")
            assertRefused("cd-dv", "files_pending_expiry")
            assertThat(rows("hog_delete_file", id)).describedAs("the vector survives the refusal").isEqualTo(1)
            assertThat(rows("hog_data_file", id)).isEqualTo(1)
        }

    @Test
    fun `a vector cannot outlive its data file, which is why no vector arm exists`() {
        // DROP_BLOCKERS_SQL has no hog_delete_file arm because a vector's
        // data_file_id is NOT NULL and a foreign key into hog_data_file, so
        // the data-file arms always fire first (CatalogRepo's KDoc). This
        // pins that premise: lose the foreign key and this reds, which is
        // the cue to put the vector arm back before Puffin objects can be
        // orphaned by a cascade.
        jdbi.useHandleUnchecked { h ->
            val fk =
                h.createQuery(
                    """
                    SELECT count(*) FROM pg_constraint
                    WHERE contype = 'f' AND conrelid = 'hog_delete_file'::regclass
                      AND confrelid = 'hog_data_file'::regclass
                      AND conkey = ARRAY[
                        (SELECT attnum FROM pg_attribute WHERE attrelid = 'hog_delete_file'::regclass
                           AND attname = 'catalog_id'),
                        (SELECT attnum FROM pg_attribute WHERE attrelid = 'hog_delete_file'::regclass
                           AND attname = 'data_file_id')]::int2[]
                    """,
                ).mapTo(Long::class.java).one()
            assertThat(fk).isEqualTo(1)
            val notNull =
                h.createQuery(
                    "SELECT attnotnull FROM pg_attribute WHERE attrelid = 'hog_delete_file'::regclass " +
                        "AND attname = 'data_file_id'",
                ).mapTo(Boolean::class.java).one()
            assertThat(notNull).isTrue()
        }
    }

    @Test
    fun `an unexpired compaction claim refuses the drop, an expired one cascades`() =
        api {
            // A claim is the only fence for a rewrite whose staging ticket
            // cleanup already drained `absent` (CompactionService re-stages
            // at commit). MUTATION: dropping the active_compaction_claims arm
            // lets this drop through mid-rewrite.
            create("cd-compaction")
            val id = catalogs.getCatalog("cd-compaction").catalogId
            jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "INSERT INTO hog_compaction_claim (catalog_id, table_id, group_key, input_file_ids, " +
                        "claimant, expires_at) VALUES (:c, 1, 'g', '{1}', :u, now() + interval '1 hour')",
                ).bind("c", id).bind("u", UUID.randomUUID()).execute()
            }
            assertRefused("cd-compaction", "active_compaction_claims")
            assertThat(rows("hog_compaction_claim", id)).isEqualTo(1)
            jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_compaction_claim SET expires_at = now() - interval '1 second' WHERE catalog_id = :c",
                )
                    .bind("c", id).execute()
            }
            assertThat(drop("cd-compaction").status).isEqualTo(HttpStatusCode.OK)
            assertThat(rows("hog_compaction_claim", id)).isZero()
        }

    @Test
    fun `an undrained removal row refuses the drop`() =
        api {
            // The production shape: an upload claim that expired, swept by
            // schedule-expired, which queues its path for cleanup.
            create("cd-queue")
            val uploads = UploadService(jdbi)
            val owner = UUID.randomUUID()
            uploads.claim("cd-queue", UUID.randomUUID(), owner, "s3://$BUCKET/cd-queue", "data")
            val id = catalogs.getCatalog("cd-queue").catalogId
            jdbi.useHandleUnchecked { h ->
                h.createUpdate("UPDATE hog_upload SET expires_at = now() - interval '1 hour' WHERE catalog_id = :c")
                    .bind("c", id).execute()
            }
            assertThat(uploads.scheduleExpired("cd-queue")).isEqualTo(1)
            assertRefused("cd-queue", "removals_pending")
            assertThat(rows("hog_file_removal", id, "drained_at IS NULL")).isEqualTo(1)
        }

    @Test
    fun `a consumer offset refuses the drop and names the consumer`() =
        api {
            create("cd-offset")
            catalogs.createNamespace("cd-offset", "ns")
            val table = catalogs.createTable("cd-offset", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            catalogs.commitOffset("cd-offset", "hedgerow-events", table.tableUuid, 1)
            // Offsets survive the drops on purpose (AGENT.md): the consumer
            // still has a position after its table is gone.
            dropTableAndNamespace("cd-offset")
            val body = assertRefused("cd-offset", "consumer_offsets")
            assertThat(body["detail"].asText()).contains("hedgerow-events")
            assertThat(rows("hog_consumer_offset", catalogs.getCatalog("cd-offset").catalogId)).isEqualTo(1)
        }

    @Test
    fun `the consumer sample marks truncation only when there are more than it names`() =
        api {
            // The SQL fetches cap + 1 names, so "..." is a fact about the
            // rows. MUTATION: fetching exactly the cap (and guessing at
            // `size >= cap`) prints "..." at exactly ten consumers.
            for ((catalog, consumers) in listOf("cd-ten" to 10, "cd-eleven" to 11)) {
                create(catalog)
                val id = catalogs.getCatalog(catalog).catalogId
                jdbi.useHandleUnchecked { h ->
                    h.createUpdate(
                        "INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot) " +
                            "SELECT :c, 'consumer-' || lpad(g::text, 2, '0'), gen_random_uuid(), 0 " +
                            "FROM generate_series(1, :n) g",
                    ).bind("c", id).bind("n", consumers).execute()
                }
                val detail = assertRefused(catalog, "consumer_offsets")["detail"].asText()
                assertThat(detail).contains("$consumers consumer offset(s)").contains("consumer-10")
                if (consumers == 10) {
                    assertThat(detail).doesNotContain("...")
                } else {
                    assertThat(detail).contains("consumer-10, ...").doesNotContain("consumer-11")
                }
            }
        }

    @Test
    fun `an active upload claim refuses the drop`() =
        api {
            create("cd-upload")
            UploadService(
                jdbi,
            ).claim("cd-upload", UUID.randomUUID(), UUID.randomUUID(), "s3://$BUCKET/cd-upload", "data")
            assertRefused("cd-upload", "active_uploads")
            assertThat(rows("hog_upload", catalogs.getCatalog("cd-upload").catalogId, "state = 'active'")).isEqualTo(1)
        }

    @Test
    fun `an abandoned upload never offered to cleanup refuses the drop`() =
        api {
            // Abandon fences the claim and queues NOTHING; only
            // schedule-expired offers its path to cleanup. MUTATION: dropping
            // the unscheduled arm lets the cascade orphan an object that may
            // well have been PUT.
            create("cd-tombstone")
            val uploads = UploadService(jdbi)
            val owner = UUID.randomUUID()
            val claim = uploads.claim("cd-tombstone", UUID.randomUUID(), owner, "s3://$BUCKET/cd-tombstone", "data")
            assertThat(uploads.abandon("cd-tombstone", owner, listOf(claim.path))).isEqualTo(1)
            val id = catalogs.getCatalog("cd-tombstone").catalogId
            assertRefused("cd-tombstone", "abandoned_uploads_unsettled")
            // Scheduling it moves the block to the queue, which is the chain
            // the reasons are ordered by.
            assertThat(uploads.scheduleExpired("cd-tombstone")).isEqualTo(1)
            assertRefused("cd-tombstone", "removals_pending")
            assertThat(rows("hog_upload", id, "state = 'abandoned'")).isEqualTo(1)
        }

    @Test
    fun `a scheduled tombstone refuses while its PUT can still land, and cascades once expired`() =
        api {
            // Abandoned, scheduled, and drained by cleanup (the object was
            // never PUT, so `absent`): every queue-side check passes. But the
            // claim is still inside its expiry window, so the writer's PUT
            // can land AFTER that DELETE, and once the catalog is gone
            // nothing would ever re-offer the path. MUTATION: dropping the
            // `expires_at > now()` term lets this drop through.
            create("cd-late-put")
            objects // the bucket, for the drain
            val uploads = UploadService(jdbi)
            val owner = UUID.randomUUID()
            val claim = uploads.claim("cd-late-put", UUID.randomUUID(), owner, "s3://$BUCKET/cd-late-put", "data")
            uploads.abandon("cd-late-put", owner, listOf(claim.path))
            assertThat(uploads.scheduleExpired("cd-late-put")).isEqualTo(1)
            CleanupService(jdbi, removals).runOnce("cd-late-put", batchSize = 100)
            val id = catalogs.getCatalog("cd-late-put").catalogId
            assertThat(rows("hog_file_removal", id, "drained_at IS NULL")).isZero()
            assertRefused("cd-late-put", "abandoned_uploads_unsettled")
            // Past expiry no PUT can land: the tombstone is settled and
            // cascades with the catalog.
            jdbi.useHandleUnchecked { h ->
                h.createUpdate("UPDATE hog_upload SET expires_at = now() - interval '1 second' WHERE catalog_id = :c")
                    .bind("c", id).execute()
            }
            assertThat(drop("cd-late-put").status).isEqualTo(HttpStatusCode.OK)
            assertThat(rows("hog_upload", id)).isZero()
        }

    @Test
    fun `the reason is the first failing precondition and the detail names every one`() =
        api {
            // MUTATION: reporting the LAST failing reason (or any order but
            // the enum's) reds the `reason` assertion.
            create("cd-several")
            catalogs.createNamespace("cd-several", "main")
            UploadService(
                jdbi,
            ).claim("cd-several", UUID.randomUUID(), UUID.randomUUID(), "s3://$BUCKET/cd-several", "data")
            val body = assertRefused("cd-several", "live_namespaces")
            assertThat(body["detail"].asText()).contains("live namespace").contains("active upload")
        }

    // ---- metadata, 404 ----------------------------------------------------------

    @Test
    fun `missing or blank author or message is a 422 that deletes nothing`() =
        api {
            // The catalog is EMPTY, so every 422 here is the metadata rule and
            // nothing else — the valid drop at the end proves the catalog was
            // droppable all along. Same vocabulary as
            // DdlMetadataIntegrationTest's destructive routes: Unicode spaces
            // and NEXT LINE count as blank, matching Python's str.strip().
            // MUTATION: moving requireOperationMetadata after the DELETE (or
            // dropping it) deletes the catalog on the first blank attempt.
            create("cd-metadata")
            for (field in listOf("author", "message")) {
                for (value in listOf(null, "", " \t\r\n", "\u2003\u00a0", "\u0085")) {
                    val response =
                        if (field == "author") {
                            drop(
                                "cd-metadata",
                                author = value,
                            )
                        } else {
                            drop("cd-metadata", message = value)
                        }
                    assertThat(
                        response.status,
                    ).describedAs("%s=%s", field, value).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    assertThat(response.body()["detail"].asText()).contains(field)
                    assertThat(catalogRows("cd-metadata")).isEqualTo(1)
                }
            }
            assertThat(drop("cd-metadata").status).isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `a blank-metadata drop audits validation, and a refused namespace drop audits refused`() {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val audit = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        audit.addAppender(capture)
        try {
            api {
                create("cd-audit-422")
                catalogs.createNamespace("cd-audit-422", "ns")
                catalogs.createTable("cd-audit-422", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                capture.list.clear()
                assertThat(drop("cd-audit-422", author = " ").status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                val line = capture.list.single { it.formattedMessage.startsWith("catalog_drop ") }
                assertThat(line.formattedMessage).isEqualTo("catalog_drop validation")
                // #412 review: NamespaceNotEmpty fell to Audit's `else` and
                // audited as "error", a server fault, for a plain refusal.
                capture.list.clear()
                val refused =
                    delete("/v1/catalogs/cd-audit-422/namespaces/ns") {
                        parameter("author", AUTHOR)
                        parameter("message", MESSAGE)
                    }
                assertThat(refused.status).isEqualTo(HttpStatusCode.Conflict)
                val ns = capture.list.single { it.formattedMessage.startsWith("namespace_drop ") }
                assertThat(ns.formattedMessage).isEqualTo("namespace_drop refused")
            }
        } finally {
            audit.detachAppender(capture)
        }
    }

    @Test
    fun `an unknown catalog is a 404`() =
        api {
            val response = drop("cd-never-existed")
            assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(response.body()["error"].asText()).isEqualTo("not_found")
        }

    // ---- the whole lifecycle --------------------------------------------------

    @Test
    fun `a catalog driven through retirement and cleanup drops, and the cascade leaves nothing`() {
        val name = "cd-lifecycle"
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val audit = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        audit.addAppender(capture)
        try {
            api {
                create(name)
                val id = catalogs.getCatalog(name).catalogId
                catalogs.createNamespace(name, "ns")
                val table =
                    catalogs.createTable(
                        name,
                        "ns",
                        "t",
                        listOf(ColumnDef("id", ColType.LONG, nullable = false)),
                    )
                // Two objects, one for each loop that queues paths: f0 is
                // ENDED by a truncate (expiry's purge queues it), f1 is live
                // at the drop (retirement queues it).
                val paths = listOf("s3://$BUCKET/$name/f0.parquet", "s3://$BUCKET/$name/f1.parquet")
                paths.forEach { objects.put(it, "bytes".toByteArray()) }
                // Idempotent, so it leaves a hog_commit_receipt to cascade.
                commits.commit(
                    name,
                    CommitRequest(
                        readSnapshot = db.head(name),
                        idempotencyKey = UUID.randomUUID(),
                        appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(paths[0], 10, 100)))),
                    ),
                )
                // More of the cascade's children, each through its own
                // service where one exists, so "the cascade leaves nothing"
                // is about rows that were there: a view (dropped below), a
                // prepared table creation, an upload tombstone that cleanup
                // drains, an EXPIRED compaction claim, and a sampler row.
                val views = ViewService(jdbi)
                views.create(name, "ns", "v", "SELECT 1")
                TableCreationService(jdbi, catalogs, commits).prepare(
                    name,
                    UUID.randomUUID(),
                    TableCreationDefinition("ns", "never_published", listOf(ColumnDef("id", ColType.LONG))),
                )
                val uploads = UploadService(jdbi)
                val owner = UUID.randomUUID()
                val tombstone = uploads.claim(name, UUID.randomUUID(), owner, "s3://$BUCKET/$name", "data")
                uploads.abandon(name, owner, listOf(tombstone.path))
                assertThat(uploads.scheduleExpired(name)).isEqualTo(1)
                jdbi.useHandleUnchecked { h ->
                    h.createUpdate(
                        "INSERT INTO hog_compaction_claim (catalog_id, table_id, group_key, input_file_ids, " +
                            "claimant, claimed_at, expires_at) VALUES (:c, :t, 'g', '{1}', :u, " +
                            "now() - interval '2 hours', now() - interval '1 hour')",
                    ).bind("c", id).bind("t", table.tableId).bind("u", UUID.randomUUID()).execute()
                    h.createUpdate(
                        "INSERT INTO hog_maintenance_summary (catalog_id) VALUES (:c)",
                    ).bind("c", id).execute()
                    h.createUpdate(
                        "INSERT INTO hog_maintenance_summary_tier (catalog_id, generation, bucket_key, table_id, " +
                            "quota, remaining, pending) VALUES (:c, 0, 'b', :t, 1, 1, 0)",
                    ).bind("c", id).bind("t", table.tableId).execute()
                }
                catalogs.truncateTable(name, "ns", "t", table.tableUuid, AUTHOR, "lifecycle: truncate")
                commits.commit(
                    name,
                    CommitRequest(
                        appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(paths[1], 10, 100)))),
                    ),
                )
                val dropSnapshot =
                    catalogs.dropTable(name, "ns", "t", author = AUTHOR, message = "lifecycle: drop table").snapshotId
                // Retention configured and every snapshot aged past it, the
                // state a production catalog reaches by waiting.
                jdbi.useHandleUnchecked { h ->
                    h.createUpdate(
                        "UPDATE hog_catalog SET snapshot_retention_seconds = 60, consumer_floor = false " +
                            "WHERE catalog_id = :c",
                    ).bind("c", id).execute()
                    h.createUpdate(
                        "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = :c",
                    )
                        .bind("c", id).execute()
                }
                ExpiryService(jdbi).runOnce(name, batchSize = 1000)
                assertThat(catalogs.getCatalog(name).earliestSnapshotId).isGreaterThanOrEqualTo(dropSnapshot)
                RetirementService(jdbi, batchSize = 100, pauseMs = 0).runOnce(name)
                ExpiryService(jdbi).runOnce(name, batchSize = 1000)
                assertThat(
                    rows("hog_data_file", id),
                ).describedAs("retirement and expiry removed every file row").isZero()
                assertThat(rows("hog_delete_file", id)).isZero()
                val cleaned = CleanupService(jdbi, removals).runOnce(name, batchSize = 100)
                // f0, f1, and the tombstone's path (never PUT, so absent).
                assertThat(cleaned.removed + cleaned.missing).isEqualTo(3)
                assertThat(rows("hog_file_removal", id, "drained_at IS NULL")).isZero()
                paths.forEach { assertThat(removals.exists(it)).describedAs("cleanup deleted %s", it).isFalse() }
                views.drop(name, "ns", "v", AUTHOR, "lifecycle: drop view")
                // The tombstone's claim expiring: the time a production
                // catalog waits out, after which no late PUT can land.
                jdbi.useHandleUnchecked { h ->
                    h.createUpdate(
                        "UPDATE hog_upload SET expires_at = now() - interval '1 second' WHERE catalog_id = :c",
                    )
                        .bind("c", id).execute()
                }
                // Every one of these has rows BEFORE the drop, so the cascade
                // assertions below are about rows that were there, not about
                // tables that were empty all along.
                // (Not hog_table_version / hog_column: expiry's fifth step
                // already deleted the dropped table's ended DDL rows below
                // the floor, which is invariant 5 working, not a gap here.)
                for (
                table in listOf(
                    "hog_snapshot", "hog_snapshot_change", "hog_namespace", "hog_table",
                    "hog_table_stats", "hog_view", "hog_file_removal", "hog_maintenance_run",
                    "hog_maintenance_summary", "hog_maintenance_summary_tier", "hog_table_creation",
                    "hog_commit_receipt", "hog_upload", "hog_compaction_claim",
                )
                ) {
                    assertThat(rows(table, id)).describedAs("%s seeded before the drop", table).isPositive()
                }
                // An object under the prefix that nothing tracks: the drop
                // must leave it exactly where it is.
                val stray = "s3://$BUCKET/$name/not-ours.bin"
                objects.put(stray, "x".toByteArray())

                // Still refused while the namespace is live — the last step.
                assertRefused(name, "live_namespaces")
                catalogs.dropNamespace(name, "ns", author = AUTHOR, message = "lifecycle: drop namespace")

                capture.list.clear()
                val response = drop(name)
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                val body = response.body()
                assertThat(body["name"].asText()).isEqualTo(name)
                assertThat(body["data_path"].asText()).isEqualTo("s3://$BUCKET/$name")
                assertThat(body.fieldNames().asSequence().toList()).containsExactlyInAnyOrder("name", "data_path")

                assertThat(get("/v1/catalogs/$name").status).isEqualTo(HttpStatusCode.NotFound)
                val listed = json.readTree(get("/v1/catalogs").bodyAsText()).map { it["name"].asText() }
                assertThat(listed).doesNotContain(name)
                // A second drop is a 404: there is nothing left to refuse.
                assertThat(drop(name).status).isEqualTo(HttpStatusCode.NotFound)
                assertThat(removals.exists(stray)).describedAs("the data path is not touched").isTrue()

                assertCascadeLeftNothing(id)

                val events = capture.list.filter { it.formattedMessage.startsWith("catalog_drop ") }
                assertThat(
                    events.map { it.formattedMessage },
                ).containsExactly("catalog_drop ok", "catalog_drop not_found")
                val args = events.first().argumentArray.map { it.toString() }
                assertThat(args).contains(
                    "action=catalog_drop",
                    "actor=$AUTHOR",
                    "outcome=ok",
                    "catalog=$name",
                    "operation_message=$MESSAGE",
                    "detail=data_path=s3://$BUCKET/$name",
                )
            }
        } finally {
            audit.detachAppender(capture)
        }
    }

    @Test
    fun `a refused drop audits outcome refused with the caller's attribution`() {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val audit = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        audit.addAppender(capture)
        try {
            api {
                create("cd-audit-refused")
                catalogs.createNamespace("cd-audit-refused", "ns")
                capture.list.clear()
                assertRefused("cd-audit-refused", "live_namespaces")
                val event = capture.list.single { it.formattedMessage.startsWith("catalog_drop ") }
                assertThat(event.formattedMessage).isEqualTo("catalog_drop refused")
                assertThat(event.argumentArray.map { it.toString() })
                    .contains("actor=$AUTHOR", "operation_message=$MESSAGE", "outcome=refused")
            }
        } finally {
            audit.detachAppender(capture)
        }
    }

    /**
     * The cascade, asserted from the database's own catalog rather than a
     * list this test keeps in step by hand: the foreign keys to
     * `hog_catalog` are exactly [CATALOG_FK_TABLES] and every one is
     * CASCADE, and EVERY `hog_*` table with a `catalog_id` column —
     * including the ones that reach the catalog only through another table
     * — has no row left for the dropped id.
     */
    private fun assertCascadeLeftNothing(catalogId: Long) =
        jdbi.useHandleUnchecked { h ->
            val fks =
                h.createQuery(
                    """
                    SELECT conrelid::regclass::text AS child, confdeltype::text AS action
                    FROM pg_constraint
                    WHERE contype = 'f' AND confrelid = 'hog_catalog'::regclass
                    """,
                ).map { rs, _ -> rs.getString("child") to rs.getString("action") }.list()
            assertThat(fks.map { it.first }.toSet())
                .describedAs("the foreign keys to hog_catalog; add a new one to CATALOG_FK_TABLES")
                .isEqualTo(CATALOG_FK_TABLES)
            assertThat(
                fks,
            ).describedAs("every catalog foreign key cascades").allSatisfy { assertThat(it.second).isEqualTo("c") }
            val withCatalogId =
                h.createQuery(
                    """
                    SELECT table_name FROM information_schema.columns
                    WHERE table_schema = current_schema() AND column_name = 'catalog_id'
                      AND table_name LIKE 'hog\_%' AND table_name <> 'hog_catalog'
                    ORDER BY table_name
                    """,
                ).mapTo(String::class.java).list()
            assertThat(withCatalogId).containsAll(CATALOG_FK_TABLES)
            for (table in withCatalogId) {
                assertThat(rows(table, catalogId)).describedAs("%s rows left for the dropped catalog", table).isZero()
            }
            assertThat(
                h.createQuery("SELECT count(*) FROM hog_catalog WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one(),
            ).isZero()
        }
}
