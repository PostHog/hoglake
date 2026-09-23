package com.posthog.hoglake.api

import com.posthog.hoglake.BuildInfo
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.observability.CatalogTotals
import com.posthog.hoglake.observability.InstanceTotals
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID

/**
 * The /v1 routes, one-to-one with openapi/hoglake.yaml. Handlers only
 * translate wire <-> model and pick status codes; behavior (locking,
 * validation, conflict detection) lives in CatalogService/CommitService,
 * whose HoglakeExceptions are mapped by ErrorMapping.
 */
fun Application.installApiRoutes(
    catalogs: CatalogService,
    commits: CommitService,
    instanceName: String = "",
    instanceTotals: () -> InstanceTotals? = { null },
    catalogTotals: () -> Map<String, CatalogTotals> = { emptyMap() },
) {
    routing {
        get("/v1/info") {
            // Totals come from the metrics sampler's last pass — never
            // computed here (a live-manifest sum per request would tax
            // the commit tail's RDS at fleet scale). Null (fields
            // omitted) only in the boot window before the first sample.
            val totals = instanceTotals()
            call.respond(
                InstanceInfoDto(
                    name = instanceName.ifBlank { null },
                    version = BuildInfo.version,
                    build = BuildInfo.buildStamp,
                    totalRows = totals?.totalRows,
                    totalSizeBytes = totals?.totalSizeBytes,
                ),
            )
        }
        route("/v1/catalogs") {
            get {
                // Totals ride the sampler's last pass (catalogTotals), so
                // listing catalogs never sums the manifest per request.
                val totals = catalogTotals()
                call.respond(catalogs.listCatalogs().map { it.toDto(totals[it.name]) })
            }
            post {
                val req = call.receive<CreateCatalogRequestDto>()
                call.respond(
                    HttpStatusCode.Created,
                    catalogs.createCatalog(req.name, req.dataPath).toDto(),
                )
            }

            route("/{catalog}") {
                get {
                    call.respond(catalogs.getCatalog(call.catalog()).toDto())
                }

                route("/namespaces") {
                    get {
                        call.respond(catalogs.listNamespaces(call.catalog()).map { it.toDto() })
                    }
                    post {
                        val req = call.receive<CreateNamespaceRequestDto>()
                        call.respond(
                            HttpStatusCode.Created,
                            catalogs.createNamespace(call.catalog(), req.name).toDto(),
                        )
                    }
                    get("/{namespace}") {
                        call.respond(
                            catalogs.getNamespace(call.catalog(), call.namespace()).toDto(),
                        )
                    }
                    delete("/{namespace}") {
                        call.respond(
                            catalogs.dropNamespace(
                                call.catalog(),
                                call.namespace(),
                                call.longQuery("expected_namespace_id"),
                            ).toDto(),
                        )
                    }

                    route("/{namespace}/tables") {
                        get {
                            call.respond(
                                catalogs.listTables(call.catalog(), call.namespace())
                                    .map { it.toSummaryDto() },
                            )
                        }
                        post {
                            val req = call.receive<CreateTableRequestDto>()
                            call.respond(
                                HttpStatusCode.Created,
                                catalogs.createTable(
                                    call.catalog(),
                                    call.namespace(),
                                    req.name,
                                    req.columns.map { it.toModel() },
                                ).toDto(),
                            )
                        }

                        route("/{table}") {
                            get {
                                call.respond(
                                    catalogs.getTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            delete {
                                call.respond(
                                    catalogs.dropTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.expectedTableUuid(),
                                    ).toDto(),
                                )
                            }
                            post("/truncate") {
                                val expected =
                                    call.expectedTableUuid()
                                        ?: throw BadRequestException("expected_table_uuid is required")
                                call.respond(
                                    catalogs.truncateTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        expected,
                                    ).toDto(),
                                )
                            }
                            get("/files") {
                                val sort =
                                    call.request.queryParameters["sort"]?.let {
                                        FileRepo.FileSortColumn.fromWire(it)
                                            ?: throw BadRequestException(
                                                "query parameter 'sort' must be one of " +
                                                    FileRepo.FileSortColumn.entries.joinToString(", ") { c -> c.wire } +
                                                    ", got '$it'",
                                            )
                                    }
                                val desc =
                                    when (val o = call.request.queryParameters["order"]) {
                                        null, "asc" -> false
                                        "desc" -> true
                                        else ->
                                            throw BadRequestException(
                                                "query parameter 'order' must be 'asc' or 'desc', got '$o'",
                                            )
                                    }
                                // No 'limit' -> unbounded, the historical
                                // behavior every non-webui caller and the
                                // existing tests rely on. The webui always
                                // sends one.
                                val limit =
                                    call.intQuery("limit")?.also {
                                        if (it < 1) {
                                            throw BadRequestException(
                                                "query parameter 'limit' must be positive, got '$it'",
                                            )
                                        }
                                    }?.coerceAtMost(FILES_MAX_LIMIT)
                                val offset =
                                    call.intQuery("offset")?.also {
                                        if (it < 0) {
                                            throw BadRequestException(
                                                "query parameter 'offset' must be >= 0, got '$it'",
                                            )
                                        }
                                    } ?: 0
                                call.respond(
                                    catalogs.listFiles(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                        sort = sort,
                                        desc = desc,
                                        limit = limit,
                                        offset = offset,
                                    ).map { it.toDto() },
                                )
                            }
                            get("/files/{fileId}/stats") {
                                call.respond(
                                    catalogs.fileStats(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longPath("fileId"),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            get("/changes") {
                                val from =
                                    call.longQuery("from_snapshot")
                                        ?: throw BadRequestException(
                                            "missing required query parameter 'from_snapshot'",
                                        )
                                call.respond(
                                    catalogs.changes(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        from,
                                        call.longQuery("to_snapshot"),
                                    ).toDto(),
                                )
                            }
                        }
                    }
                }

                get("/snapshots") {
                    val after = call.longQuery("after") ?: 0L
                    val before = call.longQuery("before")
                    val limit = (call.intQuery("limit") ?: 1000).coerceAtMost(10_000)
                    val (page, hasMore) =
                        catalogs.listSnapshots(call.catalog(), after, limit, before)
                    call.respond(SnapshotPageDto(page.map { it.toDto() }, hasMore))
                }

                // Dedicated endpoint prevents old servers silently ignoring the
                // new request key and degrading retries to at-least-once.
                post("/commit/prepared") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation("idempotency_key is required")
                    }
                    call.respond(commits.commit(call.catalog(), req.toModel()).toDto())
                }

                // Distinct endpoint fences replicas that predate the DELETE contract.
                post("/commit/deletes/prepared") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null || req.appends.isNotEmpty() ||
                        req.deletes.isEmpty() || req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "prepared DELETE requires idempotency_key, read_snapshot, guarded deletes and no appends",
                        )
                    }
                    call.respond(commits.commit(call.catalog(), req.toModel(allowEmptyDeletes = true)).toDto())
                }

                // A separate endpoint fences old replicas and includes the stronger
                // conflict contract in the durable full-payload receipt.
                post("/commit/mutations/prepared") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null || req.deletes.isEmpty() ||
                        req.deletes.any { it.expectedTableUuid == null } ||
                        req.appends.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "prepared mutation requires idempotency_key, read_snapshot and guarded target tables",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true).copy(requireUnchangedTables = true),
                        ).toDto(),
                    )
                }

                post("/commit/uploads") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null ||
                        req.appends.any { it.expectedTableUuid == null } ||
                        req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "claimed uploads require a guarded idempotent commit",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true)
                                .copy(requireUnchangedTables = req.deletes.isNotEmpty()),
                        ).toDto(),
                    )
                }

                // requireUnchangedTables is unconditional here because the
                // transaction contract demands it, but it binds only the
                // DELETE targets (CommitService.checkConflicts): an
                // append-only transaction keeps the ordinary DDL-only
                // conflict rule and does not 409 on a concurrent INSERT.
                post("/commit/transaction") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null ||
                        req.appends.any { it.expectedTableUuid == null } ||
                        req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "transaction requires idempotency_key, read_snapshot and guarded targets",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true)
                                .copy(requireUnchangedTables = true, allowPendingDeletes = true),
                        ).toDto(),
                    )
                }

                get("/commit/receipts/{operation}") {
                    val operation =
                        try {
                            UUID.fromString(call.parameters["operation"])
                        } catch (_: IllegalArgumentException) {
                            throw BadRequestException("invalid operation UUID")
                        }
                    val receipt = commits.receipt(call.catalog(), operation)
                    call.respond(CommitReceiptDto(operation, receipt.snapshotId, receipt.schemaVersion))
                }

                post("/commit") {
                    val req = call.receive<CommitRequestDto>()
                    call.respond(commits.commit(call.catalog(), req.toModel()).toDto())
                }

                get("/consumers") {
                    call.respond(
                        catalogs.listConsumers(call.catalog()).toConsumerListDto(),
                    )
                }

                route("/consumers/{consumer}/offsets") {
                    get {
                        call.respond(
                            catalogs.listOffsets(call.catalog(), call.consumer())
                                .map { it.toDto() },
                        )
                    }
                    get("/{tableUuid}") {
                        call.respond(
                            catalogs.getOffset(
                                call.catalog(),
                                call.consumer(),
                                call.uuidPath("tableUuid"),
                            ).toDto(),
                        )
                    }
                    put("/{tableUuid}") {
                        val tableUuid = call.uuidPath("tableUuid")
                        val req = call.receive<CommitOffsetRequestDto>()
                        call.respond(
                            catalogs.commitOffset(
                                call.catalog(),
                                call.consumer(),
                                tableUuid,
                                req.snapshotId,
                            ).toDto(),
                        )
                    }
                }
            }
        }
    }
}

/**
 * GET .../tables/{table}/scan?snapshot= — read planning (openapi
 * planScan): data files paired with their visible deletion vectors.
 * Installed separately so App.kt wires it with its own ScanService.
 */
fun Application.installScanRoutes(scan: ScanService) {
    routing {
        get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan") {
            call.respond(
                scan.planScan(
                    call.catalog(),
                    call.namespace(),
                    call.table(),
                    call.longQuery("snapshot"),
                    call.instantQuery("at_timestamp"),
                ).map { it.toDto() },
            )
        }
    }
}

// ---- parameter helpers ---------------------------------------------------
// internal (not private): ViewRoutes.kt shares them.

internal fun ApplicationCall.pathParam(name: String): String =
    parameters[name] ?: throw BadRequestException("missing path parameter '$name'")

internal fun ApplicationCall.catalog() = pathParam("catalog")

internal fun ApplicationCall.namespace() = pathParam("namespace")

private fun ApplicationCall.table() = pathParam("table")

private fun ApplicationCall.consumer() = pathParam("consumer")

private fun ApplicationCall.longPath(name: String): Long {
    val raw = pathParam(name)
    return raw.toLongOrNull()
        ?: throw BadRequestException("path parameter '$name' must be an integer, got '$raw'")
}

private fun ApplicationCall.uuidPath(name: String): UUID {
    val raw = pathParam(name)
    return try {
        UUID.fromString(raw)
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("path parameter '$name' must be a UUID, got '$raw'")
    }
}

/** Hard cap on a single file page, matching the snapshots endpoint's cap. */
private const val FILES_MAX_LIMIT = 10_000

internal fun ApplicationCall.longQuery(name: String): Long? =
    request.queryParameters[name]?.let { parseLongQuery(name, it) }

internal fun parseLongQuery(
    name: String,
    raw: String,
): Long = raw.toLongOrNull() ?: throw BadRequestException("query parameter '$name' must be an integer")

private fun ApplicationCall.intQuery(name: String): Int? =
    request.queryParameters[name]?.let {
        it.toIntOrNull()
            ?: throw BadRequestException("query parameter '$name' must be an integer, got '$it'")
    }

/** ISO-8601 instant query parameter (e.g. 2026-09-04T12:00:00Z); unparseable -> 400. */
private fun ApplicationCall.instantQuery(name: String): Instant? =
    request.queryParameters[name]?.let {
        try {
            Instant.parse(it)
        } catch (_: DateTimeParseException) {
            throw BadRequestException(
                "query parameter '$name' must be an ISO-8601 instant, got '$it'",
            )
        }
    }

/** Optional on legacy DDL endpoints; mandatory for truncate. */
internal fun ApplicationCall.expectedTableUuid(): UUID? {
    val raw = request.queryParameters["expected_table_uuid"] ?: return null
    return parseExpectedTableUuid(raw)
}

internal fun parseExpectedTableUuid(raw: String): UUID =
    try {
        UUID.fromString(raw)
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("expected_table_uuid must be a UUID")
    }
