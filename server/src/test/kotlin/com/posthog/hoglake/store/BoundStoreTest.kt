package com.posthog.hoglake.store

import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.TableFileEnds
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.store.BoundStoreCalls.DATA_PATH
import com.posthog.hoglake.store.BoundStoreCalls.arguments
import com.posthog.hoglake.store.BoundStoreCalls.creationId
import com.posthog.hoglake.store.BoundStoreCalls.ops
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.security.interfaces.ECPublicKey
import java.util.UUID
import kotlin.reflect.KClass
import kotlin.reflect.full.declaredMemberFunctions

/**
 * A request's view of the store and of each sub-backend (BoundCatalogStore.kt,
 * BoundBackends.kt) holds every operation to the catalog it was handed out
 * for, and a scoped caller to the namespaces its token allows
 * (auth/StoreGrant.kt).
 *
 * Every method of the fifteen interfaces is driven, found by reflection, by
 * the rule BoundStoreCalls lists for it. The handlers here are the test's
 * own: they ask the views for whatever catalog and namespace a case names,
 * which is what a handler that passes other values than its route's would do.
 */
class BoundStoreTest {
    private val pair = TestTokens.keyPair()

    /** `mine` and `shared` are the default scoped token's (team 42 for write, team 7 for read); `plain` is uncontained. */
    private val contained =
        mapOf(
            "mine" to NamespaceContainment("team_id", 42),
            "shared" to NamespaceContainment("team_id", 7),
            "theirs" to NamespaceContainment("team_id", 99),
        )
    private val everyNamespace = listOf("mine", "shared", "theirs", "plain")

    /** READ in the route table, and open to a scoped caller by `cat` alone (AuthPolicy.SCOPED_ROUTES). */
    private val namespaces = "/v1/catalogs/alpha/namespaces"
    private val closed = "a scoped credential cannot use route /v1/catalogs/{catalog}/namespaces on 'alpha'"

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    private fun service(mode: AuthMode) =
        if (mode == AuthMode.OFF) {
            AuthService(mode, null)
        } else {
            AuthService(
                mode,
                JwtVerifier(
                    TestTokens.ISSUER,
                    TestTokens.AUDIENCE,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                    predicateFields = setOf("team_id"),
                ) { 1_700_000_100L },
            )
        }

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf("alpha" to listOf("admin"))))

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun scoped(
        catalogs: String = """{"alpha":["write"]}""",
        prd: String = """{"team_id":{"42":"write","7":"read"}}""",
    ) = TestTokens.mint(pair, TestTokens.scopedClaims(prd = prd, catalogs = catalogs))

    private fun callers() = listOf("tenant" to tenant(), "operator" to operator(), "scoped" to scoped())

    /**
     * One request under [mode] ([mode] null installs no auth plugin). Its
     * handler runs [block] with what the selector hands out for `alpha`.
     */
    private fun request(
        mode: AuthMode?,
        token: String?,
        stores: RecordingStores = RecordingStores(contained),
        path: String = namespaces,
        block: (Map<KClass<*>, Any>) -> Unit,
    ): Pair<HttpStatusCode, String> =
        serve(mode, token, stores, path) { selector, call ->
            block(BoundStoreCalls.views(selector, call, "alpha", stores))
        }

    private fun serve(
        mode: AuthMode?,
        token: String?,
        stores: RecordingStores,
        path: String,
        handler: (CatalogStoreSelector, ApplicationCall) -> Unit,
    ): Pair<HttpStatusCode, String> {
        var answer: Pair<HttpStatusCode, String>? = null
        testApplication {
            val selector = CatalogStoreSelector(stores.store)
            application {
                mode?.let { installAuth(service(it)) }
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    for (route in listOf("/v1/catalogs/{catalog}/namespaces", "/v1/catalogs", "/livez")) {
                        get(route) {
                            handler(selector, call)
                            call.respondText("done")
                        }
                    }
                }
            }
            val response = client.get(path) { token?.let { header(HttpHeaders.Authorization, "Bearer $it") } }
            answer = response.status to response.bodyAsText()
        }
        return answer!!
    }

    private fun SimpleMeterRegistry.outcomes(): Map<String, Double> =
        find("hoglake_auth_requests_total").counters()
            .groupBy { "${it.id.getTag("mode")}/${it.id.getTag("outcome")}" }
            .mapValues { (_, c) -> c.sumOf { it.count() } }

    private fun auditLines(block: () -> Unit): List<String> = auditRefusals(block).second

    private inline fun <reified T : Any> Map<KClass<*>, Any>.view(): T = getValue(T::class) as T

    // ---- the list of methods ------------------------------------------------

    @Test
    fun `every method of the store and of each sub-backend has a rule, and the selector hands out nothing else`() {
        // MUTATION: add a method to CatalogStore or to a sub-backend. The views
        // stop compiling (they delegate nothing implicitly); with the view
        // written and no rule listed, this fails.
        for ((type, byName) in BoundStoreCalls.rules) {
            assertThat(type.declaredMemberFunctions.map { it.name }).describedAs(type.simpleName)
                .containsExactlyInAnyOrderElementsOf(byName.keys)
        }
        assertThat(ops).hasSize(55)
        // Every sub-backend interface the selector hands out per catalog is one of them.
        val handedOut =
            CatalogStoreSelector::class.java.methods
                .filter { it.parameterTypes.size == 3 && it.parameterTypes[2] == Function0::class.java }
                .map { it.returnType.kotlin }.toSet()
        assertThat(BoundStoreCalls.rules.keys - CatalogStore::class).isEqualTo(handedOut).hasSize(14)
    }

    @Test
    fun `under every resolving mode the selector hands out a view of the store and of each sub-backend`() {
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL, AuthMode.WARN)) {
            for ((kind, token) in callers()) {
                val stores = RecordingStores(contained)
                val notViews = mutableListOf<String>()
                request(mode, token, stores) { views ->
                    for ((type, view) in views) {
                        // MUTATION: hand out the store itself for a tenant or an operator and this lists it.
                        val behind = if (type == CatalogStore::class) stores.store else null
                        if (view.plain() === view || (behind != null && view.plain() !== behind)) {
                            notViews += "${type.simpleName} as $kind under $mode"
                        }
                    }
                }
                assertThat(notViews).isEmpty()
                // Handing out a view reads nothing.
                assertThat(stores.calls).isEmpty()
            }
        }
    }

    @Test
    fun `with auth off, or with no plugin, the selector hands out the store and the backends themselves`() {
        for (mode in listOf(null, AuthMode.OFF)) {
            val stores = RecordingStores(contained)
            val views = mutableListOf<String>()
            val refused = mutableListOf<String>()
            request(mode, null, stores) { handed ->
                for ((type, view) in handed) if (view.plain() !== view) views += type.simpleName!!
                // Any catalog name, as before.
                for (op in ops) {
                    BoundStoreCalls.call(handed.getValue(op.type), op, arguments(op, "beta", "theirs"))
                        ?.let { refused += "$op: $it" }
                }
            }
            assertThat(views).describedAs("mode $mode").isEmpty()
            assertThat(refused).isEmpty()
            assertThat(stores.calls.map { it.name }).isEqualTo(ops.map { it.name })
        }
    }

    // ---- the catalog, for every caller kind ---------------------------------

    @Test
    fun `no caller kind gets another catalog from a store handed out for one`() {
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL, AuthMode.WARN)) {
            for ((kind, token) in callers()) {
                // `warn` lets a tenant and an operator through; a scoped caller is refused there too.
                if (mode == AuthMode.WARN && kind != "scoped") continue
                val stores = RecordingStores(contained)
                val wrong = mutableListOf<String>()
                request(mode, token, stores) { views ->
                    for (op in ops) {
                        val refusal = BoundStoreCalls.call(views.getValue(op.type), op, arguments(op, "beta", "mine"))
                        // A listing of catalogs is held to the token, and an operator's names every catalog.
                        if (op.rule == Rule.CATALOGS && kind == "operator") {
                            if (refusal != null) wrong += "$op as $kind: $refusal"
                            continue
                        }
                        // MUTATION: drop `open(name)` from a StoreGrant rule, or the
                        // rule from a view's method, and that method reaches `beta`.
                        if (refusal !is HoglakeException.NotFound || refusal.message != "catalog 'beta' not found") {
                            wrong += "$op as $kind under $mode: $refusal"
                        }
                    }
                }
                assertThat(wrong).isEmpty()
                val reached = if (kind == "operator") listOf("CatalogStore.listCatalogs") else emptyList()
                assertThat(stores.calls.map { it.name }).describedAs("$kind under $mode").isEqualTo(reached)
            }
        }
    }

    @Test
    fun `a tenant and an operator get every operation of their catalog, unchanged, and nothing is read besides`() {
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL, AuthMode.WARN)) {
            for ((kind, token) in callers().filter { it.first != "scoped" }) {
                val stores = RecordingStores(contained)
                val sent = mutableListOf<RecordingStores.Call>()
                val refused = mutableListOf<String>()
                request(mode, token, stores) { views ->
                    for (op in ops) {
                        // A namespace no scoped token here holds: it is not a tenant's or an operator's concern.
                        val given = arguments(op, "alpha", "theirs")
                        sent += RecordingStores.Call(op.name, given)
                        BoundStoreCalls.call(views.getValue(op.type), op, given)?.let { refused += "$op: $it" }
                    }
                }
                assertThat(refused).describedAs("$kind under $mode").isEmpty()
                // MUTATION: apply a namespace rule to a caller that is not scoped
                // and this shows a refusal, or a containment read that no one asked for.
                assertThat(stores.calls).describedAs("$kind under $mode").isEqualTo(sent)
            }
        }
    }

    @Test
    fun `under warn another catalog is counted and audited once for the request, and let through`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val stores = RecordingStores(contained)
        val refused = mutableListOf<String>()
        val lines =
            auditLines {
                request(AuthMode.WARN, tenant(), stores) { views ->
                    for (op in ops) {
                        // MUTATION: refuse under `warn` and every one of these is a 404.
                        BoundStoreCalls.call(views.getValue(op.type), op, arguments(op, "beta", "mine"))
                            ?.let { refused += "$op: $it" }
                    }
                }
            }
        assertThat(refused).isEmpty()
        assertThat(stores.calls.map { it.name }).isEqualTo(ops.map { it.name })
        // MUTATION: skip the count under `warn` and `not_entitled` is missing;
        // drop the once-per-request mark and it is one per method.
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0, "warn/not_entitled" to 1.0))
        assertThat(lines).singleElement().asString()
            .startsWith("auth_refused warn_allowed")
            .contains("catalog=beta", "catalog 'beta' not found", "at=store")
    }

    @Test
    fun `under warn a request the catalog check already counted is not counted again`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val stores = RecordingStores(contained)
        val lines =
            auditLines {
                // No caller on a public route: handing out the store counts `missing` (StoreEntitlement.kt).
                request(AuthMode.WARN, null, stores, path = "/livez") { views ->
                    for (op in ops) BoundStoreCalls.call(views.getValue(op.type), op, arguments(op, "beta", "mine"))
                }
            }
        assertThat(stores.calls).hasSize(ops.size)
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0, "warn/missing" to 1.0))
        assertThat(lines).singleElement().asString().contains("catalog=alpha")
    }

    @Test
    fun `a listing of catalogs is held to the names the token names`() {
        val wrong = mutableListOf<String>()
        for ((kind, token) in callers()) {
            val stores = RecordingStores(contained)
            request(AuthMode.ENFORCE, token, stores) { views ->
                val store = views.view<CatalogStore>()
                store.listCatalogs(setOf("alpha"))
                store.listCatalogs(emptySet())
                for (names in listOf(setOf("alpha", "beta"), null)) {
                    // MUTATION: let a null list through for a tenant and it lists every catalog.
                    val refusal = runCatching { store.listCatalogs(names) }.exceptionOrNull()
                    val expected = if (names == null) "catalog '*' not found" else "catalog 'beta' not found"
                    if (kind == "operator") {
                        if (refusal != null) wrong += "operator, $names: $refusal"
                    } else if (refusal !is HoglakeException.NotFound || refusal.message != expected) {
                        wrong += "$kind, $names: $refusal"
                    }
                }
            }
            assertThat(
                stores.count("CatalogStore.listCatalogs"),
            ).describedAs(kind).isEqualTo(if (kind == "operator") 4 else 2)
        }
        assertThat(wrong).isEmpty()
    }

    @Test
    fun `the catalog listing's store lists the token's catalogs and answers for no catalog`() {
        val wrong = mutableListOf<String>()
        for ((kind, token) in callers()) {
            val stores = RecordingStores(contained)
            serve(AuthMode.ENFORCE, token, stores, "/v1/catalogs") { selector, call ->
                val listing = selector.forListing(call)
                val store = checkNotNull(listing.store)
                // As the handler lists them (api/Routes.kt): the names the selector gives it.
                store.listCatalogs(listing.catalogs)
                // MUTATION: hand the listing the store itself and it answers for any catalog by name.
                val asked =
                    listOf(
                        runCatching { store.listCatalogs(setOf("alpha", "beta")) },
                        runCatching { store.getCatalog("alpha") },
                        runCatching { store.listNamespaces("beta") },
                        runCatching { store.storageUsage("alpha") },
                    ).map { it.exceptionOrNull()?.message }
                val expected =
                    listOf(
                        "catalog 'beta' not found".takeIf { kind != "operator" },
                        "catalog 'alpha' not found",
                        "catalog 'beta' not found",
                        "catalog 'alpha' not found",
                    )
                if (asked != expected) wrong += "$kind: $asked"
            }
            val listed = if (kind == "operator") 2 else 1
            assertThat(
                stores.calls.map { it.name },
            ).describedAs(kind).isEqualTo(List(listed) { "CatalogStore.listCatalogs" })
        }
        assertThat(wrong).isEmpty()
        // With auth off the listing gets the store itself, as before.
        val plain = RecordingStores(contained)
        serve(null, null, plain, "/v1/catalogs") { selector, call ->
            if (selector.forListing(call).store !== plain.store) wrong += "off: a view"
        }
        assertThat(wrong).isEmpty()
    }

    // ---- the namespaces, for a scoped caller --------------------------------

    /** Drive every method in every namespace as the scoped caller [token]; what did not go as its rule says. */
    private fun scopedSweep(
        mode: AuthMode,
        token: String,
        readable: Set<String>,
        writable: Set<String>,
    ): List<String> {
        val wrong = mutableListOf<String>()
        // What a check may read on the way to a refusal: never the operation itself.
        val lookups = setOf("CatalogStore.liveContainment", "CatalogStore.getCatalog", "TableCreationBackend.status")
        for (namespace in everyNamespace) {
            val stores = RecordingStores(contained)
            request(mode, token, stores) { views ->
                for (op in ops) {
                    stores.calls.clear()
                    val given = arguments(op, "alpha", namespace)
                    val refusal = BoundStoreCalls.call(views.getValue(op.type), op, given)
                    val reached = RecordingStores.Call(op.name, given) in stores.calls
                    val what = "$op in '$namespace' under $mode"
                    if (BoundStoreCalls.reaches(op, namespace, readable, writable)) {
                        // The lookup answers from what the request already read.
                        if (refusal != null || (!reached && op.rule != Rule.LOOKUP)) wrong += "$what: refused, $refusal"
                        continue
                    }
                    val expected =
                        if (op.rule == Rule.CLOSED) {
                            HoglakeException.Forbidden::class to closed
                        } else {
                            // MUTATION: answer a creation by id with its namespace's 404 and this differs.
                            HoglakeException.NotFound::class to BoundStoreCalls.missing(op, namespace)
                        }
                    if (refusal == null || refusal::class != expected.first || refusal.message != expected.second) {
                        wrong += "$what: answered $refusal"
                    }
                    // A creation is read before its namespace is known; the caller is not shown it.
                    if (reached && op.rule != Rule.ID_READ) wrong += "$what: reached the store"
                    stores.calls.filter { it.name !in lookups }.forEach { wrong += "$what: called ${it.name}" }
                }
            }
        }
        return wrong
    }

    @Test
    fun `a scoped caller gets a namespace its token allows and a 404 for any other, in every mode`() {
        // `warn` too: a verified scoped token is refused there as under `enforce`.
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL, AuthMode.WARN)) {
            // MUTATION: skip the namespace rule, or let it pass on a missing
            // containment, and `theirs` or `plain` is served. Check write as
            // read and `shared` (held for read) takes a write.
            assertThat(scopedSweep(mode, scoped(), readable = setOf("mine", "shared"), writable = setOf("mine")))
                .isEmpty()
        }
    }

    @Test
    fun `read and write come from the token, the lower of its catalog scope and its predicate`() {
        val everyTeam = """{"team_id":{"*":"write"}}"""
        val contained = setOf("mine", "shared", "theirs")
        // `cat` read: the predicate's write on team 42 is not a write.
        val catRead = scoped(catalogs = """{"alpha":["read"]}""")
        assertThat(scopedSweep(AuthMode.ENFORCE, catRead, readable = setOf("mine", "shared"), writable = emptySet()))
            .isEmpty()
        // Every value for write: every contained namespace, and still not an uncontained one.
        assertThat(scopedSweep(AuthMode.ENFORCE, scoped(prd = everyTeam), readable = contained, writable = contained))
            .isEmpty()
        // Every value for read under a `cat` write: nothing is a write.
        val everyTeamRead = scoped(prd = """{"team_id":{"*":"read"}}""")
        assertThat(scopedSweep(AuthMode.ENFORCE, everyTeamRead, readable = contained, writable = emptySet())).isEmpty()
    }

    @Test
    fun `a scoped caller's listing and containment lookup answer for its own namespaces only`() {
        var listed: List<String>? = null
        val held = mutableMapOf<String, NamespaceContainment?>()
        request(AuthMode.ENFORCE, scoped()) { views ->
            val store = views.view<CatalogStore>()
            // MUTATION: return the store's list as it is and `theirs` and `plain` are listed.
            listed = store.listNamespaces("alpha").map { it.name }
            // MUTATION: answer the store's containment as it is and `theirs` shows team 99.
            for (namespace in everyNamespace + "gone") held[namespace] = store.liveContainment("alpha", namespace)
        }
        assertThat(listed).containsExactly("mine", "shared")
        assertThat(held).isEqualTo(
            mapOf(
                "mine" to contained["mine"],
                "shared" to contained["shared"],
                "theirs" to null,
                "plain" to null,
                "gone" to null,
            ),
        )
    }

    @Test
    fun `a commit is held to every namespace it names`() {
        val stores = RecordingStores(contained)
        val refusals = mutableListOf<Throwable?>()

        fun append(namespace: String) = TableAppend(namespace, "t", emptyList())

        fun delete(namespace: String) = TableDeletes(namespace, "t", emptyList())

        fun ends(namespace: String) = TableFileEnds(namespace, "t", UUID(0, 2), allFiles = true)
        request(AuthMode.ENFORCE, scoped(), stores) { views ->
            val store = views.view<CatalogStore>()
            val own = listOf(append("mine"))
            val mine = CommitRequest(appends = own, deletes = listOf(delete("mine")), fileEnds = listOf(ends("mine")))
            store.commit("alpha", mine)
            // MUTATION: check the appends only, the first namespace only, or not the
            // tables whose files a replacement ends, and one of these commits.
            for (request in listOf(
                CommitRequest(appends = own, deletes = listOf(delete("theirs"))),
                CommitRequest(appends = listOf(append("mine"), append("shared"))),
                CommitRequest(appends = own, fileEnds = listOf(ends("theirs"))),
                CommitRequest(appends = own, fileEnds = listOf(ends("mine"), ends("shared"))),
            )) {
                refusals += runCatching { store.commit("alpha", request) }.exceptionOrNull()
            }
        }
        assertThat(refusals.map { it?.message }).containsExactly(
            "namespace 'theirs' in catalog 'alpha'",
            "namespace 'shared' in catalog 'alpha'",
            "namespace 'theirs' in catalog 'alpha'",
            "namespace 'shared' in catalog 'alpha'",
        )
        assertThat(refusals).allMatch { it is HoglakeException.NotFound }
        assertThat(stores.count("CatalogStore.commit")).isEqualTo(1)
    }

    @Test
    fun `partition stats with no namespace cover the catalog and are not a scoped caller's`() {
        val stores = RecordingStores(contained)
        var refusal: Throwable? = null
        request(AuthMode.ENFORCE, scoped(), stores) { views ->
            val stats = views.view<PartitionStatsBackend>()
            stats.partitionStats("alpha", "mine", null, 5)
            // MUTATION: treat a missing namespace as nothing to check and this reports the whole catalog.
            refusal = runCatching { stats.partitionStats("alpha", null, null, 5) }.exceptionOrNull()
        }
        assertThat(refusal).isInstanceOf(HoglakeException.Forbidden::class.java).hasMessage(closed)
        assertThat(stores.count("PartitionStatsBackend.partitionStats")).isEqualTo(1)
    }

    @Test
    fun `the lookup by value answers a scoped caller only for a value it may read`() {
        val found = mutableMapOf<Long, Pair<String, NamespaceContainment>?>()
        request(AuthMode.ENFORCE, scoped(), RecordingStores(contained)) { views ->
            val store = views.view<CatalogStore>()
            // MUTATION: drop the read filter in StoreGrant.containedFor and team 99 is answered.
            for (value in listOf(42L, 7L, 99L, 5L)) found[value] = store.liveContainedFor("alpha", "team_id", value)
        }
        assertThat(found).containsExactly(
            entry(42L, "mine" to contained.getValue("mine")),
            entry(7L, "shared" to contained.getValue("shared")),
            entry(99L, null),
            entry(5L, null),
        )
    }

    @Test
    fun `an upload claim is held to the namespace its prefix spells`() {
        val stores = RecordingStores(contained)
        val refusals = mutableListOf<Throwable?>()
        val id = UUID(0, 1)
        request(AuthMode.ENFORCE, scoped(), stores) { views ->
            val uploads = views.view<UploadBackend>()
            uploads.claim("alpha", id, id, "$DATA_PATH/data/mine/", "data")
            // MUTATION: claim without the prefix check and each of these is claimed.
            for (prefix in listOf(
                "$DATA_PATH/data/theirs/sub",
                "$DATA_PATH/staging/mine",
                "s3://bucket/other/data/mine",
            )) {
                refusals += runCatching { uploads.claim("alpha", id, id, prefix, "data") }.exceptionOrNull()
            }
            // Keyed by the owner the writer chose: open (AuthPolicy.SCOPED_ROUTES).
            uploads.renew("alpha", id)
            uploads.abandon("alpha", id, listOf("$DATA_PATH/data/theirs/x"))
        }
        val under =
            "a scoped credential's upload must be claimed under $DATA_PATH/data/<namespace>/, " +
                "or under its namespace's org prefix"
        assertThat(refusals.map { it?.message }).containsExactly("namespace 'theirs' in catalog 'alpha'", under, under)
        assertThat(refusals.map { it!!::class })
            .containsExactly(
                HoglakeException.NotFound::class,
                HoglakeException.Forbidden::class,
                HoglakeException.Forbidden::class,
            )
        assertThat(stores.calls.map { it.name }.filter { it.startsWith("UploadBackend") })
            .containsExactly("UploadBackend.claim", "UploadBackend.renew", "UploadBackend.abandon")
        // The data_path is read once for the request.
        assertThat(stores.count("CatalogStore.getCatalog")).isEqualTo(1)
    }

    @Test
    fun `a table creation that does not exist answers as it does for anyone`() {
        val stores = RecordingStores(contained)
        val refusals = mutableListOf<Throwable?>()
        val unknown = UUID(7, 7)
        request(AuthMode.ENFORCE, scoped(), stores) { views ->
            val creations = views.view<TableCreationBackend>()
            refusals += runCatching { creations.status("alpha", unknown) }.exceptionOrNull()
            refusals += runCatching { creations.abort("alpha", unknown) }.exceptionOrNull()
            refusals += runCatching { creations.publish("alpha", unknown, emptyList()) }.exceptionOrNull()
        }
        assertThat(refusals).allMatch {
            it is HoglakeException.NotFound && it.message == "table creation operation '$unknown'"
        }
        // MUTATION: abort or publish before the creation's namespace is known and the backend is asked to.
        assertThat(stores.calls.map { it.name }).containsOnly("TableCreationBackend.status")
    }

    @Test
    fun `a table creation in a namespace the token does not allow answers as one that does not exist`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val asks: Map<String, (TableCreationBackend, UUID) -> Unit> =
            mapOf(
                "status" to { creations, id -> creations.status("alpha", id) },
                "abort" to { creations, id -> creations.abort("alpha", id) },
                "publish" to { creations, id -> creations.publish("alpha", id, emptyList()) },
            )

        // The body a handler that asks [ask] for [id] answers, with the id taken out, and the audit's refusals.
        fun answer(
            ask: (TableCreationBackend, UUID) -> Unit,
            id: UUID,
        ): Pair<String, List<String>> {
            var answered: Pair<HttpStatusCode, String>? = null
            val lines = auditLines { answered = request(AuthMode.ENFORCE, scoped()) { views -> ask(views.view(), id) } }
            val (status, body) = checkNotNull(answered)
            assertThat(status).describedAs(body).isEqualTo(HttpStatusCode.NotFound)
            return body.replace(id.toString(), "<id>") to lines
        }
        for ((name, ask) in asks) {
            for (namespace in listOf("theirs", "plain")) {
                val (foreign, refused) = answer(ask, creationId(namespace))
                val (unknown, missing) = answer(ask, UUID.randomUUID())
                // MUTATION: answer with the namespace's 404 and the two bodies differ.
                assertThat(foreign).describedAs("$name in '$namespace'").isEqualTo(unknown)
                // The audit still names the namespace, at the store; a creation that does not exist is no refusal.
                assertThat(refused).singleElement().asString()
                    .contains("namespace '$namespace' in catalog 'alpha'", "at=store")
                assertThat(missing).isEmpty()
            }
        }
        assertThat(registry.outcomes()).isEqualTo(mapOf("enforce/ok" to 12.0, "enforce/not_entitled" to 6.0))
    }

    // ---- what a consistent request reads ------------------------------------

    @Test
    fun `a namespace's containment is read once for the request, whoever asks first`() {
        val stores = RecordingStores(contained)
        request(AuthMode.ENFORCE, scoped(), stores) { views ->
            val store = views.view<CatalogStore>()
            // As the namespace gate and `requireScopedNamespaces` ask it (auth/NamespaceGate.kt).
            store.liveContainment("alpha", "mine")
            store.getTable("alpha", "mine", "t")
            store.listFiles("alpha", "mine", "t")
            views.view<AlterBackend>().alterTable("alpha", "mine", "t", emptyList())
            store.commit("alpha", CommitRequest(appends = listOf(TableAppend("mine", "t", emptyList()))))
            // A refused namespace is remembered too.
            repeat(2) { runCatching { store.getTable("alpha", "theirs", "t") } }
        }
        // MUTATION: read the containment in each check and this is one per call.
        assertThat(stores.calls.filter { it.name == "CatalogStore.liveContainment" }.map { it.arguments })
            .containsExactly(listOf("alpha", "mine"), listOf("alpha", "theirs"))
    }

    @Test
    fun `an upload claim and a table creation's publication read nothing the handler's own check did not`() {
        val id = UUID(0, 1)
        // The handler's order: the data_path lookup of `requireScopedUploadPrefix`, then the claim.
        val claimed = RecordingStores(contained)
        request(AuthMode.ENFORCE, scoped(), claimed) { views ->
            views.view<CatalogStore>().getCatalog("alpha")
            views.view<CatalogStore>().liveContainment("alpha", "mine")
            views.view<UploadBackend>().claim("alpha", id, id, "$DATA_PATH/data/mine", "data")
        }
        assertThat(claimed.calls.map { it.name })
            .containsExactly("CatalogStore.getCatalog", "CatalogStore.liveContainment", "UploadBackend.claim")

        // The handler's order (api/TableCreationRoutes.kt): `requireScopedCreation` reads the creation's
        // namespace through the selector and checks it, then the handler publishes through its view.
        val published = RecordingStores(contained)
        serve(AuthMode.ENFORCE, scoped(), published, namespaces) { selector, call ->
            val backend = { published.backend(TableCreationBackend::class) as TableCreationBackend }
            val namespace = selector.creationNamespace(call, "alpha", creationId("mine"), backend)
            assertThat(namespace).isEqualTo("mine")
            selector.forCatalog(call, "alpha").liveContainment("alpha", namespace)
            selector.tableCreations(call, "alpha", backend).publish("alpha", creationId("mine"), emptyList())
        }
        // MUTATION: read the creation again before publishing and `status` is here twice.
        assertThat(published.calls.map { it.name })
            .containsExactly(
                "TableCreationBackend.status",
                "CatalogStore.liveContainment",
                "TableCreationBackend.publish",
            )

        // With no check before it, the view reads the creation itself, once for the request.
        val alone = RecordingStores(contained)
        request(AuthMode.ENFORCE, scoped(), alone) { views ->
            val creations = views.view<TableCreationBackend>()
            creations.publish("alpha", creationId("mine"), emptyList())
            creations.abort("alpha", creationId("mine"))
        }
        assertThat(alone.calls.map { it.name })
            .containsExactly(
                "TableCreationBackend.status",
                "CatalogStore.liveContainment",
                "TableCreationBackend.publish",
                "TableCreationBackend.abort",
            )
    }

    // ---- counting -----------------------------------------------------------

    @Test
    fun `a scoped caller's refusal is counted and audited once, against the namespace that was refused`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        var answer: Pair<HttpStatusCode, String>? = null
        val lines =
            auditLines {
                answer =
                    request(AuthMode.ENFORCE, scoped()) { views ->
                        val store = views.view<CatalogStore>()
                        // A handler that goes on after a refusal: the second one is not counted.
                        runCatching { store.getTable("alpha", "theirs", "t") }
                        runCatching { store.storageUsage("alpha") }
                        store.getTable("alpha", "theirs", "t")
                    }
            }
        // The answer is the one the namespace gate gives for a missing namespace.
        val (status, body) = checkNotNull(answer)
        assertThat(status).isEqualTo(HttpStatusCode.NotFound)
        assertThat(body).contains("not_found", "namespace 'theirs' in catalog 'alpha'")
        // The plugin allowed the route and counted `ok`; the store refused what the handler asked for.
        // MUTATION: drop the once-per-request mark and `not_entitled` is 2, beside a `forbidden`.
        assertThat(registry.outcomes()).isEqualTo(mapOf("enforce/ok" to 1.0, "enforce/not_entitled" to 1.0))
        assertThat(lines).singleElement().asString()
            .startsWith("auth_refused refused")
            .contains("catalog=alpha", "namespace 'theirs' in catalog 'alpha'", "at=store")
            .contains("pattern=/v1/catalogs/{catalog}/namespaces")
        assertThat(registry.find("hoglake_auth_requests_total").counters().flatMap { c -> c.id.tags.map { it.key } })
            .containsOnly("mode", "listener", "outcome", "client")
    }

    @Test
    fun `an operation closed to a scoped caller answers 403 and is counted as the plugin counts one`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val answer = request(AuthMode.WARN, scoped()) { views -> views.view<CatalogStore>().storageUsage("alpha") }
        assertThat(answer.first).isEqualTo(HttpStatusCode.Forbidden)
        assertThat(answer.second).contains(closed)
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0, "warn/forbidden" to 1.0))
        val denials = registry.find("hoglake_auth_denials_total").counters()
        assertThat(denials.map { "${it.id.getTag("catalog")}/${it.id.getTag("scope")}/${it.id.getTag("reason")}" })
            .containsExactly("alpha/read/forbidden")
    }
}
