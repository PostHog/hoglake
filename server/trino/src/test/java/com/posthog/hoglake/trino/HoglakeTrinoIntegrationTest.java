package com.posthog.hoglake.trino;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.posthog.hoglake.trino.testing.TestHoglakeServer;
import com.posthog.hoglake.trino.testing.TestParquet;
import com.posthog.hoglake.trino.testing.TestPuffin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The milestone gate: Trino reads a hoglake table through the native
 * connector. Full stack — Postgres + the hoglake server in-process,
 * MinIO holding real parquet (written with Hardwood, the same writer the
 * root hydrator tests use), and a Trino fork image containing the
 * bundled Hoglake connector, its catalog pointed back at the
 * host via host.testcontainers.internal.
 */
@Tag("integration")
class HoglakeTrinoIntegrationTest
{
    private static final String BUCKET = "hoglake-trino";
    private static final int ROWS = TestParquet.ROWS;
    private static final long EPOCH0 = TestParquet.EPOCH0;

    /**
     * The deleted file row positions of {@code deleted_events} — 0-based
     * physical ordinals in its single data file, not hoglake row ids. The
     * fixture writes row groups of 10, so its groups are [0,9], [10,19] and
     * [20,24]: this set deletes the first and last row of the file, both
     * rows straddling each row-group boundary, and position 13, the
     * fixture's only null {@code name}. A vector that were applied one
     * batch late, or applied to a post-pruning row number, lands on a
     * different set than this one.
     */
    private static final List<Long> DELETED_POSITIONS = List.of(0L, 9L, 10L, 13L, 19L, 20L, 24L);

    /** Every row of {@code deleted_events} the vector leaves visible. */
    private static final List<Long> SURVIVING_IDS = LongStream.range(0, ROWS)
            .filter(position -> !DELETED_POSITIONS.contains(position))
            .boxed()
            .toList();

    /** A vector over every row of {@code fully_deleted_events}' one file. */
    private static final List<Long> ALL_POSITIONS = LongStream.range(0, ROWS).boxed().toList();

    /** Tables created up front, before the connector is ever queried. */
    private static final List<String> SEEDED_TABLES = List.of(
            "events",
            "deleted_events",
            "fully_deleted_events",
            "mismatched_dv_events",
            "misreferenced_dv_events",
            "multi_file_events");

    /** Tables the schema-evolution tests create as they run. */
    private static final List<String> EVOLVED_TABLES = List.of("renamed_events", "int_events");

    /**
     * The data file {@code misreferenced_dv_events}' vector wrongly claims
     * to belong to. Nothing registers it and no object exists at it, so the
     * only place this string can come from is the vector's own bytes.
     */
    private static final String MISREFERENCED_TARGET =
            "s3://" + BUCKET + "/nonexistent/never-registered.parquet";

    /** The JDBC driver's own prefix on every failure message. */
    private static final Pattern JDBC_PREAMBLE = Pattern.compile("^Query failed \\(#[^)]*\\): ");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static PostgreSQLContainer<?> postgres;
    private static MinIOContainer minio;
    private static TrinoContainer trino;
    private static TestHoglakeServer server;

    @BeforeAll
    static void setUpStack()
            throws Exception
    {
        // Same major as PgTestSupport's default: this harness backs the
        // server with its own container, so leaving it behind would make
        // the connector job the one place hoglake runs on an old Postgres.
        postgres = new PostgreSQLContainer<>("postgres:18")
                .withUsername("hoglake")
                .withPassword("hoglake");
        // PGSTY Silo, the maintained MinIO build (MinIO's own images are no
        // longer pullable). Same pin as the server suite's TestImages.SILO.
        minio = new MinIOContainer(
                DockerImageName.parse("docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z@sha256:635197cb9f36d01bee221d34d1c7d7960f6a95c48b0b6c01d99cd13bdae51a46")
                        .asCompatibleSubstituteFor("minio/minio"));
        postgres.start();
        minio.start();

        server = TestHoglakeServer.start(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        byte[] parquet = TestParquet.writeSampleParquet();
        uploadParquet(parquet);
        seedCatalog(parquet.length);

        startTrino();
    }

    @AfterAll
    static void tearDownStack()
    {
        if (trino != null) {
            trino.stop();
        }
        if (server != null) {
            server.close();
        }
        if (minio != null) {
            minio.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    private static void uploadParquet(byte[] parquet)
    {
        try (S3Client s3 = s3Client()) {
            s3.createBucket(b -> b.bucket(BUCKET));
            for (String table : SEEDED_TABLES) {
                s3.putObject(b -> b.bucket(BUCKET).key(table + "/part-0.parquet"),
                        RequestBody.fromBytes(parquet));
            }
            // multi_file_events gets a second, byte-identical file.
            s3.putObject(b -> b.bucket(BUCKET).key("multi_file_events/part-1.parquet"),
                    RequestBody.fromBytes(parquet));
        }
    }

    private static S3Client s3Client()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.getUserName(), minio.getPassword())))
                .forcePathStyle(true)
                .build();
    }

    private static void uploadObject(String key, byte[] bytes)
    {
        try (S3Client s3 = s3Client()) {
            s3.putObject(b -> b.bucket(BUCKET).key(key), RequestBody.fromBytes(bytes));
        }
    }

    /** Catalog + namespace + the seeded tables; commits with footer stats. */
    private static void seedCatalog(long fileSize)
            throws Exception
    {
        post("/v1/catalogs",
                "{\"name\": \"lake\", \"data_path\": \"s3://" + BUCKET + "/\"}", 201);
        post("/v1/catalogs/lake/namespaces", "{\"name\": \"analytics\"}", 201);

        for (String table : SEEDED_TABLES) {
            createTable(table);
        }

        commitAppend("events", s3("events/part-0.parquet"), fileSize);
        seedDeletionVector("deleted_events", fileSize, DELETED_POSITIONS, DELETED_POSITIONS.size());
        seedDeletionVector("fully_deleted_events", fileSize, ALL_POSITIONS, ALL_POSITIONS.size());
        // A vector the catalog misdescribes: the bitmap deletes three rows,
        // the registration claims four. The server never opens DV files, so
        // it accepts the registration and only the reader can catch it.
        seedDeletionVector("mismatched_dv_events", fileSize, List.of(1L, 2L, 3L), 4);
        seedMisreferencedDeletionVector(fileSize);
        seedMultiFileTable(fileSize);
    }

    private static String s3(String key)
    {
        return "s3://" + BUCKET + "/" + key;
    }

    /**
     * Appends the sample parquet to {@code table}, writes a real puffin
     * deletion vector over {@code positions} beside it, and registers that
     * vector against the file. {@code declaredDeleteCount} is what the
     * catalog is told, normally the vector's own cardinality.
     *
     * <p>The bytes are the real encoding, not a placeholder: the connector
     * opens this object, decodes the roaring bitmap, and checks its
     * {@code referenced-data-file} and cardinality against the scan's
     * pairing, so a nominal path would only ever produce a read failure.
     */
    private static void seedDeletionVector(
            String table, long dataFileSize, List<Long> positions, int declaredDeleteCount)
            throws Exception
    {
        String dataFilePath = s3(table + "/part-0.parquet");
        long snapshot = commitAppend(table, dataFilePath, dataFileSize);
        registerDeletionVector(table, snapshot, dataFilePath, table + "/part-0.dv",
                TestPuffin.deletionVector(dataFilePath, positions), declaredDeleteCount);
    }

    /**
     * A vector whose blob names a data file it was not registered against.
     * Its cardinality agrees with the registration, so the pairing is the
     * only thing wrong and the blob's {@code referenced-data-file} is the
     * only thing that can catch it.
     *
     * <p>The name it carries is deliberately a path no table registered and
     * no object exists at: pointing it at another table's live file would
     * put a second table inside this test's blast radius for nothing.
     */
    private static void seedMisreferencedDeletionVector(long fileSize)
            throws Exception
    {
        String table = "misreferenced_dv_events";
        String dataFilePath = s3(table + "/part-0.parquet");
        long snapshot = commitAppend(table, dataFilePath, fileSize);
        registerDeletionVector(table, snapshot, dataFilePath, table + "/part-0.dv",
                TestPuffin.deletionVector(MISREFERENCED_TARGET, List.of(1L, 2L, 3L)), 3);
    }

    /**
     * Two identical appends of the same 25-row fixture, with a vector over
     * the SECOND file only. This is the one fixture where file-relative and
     * table-global row numbering differ: every other table in this suite
     * holds exactly one file starting at position 0, where the two
     * numberings are identical and neither can be distinguished from the
     * other. Production tables are always multi-file.
     *
     * <p>Limitation: the two files are byte-identical, which is what makes
     * the numbering axis clean to read but also means applying the vector
     * to part-0 instead of part-1 produces the same answers. This fixture
     * separates file-relative from table-global numbering; it does NOT
     * detect a split-to-vector mis-assignment. That is covered instead by
     * the connector's {@code referenced-data-file} check, which
     * {@code deletionVectorNamingAnotherDataFileFailsTheQuery} asserts.
     */
    private static void seedMultiFileTable(long fileSize)
            throws Exception
    {
        String table = "multi_file_events";
        commitAppend(table, s3(table + "/part-0.parquet"), fileSize);
        String second = s3(table + "/part-1.parquet");
        long snapshot = commitAppend(table, second, fileSize);
        registerDeletionVector(table, snapshot, second, table + "/part-1.dv",
                TestPuffin.deletionVector(second, DELETED_POSITIONS), DELETED_POSITIONS.size());
    }

    /** Uploads a vector's bytes and registers it against one data file. */
    private static void registerDeletionVector(
            String table,
            long snapshot,
            String dataFilePath,
            String vectorKey,
            byte[] vector,
            int declaredDeleteCount)
            throws Exception
    {
        uploadObject(vectorKey, vector);
        post("/v1/catalogs/lake/commit",
                """
                {
                  "read_snapshot": %d,
                  "deletes": [{
                    "namespace": "analytics", "table": "%s",
                    "files": [{
                      "data_file_id": %d,
                      "path": "%s",
                      "delete_count": %d,
                      "file_size_bytes": %d
                    }]
                  }]
                }
                """.formatted(snapshot, table, dataFileId(table, dataFilePath), s3(vectorKey),
                        declaredDeleteCount, vector.length),
                200);
    }

    /** The catalog's id for {@code table}'s registered file at {@code path}. */
    private static long dataFileId(String table, String path)
            throws Exception
    {
        for (JsonNode file : JSON.readTree(
                get("/v1/catalogs/lake/namespaces/analytics/tables/" + table + "/files"))) {
            if (path.equals(file.get("path").asText())) {
                return file.get("data_file_id").asLong();
            }
        }
        throw new IllegalStateException("no data file " + path + " registered for " + table);
    }

    private static void createTable(String name)
            throws Exception
    {
        post("/v1/catalogs/lake/namespaces/analytics/tables",
                """
                {
                  "name": "%s",
                  "columns": [
                    {"name": "id", "type": "long", "nullable": false},
                    {"name": "score", "type": "double"},
                    {"name": "name", "type": "string"},
                    {"name": "ts", "type": "timestamptz"}
                  ]
                }
                """.formatted(name),
                201);
    }

    private static long commitAppend(String table, String path, long fileSize)
            throws Exception
    {
        // Footer-derived stats shipped with the commit (value/null counts;
        // bounds are optional and omitted).
        String body = """
                {
                  "appends": [{
                    "namespace": "analytics", "table": "%s",
                    "files": [{
                      "path": "%s",
                      "record_count": %d,
                      "file_size_bytes": %d,
                      "column_stats": [
                        {"field_id": 1, "value_count": 25, "null_count": 0},
                        {"field_id": 2, "value_count": 25, "null_count": 5},
                        {"field_id": 3, "value_count": 25, "null_count": 1},
                        {"field_id": 4, "value_count": 25, "null_count": 0}
                      ]
                    }]
                  }],
                  "author": "trino-integration-test"
                }
                """.formatted(table, path, ROWS, fileSize);
        JsonNode result = JSON.readTree(post("/v1/catalogs/lake/commit", body, 200));
        return result.get("snapshot_id").asLong();
    }

    private static void startTrino()
    {
        String image = System.getProperty("hoglake.trino.image");
        assertThat(image).as("hoglake.trino.image system property").isNotBlank();

        int minioPort = minio.getMappedPort(9000);
        org.testcontainers.Testcontainers.exposeHostPorts(server.getPort(), minioPort);

        String catalogProperties = """
                connector.name=hoglake
                hoglake.uri=http://host.testcontainers.internal:%d
                hoglake.catalog=lake
                hoglake.s3.endpoint=http://host.testcontainers.internal:%d
                hoglake.s3.region=us-east-1
                hoglake.s3.access-key=%s
                hoglake.s3.secret-key=%s
                hoglake.s3.path-style=true
                """.formatted(server.getPort(), minioPort, minio.getUserName(), minio.getPassword());

        trino = new TrinoContainer(DockerImageName.parse(image).asCompatibleSubstituteFor("trinodb/trino"))
                .withCopyToContainer(
                        Transferable.of(catalogProperties.getBytes(UTF_8)),
                        "/etc/trino/catalog/hoglake.properties")
                .withStartupTimeout(Duration.ofMinutes(5));
        trino.start();
        awaitCatalogSchedulable();
    }

    /**
     * "SERVER STARTED" precedes the catalog-to-node announcement by a few
     * seconds; until it lands, any distributed query fails with "No nodes
     * available to run query". Wait for the catalog to become schedulable
     * — any other failure is a real bug and surfaces immediately.
     */
    private static void awaitCatalogSchedulable()
    {
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (true) {
            try {
                query("SELECT count(*) FROM hoglake.analytics.events");
                return;
            }
            catch (SQLException e) {
                if (!String.valueOf(e.getMessage()).contains("No nodes available")) {
                    throw new IllegalStateException("hoglake catalog failed its first read", e);
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("hoglake catalog never became schedulable", e);
                }
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        }
    }

    // ---- REST plumbing -----------------------------------------------------

    private static String post(String path, String body, int expectedStatus)
            throws Exception
    {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(server.getBaseUri() + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as("POST %s -> %s", path, response.body())
                .isEqualTo(expectedStatus);
        return response.body();
    }

    private static String get(String path)
            throws Exception
    {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(server.getBaseUri() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("GET %s -> %s", path, response.body()).isEqualTo(200);
        return response.body();
    }

    private static Connection connect()
            throws SQLException
    {
        return DriverManager.getConnection(trino.getJdbcUrl(), "test", null);
    }

    // ---- the assertions ----------------------------------------------------

    @Test
    void showSchemasSeesHoglakeNamespaces()
            throws Exception
    {
        assertThat(query("SHOW SCHEMAS FROM hoglake"))
                .extracting(row -> row.get("Schema"))
                .contains("analytics");
    }

    @Test
    void showTablesSeesHoglakeTables()
            throws Exception
    {
        List<Object> tables = query("SHOW TABLES FROM hoglake.analytics").stream()
                .map(row -> row.get("Table"))
                .toList();
        // Every seeded table is listed. The schema-evolution tests create two
        // more as they run, so an exact match here would depend on test
        // order — but containment alone would not notice the connector
        // LEAKING a table, which matters now that it can CREATE TABLE. The
        // subset bound catches that without the order dependency: nothing
        // may appear here that this harness did not create.
        assertThat(tables).containsAll(SEEDED_TABLES);
        assertThat(tables).isSubsetOf(
                Stream.concat(SEEDED_TABLES.stream(), EVOLVED_TABLES.stream()).toArray());
    }

    @Test
    void describeMapsTypes()
            throws Exception
    {
        List<Map<String, Object>> columns = query("DESCRIBE hoglake.analytics.events");
        assertThat(columns).extracting(row -> row.get("Column"))
                .containsExactly("id", "score", "name", "ts");
        assertThat(columns).extracting(row -> row.get("Type"))
                .containsExactly("bigint", "double", "varchar", "timestamp(6) with time zone");
    }

    @Test
    void countStar()
            throws Exception
    {
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(25L));
    }

    @Test
    void projectionAndRowValues()
            throws Exception
    {
        List<Map<String, Object>> rows = query("""
                SELECT id, name, score, to_unixtime(ts) AS epoch
                FROM hoglake.analytics.events
                ORDER BY id
                """);
        assertThat(rows).hasSize(ROWS);
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> row = rows.get(i);
            assertThat(row.get("id")).isEqualTo((long) i);
            assertThat(row.get("name")).isEqualTo(i == 13 ? null : String.format("row-%02d", i));
            assertThat(row.get("score")).isEqualTo(i % 5 == 0 ? null : i * 1.5);
            assertThat(row.get("epoch")).isEqualTo((double) (EPOCH0 + i));
        }
    }

    @Test
    void filteredProjection()
            throws Exception
    {
        // This asserts the result only. Where the predicate is evaluated is
        // not observable from here: the connector pushes it into Parquet
        // row-group pruning (PostHog/trino f3bddd3) and Trino re-applies it
        // above the scan, and the answer is the same either way.
        assertThat(query("SELECT name, score FROM hoglake.analytics.events WHERE id = 7"))
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.get("name")).isEqualTo("row-07");
                    assertThat(row.get("score")).isEqualTo(10.5);
                });
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.events WHERE score IS NULL"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(5L));
    }

    /**
     * The field-id guard's inline-stats blind spot, end to end.
     *
     * <p>The guard itself SHIPPED: {@code AlterService} refuses
     * {@code rename_column} with 409 {@code idless_files_present} while any
     * live file is flagged {@code missing_field_ids} or is still
     * {@code pending}. This test is not evidence against it — it is the
     * documented hole in it (README "The field-id contract's blind spot"):
     * {@code missing_field_ids} is written only by the hydrator's footer
     * read, and the hydrator only sweeps {@code pending} files. A commit
     * that ships inline {@code column_stats} lands {@code provided}, never
     * reaches the hydrator, and so is invisible to the guard no matter what
     * its parquet schema actually contains.
     *
     * <p>That is this table: Hardwood writes no {@code PARQUET:field_id},
     * {@code commitAppend} ships inline stats, the rename is allowed, and
     * the connector's name-fallback binding then misses — every row of the
     * renamed column reads NULL. Closing the blind spot (the open work) is
     * what changes this test; the guard landing already happened.
     */
    @Test
    void renameColumnEvadesTheFieldIdGuardViaInlineStats()
            throws Exception
    {
        createTable("renamed_events");
        byte[] parquet = TestParquet.writeSampleParquet();
        uploadObject("renamed_events/part-0.parquet", parquet);
        commitAppend("renamed_events",
                "s3://" + BUCKET + "/renamed_events/part-0.parquet", parquet.length);

        // Half the premise, asserted rather than assumed. The guard refuses
        // when missing_field_ids OR stats_state = 'pending'; this pins the
        // second disjunct only — inline stats land 'provided', so the
        // pending branch cannot fire and the hydrator never reads this
        // file's footer. A change that made these commits defer their stats
        // reds HERE.
        //
        // The first disjunct is not observable from the wire at all:
        // DataFileDto carries no missing_field_ids. So a change that flips
        // it — hydrating provided files, or checking ids at registration —
        // leaves this assertion passing and reds the alter POST below on
        // its status check instead.
        assertThat(JSON.readTree(
                get("/v1/catalogs/lake/namespaces/analytics/tables/renamed_events/files"))
                .get(0).get("stats_state").asText())
                .isEqualTo("provided");

        post("/v1/catalogs/lake/namespaces/analytics/tables/renamed_events/alter",
                "{\"ops\": [{\"op\": \"rename_column\", \"from\": \"name\", \"to\": \"title\"}]}",
                200);

        // S2, the agreed connector behavior (see TestHoglakeParquetBinding in
        // PostHog/trino for the unit-level matrix): the data is still in the
        // file under the old name, the file has no field ids, the name
        // fallback misses, and the column reads NULL for every row.
        List<Map<String, Object>> rows = query(
                "SELECT id, title FROM hoglake.analytics.renamed_events ORDER BY id");
        assertThat(rows).hasSize(ROWS);
        assertThat(rows).allSatisfy(row -> assertThat(row.get("title")).isNull());
    }

    /**
     * Type promotion against id-less files, end to end: an int column's
     * int32 files must read as bigint after PROMOTE COLUMN. This is the
     * legal widening direction; trino-parquet coerces at decode time.
     */
    @Test
    void promoteIntToLongReadsWidenedValues()
            throws Exception
    {
        post("/v1/catalogs/lake/namespaces/analytics/tables",
                """
                {
                  "name": "int_events",
                  "columns": [
                    {"name": "id", "type": "long", "nullable": false},
                    {"name": "n", "type": "int"}
                  ]
                }
                """,
                201);
        byte[] parquet = TestParquet.writeIntColumnParquet();
        uploadObject("int_events/part-0.parquet", parquet);
        post("/v1/catalogs/lake/commit",
                """
                {
                  "appends": [{
                    "namespace": "analytics", "table": "int_events",
                    "files": [{
                      "path": "s3://%s/int_events/part-0.parquet",
                      "record_count": %d,
                      "file_size_bytes": %d,
                      "column_stats": [
                        {"field_id": 1, "value_count": %d, "null_count": 0},
                        {"field_id": 2, "value_count": %d, "null_count": 0}
                      ]
                    }]
                  }],
                  "author": "trino-integration-test"
                }
                """.formatted(BUCKET, TestParquet.INT_ROWS, parquet.length,
                        TestParquet.INT_ROWS, TestParquet.INT_ROWS),
                200);
        post("/v1/catalogs/lake/namespaces/analytics/tables/int_events/alter",
                "{\"ops\": [{\"op\": \"promote_column\", \"name\": \"n\", \"to\": \"long\"}]}",
                200);

        List<Map<String, Object>> columns = query("DESCRIBE hoglake.analytics.int_events");
        assertThat(columns).extracting(row -> row.get("Column")).containsExactly("id", "n");
        assertThat(columns).extracting(row -> row.get("Type")).containsExactly("bigint", "bigint");

        List<Map<String, Object>> rows = query(
                "SELECT id, n FROM hoglake.analytics.int_events ORDER BY id");
        assertThat(rows).hasSize(TestParquet.INT_ROWS);
        for (int i = 0; i < TestParquet.INT_ROWS; i++) {
            assertThat(rows.get(i).get("n")).isEqualTo((long) (i * 10));
        }
    }

    /**
     * Counting a DV-backed table answers with the SURVIVING rows, on both
     * of the connector's two counting paths. An unfiltered {@code
     * count(*)} needs no columns, so the connector answers it from catalog
     * metadata — record count minus the vector's cardinality — after
     * reading and validating the vector. {@code count(id)} needs the
     * column, so the rows come off the Parquet reader with the vector
     * applied per page. Both must agree with each other and with the
     * fixture; a connector that ignored the vector would say {@link #ROWS}
     * on both.
     */
    @Test
    void deletionVectorCountsOnlySurvivingRows()
            throws Exception
    {
        long surviving = SURVIVING_IDS.size();
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.deleted_events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(surviving));
        assertThat(query("SELECT count(id) AS n FROM hoglake.analytics.deleted_events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(surviving));
        // The physical file is untouched: the same parquet bytes under a
        // table with no vector still read in full, so the difference above
        // is the vector and nothing else.
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo((long) ROWS));
    }

    /**
     * A projected read skips the deleted positions and keeps every
     * surviving row whole. Dropping rows is not enough: each surviving id
     * must still carry its own name, score and timestamp, so a mask
     * applied to one channel and not another — or applied one batch late —
     * fails here even though the row count would look right.
     */
    @Test
    void deletionVectorProjectionSkipsDeletedPositions()
            throws Exception
    {
        List<Map<String, Object>> rows = query("""
                SELECT id, name, score, to_unixtime(ts) AS epoch
                FROM hoglake.analytics.deleted_events
                ORDER BY id
                """);
        assertThat(rows).extracting(row -> row.get("id"))
                .containsExactlyElementsOf(SURVIVING_IDS);
        for (Map<String, Object> row : rows) {
            long id = (Long) row.get("id");
            assertThat(row.get("name")).isEqualTo(String.format("row-%02d", id));
            assertThat(row.get("score")).isEqualTo(id % 5 == 0 ? null : id * 1.5);
            assertThat(row.get("epoch")).isEqualTo((double) (EPOCH0 + id));
        }
    }

    /**
     * The vector applies underneath filters and aggregates, not only to a
     * bare row count. The fixture's landmarks make each of these differ
     * from the undeleted table: position 13 is its only null {@code name},
     * positions 0/10/20 are three of its five null {@code score}s, and the
     * first and last rows of the file are deleted, so min and max move.
     */
    @Test
    void deletionVectorAppliesUnderFiltersAndAggregates()
            throws Exception
    {
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.deleted_events WHERE name IS NULL"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(0L));
        assertThat(query("SELECT id FROM hoglake.analytics.deleted_events WHERE score IS NULL ORDER BY id"))
                .extracting(row -> row.get("id"))
                .containsExactly(5L, 15L);
        assertThat(query("SELECT min(id) AS lo, max(id) AS hi FROM hoglake.analytics.deleted_events"))
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.get("lo")).isEqualTo(1L);
                    assertThat(row.get("hi")).isEqualTo(23L);
                });
        // Across a row-group boundary, with a predicate the connector may
        // push into row-group pruning: deleted positions are file-relative,
        // so pruning must not shift them. The fixture's groups are [0,9],
        // [10,19] and [20,24]; 9 and 10 are deleted, 8 and 11 are not.
        assertThat(query(
                "SELECT id FROM hoglake.analytics.deleted_events WHERE id BETWEEN 8 AND 11 ORDER BY id"))
                .extracting(row -> row.get("id"))
                .containsExactly(8L, 11L);
    }

    /**
     * A vector covering every row of the table's only data file reads as
     * an empty table — cleanly, on both the metadata-count path and the
     * data path, rather than as an error or as 25 rows.
     */
    @Test
    void deletionVectorOverEveryRowYieldsNoRows()
            throws Exception
    {
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.fully_deleted_events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(0L));
        assertThat(query("SELECT count(id) AS n FROM hoglake.analytics.fully_deleted_events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(0L));
        assertThat(query("SELECT id, name, score FROM hoglake.analytics.fully_deleted_events ORDER BY id"))
                .isEmpty();
    }

    /**
     * Deleted positions are ordinals inside their OWN data file, not row
     * numbers of the table. Every other fixture here holds one file
     * starting at position 0, where the two numberings coincide and no
     * assertion can tell them apart; {@code multi_file_events} holds two
     * byte-identical files with the vector on the second, which is the case
     * that separates them. Production tables are always multi-file.
     */
    @Test
    void deletionVectorPositionsAreRelativeToTheirOwnFile()
            throws Exception
    {
        // 50 physical rows, 7 deleted from the second file. Read as
        // table-global ordinals the vector's positions would all land in the
        // FIRST file's range, delete nothing from the file it is paired
        // with, and leave 50.
        long surviving = ROWS + SURVIVING_IDS.size();
        assertThat(query("SELECT count(id) AS n FROM hoglake.analytics.multi_file_events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(surviving));
        // The metadata-count path agrees; it subtracts delete_count and so
        // cannot distinguish the two numberings on its own.
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.multi_file_events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(surviving));
        // Each id appears once per file, so a deleted id drops to a single
        // copy and an undeleted neighbour keeps both. Table-global numbering
        // answers 2 for each.
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.multi_file_events WHERE id = 13"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(1L));
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.multi_file_events WHERE id = 12"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(2L));
    }

    /**
     * A vector whose blob names a different data file than the one the scan
     * paired it with fails the query. The catalog never opens DV files, so
     * the blob's {@code referenced-data-file} is the only thing that can
     * catch a mispairing before rows are deleted from the wrong file.
     */
    @Test
    void deletionVectorNamingAnotherDataFileFailsTheQuery()
    {
        assertThatThrownBy(() -> query("SELECT id FROM hoglake.analytics.misreferenced_dv_events"))
                .isInstanceOf(SQLException.class)
                .satisfies(failure -> {
                    // Requiring the PAIRED file as well narrows this to
                    // messages naming both sides. The referenced path alone
                    // would also be satisfied by a failure to OPEN it, which
                    // pointing the blob at an unreachable path made
                    // structurally possible. It does not prove the failure
                    // was the pairing check: a message naming both paths
                    // while failing for some other reason still passes.
                    String body = deletionVectorFailureBody(
                            failure,
                            "misreferenced_dv_events/part-0.dv",
                            s3("misreferenced_dv_events/part-0.parquet"));
                    // The discriminator, asserted here rather than by the
                    // helper: this path exists only inside the blob's bytes,
                    // so nothing that has not decoded the vector can print
                    // it. Everything above is printed by the pre-DV
                    // connector too.
                    assertThat(body).contains(MISREFERENCED_TARGET);
                });
    }

    /**
     * The other refusal that is still the connector's behavior, and the
     * wire contract behind it: {@code delete_count} on the scan's delete
     * file must equal the vector's real cardinality. The catalog never
     * opens DV files, so only the reader can catch a disagreement — and it
     * must, because silently preferring either number would return a row
     * count no writer ever wrote. This is also the only assertion in the
     * suite that proves the metadata-count path decodes the bitmap instead
     * of trusting {@code delete_count}: were it trusting, the first query
     * would happily answer 21.
     */
    @Test
    void deletionVectorDisagreeingWithTheCatalogFailsTheQuery()
    {
        for (String sql : List.of(
                "SELECT count(*) AS n FROM hoglake.analytics.mismatched_dv_events",
                "SELECT id FROM hoglake.analytics.mismatched_dv_events")) {
            assertThatThrownBy(() -> query(sql))
                    .isInstanceOf(SQLException.class)
                    .satisfies(failure -> assertReportsDecodedCardinality(
                            deletionVectorFailureBody(failure, "mismatched_dv_events/part-0.dv"),
                            4,
                            3));
        }
    }

    /**
     * The connector's own words for a rejected deletion vector, with the
     * JDBC preamble removed.
     *
     * <p>This checks only paths hoglake itself registered: the vector's
     * object key, and any other path the caller requires. No fork prose is
     * matched, in either direction — not a required phrase, not an excluded
     * one. Two earlier versions did both and both were wrong: an exclusion
     * of the pre-DV refusal string, which would have evaporated silently on
     * any reword of it, and a required {@code "deletion vector"}, which
     * reds a correct image the day upstream writes {@code deletion-vector}
     * — the spelling hoglake's own puffin blob type uses.
     *
     * <p>NOTHING here discriminates. The pre-DV connector refused every
     * DV-bearing scan while reading nothing, and its refusal still named
     * the deletion vector, the vector's path, the paired data file, and the
     * catalog's declared {@code delete_count}. This method cannot tell that
     * connector from one that validates.
     *
     * <p><b>So if you are adding a refusal test: this call is not enough.</b>
     * The connector refuses more than the two cases asserted here (missing,
     * truncated, bad checksum, wrong format, position past the row count),
     * and a test written as a bare {@code deletionVectorFailureBody(failure,
     * key)} with no further assertion passes on a pre-DV image — green,
     * silent, and worthless. Assert, at your call site, some fact that only
     * a connector which opened and decoded the file could state, and write
     * down why it qualifies. A path carried inside the blob is the
     * strongest kind available; a number is weaker (see
     * {@link #assertStandaloneNumber}). An earlier structural guard here
     * required callers to pass "evidence" but could only check the list was
     * non-empty, which was theatre; this paragraph replaces it, and relies
     * on you.
     */
    private static String deletionVectorFailureBody(
            Throwable failure, String vectorKey, String... alsoNames)
    {
        String raw = String.valueOf(failure.getMessage());
        // "Query failed (#20260917_045958_00029_y2vns): ". The trailing
        // coordinator id is five characters of Trino base32 (a-z and 2-7)
        // regenerated on every container start, so a digit matched inside
        // it is luck, not an assertion — about one start in forty would
        // satisfy a value check by itself. Asserted rather than
        // best-effort: a silent no-op here restores that hazard, and the
        // format belongs to trino-jdbc, which is pinned in build.gradle.kts
        // and does get bumped.
        Matcher preamble = JDBC_PREAMBLE.matcher(raw);
        assertThat(preamble.find()).as("JDBC preamble in: %s", raw).isTrue();
        String body = raw.substring(preamble.end());

        assertThat(body).contains(vectorKey);
        if (alsoNames.length > 0) {
            assertThat(body).contains(alsoNames);
        }
        return body;
    }

    /**
     * Asserts the message quotes BOTH the count the catalog declared and
     * the different count the connector found in the bitmap.
     *
     * <p>What carries the discrimination is the pair, not either number:
     * the pre-DV refusal prints exactly one number, the declared count it
     * echoed without opening anything, so requiring two distinct numbers is
     * what it cannot satisfy. That makes {@code declared != actual} the
     * load-bearing precondition rather than a tidiness check — drop it and
     * the two assertions collapse onto the one number the pre-DV connector
     * already prints, and this test goes green against it.
     *
     * <p>This is role-BLIND: the parameters are named for the reader, but
     * {@code (body, 4, 3)} and {@code (body, 3, 4)} are the same predicate,
     * so a connector that reported the two counts the wrong way round would
     * pass. Checking roles would mean matching the words around the numbers
     * — upstream prose, which this harness does not pin.
     */
    private static void assertReportsDecodedCardinality(String body, long declared, long actual)
    {
        assertThat(actual)
                .as("two equal counts collapse to the one number a pre-DV refusal prints")
                .isNotEqualTo(declared);
        assertStandaloneNumber(body, declared);
        assertStandaloneNumber(body, actual);
    }

    /**
     * Asserts a number appears on its own, rather than as digits inside
     * some longer token.
     *
     * <p>The two sides are deliberately NOT symmetric, because they guard
     * against opposite failures.
     *
     * <p>The lookbehind is wide — the whole class of token characters —
     * because that is where every false ACCEPT has lived: the {@code 3} of
     * {@code s3://} under one version, then 0 and 1 inside this suite's own
     * {@code part-0.dv} and {@code part-1.parquet} under its replacement,
     * which demanded only a non-alphanumeric predecessor.
     *
     * <p>The lookahead is narrow, because a wide one causes false REJECTS
     * instead. Excluding {@code .} and {@code -} after the digit makes the
     * assertion depend on the number being the last byte of an upstream
     * sentence: a trailing full stop, or {@code 3/25}, would red a correct
     * image — the brittleness this harness exists to avoid. It excludes
     * letters and digits, so {@code 30} and {@code 3rd} still fail, plus a
     * following {@code .} only when a digit follows it, so the {@code 3} of
     * {@code 3.5} does not match while {@code deletes 3.} does.
     *
     * <p>Residual, accepted: this cannot tell the number it wants from a
     * coincidental one elsewhere in the sentence ({@code offset=0},
     * {@code split 3 of 4}, {@code format version 3}). It is unreachable
     * against the frozen pre-DV messages, but it does make a number a
     * weaker discriminator than a path — see
     * {@link #assertReportsDecodedCardinality}.
     */
    private static void assertStandaloneNumber(String body, long value)
    {
        String tokenChar = "[0-9A-Za-z._/-]";
        assertThat(body)
                .as("%d standing alone in: %s", value, body)
                .containsPattern(
                        "(?<!" + tokenChar + ")" + value + "(?![0-9A-Za-z])(?!\\.[0-9])");
    }

    private static List<Map<String, Object>> query(String sql)
            throws SQLException
    {
        try (Connection connection = connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<Map<String, Object>> rows = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(resultSet.getMetaData().getColumnLabel(i), resultSet.getObject(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }
}
