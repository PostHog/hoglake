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
import java.util.stream.LongStream;

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
        postgres = new PostgreSQLContainer<>("postgres:16")
                .withUsername("hoglake")
                .withPassword("hoglake");
        // quay.io: Docker Hub stopped serving minio/minio anonymously.
        minio = new MinIOContainer(
                DockerImageName.parse("quay.io/minio/minio:RELEASE.2023-09-04T19-57-37Z")
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
            for (String table : List.of("events", "deleted_events", "fully_deleted_events", "mismatched_dv_events")) {
                s3.putObject(b -> b.bucket(BUCKET).key(table + "/part-0.parquet"),
                        RequestBody.fromBytes(parquet));
            }
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

    /** Catalog + namespace + the four seeded tables; commits with footer stats. */
    private static void seedCatalog(long fileSize)
            throws Exception
    {
        post("/v1/catalogs",
                "{\"name\": \"lake\", \"data_path\": \"s3://" + BUCKET + "/\"}", 201);
        post("/v1/catalogs/lake/namespaces", "{\"name\": \"analytics\"}", 201);

        createTable("events");
        createTable("deleted_events");
        createTable("fully_deleted_events");
        createTable("mismatched_dv_events");

        commitAppend("events", "s3://" + BUCKET + "/events/part-0.parquet", fileSize);
        seedDeletionVector("deleted_events", fileSize, DELETED_POSITIONS, DELETED_POSITIONS.size());
        seedDeletionVector("fully_deleted_events", fileSize, ALL_POSITIONS, ALL_POSITIONS.size());
        // A vector the catalog misdescribes: the bitmap deletes three rows,
        // the registration claims four. The server never opens DV files, so
        // it accepts the registration and only the reader can catch it.
        seedDeletionVector("mismatched_dv_events", fileSize, List.of(1L, 2L, 3L), 4);
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
        String dataFilePath = "s3://" + BUCKET + "/" + table + "/part-0.parquet";
        long snapshot = commitAppend(table, dataFilePath, dataFileSize);

        byte[] vector = TestPuffin.deletionVector(dataFilePath, positions);
        uploadObject(table + "/part-0.dv", vector);

        long dataFileId = JSON.readTree(
                get("/v1/catalogs/lake/namespaces/analytics/tables/" + table + "/files"))
                .get(0).get("data_file_id").asLong();
        post("/v1/catalogs/lake/commit",
                """
                {
                  "read_snapshot": %d,
                  "deletes": [{
                    "namespace": "analytics", "table": "%s",
                    "files": [{
                      "data_file_id": %d,
                      "path": "s3://%s/%s/part-0.dv",
                      "delete_count": %d,
                      "file_size_bytes": %d
                    }]
                  }]
                }
                """.formatted(snapshot, table, dataFileId, BUCKET, table, declaredDeleteCount, vector.length),
                200);
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
        // The seeded set. Two more tables are created by the schema-evolution
        // tests, so this asserts containment, not the whole schema.
        assertThat(query("SHOW TABLES FROM hoglake.analytics"))
                .extracting(row -> row.get("Table"))
                .contains("events", "deleted_events", "fully_deleted_events", "mismatched_dv_events");
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
        // The connector pushes the predicate into Parquet row-group pruning
        // and Trino re-applies it above the scan; either way the answer is
        // the same, which is what this asserts.
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
     * Schema evolution against id-less files, end to end. Every file the
     * hydrator (Hardwood) writes carries no PARQUET:field_id, so the
     * connector's name-fallback binding is what production reads use —
     * and a RENAME COLUMN breaks it silently.
     */
    @Test
    void renameColumnOnIdlessFilesSilentlyReadsNulls()
            throws Exception
    {
        createTable("renamed_events");
        byte[] parquet = TestParquet.writeSampleParquet();
        uploadObject("renamed_events/part-0.parquet", parquet);
        commitAppend("renamed_events",
                "s3://" + BUCKET + "/renamed_events/part-0.parquet", parquet.length);
        post("/v1/catalogs/lake/namespaces/analytics/tables/renamed_events/alter",
                "{\"ops\": [{\"op\": \"rename_column\", \"from\": \"name\", \"to\": \"title\"}]}",
                200);

        List<Map<String, Object>> rows = query(
                "SELECT id, title FROM hoglake.analytics.renamed_events ORDER BY id");

        // S2 (agreed connector behavior; see TestHoglakeParquetBinding in PostHog/trino for
        // the unit-level matrix): the data exists in the file under the old
        // name "name", but the file has no field ids, the name fallback
        // misses, and every row of the renamed column reads NULL. The fix
        // is catalog-side — field ids become a registration contract and
        // the server will refuse renames while id-less files are live —
        // the connector's id-authoritative binding stays as-is. Once the
        // server-side refusal lands, this rename will 4xx and this test
        // changes to assert that refusal.
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
     * The one refusal that is still the connector's behavior, and the wire
     * contract behind it: {@code delete_count} on the scan's delete file
     * must equal the vector's real cardinality. The catalog never opens DV
     * files, so only the reader can catch a disagreement — and it must,
     * because silently preferring either number would return a row count
     * no writer ever wrote. This also proves the metadata-count path reads
     * the vector rather than trusting {@code delete_count} alone.
     */
    @Test
    void deletionVectorDisagreeingWithTheCatalogFailsTheQuery()
    {
        assertThatThrownBy(() -> query("SELECT count(*) AS n FROM hoglake.analytics.mismatched_dv_events"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("the catalog reports 4 deleted rows but the vector deletes 3");
        assertThatThrownBy(() -> query("SELECT id FROM hoglake.analytics.mismatched_dv_events"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("the catalog reports 4 deleted rows but the vector deletes 3");
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
