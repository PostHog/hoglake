package com.posthog.hoglake.commit

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * The containment check, stated out loud.
 *
 * Hoglake's own S3 client (hydrator, compaction, cleanup) runs under one IAM
 * identity for every tenant, so a registered path that escapes its catalog's
 * data_path is a cross-tenant read, rewrite or delete with nothing in IAM to
 * stop it. Managed catalogs share one bucket with a prefix per tenant, and the
 * prefixes are identifiers like `org1` and `org10` — so the sibling-prefix case
 * below is the realistic one, not a curiosity.
 */
class PathStaysUnderTest {
    private val prefix = "s3://lake/tenants/org1/"

    @ParameterizedTest
    @ValueSource(
        strings = [
            "s3://lake/tenants/org1/t/data/a.parquet",
            "s3://lake/tenants/org1/a.parquet",
            "s3://lake/tenants/org1/t/x%20y.parquet",
        ],
    )
    fun `a path inside the prefix is accepted`(path: String) {
        assertThat(pathStaysUnder(path, prefix)).isTrue()
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            // the sibling tenant whose name extends this one
            "s3://lake/tenants/org10/t/a.parquet",
            "s3://lake/tenants/org1",
            "s3://lake/tenants/org1/",
            // dot segments, literal and encoded
            "s3://lake/tenants/org1/../org2/a.parquet",
            "s3://lake/tenants/org1/./a.parquet",
            "s3://lake/tenants/org1/%2e%2e/org2/a.parquet",
            "s3://lake/tenants/org1/%2E%2E/org2/a.parquet",
            // encoded and backslash separators
            "s3://lake/tenants/org1/..%2Forg2/a.parquet",
            "s3://lake/tenants/org1/..\\org2\\a.parquet",
            // empty segments
            "s3://lake/tenants/org1//a.parquet",
            "s3://lake/tenants/org1/t/",
            // another bucket, another scheme, another case
            "s3://other/tenants/org1/a.parquet",
            "s3a://lake/tenants/org1/a.parquet",
            "S3://lake/tenants/org1/a.parquet",
            "s3://lake/tenants/ORG1/a.parquet",
            // whitespace and control characters
            "s3://lake/tenants/org1/a b.parquet",
            "s3://lake/tenants/org1/a\u0000.parquet",
        ],
    )
    fun `a path outside the prefix, or spelled to leave it, is refused`(path: String) {
        assertThat(pathStaysUnder(path, prefix)).isFalse()
    }
}
