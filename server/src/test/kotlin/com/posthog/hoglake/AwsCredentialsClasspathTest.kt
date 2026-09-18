package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.awssdk.auth.credentials.internal.WebIdentityCredentialsUtils
import software.amazon.awssdk.auth.credentials.internal.WebIdentityTokenCredentialProperties
import software.amazon.awssdk.http.SdkHttpService
import software.amazon.awssdk.utils.SdkAutoCloseable
import java.nio.file.Path
import java.util.ServiceLoader
import kotlin.io.path.writeText

/**
 * IRSA is a CLASSPATH fact, not a code fact, and that is the whole reason
 * this file exists.
 *
 * In-cluster the server sets no `HOGLAKE_S3_ACCESS_KEY`/`_SECRET_KEY`, so
 * neither [com.posthog.hoglake.hydrator.ObjectStore] nor
 * [com.posthog.hoglake.service.RemovalStore] pins a credentials provider and
 * the SDK falls through to `DefaultCredentialsProvider`. Its third link is
 * `WebIdentityTokenFileCredentialsProvider`, which is how an EKS projected
 * service-account token becomes S3 credentials.
 *
 * That link is satisfied by a module NO SOURCE FILE IMPORTS:
 * `WebIdentityTokenFileCredentialsProvider`'s constructor calls
 * [WebIdentityCredentialsUtils.factory], which reflectively loads
 * `software.amazon.awssdk.services.sts.internal.StsWebIdentityCredentialsProviderFactory`
 * out of `software.amazon.awssdk:sts` to build the `StsClient` that calls
 * AssumeRoleWithWebIdentity. Drop that dependency and nothing fails to
 * compile, no test touches it, and the constructor SWALLOWS the resulting
 * `IllegalStateException` into a private `loadException` field — the only
 * symptom before the credentials come back empty is one WARN line,
 * "To use web identity tokens, the 'sts' service module must be on the class
 * path". That is exactly how it reached gigahog dev.
 *
 * So these cases assert the classpath through the SDK's own entry points
 * rather than through a build-file string: they go red on the dependency
 * being removed, and they need no AWS account, no network and no container.
 */
class AwsCredentialsClasspathTest {
    @Test
    fun `the SDK can load the STS web-identity factory it looks up reflectively`() {
        // WebIdentityCredentialsUtils.factory() IS the call that logged the
        // warning in dev; calling it here means the test cannot drift from
        // the mechanism it is protecting.
        val factory = WebIdentityCredentialsUtils.factory()

        assertThat(factory).isNotNull()
        // The FQCN is a string literal in the auth module's bytecode, so
        // pinning it here is pinning the contract between the two artifacts,
        // not restating one of our own constants.
        assertThat(factory.javaClass.name)
            .isEqualTo("software.amazon.awssdk.services.sts.internal.StsWebIdentityCredentialsProviderFactory")
    }

    @Test
    fun `a synchronous HTTP transport is on the classpath for the STS client`() {
        // The factory builds a SYNCHRONOUS StsClient, which resolves its
        // transport through this SPI. `s3` and `sts` each carry a runtime
        // apache5-client, so this is satisfied today — but an exclusion or a
        // switch to an async-only client would make the web-identity path
        // fail with "Unable to load an HTTP implementation" instead, which
        // looks nothing like a credentials problem.
        val impls = ServiceLoader.load(SdkHttpService::class.java).toList()

        assertThat(impls).isNotEmpty()
    }

    @Test
    fun `the web-identity provider builds end to end from a token file and role ARN`(
        @TempDir tmp: Path,
    ) {
        // The shape IRSA injects: a projected token file plus AWS_ROLE_ARN.
        // Building the provider is the whole assertion — it constructs the
        // StsClient (module + transport + region resolution) without
        // resolving anything, so no call leaves the JVM. Credentials are
        // deliberately NOT resolved: that would need a real STS.
        val tokenFile = tmp.resolve("token")
        tokenFile.writeText("not-a-real-token")

        val properties =
            WebIdentityTokenCredentialProperties.builder()
                .roleArn("arn:aws:iam::000000000000:role/hoglake-test")
                .roleSessionName("hoglake-classpath-test")
                .webIdentityTokenFile(tokenFile)
                .build()

        // A region must be resolvable for the StsClient to build at all; in
        // the pod it comes from the environment, here from the property the
        // SDK reads first.
        val previousRegion = System.getProperty("aws.region")
        System.setProperty("aws.region", "us-east-1")
        try {
            var provider: Any? = null
            assertThatCode { provider = WebIdentityCredentialsUtils.factory().create(properties) }
                .doesNotThrowAnyException()
            assertThat(provider).isNotNull()
            (provider as? SdkAutoCloseable)?.close()
        } finally {
            if (previousRegion == null) {
                System.clearProperty("aws.region")
            } else {
                System.setProperty("aws.region", previousRegion)
            }
        }
    }
}
