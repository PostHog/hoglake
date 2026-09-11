// Server integration harness. The connector is built and shipped by PostHog/trino.
plugins {
    java
    kotlin("jvm")
}

group = "com.posthog.hoglake"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val testcontainersVersion = "1.21.3"
val ktorVersion = "3.1.3"

dependencies {
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.26.3")
    // Integration tests: the hoglake server runs in-process from the
    // root project's main classes (patterns copied from the root test
    // fixtures — cross-subproject test-source imports are not a thing).
    testImplementation(project(":"))
    // Compile-time visibility for the in-process server helper (these are
    // `implementation` deps of the root project, so not exported to us).
    testImplementation("io.ktor:ktor-server-core:$ktorVersion")
    testImplementation("io.ktor:ktor-server-netty:$ktorVersion")
    testImplementation("com.zaxxer:HikariCP:6.2.1")
    testImplementation("org.jdbi:jdbi3-core:3.45.4")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:minio:$testcontainersVersion")
    testImplementation("org.testcontainers:trino:$testcontainersVersion")
    // The JDBC client is independent of the server SPI and remains Java 21 compatible.
    testImplementation("io.trino:trino-jdbc:446")
    // Real parquet files for the end-to-end read test (same writer the
    // root hydrator tests use).
    testImplementation("dev.hardwood:hardwood-core:1.1.0.Beta1")
    // Bucket creation + parquet upload to MinIO.
    testImplementation("software.amazon.awssdk:s3:2.29.29")
}

kotlin {
    jvmToolchain(21)
}

val trinoImage = providers.gradleProperty("hoglakeTrinoImage")
    .orElse(providers.environmentVariable("HOGLAKE_TRINO_IMAGE"))

tasks.test {
    useJUnitPlatform()
    if (project.hasProperty("unitOnly")) {
        systemProperty("junit.jupiter.tags.exclude", "integration")
        exclude("**/*IntegrationTest*")
    } else {
        // Require the exact image under test; never silently use an unrelated SPI.
        doFirst {
            require(trinoImage.isPresent && trinoImage.get().isNotBlank()) {
                "Set -PhoglakeTrinoImage=<image> or HOGLAKE_TRINO_IMAGE to a Trino image containing the hoglake connector"
            }
            systemProperty("hoglake.trino.image", trinoImage.get())
        }
    }
    inputs.property("hoglakeTrinoImage", trinoImage.orElse(""))
    testLogging {
        events("failed", "skipped")
        showStackTraces = true
    }
}
