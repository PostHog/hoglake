plugins {
    kotlin("jvm") version "2.4.20"
    application
    id("org.jlleitschuh.gradle.ktlint") version "12.1.2"
}

group = "com.posthog.hoglake"
version = "1.1.1-dev"

repositories {
    mavenCentral()
}

val ktorVersion = "3.5.2"
val jdbiVersion = "3.54.0"
val flywayVersion = "11.8.2"
// >= 1.21.1: older versions pin Docker API 1.32, which OrbStack's Docker 29 rejects.
val testcontainersVersion = "1.21.4"
val awsSdkVersion = "2.54.13"

dependencies {
    // Background loops (BackgroundLoops.kt): explicit pin of the
    // kotlinx-coroutines line ktor already carries transitively.
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    // HTTP server
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-jackson:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages:$ktorVersion")
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.22.2")

    // Persistence
    implementation("org.postgresql:postgresql:42.7.13")
    implementation("com.zaxxer:HikariCP:6.3.0")
    implementation("org.jdbi:jdbi3-core:$jdbiVersion")
    implementation("org.jdbi:jdbi3-kotlin:$jdbiVersion")
    implementation("org.jdbi:jdbi3-postgres:$jdbiVersion")
    implementation("org.flywaydb:flyway-core:$flywayVersion")
    implementation("org.flywaydb:flyway-database-postgresql:$flywayVersion")

    // Object store
    implementation("software.amazon.awssdk:s3:$awsSdkVersion")

    // parquet-java is THE parquet library (decision 2026-09-05: Hardwood is
    // out entirely — field ids are a contract, and parquet-java reads AND
    // writes them): the hydrator's footer reads and the compaction rewrite
    // writer both live on it. hadoop-client-api is the shaded,
    // dependency-free jar; the wider Hadoop dependency tree must not leak
    // into the codebase.
    implementation("org.apache.parquet:parquet-hadoop:1.18.1")
    implementation("org.apache.hadoop:hadoop-client-api:3.5.0")
    runtimeOnly("org.apache.hadoop:hadoop-client-runtime:3.5.0")

    // Deletion vectors: hoglake DVs are Iceberg v3 puffin `deletion-vector-v1`
    // blobs — a portable 64-bit roaring bitmap of deleted row positions.
    // Compaction applies DVs at rewrite time (PuffinDeletionVector.kt), and
    // the Java RoaringBitmap serialize/deserialize format IS the portable
    // interoperable format the spec requires.
    implementation("org.roaringbitmap:RoaringBitmap:1.6.21")

    // Logging + observability
    implementation("ch.qos.logback:logback-classic:1.6.3")
    implementation("io.github.oshai:kotlin-logging-jvm:7.0.7")
    implementation("net.logstash.logback:logstash-logback-encoder:8.1")
    implementation("io.ktor:ktor-server-metrics-micrometer:$ktorVersion")
    implementation("io.micrometer:micrometer-registry-prometheus:1.17.1")

    // Tests
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:minio:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
    testImplementation("org.awaitility:awaitility:4.3.0")
    testImplementation("io.kotest:kotest-property:5.9.1")

    // Coverage-guided fuzzing (docs/fuzzing.md layer 4): jazzer-junit @FuzzTest
    // targets in src/test/kotlin/com/posthog/hoglake/fuzz. Inside the normal
    // :test run they replay the committed corpus deterministically
    // (regression mode); the `fuzz` task reruns the same targets under
    // libFuzzer with a time budget (see the fuzzing tasks below).
    testImplementation("com.code-intelligence:jazzer-junit:0.30.0")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.posthog.hoglake.MainKt")
}

tasks.test {
    useJUnitPlatform()
    // Integration tests need Docker (Testcontainers); tag-gated so `gradle
    // test -PunitOnly` stays runnable without it.
    if (project.hasProperty("unitOnly")) {
        systemProperty("junit.jupiter.tags.exclude", "integration")
        exclude("**/*IntegrationTest*")
    }
    // jazzer-junit self-attaches its instrumentation agent for the corpus
    // replay of the fuzz targets; JDK 21 warns on dynamic attach otherwise.
    // -XX:-OmitStackTraceInFastThrow: see the fuzz tasks below — a hot
    // NPE otherwise arrives with no stack and no message, which makes a
    // failure report useless and any frame-based assertion unreliable.
    jvmArgs("-XX:+EnableDynamicAgentLoading", "-XX:-OmitStackTraceInFastThrow")
    testLogging {
        events("failed", "skipped")
        showStackTraces = true
    }
}

// ---- fuzzing (docs/fuzzing.md layer 4) -----------------------------------------
//
// `./gradlew fuzz -PfuzzSeconds=300` runs every @FuzzTest target under
// libFuzzer for the given per-target budget (default 60s). jazzer-junit
// permits one fuzz test per JVM run, so each target gets its own Test task,
// chained sequentially. Committed seed corpus lives under
// src/test/resources/com/posthog/hoglake/fuzz/<Target>Inputs/<method>/
// (jazzer-junit's inputs convention — the same files the normal :test run
// replays deterministically); crashing inputs found while fuzzing are
// written back into those directories, and the growing generated corpus
// lands in .cifuzz-corpus/ (transient, not committed).

val fuzzTargets =
    listOf(
        "IcebergSingleValueDecodeFuzzTest",
        "BoundWireFuzzTest",
        "IcebergSingleValueCompareFuzzTest",
        "ParquetFooterFuzzTest",
        "PuffinDeletionVectorFuzzTest",
        "IdentifiersFuzzTest",
        "WireDtoParseFuzzTest",
        "NestedAgreementFuzzTest",
    )

val fuzzSeconds = (project.findProperty("fuzzSeconds") as String?)?.toLongOrNull() ?: 60L

val fuzzTasks =
    fuzzTargets.map { target ->
        tasks.register<Test>("fuzz$target") {
            description = "Coverage-guided Jazzer run of $target (budget ${fuzzSeconds}s)"
            group = "verification"
            testClassesDirs = sourceSets.test.get().output.classesDirs
            classpath = sourceSets.test.get().runtimeClasspath
            useJUnitPlatform()
            filter { includeTestsMatching("com.posthog.hoglake.fuzz.$target") }
            // Truthy JAZZER_FUZZ (env var or system property) flips
            // jazzer-junit from corpus replay to real fuzzing.
            systemProperty("JAZZER_FUZZ", "1")
            // Budget override: extra libFuzzer args are appended after the
            // annotation's -max_total_time, and the last occurrence wins.
            // (arg 0 is argv0 and skipped by jazzer-junit.)
            systemProperty("jazzer.internal.arg.0", "jazzer")
            systemProperty("jazzer.internal.arg.1", "-max_total_time=$fuzzSeconds")
            // -XX:-OmitStackTraceInFastThrow is load-bearing here. Fuzzing
            // makes an exception site hot, and HotSpot then throws a
            // preallocated instance with NO stack trace and NO message.
            // A finding reported that way cannot be diagnosed at all (#15
            // was misfiled against the wrong class for exactly this
            // reason) and any stack-frame check silently stops matching.
            jvmArgs("-XX:+EnableDynamicAgentLoading", "-XX:-OmitStackTraceInFastThrow")
            outputs.upToDateWhen { false }
            testLogging {
                events("passed", "failed")
                showStackTraces = true
                showStandardStreams = true
            }
        }
    }

// Serialize the per-target runs: concurrent libFuzzer instances would fight
// over CPU and the shared build directory.
fuzzTasks.zipWithNext().forEach { (a, b) -> b.configure { mustRunAfter(a) } }

tasks.register("fuzz") {
    description = "Run every Jazzer fuzz target for -PfuzzSeconds seconds each (default 60)"
    group = "verification"
    dependsOn(fuzzTasks)
}

// Deterministic seed-loop soak runner for the nested campaigns
// (NestedFuzzSoak): the same oracles the jazzer target uses, driven by a
// seeded RNG instead of libFuzzer, so every finding replays exactly with
// -Pseeds=<seed>..<seed>. Complements `fuzz` rather than replacing it —
// libFuzzer brings coverage feedback, this brings reproducibility and a
// per-iteration hang timeout.
//
//   ./gradlew nestedSoak -Pcampaign=agreement -Pseeds=0..100000 -PtimeBudget=1200
//
// campaigns: agreement | footer | data | trees | codec
tasks.register<JavaExec>("nestedSoak") {
    description = "Deterministic nested fuzz soak (manual; -Pcampaign, -Pseeds, -PtimeBudget)"
    group = "verification"
    mainClass.set("com.posthog.hoglake.fuzz.NestedFuzzSoak")
    classpath = sourceSets.test.get().runtimeClasspath
    val seeds = (project.findProperty("seeds") as String?) ?: "0..10000"
    val range = seeds.split("..")
    args(
        (project.findProperty("campaign") as String?) ?: "agreement",
        range.first(),
        range.getOrElse(1) { range.first() },
        (project.findProperty("timeBudget") as String?) ?: "600",
        (project.findProperty("iterationTimeout") as String?) ?: "30",
        (project.findProperty("strictDomains") as String?) ?: "true",
    )
}

// The soak FLEET: `nestedSoak` is one JVM, and one JVM saturates one of
// twelve cores. A real campaign partitions the seed space across ~9-10
// detached workers, which needs the test classpath as a plain file so a
// worker can be launched without Gradle (a Gradle daemon per worker
// would spend the cores on Gradle).
//
//   ./gradlew writeTestClasspath
//   for w in 0 1 2 ...; do
//     java -cp "$(cat build/test-classpath.txt)" \
//       com.posthog.hoglake.fuzz.NestedFuzzSoak agreement $from $to 1400 90 true &
//   done
tasks.register("writeTestClasspath") {
    description = "Write the test runtime classpath to build/test-classpath.txt (soak fleet)"
    group = "verification"
    val cp = sourceSets.test.get().runtimeClasspath
    val out = layout.buildDirectory.file("test-classpath.txt")
    dependsOn(cp)
    outputs.file(out)
    doLast {
        out.get().asFile.writeText(cp.asPath)
    }
}

// One-shot (manual) seed-corpus generator: writes the committed corpus under
// src/test/resources/com/posthog/hoglake/fuzz from the cross-language vector
// file plus freshly built parquet footers / puffin DV blobs. Rerun only when
// adding targets or new vector-derived seeds; the output is committed.
tasks.register<JavaExec>("generateFuzzSeeds") {
    description = "Regenerate the committed fuzz seed corpus (manual)"
    group = "verification"
    mainClass.set("com.posthog.hoglake.fuzz.FuzzSeedGenerator")
    classpath = sourceSets.test.get().runtimeClasspath
    args(
        layout.projectDirectory.dir("src/test/resources/com/posthog/hoglake/fuzz").asFile.absolutePath,
        layout.projectDirectory.file("../pyhoglake/tests/vectors/bounds_vectors.json").asFile.absolutePath,
    )
}

// The OpenAPI spec's info.version must match the server version. The
// The running version, readable at runtime. A generated resource rather
// than the jar manifest: the manifest is absent when the server runs from
// classes (Gradle run, every test), so a manifest read would answer
// "unknown" in exactly the environments where a version banner is most
// likely to be wrong. project.version is the single source (build.gradle
// -> here -> GET /v1/info -> webui badge), so there is nothing to keep in
// sync by hand.
// The build stamp is SUPPLIED, never generated here: an ordinary local
// build has nothing to stamp with and reports no build, which is the
// point — only a packaged image (the CD Docker build) carries one, so a
// stamp in the webui always means "this came off the pipeline". Gradle
// generating a timestamp per invocation would make every local `gradle
// build` claim a distinct build and make the field meaningless.
val buildStamp =
    providers.gradleProperty("buildStamp")
        .orElse(providers.environmentVariable("HOGLAKE_BUILD_STAMP"))
        .orElse("")

val generateVersionResource =
    tasks.register("generateVersionResource") {
        description = "Write the project version and build stamp into a resource the server reads at runtime"
        val outputDir = layout.buildDirectory.dir("generated/version")
        val projectVersion = version.toString()
        val stamp = buildStamp
        inputs.property("version", projectVersion)
        inputs.property("buildStamp", stamp)
        outputs.dir(outputDir)
        doLast {
            val file = outputDir.get().file("com/posthog/hoglake/version.properties").asFile
            file.parentFile.mkdirs()
            file.writeText("version=$projectVersion\nbuild=${stamp.get()}\n")
        }
    }

sourceSets.main {
    output.dir(mapOf("builtBy" to generateVersionResource), layout.buildDirectory.dir("generated/version"))
}

// v1.0.0 tag shipped a spec that still said 0.1.0 because nothing
// enforced the pairing; this check makes the drift a build failure.
tasks.register("checkOpenapiVersion") {
    description = "Verify openapi/hoglake.yaml info.version matches project.version"
    group = "verification"
    val specFile = layout.projectDirectory.file("src/main/resources/openapi/hoglake.yaml")
    val expected = version.toString()
    inputs.file(specFile)
    doLast {
        val spec = specFile.asFile.readText()
        val match =
            Regex("""(?m)^\s{2}version:\s*(\S+)\s*$""").find(spec)
                ?: error("openapi/hoglake.yaml: info.version not found")
        val actual = match.groupValues[1]
        if (actual != expected) {
            error("openapi/hoglake.yaml info.version is $actual; project.version is $expected")
        }
    }
}
tasks.named("check") { dependsOn("checkOpenapiVersion") }

// Scratch runner for the imported fuzz repro mains (temporary).
