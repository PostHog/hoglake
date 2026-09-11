# Control-plane image. Build context is server/ (see justfile
# docker-build). Two stages: Gradle assembles the installDist layout,
# the runtime stage is a bare JRE. Tests do NOT run here — the suite
# needs Docker (Testcontainers), unavailable inside a build; CI runs
# it beside (not before) the image build.

# --platform=$BUILDPLATFORM: the build stage always runs native on the
# build host — gradle output is arch-neutral bytecode, so only the JRE
# runtime stage below is per-target-arch. Without this, a multi-arch
# buildx run executes gradle under qemu for the foreign arch.
FROM --platform=$BUILDPLATFORM eclipse-temurin:21-jdk-noble AS build
WORKDIR /src

# Wrapper + build scripts first so the dependency layer caches across
# source-only changes.
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle/ gradle/
# settings.gradle.kts includes :trino; its build file must exist for
# configuration, but the connector is not part of this image.
COPY trino/build.gradle.kts trino/build.gradle.kts
# Priming may fail on config changes (|| true keeps the build going —
# the real build below still resolves); output stays visible so a
# silently-degraded cache layer is diagnosable from the build log.
RUN ./gradlew --no-daemon dependencies --configuration runtimeClasspath -x :trino:dependencies || true

COPY src/ src/
RUN ./gradlew --no-daemon :installDist -x test -x ktlintCheck

FROM eclipse-temurin:21-jre-noble
# Non-root: the server needs no filesystem beyond /tmp (parquet footer
# reads stream from S3; compaction spills under java.io.tmpdir).
RUN useradd --system --uid 10001 hoglake
COPY --from=build /src/build/install/hoglake-server/ /app/
USER 10001
EXPOSE 8080
# All configuration via HOGLAKE_* env vars (Config.kt defaults are
# dev-stack values; every real deploy overrides JDBC/S3/credentials).
ENTRYPOINT ["/app/bin/hoglake-server"]
