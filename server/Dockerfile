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
# Build identity for GET /v1/info. Declared HERE, after the dependency
# priming above, so a stamp that changes every build cannot invalidate
# the dependency layer. Empty by default and left empty by local and PR
# builds on purpose: only the CD image build stamps one, so a stamp in
# the webui always means "this came off the pipeline".
ARG HOGLAKE_BUILD_STAMP=""
RUN ./gradlew --no-daemon :installDist -x test -x ktlintCheck -PbuildStamp="$HOGLAKE_BUILD_STAMP"

FROM eclipse-temurin:21-jre-noble
# Non-root: the server needs no filesystem beyond /tmp (parquet footer
# reads stream from S3; compaction spills under java.io.tmpdir).
RUN useradd --system --uid 10001 hoglake
COPY --from=build /src/build/install/hoglake-server/ /app/
USER 10001
EXPOSE 8080
# All configuration via HOGLAKE_* env vars (Config.kt defaults are
# dev-stack values; every real deploy overrides JDBC/S3/credentials).
#
# JVM flags are NOT set here. `bin/hoglake-server` is the gradle
# application start script and it already carries them as
# DEFAULT_JVM_OPTS, from `applicationDefaultJvmArgs` in build.gradle.kts
# — one place, and it covers `just server run` and the compose stack too,
# not only this image. Setting ENV JAVA_OPTS here would instead be
# clobbered wholesale by any deploy that sets JAVA_OPTS for its own
# reason, silently taking the heap sizing with it. To ADD a flag at run
# time, set JAVA_OPTS or HOGLAKE_SERVER_OPTS: the script appends both
# after DEFAULT_JVM_OPTS, so they override rather than replace.
ENTRYPOINT ["/app/bin/hoglake-server"]
