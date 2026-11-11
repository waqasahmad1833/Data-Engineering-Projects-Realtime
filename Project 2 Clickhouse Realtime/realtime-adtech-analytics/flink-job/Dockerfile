# ---- Build ------------------------------------------------------------------
FROM maven:3.9.6-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# ---- Runtime (application mode: the job jar lives in usrlib) -----------------
FROM flink:1.18.1-scala_2.12-java11

# Plugins: S3 filesystem for checkpoints/savepoints, Prometheus metrics reporter
RUN mkdir -p /opt/flink/plugins/s3-fs-presto /opt/flink/plugins/metrics-prometheus /opt/flink/usrlib \
    && cp /opt/flink/opt/flink-s3-fs-presto-1.18.1.jar /opt/flink/plugins/s3-fs-presto/ \
    && cp /opt/flink/opt/flink-metrics-prometheus-1.18.1.jar /opt/flink/plugins/metrics-prometheus/ \
    && chown -R flink:flink /opt/flink/plugins /opt/flink/usrlib

COPY --from=build --chown=flink:flink /build/target/adtech-flink-job.jar /opt/flink/usrlib/adtech-flink-job.jar
