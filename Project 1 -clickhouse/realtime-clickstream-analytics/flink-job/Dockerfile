# ---- Build stage: compile and shade the job jar -----------------------------
FROM maven:3.9.6-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# ---- Runtime stage: Flink with the job in usrlib (application mode) ---------
FROM flink:1.18.1-scala_2.12-java11
RUN mkdir -p /opt/flink/usrlib /opt/flink/checkpoints \
    && chown -R flink:flink /opt/flink/usrlib /opt/flink/checkpoints
COPY --from=build --chown=flink:flink /build/target/clickstream-flink-job.jar /opt/flink/usrlib/clickstream-flink-job.jar
