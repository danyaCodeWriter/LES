# syntax=docker/dockerfile:1
#
# coordination-server (Spring Boot 3 + WebFlux + gRPC), JDK 21.
# Сборка: из корня репозитория
#   docker build -t coordination-server:local .
#
# HTTP :8080, gRPC :9090 (см. application.yml). etcd по умолчанию в образе — http://etcd:2379
# (удобно для docker compose; иначе переопределите, например):
#   docker run --rm -p 8080:8080 -p 9090:9090 \
#     -e COORDINATION_ETCD_ENDPOINTS_0=http://host.docker.internal:2379 \
#     coordination-server:local
#
# All-in-one (app + etcd в одном контейнере): см. Dockerfile.with-etcd
#   docker build -f Dockerfile.with-etcd -t coordination-server:with-etcd .
#   docker run --rm -p 8080:8080 -p 9090:9090 -p 2379:2379 coordination-server:with-etcd
#   # или: docker compose --profile with-etcd up -d --build coordination-with-etcd
#
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /build

COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle
COPY coordination-hash ./coordination-hash
COPY coordination-server ./coordination-server
COPY coordination-client ./coordination-client
COPY proto ./proto
COPY openapi ./openapi

RUN chmod +x gradlew \
  && ./gradlew :coordination-server:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

RUN groupadd --system --gid 1000 coordination \
  && useradd --system --uid 1000 --gid coordination --home /app --shell /usr/sbin/nologin coordination

COPY --from=build /build/coordination-server/build/libs/coordination-server-*.jar /app/app.jar
RUN chown coordination:coordination /app/app.jar

USER coordination:coordination

EXPOSE 8080 9090

# Список endpoints jetcd; для одного узла достаточно _0 (Spring Boot relaxed binding).
ENV COORDINATION_ETCD_ENDPOINTS_0=http://etcd:2379

ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
