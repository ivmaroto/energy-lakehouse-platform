FROM alpine/curl:8.14.1 AS downloader

ARG ICEBERG_VERSION=1.10.0
ARG POSTGRES_JDBC_VERSION=42.7.5

RUN curl -fsSL \
    -o /tmp/iceberg-spark-runtime.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl -fsSL \
    -o /tmp/iceberg-aws-bundle.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && curl -fsSL \
    -o /tmp/postgresql.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

COPY --from=downloader /tmp/iceberg-spark-runtime.jar /opt/spark/jars/
COPY --from=downloader /tmp/iceberg-aws-bundle.jar /opt/spark/jars/
COPY --from=downloader /tmp/postgresql.jar /opt/spark/jars/

USER spark