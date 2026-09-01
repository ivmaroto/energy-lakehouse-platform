FROM alpine/curl:8.14.1 AS downloader

ARG ICEBERG_VERSION=1.10.0
ARG POSTGRES_JDBC_VERSION=42.7.5
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

RUN curl -fsSL \
    -o /tmp/iceberg-spark-runtime.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl -fsSL \
    -o /tmp/iceberg-aws-bundle.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && curl -fsSL \
    -o /tmp/postgresql.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    && curl -fsSL \
    -o /tmp/hadoop-aws.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    && curl -fsSL \
    -o /tmp/aws-java-sdk-bundle.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"


FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu AS spark-runtime

USER root

COPY --from=downloader \
    /tmp/iceberg-spark-runtime.jar \
    /opt/spark/jars/

COPY --from=downloader \
    /tmp/iceberg-aws-bundle.jar \
    /opt/spark/jars/

COPY --from=downloader \
    /tmp/postgresql.jar \
    /opt/spark/jars/

COPY --from=downloader \
    /tmp/hadoop-aws.jar \
    /opt/spark/jars/

COPY --from=downloader \
    /tmp/aws-java-sdk-bundle.jar \
    /opt/spark/jars/


FROM apache/airflow:2.10.5-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark-runtime /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY docker/airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "${HOME}/constraints.txt"