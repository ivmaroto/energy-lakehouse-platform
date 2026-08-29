# Docker

Docker configuration for the local infrastructure of the Energy Lakehouse Platform.

## 1. Purpose

Docker and Docker Compose are used to provide a reproducible local environment for the complete platform.

The containerized infrastructure includes the services required for:

- object storage;
- distributed data processing;
- Apache Iceberg metadata management;
- SQL querying;
- workflow orchestration;
- analytical visualization.

The main infrastructure definition is located at the project root:

```text
docker-compose.yml
```

---

## 2. Platform Services

The Docker Compose environment includes the following main services:

| Service | Purpose |
|---|---|
| PostgreSQL | Platform metadata and Apache Iceberg JDBC catalog |
| MinIO | S3-compatible object storage |
| Spark Master | Apache Spark cluster coordination |
| Spark Worker | Distributed Spark processing |
| Trino | SQL query layer over Apache Iceberg |
| Airflow Webserver | Airflow user interface |
| Airflow Scheduler | Workflow scheduling and execution |
| Superset | Analytical visualization |

Initialization services are also used where required by Airflow and Superset.

---

## 3. Local Ports

The main host ports are:

| Service | Port |
|---|---:|
| PostgreSQL | 5432 |
| Spark Master | 7077 |
| Spark Master UI | 8080 |
| Spark Worker UI | 8081 |
| Trino | 8082 |
| Airflow | 8083 |
| Superset | 8088 |
| MinIO API | 9000 |
| MinIO Console | 9001 |

Internal communication between containers uses Docker service names through the shared platform network.

Examples:

```text
postgres:5432
minio:9000
spark-master:7077
```

---

## 4. Custom Images

Custom Docker images are used where project-specific dependencies or configuration are required.

The platform includes custom container configuration for:

```text
Apache Spark
Apache Airflow
Apache Superset
```

Spark requires the dependencies and configuration necessary to work with:

```text
Apache Iceberg
MinIO / S3
PostgreSQL JDBC catalog
```

Airflow contains the Python environment required to execute project ingestion and orchestration code.

Superset contains the dependencies required by the analytical visualization layer.

---

## 5. Environment Configuration

Environment-specific configuration and credentials are externalized from the Docker definitions.

The repository contains:

```text
.env.example
```

A local deployment must create:

```text
.env
```

The real `.env` file must not be committed to Git.

It contains configuration required by services such as:

```text
PostgreSQL
MinIO
Airflow
Superset
AEMET
ESIOS
```

---

## 6. Starting the Platform

Validate the Docker Compose configuration:

```bash
docker compose config
```

Build the custom images:

```bash
docker compose build
```

Start the complete environment:

```bash
docker compose up -d
```

Inspect the running services:

```bash
docker compose ps -a
```

---

## 7. Stopping the Platform

Stop the containers while preserving persistent volumes:

```bash
docker compose down
```

A complete reset, including Docker volumes, can be performed with:

```bash
docker compose down -v
```

This command removes persistent Docker volumes and must only be used when a complete environment reset is intended.

---

## 8. Logs

Service logs can be inspected with:

```bash
docker compose logs <service>
```

For example:

```bash
docker compose logs trino --tail 100
```

or:

```bash
docker compose logs airflow-scheduler --tail 100
```

---

## 9. Role in the Lakehouse

Docker Compose provides the infrastructure supporting the complete processing flow:

```text
External APIs
     │
     ▼
Python Ingestion
     │
     ▼
MinIO / Bronze
     │
     ▼
Apache Spark
     │
     ▼
Apache Iceberg
Silver / Gold
     │
     ▼
Trino
     │
     ▼
Apache Superset
```

Apache Airflow coordinates pipeline execution, while PostgreSQL provides persistent metadata required by platform services and the Apache Iceberg catalog.

---

## 10. Deployment Documentation

Detailed local deployment instructions are available in:

```text
docs/Deployment/01_local_deployment.md
```