# Docker

Docker configuration files for the local infrastructure of the Energy Lakehouse Platform.

## Purpose

The Docker configuration provides the reproducible local environment required by
the platform.

The main infrastructure definition is located at:

```text
docker-compose.yml
```

The validated stack includes:

```text
PostgreSQL 17
MinIO
Apache Spark 3.5
Trino 483
Apache Airflow
Apache Superset
```

Custom container configuration is used where project-specific dependencies are
required.

## Platform Role

Docker Compose provides the infrastructure for the complete processing path:

```text
External sources
      |
      v
Python ingestion
      |
      v
MinIO / Bronze
      |
      v
Apache Spark
      |
      v
Apache Iceberg
Silver / Gold
      |
      v
Trino
      |
      v
Apache Superset
```

Apache Airflow coordinates ingestion and historical orchestration.

PostgreSQL provides persistent metadata required by platform services and the
Apache Iceberg JDBC catalog.

MinIO provides S3-compatible object storage for Bronze data and the physical
Silver/Gold Iceberg warehouse.

Superset infrastructure is available, while the final datasets, charts and
dashboards are validated separately as part of the visualization phase.

## Environment Configuration

Environment-specific configuration is externalized through environment
variables.

The repository contains:

```text
.env.example
```

A local deployment uses:

```text
.env
```

The real `.env` file must remain outside version control.

Real API keys, passwords, tokens and other secrets must never be committed to
the repository.

## Basic Commands

Validate the Docker Compose configuration:

```bash
docker compose config
```

Build custom images:

```bash
docker compose build
```

Start the platform:

```bash
docker compose up -d
```

Inspect container state:

```bash
docker compose ps -a
```

Stop the platform while preserving persistent volumes:

```bash
docker compose down
```

A complete Docker-volume reset can be performed with:

```bash
docker compose down -v
```

This removes persistent Docker volumes and must only be used when a complete
local infrastructure reset is intended.

## Documentation

Detailed local deployment instructions are available in:

```text
docs/Deployment/01_local_deployment.md
```

Architecture, ingestion, Silver, Gold and Airflow behaviour are documented in
their corresponding project documentation.
