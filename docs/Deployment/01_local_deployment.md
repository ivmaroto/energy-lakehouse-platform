# Local Deployment Guide

## 1. Overview

This document describes how to deploy the Energy Lakehouse Platform in a local environment using Docker Compose.

The platform is composed of the following services:

- PostgreSQL
- MinIO
- Apache Spark
- Apache Iceberg
- Trino
- Apache Airflow
- Apache Superset

Docker Compose is used to orchestrate the infrastructure and provide a reproducible local deployment.

---

## 2. Prerequisites

The following software is required:

- Git
- Docker Desktop
- Docker Compose

Recommended resources:

- 16 GB RAM
- 30 GB free disk space

Verify the Docker installation:

```bash
docker --version
docker compose version
```

---

## 3. Clone the repository

Clone the project repository:

```bash
git clone <REPOSITORY_URL>
cd energy-lakehouse-platform
```

---

## 4. Environment configuration

The project uses environment variables for credentials and service configuration.

Create the local `.env` file from the provided template.

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Linux / macOS

```bash
cp .env.example .env
```

Review the values before starting the platform.

The `.env` file must not be committed to Git because it may contain local credentials or secrets.

---

## 5. Validate the Docker Compose configuration

Before starting the infrastructure, validate the Compose configuration:

```bash
docker compose config
```

This command checks the Docker Compose syntax and resolves the configured environment variables.

---

## 6. Build the platform

The project contains custom Docker images for Apache Spark, Apache Airflow and Apache Superset.

Build the required images with:

```bash
docker compose build
```

To force a complete rebuild:

```bash
docker compose build --no-cache
```

---

## 7. Start the platform

Start all services:

```bash
docker compose up -d
```

Check their status:

```bash
docker compose ps -a
```

The main services should remain running.

The initialization services `airflow-init` and `superset-init` are expected to finish with:

```text
Exited (0)
```

This indicates successful initialization.

---

## 8. Services and ports

| Component | Purpose | Local access |
|---|---|---|
| PostgreSQL | Metadata and relational storage | `localhost:5432` |
| Spark Master | Spark cluster master | `http://localhost:8080` |
| Spark Worker | Spark worker node | `http://localhost:8081` |
| Trino | Distributed SQL query engine | `http://localhost:8082` |
| Airflow | Workflow orchestration | `http://localhost:8083` |
| Superset | Analytics and visualization | `http://localhost:8088` |
| MinIO API | S3-compatible object storage API | `http://localhost:9000` |
| MinIO Console | Object storage administration | `http://localhost:9001` |

---

## 9. Platform architecture

The local infrastructure follows a Lakehouse architecture.

### Storage

MinIO provides S3-compatible object storage for the Lakehouse data.

The data architecture is organized into:

- Bronze
- Silver
- Gold

### Processing

Apache Spark provides distributed data processing.

Spark is deployed with:

- one Spark Master;
- one Spark Worker.

### Table format

Apache Iceberg provides the table format used by the Lakehouse.

### SQL query layer

Trino provides an interactive SQL query layer over the Lakehouse.

### Orchestration

Apache Airflow is responsible for scheduling and orchestrating data pipelines.

The deployment includes:

- Airflow Webserver
- Airflow Scheduler
- Airflow initialization service

### Visualization

Apache Superset provides the analytics and visualization layer.

### Metadata

PostgreSQL provides relational storage required by platform services and metadata components.

---

## 10. Docker network

The services communicate through the Docker network:

```text
lakehouse-network
```

This allows containers to communicate using their Docker service names instead of host addresses.

Examples include:

```text
postgres:5432
minio:9000
spark-master:7077
```

---

## 11. Persistent storage

Docker volumes are used to preserve data between container restarts.

Stopping the platform does not require deleting these volumes.

To stop the platform while preserving its persistent data:

```bash
docker compose down
```

The platform can then be started again with:

```bash
docker compose up -d
```

---

## 12. Full environment reset

To remove containers and Docker volumes:

```bash
docker compose down -v
```

> **Warning:** this removes persistent data stored in the Docker volumes.

The environment can subsequently be initialized again using:

```bash
docker compose up -d
```

---

## 13. Logs and troubleshooting

Check the status of all containers:

```bash
docker compose ps -a
```

Inspect the logs of a service:

```bash
docker compose logs <service-name>
```

For example:

```bash
docker compose logs trino --tail 100
```

Restart a service:

```bash
docker compose restart <service-name>
```

Rebuild a specific service:

```bash
docker compose build --no-cache <service-name>
```

---

## 14. Deployment validation

During the initial infrastructure deployment, the following components were successfully started and manually validated:

- PostgreSQL
- MinIO
- Spark Master
- Spark Worker
- Trino
- Airflow Webserver
- Airflow Scheduler
- Superset

The Airflow and Superset web interfaces were successfully accessed from the host environment.

The platform was also successfully stopped and restarted using Docker Compose while preserving its persistent volumes.

### Pending final validation

A complete clean-environment deployment test after deleting all Docker volumes is still pending.

The final validation will consist of:

```bash
docker compose down -v
docker compose up -d
docker compose ps -a
```

This test will verify that the complete platform can initialize automatically from an empty local Docker environment.

---

## 15. Shutdown

To stop the complete platform while preserving persistent data:

```bash
docker compose down
```

This is the recommended command for normal development shutdown.