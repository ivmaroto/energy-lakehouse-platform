# Infrastructure Design

## 1. Infrastructure Overview

The Energy Lakehouse Platform is deployed as a fully containerized local
environment using Docker Compose.

Each infrastructure component has a dedicated responsibility, allowing the
platform to remain modular, reproducible and maintainable.

The infrastructure supports the complete data lifecycle:

```text
source ingestion
       │
       ▼
object storage
       │
       ▼
distributed processing
       │
       ▼
managed Lakehouse tables
       │
       ▼
SQL querying
       │
       ▼
analytical visualization
```

The complete platform is based on Open Source technologies and is designed to
operate locally without requiring proprietary cloud infrastructure.

The principal infrastructure components are:

```text
PostgreSQL
MinIO
Apache Spark
Apache Iceberg
Trino
Apache Airflow
Apache Superset
Docker Compose
```

Apache Iceberg is a table format rather than an independent long-running
service. Its metadata and data files are persisted in MinIO and its catalog
metadata is managed through PostgreSQL.

---

## 2. Platform Components

### PostgreSQL

PostgreSQL provides persistent relational metadata required by platform
services.

Its main responsibilities include:

```text
Apache Airflow metadata
Apache Iceberg JDBC catalog metadata
service metadata where configured
```

PostgreSQL does not contain the main analytical datasets.

---

### MinIO

MinIO provides S3-compatible object storage.

It stores:

```text
Bronze raw source objects
Silver Apache Iceberg data and metadata
Gold Apache Iceberg data and metadata
```

MinIO therefore acts as the physical storage foundation of the Lakehouse.

---

### Apache Spark Master

The Spark Master coordinates the local Spark cluster and manages distributed
processing workloads.

---

### Apache Spark Worker

The Spark Worker provides execution resources for PySpark transformation jobs.

The local Spark deployment therefore follows:

```text
Spark Master
     │
     ▼
Spark Worker
```

---

### Apache Iceberg

Apache Iceberg provides the managed table format for the structured Lakehouse
layers.

Iceberg is used for:

```text
Silver
Gold
```

Bronze remains a raw object-storage layer.

Apache Iceberg enables Spark and Trino to operate over the same persisted
tables while maintaining separate processing and querying responsibilities.

---

### Trino

Trino provides the distributed SQL query layer.

It queries the Apache Iceberg tables stored in MinIO through the shared Iceberg
catalog.

Trino is used for:

- SQL inspection;
- analytical queries;
- persisted-data validation;
- exposing Gold datasets to Apache Superset.

---

### Apache Airflow

Apache Airflow provides workflow orchestration.

Its responsibilities include:

- scheduling ingestion workloads;
- coordinating processing stages;
- managing task dependencies;
- retries;
- execution logs;
- operational monitoring.

Airflow coordinates Python and Spark processes but does not contain the
transformation logic itself.

---

### Apache Superset

Apache Superset provides the Business Intelligence and visualization layer.

Its intended analytical path is:

```text
Apache Iceberg Gold
        │
        ▼
      Trino
        │
        ▼
Apache Superset
```

Superset therefore does not query the Spark processing engine directly.

---

### Docker Compose

Docker Compose defines and manages the complete local infrastructure.

It provides:

- container definitions;
- service dependencies;
- shared networking;
- persistent storage;
- environment-variable injection;
- port mappings;
- custom image builds;
- reproducible local deployment.

---

## 3. Container Architecture

The deployed infrastructure is composed of the following Docker services:

```text
Docker Compose
│
├── PostgreSQL
│
├── MinIO
│
├── Spark Master
│
├── Spark Worker
│
├── Trino
│
├── Airflow Init
│
├── Airflow Webserver
│
├── Airflow Scheduler
│
├── Superset Init
│
└── Superset
```

Apache Iceberg is used by Spark and Trino but does not run as a separate
container.

Initialization services have a different lifecycle from long-running services.

The expected successful final state for:

```text
airflow-init
superset-init
```

is:

```text
Exited (0)
```

after initialization completes.

Long-running services remain active while the platform is operating.

---

## 4. Network Architecture

All platform containers communicate through a shared Docker network:

```text
lakehouse-network
```

Containers use Docker service names for internal communication rather than
host-specific IP addresses.

Examples include:

```text
postgres:5432
minio:9000
spark-master:7077
```

This design improves portability and prevents dependencies on dynamically
assigned container addresses.

Only services requiring host access expose ports outside the Docker network.

---

## 5. Service Ports

The local infrastructure exposes the following main ports:

| Service | Port | Purpose |
|---|---:|---|
| PostgreSQL | 5432 | Relational metadata storage |
| Spark Master | 7077 | Spark cluster communication |
| Spark Master UI | 8080 | Spark Master monitoring |
| Spark Worker UI | 8081 | Spark Worker monitoring |
| Trino | 8082 | Distributed SQL query engine |
| Airflow | 8083 | Workflow orchestration UI |
| Superset | 8088 | Analytics and visualization |
| MinIO API | 9000 | S3-compatible object storage |
| MinIO Console | 9001 | Object-storage administration |

Internal service-to-service communication uses the Docker network rather than
the host port mappings where applicable.

---

## 6. Storage Architecture

The platform separates raw object storage from managed analytical tables while
using MinIO as their shared physical storage backend.

### Bronze

Bronze data is persisted as source objects:

```text
MinIO
└── bronze/
    └── <source>/
        └── <dataset>/
            └── year=YYYY/
                └── month=MM/
                    └── day=DD/
```

Bronze preserves:

- source payloads;
- ingestion metadata;
- requested temporal windows;
- source traceability.

Bronze is not implemented as Apache Iceberg tables.

---

### Silver

Silver is implemented as Apache Iceberg tables.

The current physical model contains:

```text
9 Silver tables
```

Silver data files and Iceberg metadata are persisted in MinIO.

---

### Gold

Gold is also implemented as Apache Iceberg tables.

The current physical model contains:

```text
4 Gold tables
```

Gold contains the analytical facts and dimensions consumed through Trino.

---

## 7. Persistent Storage

Persistent storage is used for components that require durable state across
normal container restarts.

### MinIO persistence

MinIO storage preserves:

- Bronze source acquisitions;
- Silver Iceberg files;
- Silver Iceberg metadata;
- Gold Iceberg files;
- Gold Iceberg metadata.

---

### PostgreSQL persistence

PostgreSQL persists platform and catalog metadata independently of the
container lifecycle.

This includes metadata required by:

```text
Apache Airflow
Apache Iceberg JDBC catalog
```

and other configured service metadata.

---

### Airflow persistence

Airflow execution metadata is stored in PostgreSQL.

DAG definitions, configuration and logs are mounted from project resources as
defined by the Docker Compose configuration.

---

### Superset persistence

Superset configuration is managed by the containerized platform, while its
application metadata is persisted according to the configured database setup.

---

Normal platform shutdown can be performed with:

```bash
docker compose down
```

without intentionally deleting persistent volumes.

A complete volume reset can be performed with:

```bash
docker compose down -v
```

This removes persistent Docker volumes and must only be used when a complete
environment reset is intended.

---

## 8. Custom Docker Images

Custom Docker images are used where additional project-specific dependencies
are required.

The platform uses custom images for:

```text
Apache Spark
Apache Airflow
Apache Superset
```

### Apache Spark image

The Spark image contains the dependencies required for integration with:

```text
Apache Iceberg
MinIO / S3
PostgreSQL JDBC catalog
```

The deployment consists of:

```text
Spark Master
Spark Worker
```

---

### Apache Airflow image

The Airflow image contains the Python dependencies required by the ingestion
and orchestration layers.

It allows Airflow tasks to invoke project ingestion logic and coordinate
downstream Spark processing.

---

### Apache Superset image

The Superset image contains the dependencies and configuration required by the
Business Intelligence layer and its SQL connection to Trino.

---

## 9. Service Communication

The main communication paths are:

```text
External APIs
      │
      ▼
Python ingestion
      │
      ▼
MinIO / Bronze
      │
      ▼
Apache Spark
      │
      ▼
Apache Iceberg Silver / Gold
      │
      ▼
Trino
      │
      ▼
Apache Superset
```

Additional infrastructure interactions include:

```text
Airflow
→ Python ingestion
→ Spark processing
```

```text
Spark
→ PostgreSQL JDBC catalog
→ MinIO
```

```text
Trino
→ Iceberg catalog
→ MinIO
```

```text
Airflow
→ PostgreSQL metadata database
```

Each component therefore communicates only with the services required for its
specific responsibility.

---

## 10. Processing and Analytical Query Separation

The infrastructure deliberately separates distributed data processing from
interactive SQL querying.

### Apache Spark

Spark is responsible for:

- Bronze-to-Silver transformations;
- data normalization;
- deduplication;
- geographical normalization;
- temporal and spatial aggregation;
- Silver-to-Gold transformations;
- meteorological and energy integration;
- Apache Iceberg writes.

### Trino

Trino is responsible for:

- SQL access to Apache Iceberg;
- interactive analytical querying;
- persisted-table validation;
- exposing analytical data to Apache Superset.

The separation is:

```text
              PROCESSING

Bronze
   │
   ▼
 Spark
   │
   ▼
Silver
   │
   ▼
 Spark
   │
   ▼
 Gold


               QUERYING

 Gold
   │
   ▼
 Trino
   │
   ▼
Superset
```

This avoids coupling Business Intelligence workloads to the Spark processing
cluster.

---

## 11. Metadata Architecture

The platform distinguishes analytical data from service metadata.

### Analytical data

Stored in MinIO:

```text
Bronze objects
Silver Iceberg tables
Gold Iceberg tables
```

### Catalog and application metadata

Stored in PostgreSQL where applicable:

```text
Iceberg JDBC catalog
Airflow metadata
service metadata
```

This prevents PostgreSQL from becoming a duplicate analytical datastore.

---

## 12. Environment Configuration

Environment-specific parameters and credentials are externalized from source
code.

The repository provides:

```text
.env.example
```

A local deployment uses:

```text
.env
```

The real `.env` file is excluded from Git.

Configuration includes values associated with:

```text
PostgreSQL
MinIO
Airflow
Superset
AEMET
ESIOS
```

Relevant API credentials include:

```text
AEMET_API_KEY
ESIOS_API_KEY
```

Open-Meteo does not require an API key for the access pattern used by the
project.

Credentials and passwords must never be embedded directly in committed source
code or documentation.

---

## 13. Infrastructure as Code

The principal infrastructure definition is:

```text
docker-compose.yml
```

Docker Compose therefore acts as the Infrastructure-as-Code mechanism for the
local platform.

The deployment strategy includes:

- version-controlled infrastructure definitions;
- container isolation;
- persistent volumes;
- shared networking;
- environment-based configuration;
- custom images;
- reproducible deployment.

The Compose configuration can be validated with:

```bash
docker compose config
```

Custom images can be built with:

```bash
docker compose build
```

The platform can be started with:

```bash
docker compose up -d
```

and inspected with:

```bash
docker compose ps -a
```

---

## 14. Validated Infrastructure

The local infrastructure has been deployed and operated successfully using
Docker Compose.

Validated long-running services include:

```text
PostgreSQL
MinIO
Spark Master
Spark Worker
Trino
Airflow Webserver
Airflow Scheduler
Superset
```

The platform infrastructure has supported real data processing through:

```text
External source acquisition
        │
        ▼
MinIO Bronze persistence
        │
        ▼
Apache Spark processing
        │
        ▼
Apache Iceberg Silver
        │
        ▼
Apache Spark processing
        │
        ▼
Apache Iceberg Gold
        │
        ▼
Trino SQL queries
```

Trino has successfully exposed the current physical Lakehouse model:

```text
9 Silver tables
4 Gold tables
```

The validated Gold namespace contains:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

This demonstrates that the infrastructure supports the implemented Lakehouse
processing path from object storage through distributed processing and SQL
access.

---

## 15. Airflow Infrastructure Status

The Airflow infrastructure itself has been validated at service level.

Validated components include:

```text
Airflow Webserver
Airflow Scheduler
PostgreSQL metadata connectivity
DAG discovery
```

Existing ingestion DAGs have previously been executed successfully for
source-ingestion scenarios.

The final complete Airflow-orchestrated execution of:

```text
Bronze
→ Silver
→ Gold
```

is part of the current orchestration closure work and must not yet be described
as fully runtime-validated until that final execution has been completed.

---

## 16. Superset Infrastructure Status

The Superset service has been deployed as part of the Docker Compose
environment.

The final dashboard and analytical visualization implementation remains a
downstream project stage.

The intended connection architecture is:

```text
Superset
   │
   ▼
 Trino
   │
   ▼
Gold Iceberg tables
```

This preserves the separation between visualization and distributed processing.

---

## 17. Deployment Validation

The infrastructure has been successfully started and inspected using Docker
Compose.

The main validation commands include:

```bash
docker compose config
docker compose build
docker compose up -d
docker compose ps -a
```

The platform has also been stopped and restarted while preserving persistent
data.

A complete clean-environment reproducibility test after intentionally deleting
all persistent Docker volumes is not considered part of the currently validated
evidence unless explicitly executed and confirmed.

Therefore, normal deployment and restart behaviour are validated, while a
destructive clean-volume reconstruction should not be documented as completed
without corresponding execution evidence.

---

## 18. Infrastructure Design Principles

The final infrastructure follows these principles.

### Reproducibility

Infrastructure definitions are version-controlled and deployable through Docker
Compose.

### Modularity

Each component has a specific responsibility.

### Persistence

Lakehouse data and platform metadata survive normal service restarts.

### Isolation

Services execute in independent containers.

### Portability

The environment does not depend on installing the complete Data Engineering
stack directly on the host.

### Open Source first

All principal infrastructure technologies are Open Source.

### Processing/query separation

Spark processes data while Trino serves interactive SQL queries.

### Raw/managed storage separation

Bronze remains raw object storage.

Silver and Gold are managed through Apache Iceberg.

### Metadata/data separation

PostgreSQL contains service and catalog metadata, while analytical data resides
in MinIO.

### Security by configuration

Credentials are externalized through environment variables and excluded from
version control.

### Local independence

The platform can operate locally without depending on proprietary cloud
infrastructure.

---

## 19. Infrastructure Summary

The implemented infrastructure can be summarized as:

```text
                         Docker Compose
                               │
       ┌───────────────────────┼────────────────────────┐
       │                       │                        │
       ▼                       ▼                        ▼
   PostgreSQL                MinIO                 Apache Airflow
   metadata                  storage               orchestration
                               │
                               ▼
                         Apache Spark
                               │
                               ▼
                         Apache Iceberg
                       Silver / Gold tables
                               │
                               ▼
                             Trino
                               │
                               ▼
                       Apache Superset
```

This infrastructure provides the local execution environment required by the
Energy Lakehouse Platform while maintaining clear separation between storage,
processing, metadata, orchestration, querying and visualization.